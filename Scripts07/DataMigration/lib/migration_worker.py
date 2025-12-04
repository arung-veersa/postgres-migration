"""
Migration Worker Module
Handles the actual data migration for individual chunks
"""

import logging
import io
from typing import Dict, Any, Optional, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import pandas as pd
import psycopg2

from .connections import SnowflakeConnectionManager, PostgresConnectionManager
from .status_tracker import StatusTracker
from .utils import quote_identifier, get_column_list_sql, format_number, Timer, logger


class MigrationWorker:
    """Handles data migration for a single chunk"""
    
    def __init__(
        self,
        sf_manager: SnowflakeConnectionManager,
        pg_manager: PostgresConnectionManager,
        status_tracker: StatusTracker,
        source_config: Dict[str, Any],
        table_config: Dict[str, Any],
        max_retries: int = 3
    ):
        self.sf_manager = sf_manager
        self.pg_manager = pg_manager
        self.status_tracker = status_tracker
        self.source_config = source_config
        self.table_config = table_config
        self.max_retries = max_retries
        self.logger = logger
        
        # Extract configuration
        self.source_db = source_config['source_sf_database']
        self.source_schema = source_config['source_sf_schema']
        self.target_db = source_config['target_pg_database']
        self.target_schema = source_config['target_pg_schema']
        
        self.source_table = table_config['source']
        self.target_table = table_config['target']
        self.source_watermark = table_config.get('source_watermark')
        self.target_watermark = table_config.get('target_watermark')
        self.uniqueness_columns = table_config.get('uniqueness_columns') or []
        self.truncate_onstart = table_config.get('truncate_onstart', False)
        
        # Cache target columns to avoid repeated queries
        self._target_columns_cache = None
    
    def _get_target_columns(self) -> List[str]:
        """Get target table columns (cached)"""
        if self._target_columns_cache is not None:
            return self._target_columns_cache
        
        conn = self.pg_manager.get_connection(self.target_db)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (self.target_schema, self.target_table))
            
            self._target_columns_cache = [row[0] for row in cursor.fetchall()]
            return self._target_columns_cache
            
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def process_chunk(self, run_id: str, chunk_id: int, chunk_filter: str) -> int:
        """
        Process a single data chunk
        
        Args:
            run_id: Migration run ID
            chunk_id: Chunk identifier
            chunk_filter: SQL WHERE clause for this chunk
        
        Returns:
            Number of rows processed
        """
        # Update chunk status to in_progress
        self.status_tracker.update_chunk_status(
            run_id, self.source_db, self.source_schema, self.source_table,
            chunk_id, 'in_progress'
        )
        
        try:
            rows_processed = self._process_chunk_with_retry(chunk_filter)
            
            # Update chunk status to completed
            self.status_tracker.update_chunk_status(
                run_id, self.source_db, self.source_schema, self.source_table,
                chunk_id, 'completed', rows_copied=rows_processed
            )
            
            return rows_processed
            
        except Exception as e:
            # Update chunk status to failed
            error_msg = str(e)[:500]  # Truncate long error messages
            self.status_tracker.update_chunk_status(
                run_id, self.source_db, self.source_schema, self.source_table,
                chunk_id, 'failed', error_message=error_msg
            )
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((psycopg2.OperationalError, Exception)),
        reraise=True
    )
    def _process_chunk_with_retry(self, chunk_filter: str) -> int:
        """Process chunk with retry logic"""
        # Build query to fetch data from Snowflake
        fetch_query = self._build_fetch_query(chunk_filter)
        
        self.logger.debug(f"Fetching data: {fetch_query[:200]}...")
        
        with Timer(f"Fetch from Snowflake: {self.source_table}", self.logger):
            df = self.sf_manager.fetch_dataframe(fetch_query)
        
        if df.empty:
            self.logger.debug(f"No rows fetched for chunk")
            return 0
        
        rows_count = len(df)
        self.logger.info(f"Fetched {format_number(rows_count)} rows from Snowflake")
        
        # Load data to PostgreSQL
        with Timer(f"Load to PostgreSQL: {self.target_table}", self.logger):
            self._load_to_postgres(df)
        
        return rows_count
    
    def _build_fetch_query(self, chunk_filter: str) -> str:
        """Build SELECT query for Snowflake"""
        # Get watermark filter if applicable
        watermark_filter = None
        if self.source_watermark and self.target_watermark and not self.truncate_onstart:
            max_watermark = self.pg_manager.get_max_watermark(
                self.target_db, self.target_schema, self.target_table, self.target_watermark
            )
            if max_watermark:
                watermark_filter = f'{quote_identifier(self.source_watermark)} > \'{max_watermark}\''
        
        # Combine filters
        filters = [chunk_filter]
        if watermark_filter:
            filters.append(watermark_filter)
        
        where_clause = " AND ".join(f"({f})" for f in filters if f and f.strip() and f != "1=1")
        if where_clause:
            where_clause = f"WHERE {where_clause}"
        else:
            where_clause = ""
        
        # Build query
        query = f"""
            SELECT *
            FROM {self.source_db}.{self.source_schema}.{self.source_table}
            {where_clause}
        """
        
        return query
    
    def _load_to_postgres(self, df: pd.DataFrame):
        """Load DataFrame to PostgreSQL using COPY or UPSERT"""
        # Filter DataFrame to only include columns that exist in PostgreSQL target table
        # (Snowflake sources may have more columns than PostgreSQL targets)
        df_filtered = self._filter_columns_for_target(df)
        
        if df_filtered.empty:
            self.logger.warning(f"No matching columns found between source and target")
            return
        
        if not self.uniqueness_columns or self.truncate_onstart:
            # Use fast COPY for inserts
            self._copy_to_postgres(df_filtered)
        else:
            # Use UPSERT for incremental loads
            self._upsert_to_postgres(df_filtered)
    
    def _filter_columns_for_target(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame columns to only those that exist in target PostgreSQL table.
        Column names have the same casing in both Snowflake and PostgreSQL.
        Uses cached column list for performance.
        """
        # Get target columns (cached after first call)
        target_columns = self._get_target_columns()
        
        # Find matching columns (case-sensitive comparison)
        matching_columns = [col for col in df.columns if col in target_columns]
        
        if len(matching_columns) < len(df.columns):
            missing = set(df.columns) - set(matching_columns)
            self.logger.warning(
                f"Excluding {len(missing)} columns not in target table: {sorted(list(missing))[:5]}"
            )
        
        self.logger.debug(f"Using {len(matching_columns)}/{len(df.columns)} columns for {self.target_table}")
        return df[matching_columns]
    
    def _copy_to_postgres(self, df: pd.DataFrame):
        """
        Use PostgreSQL COPY for fast bulk insert
        This is the fastest method but doesn't handle conflicts
        """
        conn = self.pg_manager.get_connection(self.target_db)
        cursor = conn.cursor()
        
        try:
            # Prepare CSV buffer
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            # Get column list
            columns = get_column_list_sql(df.columns.tolist(), quote=True)
            
            # COPY data
            copy_sql = f"""
                COPY {self.target_schema}.{self.target_table} ({columns})
                FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')
            """
            
            cursor.copy_expert(copy_sql, buffer)
            conn.commit()
            
            self.logger.debug(f"✓ COPYed {format_number(len(df))} rows to {self.target_table}")
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"COPY failed: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def _upsert_to_postgres(self, df: pd.DataFrame):
        """
        Use INSERT ... ON CONFLICT for incremental upserts
        Handles duplicate keys and watermark-based updates
        """
        if df.empty:
            return
        
        conn = self.pg_manager.get_connection(self.target_db)
        cursor = conn.cursor()
        
        try:
            # Build column lists
            all_columns = df.columns.tolist()
            columns_sql = get_column_list_sql(all_columns, quote=True)
            
            # Build conflict columns
            conflict_columns_sql = get_column_list_sql(self.uniqueness_columns, quote=True)
            
            # Build update SET clause (all columns except conflict columns)
            update_columns = [col for col in all_columns 
                            if col.lower() not in [c.lower() for c in self.uniqueness_columns]]
            
            if not update_columns:
                # If all columns are in uniqueness_columns, just skip on conflict
                update_clause = "NOTHING"
            else:
                update_set = ", ".join([
                    f'{quote_identifier(col)} = EXCLUDED.{quote_identifier(col)}'
                    for col in update_columns
                ])
                
                # Add watermark condition if applicable
                if self.target_watermark and self.target_watermark in all_columns:
                    update_clause = f"""
                        UPDATE SET {update_set}
                        WHERE {self.target_schema}.{self.target_table}.{quote_identifier(self.target_watermark)} < EXCLUDED.{quote_identifier(self.target_watermark)}
                           OR {self.target_schema}.{self.target_table}.{quote_identifier(self.target_watermark)} IS NULL
                    """
                else:
                    update_clause = f"UPDATE SET {update_set}"
            
            # Build INSERT statement
            placeholders = ", ".join(["%s"] * len(all_columns))
            
            upsert_sql = f"""
                INSERT INTO {self.target_schema}.{self.target_table} ({columns_sql})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_columns_sql})
                DO {update_clause}
            """
            
            # Execute batch upsert
            data = [tuple(row) for row in df.itertuples(index=False, name=None)]
            
            from psycopg2.extras import execute_batch
            execute_batch(cursor, upsert_sql, data, page_size=1000)
            
            conn.commit()
            
            self.logger.debug(f"✓ Upserted {format_number(len(df))} rows to {self.target_table}")
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"UPSERT failed: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def truncate_table(self):
        """Truncate target table before loading"""
        conn = self.pg_manager.get_connection(self.target_db)
        cursor = conn.cursor()
        
        try:
            truncate_sql = f"TRUNCATE TABLE {self.target_schema}.{self.target_table} CASCADE"
            self.logger.info(f"Truncating {self.target_schema}.{self.target_table}")
            cursor.execute(truncate_sql)
            conn.commit()
            self.logger.info(f"✓ Truncated {self.target_schema}.{self.target_table}")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to truncate {self.target_schema}.{self.target_table}: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)

