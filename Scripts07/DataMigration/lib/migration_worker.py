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
        max_retries: int = 3,
        is_initial_full_load: bool = False  # NEW: Pre-determined by orchestrator
    ):
        self.sf_manager = sf_manager
        self.pg_manager = pg_manager
        self.status_tracker = status_tracker
        self.source_config = source_config
        self.table_config = table_config
        self.max_retries = max_retries
        self.logger = logger
        
        # CRITICAL: Store the pre-determined initial load status
        # This is set by orchestrator BEFORE any threads start to avoid race conditions
        self._is_initial_full_load = is_initial_full_load
        
        # Extract configuration
        self.source_db = source_config['source_sf_database']
        self.source_schema = source_config['source_sf_schema']
        self.target_db = source_config['target_pg_database']
        self.target_schema = source_config['target_pg_schema']
        
        self.source_table = table_config['source']
        self.target_table = table_config['target']
        self.truncate_onstart = table_config.get('truncate_onstart', False)
        
        # Smart configuration: If truncate_onstart is True, ignore incremental settings
        if self.truncate_onstart:
            # Full table copy mode - ignore incremental settings
            self.source_watermark = None
            self.target_watermark = None
            self.uniqueness_columns = []
            
            # Log if we're overriding settings
            overridden = []
            if table_config.get('source_watermark'):
                overridden.append('source_watermark')
            if table_config.get('target_watermark'):
                overridden.append('target_watermark')
            if table_config.get('uniqueness_columns'):
                overridden.append('uniqueness_columns')
            
            if overridden:
                self.logger.info(
                    f"[{self.source_table}] truncate_onstart=true: "
                    f"Ignoring incremental settings ({', '.join(overridden)})"
                )
        else:
            # Incremental mode - use configured settings
            self.source_watermark = table_config.get('source_watermark')
            self.target_watermark = table_config.get('target_watermark')
            self.uniqueness_columns = table_config.get('uniqueness_columns') or []
        
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
    
    def process_chunk(self, run_id: str, chunk_id: int, chunk_filter: str, chunk_metadata: Dict[str, Any] = None) -> int:
        """
        Process a single data chunk
        
        Args:
            run_id: Migration run ID
            chunk_id: Chunk identifier
            chunk_filter: SQL WHERE clause for this chunk
            chunk_metadata: Metadata about the chunk (strategy, column info, etc.)
        
        Returns:
            Number of rows processed
        """
        # Update chunk status to in_progress
        self.status_tracker.update_chunk_status(
            run_id, self.source_db, self.source_schema, self.source_table,
            chunk_id, 'in_progress'
        )
        
        try:
            rows_processed = self._process_chunk_with_retry(chunk_filter, chunk_metadata or {})
            
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
    def _process_chunk_with_retry(self, chunk_filter: str, chunk_metadata: Dict[str, Any]) -> int:
        """Process chunk with retry logic"""
        # Build query to fetch data from Snowflake
        fetch_query = self._build_fetch_query(chunk_filter, chunk_metadata)
        
        self.logger.debug(f"Fetching data: {fetch_query[:200]}...")
        
        with Timer(f"Fetch from Snowflake: {self.source_table}", self.logger):
            result = self.sf_manager.fetch_dataframe(fetch_query)
        
        # Handle result from fetch_dataframe (dict with 'data' and 'columns')
        rows = result['data']
        columns = result['columns']
        
        # OPTIMIZATION 2: Skip empty chunks early
        if not rows or len(rows) == 0:
            self.logger.info(
                f"[{self.source_table}] Chunk has no new data, skipping load"
            )
            return 0
        
        rows_count = len(rows)
        self.logger.info(f"Fetched {format_number(rows_count)} rows from Snowflake")
        
        # Convert to pandas DataFrame for loading
        import pandas as pd
        df = pd.DataFrame(rows, columns=columns)
        
        # Load data to PostgreSQL (pass chunk_metadata for smart COPY/UPSERT decision)
        with Timer(f"Load to PostgreSQL: {self.target_table}", self.logger):
            self._load_to_postgres(df, chunk_metadata)
        
        return rows_count
    
    def _build_fetch_query(self, chunk_filter: str, chunk_metadata: Dict[str, Any]) -> str:
        """Build SELECT query for Snowflake"""
        strategy = chunk_metadata.get('strategy', '')
        
        # Get watermark filter if applicable
        watermark_filter = None
        if self.source_watermark and self.target_watermark and not self.truncate_onstart:
            # Determine if we should use chunk-scoped or global watermark
            
            if strategy in ['date_range', 'numeric_range', 'grouped_values', 'date_range_offset']:
                # Use chunk-scoped watermark for strategies with filterable chunks
                max_watermark = self._get_chunk_scoped_watermark(chunk_filter, chunk_metadata)
                if max_watermark:
                    self.logger.debug(f"Chunk-scoped watermark: {max_watermark}")
            else:
                # Use global watermark for offset-based and other strategies
                max_watermark = self.pg_manager.get_max_watermark(
                    self.target_db, self.target_schema, self.target_table, self.target_watermark
                )
                if max_watermark:
                    self.logger.debug(f"Global watermark: {max_watermark}")
            
            if max_watermark:
                watermark_filter = f'{quote_identifier(self.source_watermark)} > \'{max_watermark}\''
        
        # Handle date_range_offset strategy (LIMIT/OFFSET for large single dates)
        if strategy == 'date_range_offset':
            filters = [chunk_filter]
            if watermark_filter:
                filters.append(watermark_filter)
            
            where_clause = " AND ".join(f"({f})" for f in filters if f and f.strip() and f != "1=1")
            if where_clause:
                where_clause = f"WHERE {where_clause}"
            else:
                where_clause = ""
            
            # Build deterministic ORDER BY clause
            # CRITICAL: Include primary key columns to ensure stable pagination
            # This prevents the same row from appearing in multiple chunks
            date_column = quote_identifier(chunk_metadata['date_column'])
            limit = chunk_metadata['limit']
            offset = chunk_metadata['offset']
            
            # Get uniqueness columns (primary key) for deterministic ordering
            uniqueness_columns = chunk_metadata.get('uniqueness_columns', [])
            order_by_clause = date_column
            
            if uniqueness_columns:
                # Add uniqueness columns to ORDER BY for deterministic pagination
                uniqueness_cols_quoted = [quote_identifier(col) for col in uniqueness_columns]
                order_by_clause = f"{date_column}, {', '.join(uniqueness_cols_quoted)}"
                self.logger.debug(
                    f"Using deterministic ORDER BY: {order_by_clause}"
                )
            
            query = f"""
                SELECT *
                FROM {self.source_db}.{self.source_schema}.{self.source_table}
                {where_clause}
                ORDER BY {order_by_clause}
                LIMIT {limit} OFFSET {offset}
            """
            return query
        
        # Check if chunk_filter contains ORDER BY (offset-based strategy)
        # Format: "(filter) ORDER BY col LIMIT x OFFSET y"
        if 'ORDER BY' in chunk_filter:
            # Offset-based strategy: chunk_filter already contains complete clause
            # Extract the WHERE part and ORDER BY/LIMIT/OFFSET parts
            parts = chunk_filter.split(' ORDER BY ', 1)
            where_part = parts[0]  # "(source_filter)"
            order_limit_part = parts[1]  # "col LIMIT x OFFSET y"
            
            # Add watermark filter if exists
            if watermark_filter:
                where_part = f"{where_part} AND ({watermark_filter})"
            
            # Remove outer parentheses from where_part if present
            where_part = where_part.strip()
            if where_part.startswith('(') and where_part.endswith(')'):
                where_part = where_part[1:-1]
            
            # Build query with ORDER BY at the end
            query = f"""
                SELECT *
                FROM {self.source_db}.{self.source_schema}.{self.source_table}
                WHERE {where_part}
                ORDER BY {order_limit_part}
            """
        else:
            # Traditional chunking (range, grouped values, etc.)
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
    
    def _get_chunk_scoped_watermark(self, chunk_filter: str, chunk_metadata: Dict[str, Any]) -> Optional[str]:
        """
        Get maximum watermark value scoped to this specific chunk.
        
        Args:
            chunk_filter: Snowflake filter SQL for the chunk
            chunk_metadata: Metadata about the chunk
        
        Returns:
            Maximum watermark value for this chunk, or None
        """
        # Skip chunk-scoped watermark if this is an initial full load - optimization
        if self._is_initial_full_load:
            self.logger.debug(
                f"[{self.source_table}] Initial full load - skipping chunk-scoped watermark query"
            )
            return None
        
        # Skip if no watermark configured
        if not self.target_watermark:
            return None
        
        try:
            # Translate Snowflake chunk filter to PostgreSQL
            pg_filter = self._translate_chunk_filter_to_postgres(chunk_filter, chunk_metadata)
            
            if not pg_filter or pg_filter.strip() in ("1=1", ""):
                # If no meaningful filter, fall back to global watermark
                return self.pg_manager.get_max_watermark(
                    self.target_db, self.target_schema, self.target_table, self.target_watermark
                )
            
            # Validate the filter doesn't result in syntax errors
            # Simple check: ensure it has some actual content beyond whitespace
            if len(pg_filter.replace(' ', '')) < 3:
                self.logger.debug(f"[{self.source_table}] Filter too short after translation, using global watermark")
                return self.pg_manager.get_max_watermark(
                    self.target_db, self.target_schema, self.target_table, self.target_watermark
                )
            
            # Query PostgreSQL with chunk-scoped filter
            max_watermark = self.pg_manager.get_max_watermark_for_chunk(
                self.target_db, self.target_schema, self.target_table,
                self.target_watermark, pg_filter
            )
            
            return max_watermark
            
        except Exception as e:
            self.logger.debug(
                f"Could not get chunk-scoped watermark, falling back to global: {str(e)[:100]}"
            )
            # Fall back to global watermark on error
            try:
                return self.pg_manager.get_max_watermark(
                    self.target_db, self.target_schema, self.target_table, self.target_watermark
                )
            except Exception:
                # If even global fails, return None
                return None
    
    def _translate_chunk_filter_to_postgres(self, chunk_filter: str, chunk_metadata: Dict[str, Any]) -> str:
        """
        Translate Snowflake chunk filter syntax to PostgreSQL WHERE clause.
        
        Args:
            chunk_filter: Snowflake filter SQL
            chunk_metadata: Metadata about the chunk (strategy, columns, etc.)
        
        Returns:
            PostgreSQL-compatible WHERE clause (without 'WHERE' keyword)
        """
        # Remove outer parentheses if present
        pg_filter = chunk_filter.strip()
        if pg_filter.startswith('(') and pg_filter.endswith(')'):
            pg_filter = pg_filter[1:-1]
        
        # Get source_filter from table config
        source_filter = self.table_config.get('source_filter', '')
        
        # Remove source_filter part completely - it may reference source-only columns
        if source_filter:
            # Try multiple patterns to remove source_filter
            patterns_to_remove = [
                f"({source_filter}) AND ",
                f" AND ({source_filter})",
                f"({source_filter})",
                f"{source_filter} AND ",
                f" AND {source_filter}",
                source_filter
            ]
            
            for pattern in patterns_to_remove:
                if pattern in pg_filter:
                    pg_filter = pg_filter.replace(pattern, "", 1)
                    break
        
        # Clean up orphaned parentheses
        # Remove leading ) or (
        pg_filter = pg_filter.lstrip(') ')
        pg_filter = pg_filter.rstrip('( ')
        
        # Remove leading/trailing AND
        pg_filter = pg_filter.strip()
        if pg_filter.startswith('AND '):
            pg_filter = pg_filter[4:]
        if pg_filter.endswith(' AND'):
            pg_filter = pg_filter[:-4]
        
        pg_filter = pg_filter.strip()
        
        strategy = chunk_metadata.get('strategy', '')
        
        # Strategy-specific translations
        if strategy in ['date_range', 'date_range_offset']:
            # Replace source watermark column with target watermark column
            # e.g., "Updated Timestamp"::DATE = '2024-12-05'
            #   --> "Updated Datatimestamp"::DATE = '2024-12-05'
            source_watermark = self.table_config.get('source_watermark')
            target_watermark = self.table_config.get('target_watermark')
            
            if source_watermark and target_watermark and source_watermark != target_watermark:
                # Replace the source column name with target column name
                pg_filter = pg_filter.replace(
                    quote_identifier(source_watermark),
                    quote_identifier(target_watermark)
                )
                self.logger.debug(
                    f"Translated watermark column in filter: {source_watermark} -> {target_watermark}"
                )
        
        elif strategy == 'numeric_range':
            # Numeric ranges typically use the same column name in both databases
            # e.g., ID BETWEEN 1 AND 10000
            # Usually no translation needed, but handle potential differences
            pass
        
        elif strategy == 'grouped_values':
            # Grouped values use IN clauses
            # e.g., "Patient Address Id" IN ('uuid1', 'uuid2', ...)
            # Column names are typically the same
            pass
        
        # Final cleanup - remove any remaining double spaces
        pg_filter = ' '.join(pg_filter.split())
        pg_filter = pg_filter.strip()
        
        # If empty after cleanup, use "1=1" as a safe default
        if not pg_filter:
            pg_filter = "1=1"
        
        return pg_filter
    
    def _load_to_postgres(self, df: pd.DataFrame, chunk_metadata: Dict[str, Any] = None):
        """Load DataFrame to PostgreSQL using COPY or UPSERT"""
        # Filter DataFrame to only include columns that exist in PostgreSQL target table
        # (Snowflake sources may have more columns than PostgreSQL targets)
        df_filtered = self._filter_columns_for_target(df)
        
        if df_filtered.empty:
            self.logger.warning(f"No matching columns found between source and target")
            return
        
        # Replace NaT (Not-a-Time) with None for PostgreSQL compatibility
        df_filtered = df_filtered.replace({pd.NaT: None})
        
        # Smart decision: Use COPY (fast) if:
        # 1. truncate_onstart is True, OR
        # 2. No uniqueness_columns defined, OR
        # 3. Initial full load (pre-determined by orchestrator before threads started)
        
        chunk_metadata = chunk_metadata or {}
        
        # Check if we can safely use COPY
        use_copy = (
            self.truncate_onstart or 
            not self.uniqueness_columns or
            self._is_initial_full_load  # Use the pre-determined value
        )
        
        # Log the decision once (first chunk only)
        if use_copy and self._is_initial_full_load and not hasattr(self, '_logged_initial_load'):
            self._logged_initial_load = True
            self.logger.info(
                f"[{self.source_table}] Initial full load detected "
                f"(empty table, no watermark) - will use fast COPY mode for all chunks"
            )
        
        if use_copy:
            # Use fast COPY for inserts
            self._copy_to_postgres(df_filtered)
        else:
            # Use UPSERT for incremental loads
            self._upsert_to_postgres(df_filtered)
    
    def _filter_columns_for_target(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame columns to only those that exist in target PostgreSQL table.
        Applies column mapping if configured to handle name differences between source and target.
        Also auto-infers mapping from watermark columns if they differ.
        Uses cached column list for performance.
        """
        # Build column mapping
        column_mapping = self.table_config.get('column_mapping', {}).copy() or {}
        
        # Auto-infer watermark column mapping if source != target
        source_watermark = self.table_config.get('source_watermark')
        target_watermark = self.table_config.get('target_watermark')
        
        if source_watermark and target_watermark and source_watermark != target_watermark:
            # Only add if not already in explicit mapping
            if source_watermark not in column_mapping:
                column_mapping[source_watermark] = target_watermark
                self.logger.debug(
                    f"Auto-inferred watermark column mapping: "
                    f"{source_watermark} -> {target_watermark}"
                )
        
        # Apply column mapping (explicit + auto-inferred)
        if column_mapping:
            df = df.rename(columns=column_mapping)
            self.logger.debug(
                f"Applied column mapping: {len(column_mapping)} columns renamed"
            )
        
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

