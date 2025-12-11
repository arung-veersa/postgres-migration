"""
Chunking Strategy Module
Determines optimal chunking strategies based on data distribution
"""

import logging
import math
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from .connections import SnowflakeConnectionManager
from .utils import quote_identifier, format_number, logger


@dataclass
class ChunkInfo:
    """Information about a data chunk"""
    chunk_id: int
    filter_sql: str
    estimated_rows: int
    metadata: Dict[str, Any]


class ChunkingStrategy:
    """Base class for chunking strategies"""
    
    def __init__(
        self,
        sf_manager: SnowflakeConnectionManager,
        source_db: str,
        source_schema: str,
        source_table: str,
        table_config: Dict[str, Any],
        batch_size: int,
        pg_manager=None  # Optional PostgreSQL manager for smart mode
    ):
        self.sf_manager = sf_manager
        self.pg_manager = pg_manager  # Store for smart mode checks
        self.source_db = source_db
        self.source_schema = source_schema
        self.source_table = source_table
        self.table_config = table_config
        self.batch_size = batch_size
        self.logger = logger
        
        self.chunking_columns = table_config.get('chunking_columns')
        self.chunking_column_types = table_config.get('chunking_column_types')
        self.source_filter = table_config.get('source_filter', '1=1') or '1=1'
        if isinstance(self.source_filter, str) and self.source_filter.strip() == '':
            self.source_filter = '1=1'
    
    def get_total_rows(self) -> int:
        """Get total number of rows matching filter"""
        query = f"""
            SELECT COUNT(*) as cnt
            FROM {self.source_db}.{self.source_schema}.{self.source_table}
            WHERE {self.source_filter}
        """
        result = self.sf_manager.execute_query(query)
        return int(result[0][0]) if result else 0
    
    def create_chunks(self) -> List[ChunkInfo]:
        """Create list of chunks - to be implemented by subclasses"""
        raise NotImplementedError


class SingleChunkStrategy(ChunkingStrategy):
    """Strategy for small tables or tables without chunking columns"""
    
    def create_chunks(self) -> List[ChunkInfo]:
        total_rows = self.get_total_rows()
        
        self.logger.info(
            f"Using single chunk strategy for {self.source_table} "
            f"({format_number(total_rows)} rows)"
        )
        
        return [ChunkInfo(
            chunk_id=0,
            filter_sql=self.source_filter,
            estimated_rows=total_rows,
            metadata={'strategy': 'single_chunk'}
        )]


class NumericRangeStrategy(ChunkingStrategy):
    """Strategy for numeric ID columns (int, bigint)"""
    
    def create_chunks(self) -> List[ChunkInfo]:
        if not self.chunking_columns or len(self.chunking_columns) == 0:
            return SingleChunkStrategy(
                self.sf_manager, self.source_db, self.source_schema,
                self.source_table, self.table_config, self.batch_size
            ).create_chunks()
        
        id_column = self.chunking_columns[0]
        quoted_col = quote_identifier(id_column)
        
        # Check if we need to CAST the column (e.g., VARCHAR storing numeric strings)
        # If chunking_column_types says "int", we should cast for proper numeric comparison
        column_types = self.table_config.get('chunking_column_types', [])
        should_cast = column_types and column_types[0] in ['int', 'bigint', 'integer', 'numeric']
        
        if should_cast:
            # CAST to BIGINT for proper numeric MIN/MAX/comparison
            col_expr = f"CAST({quoted_col} AS BIGINT)"
            self.logger.info(
                f"[{self.source_table}] Column '{id_column}' marked as numeric type in config, "
                f"applying CAST for proper numeric sorting"
            )
        else:
            col_expr = quoted_col
        
        # Get min, max, and count
        query = f"""
            SELECT 
                MIN({col_expr}) as min_id,
                MAX({col_expr}) as max_id,
                COUNT(*) as total_rows,
                COUNT(DISTINCT {quoted_col}) as distinct_ids
            FROM {self.source_db}.{self.source_schema}.{self.source_table}
            WHERE {self.source_filter}
        """
        
        result = self.sf_manager.execute_query(query)
        if not result or result[0][0] is None:
            self.logger.warning(f"No data found for {self.source_table}")
            return []
        
        # Convert to int to handle Snowflake returning numeric values as strings
        min_id = int(result[0][0])
        max_id = int(result[0][1])
        total_rows = int(result[0][2])
        distinct_ids = int(result[0][3])
        
        self.logger.info(
            f"Numeric range strategy for {self.source_table}: "
            f"ID range [{min_id}, {max_id}], {format_number(total_rows)} rows, "
            f"{format_number(distinct_ids)} distinct IDs"
        )
        
        # Calculate chunk size based on ID range
        id_range = max_id - min_id + 1
        num_chunks = math.ceil(total_rows / self.batch_size)
        chunk_step = math.ceil(id_range / num_chunks)
        
        chunks = []
        current_min = min_id
        chunk_id = 0
        
        while current_min <= max_id:
            current_max = min(current_min + chunk_step - 1, max_id)
            
            # Use col_expr (with CAST if needed) for numeric comparison in filter
            if should_cast:
                filter_sql = f"({self.source_filter}) AND {col_expr} >= {current_min} AND {col_expr} <= {current_max}"
            else:
                filter_sql = f"({self.source_filter}) AND {quoted_col} >= {current_min} AND {quoted_col} <= {current_max}"
            
            chunks.append(ChunkInfo(
                chunk_id=chunk_id,
                filter_sql=filter_sql,
                estimated_rows=self.batch_size,
                metadata={
                    'strategy': 'numeric_range',
                    'id_column': id_column,
                    'min_id': current_min,
                    'max_id': current_max,
                    'uses_cast': should_cast  # Track for debugging
                }
            ))
            
            current_min = current_max + 1
            chunk_id += 1
        
        self.logger.info(f"Created {len(chunks)} chunks for {self.source_table}")
        
        # Smart mode: Check which chunks already exist in target
        chunks = self._apply_smart_copy_mode(chunks, id_column)
        
        return chunks
    
    def _apply_smart_copy_mode(self, chunks: List[ChunkInfo], id_column: str) -> List[ChunkInfo]:
        """
        Apply smart COPY/UPSERT mode detection to chunks.
        Checks which numeric ranges already exist in target table.
        """
        from .config_loader import ConfigLoader
        
        # CRITICAL: Skip smart mode if truncate_onstart is True
        # The table will be empty, so all chunks should use COPY mode
        if self.table_config.get('truncate_onstart', False):
            self.logger.info(
                f"[{self.source_table}] truncate_onstart=true: "
                f"Skipping smart mode analysis (all chunks will use COPY mode)"
            )
            for chunk in chunks:
                chunk.metadata['use_copy_mode'] = True
            return chunks
        
        # Only apply if we have PostgreSQL manager and uniqueness columns
        if not hasattr(self, 'pg_manager') or not self.table_config.get('uniqueness_columns'):
            # No smart mode possible, mark all as COPY (safe default)
            for chunk in chunks:
                chunk.metadata['use_copy_mode'] = True
            return chunks
        
        target_db = self.table_config.get('target_pg_database')
        target_schema = self.table_config.get('target_pg_schema')
        target_table = self.table_config.get('target')
        
        if not all([target_db, target_schema, target_table]):
            # Missing target info, use COPY mode
            for chunk in chunks:
                chunk.metadata['use_copy_mode'] = True
            return chunks
        
        self.logger.info("=" * 80)
        self.logger.info("🔍 SMART COPY/UPSERT MODE ANALYSIS")
        self.logger.info("=" * 80)
        self.logger.info(f"Table: {self.source_table}")
        self.logger.info(f"Strategy: NumericRangeStrategy")
        self.logger.info(f"Total chunks: {len(chunks)}")
        self.logger.info(f"Uniqueness column: {self.table_config.get('uniqueness_columns')}")
        self.logger.info(f"Checking existence in target...")
        
        # Check each chunk's range
        copy_count = 0
        upsert_count = 0
        
        for chunk in chunks:
            min_value = chunk.metadata.get('min_id')
            max_value = chunk.metadata.get('max_id')
            
            if min_value is not None and max_value is not None:
                # Check if this range exists in target
                exists = self.pg_manager.check_range_exists(
                    target_db, target_schema, target_table,
                    id_column, min_value, max_value
                )
                
                chunk.metadata['use_copy_mode'] = not exists
                
                if exists:
                    upsert_count += 1
                else:
                    copy_count += 1
            else:
                # Can't determine, use UPSERT (safe)
                chunk.metadata['use_copy_mode'] = False
                upsert_count += 1
        
        # Calculate percentages and estimates
        copy_pct = (copy_count / len(chunks) * 100) if chunks else 0
        upsert_pct = (upsert_count / len(chunks) * 100) if chunks else 0
        
        # Estimate time savings (COPY ~3s vs UPSERT ~40s per 100K rows)
        avg_copy_time = 3  # seconds per chunk
        avg_upsert_time = 40  # seconds per chunk
        
        estimated_time_with_smart = (copy_count * avg_copy_time + upsert_count * avg_upsert_time) / 3600
        estimated_time_all_upsert = (len(chunks) * avg_upsert_time) / 3600
        time_saved_hours = estimated_time_all_upsert - estimated_time_with_smart
        
        self.logger.info("")
        self.logger.info("📊 SMART MODE RESULTS:")
        self.logger.info(f"  ✅ COPY mode chunks: {copy_count} ({copy_pct:.1f}%) - NEW data")
        self.logger.info(f"  🔄 UPSERT mode chunks: {upsert_count} ({upsert_pct:.1f}%) - EXISTING data")
        self.logger.info(f"  ⚡ Estimated time: {estimated_time_with_smart:.1f}h (saves {time_saved_hours:.1f}h vs all UPSERT)")
        self.logger.info("=" * 80)
        
        return chunks


class GroupedValuesStrategy(ChunkingStrategy):
    """Strategy for high-cardinality columns (UUID, varchar) where each value has few rows"""
    
    def create_chunks(self) -> List[ChunkInfo]:
        if not self.chunking_columns or len(self.chunking_columns) == 0:
            return SingleChunkStrategy(
                self.sf_manager, self.source_db, self.source_schema,
                self.source_table, self.table_config, self.batch_size
            ).create_chunks()
        
        id_column = self.chunking_columns[0]
        quoted_col = quote_identifier(id_column)
        sort_columns = self.table_config.get('sort_columns', self.chunking_columns)
        
        # Get distinct values with row counts
        query = f"""
            SELECT 
                {quoted_col},
                COUNT(*) as row_count
            FROM {self.source_db}.{self.source_schema}.{self.source_table}
            WHERE {self.source_filter}
            GROUP BY {quoted_col}
            ORDER BY {quote_identifier(sort_columns[0])}
        """
        
        results = self.sf_manager.execute_query(query)
        total_rows = sum(r[1] for r in results)
        
        self.logger.info(
            f"Grouped values strategy for {self.source_table}: "
            f"{len(results)} distinct values, {format_number(total_rows)} total rows"
        )
        
        # Group values into chunks
        chunks = []
        current_values = []
        current_row_count = 0
        chunk_id = 0
        
        for value, row_count in results:
            # Check if adding this value would exceed batch size
            if current_row_count > 0 and (current_row_count + row_count) > self.batch_size * 1.2:
                # Create chunk with current values
                chunks.append(self._create_value_chunk(
                    chunk_id, id_column, current_values, current_row_count
                ))
                current_values = []
                current_row_count = 0
                chunk_id += 1
            
            current_values.append(value)
            current_row_count += row_count
            
            # Also create chunk if we've accumulated many values (for SQL IN clause limit)
            if len(current_values) >= 1000:
                chunks.append(self._create_value_chunk(
                    chunk_id, id_column, current_values, current_row_count
                ))
                current_values = []
                current_row_count = 0
                chunk_id += 1
        
        # Add remaining values
        if current_values:
            chunks.append(self._create_value_chunk(
                chunk_id, id_column, current_values, current_row_count
            ))
        
        self.logger.info(f"Created {len(chunks)} chunks for {self.source_table}")
        return chunks
    
    def _create_value_chunk(self, chunk_id: int, column: str, values: list, row_count: int) -> ChunkInfo:
        """Create a chunk for a list of values"""
        quoted_col = quote_identifier(column)
        
        # Format values for SQL IN clause
        formatted_values = []
        for v in values:
            if v is None:
                formatted_values.append('NULL')
            elif isinstance(v, str):
                # Escape single quotes
                escaped = v.replace("'", "''")
                formatted_values.append(f"'{escaped}'")
            else:
                formatted_values.append(str(v))
        
        values_str = ", ".join(formatted_values)
        filter_sql = f"({self.source_filter}) AND {quoted_col} IN ({values_str})"
        
        return ChunkInfo(
            chunk_id=chunk_id,
            filter_sql=filter_sql,
            estimated_rows=row_count,
            metadata={
                'strategy': 'grouped_values',
                'id_column': column,
                'value_count': len(values)
            }
        )


class DateRangeStrategy(ChunkingStrategy):
    """Strategy for date/timestamp columns"""
    
    def __init__(
        self,
        sf_manager: SnowflakeConnectionManager,
        source_db: str,
        source_schema: str,
        source_table: str,
        table_config: Dict[str, Any],
        batch_size: int,
        max_target_watermark: Optional[str] = None
    ):
        super().__init__(sf_manager, source_db, source_schema, source_table, table_config, batch_size)
        self.max_target_watermark = max_target_watermark
    
    def create_chunks(self) -> List[ChunkInfo]:
        if not self.chunking_columns or len(self.chunking_columns) == 0:
            return SingleChunkStrategy(
                self.sf_manager, self.source_db, self.source_schema,
                self.source_table, self.table_config, self.batch_size
            ).create_chunks()
        
        date_column = self.chunking_columns[0]
        quoted_col = quote_identifier(date_column)
        
        # Build filter with watermark optimization for incremental loads
        where_clause = self.source_filter
        
        # OPTIMIZATION: Only chunk data newer than target's max watermark
        if self.max_target_watermark:
            watermark_filter = f"{quoted_col} > '{self.max_target_watermark}'"
            where_clause = f"({self.source_filter}) AND ({watermark_filter})"
            self.logger.info(
                f"[{self.source_table}] Incremental chunking: Only processing data after {self.max_target_watermark}"
            )
        
        # Get date range and row distribution
        query = f"""
            SELECT 
                {quoted_col}::DATE as date_val,
                COUNT(*) as row_count
            FROM {self.source_db}.{self.source_schema}.{self.source_table}
            WHERE {where_clause}
            GROUP BY {quoted_col}::DATE
            ORDER BY date_val
        """
        
        results = self.sf_manager.execute_query(query)
        if not results:
            self.logger.info(
                f"[{self.source_table}] No new data found for incremental load"
            )
            return []
        
        total_rows = sum(r[1] for r in results)
        self.logger.info(
            f"Date range strategy for {self.source_table}: "
            f"{len(results)} distinct dates, {format_number(total_rows)} total rows"
        )
        
        # Group dates into chunks
        chunks = []
        current_dates = []
        current_row_count = 0
        chunk_id = 0
        
        for date_val, row_count in results:
            # If single date has more rows than batch_size, use LIMIT/OFFSET sub-chunking
            if row_count > self.batch_size:
                self.logger.info(
                    f"[{self.source_table}] Date {date_val.strftime('%Y-%m-%d')} has "
                    f"{format_number(row_count)} rows - creating {math.ceil(row_count / self.batch_size)} sub-chunks"
                )
                
                # If we have accumulated dates, create chunk first
                if current_dates:
                    chunks.append(self._create_date_chunk(
                        chunk_id, date_column, current_dates, current_row_count
                    ))
                    chunk_id += 1
                    current_dates = []
                    current_row_count = 0
                
                # Create multiple sub-chunks using LIMIT/OFFSET for this large date
                # Use deterministic ordering: date_column + uniqueness_columns
                num_sub_chunks = math.ceil(row_count / self.batch_size)
                for i in range(num_sub_chunks):
                    offset = i * self.batch_size
                    limit = min(self.batch_size, row_count - offset)
                    
                    date_str = date_val.strftime('%Y-%m-%d')
                    filter_sql = f"({self.source_filter}) AND {quoted_col}::DATE = '{date_str}'"
                    
                    # Get uniqueness columns for deterministic ordering
                    uniqueness_columns = self.table_config.get('uniqueness_columns', [])
                    
                    chunks.append(ChunkInfo(
                        chunk_id=chunk_id,
                        filter_sql=filter_sql,
                        estimated_rows=limit,
                        metadata={
                            'strategy': 'date_range_offset',
                            'date_column': date_column,
                            'date_value': date_str,
                            'offset': offset,
                            'limit': limit,
                            'sub_chunk_index': i,
                            'total_sub_chunks': num_sub_chunks,
                            'uniqueness_columns': uniqueness_columns
                        }
                    ))
                    chunk_id += 1
            else:
                # Check if adding this date would exceed batch size
                if current_row_count > 0 and (current_row_count + row_count) > self.batch_size:
                    chunks.append(self._create_date_chunk(
                        chunk_id, date_column, current_dates, current_row_count
                    ))
                    chunk_id += 1
                    current_dates = []
                    current_row_count = 0
                
                current_dates.append(date_val)
                current_row_count += row_count
        
        # Add remaining dates
        if current_dates:
            # CHECK: If remaining dates exceed batch_size, split them
            if current_row_count > self.batch_size:
                self.logger.warning(
                    f"[{self.source_table}] Remaining chunk has {format_number(current_row_count)} rows "
                    f"across {len(current_dates)} dates - splitting into individual date chunks"
                )
                # Create separate chunk for EACH remaining date
                for date_val in current_dates:
                    # Get row count for this specific date from results
                    date_row_count = next((r[1] for r in results if r[0] == date_val), 0)
                    chunks.append(self._create_date_chunk(
                        chunk_id, date_column, [date_val], date_row_count
                    ))
                    chunk_id += 1
            else:
                # Safe to group these dates together
                chunks.append(self._create_date_chunk(
                    chunk_id, date_column, current_dates, current_row_count
                ))
        
        self.logger.info(f"Created {len(chunks)} chunks for {self.source_table}")
        return chunks
    
    def _create_date_chunk(self, chunk_id: int, column: str, dates: list, row_count: int) -> ChunkInfo:
        """Create a chunk for a list of dates"""
        quoted_col = quote_identifier(column)
        
        if len(dates) == 1:
            date_str = dates[0].strftime('%Y-%m-%d')
            filter_sql = f"({self.source_filter}) AND {quoted_col}::DATE = '{date_str}'"
        else:
            date_strs = [d.strftime('%Y-%m-%d') for d in dates]
            values_str = "', '".join(date_strs)
            filter_sql = f"({self.source_filter}) AND {quoted_col}::DATE IN ('{values_str}')"
        
        return ChunkInfo(
            chunk_id=chunk_id,
            filter_sql=filter_sql,
            estimated_rows=row_count,
            metadata={
                'strategy': 'date_range',
                'date_column': column,
                'date_count': len(dates)
            }
        )


class OffsetBasedStrategy(ChunkingStrategy):
    """
    Strategy using LIMIT/OFFSET with ORDER BY
    More efficient than IN clause for high-cardinality columns (UUIDs, etc.)
    """
    
    def create_chunks(self) -> List[ChunkInfo]:
        if not self.chunking_columns or len(self.chunking_columns) == 0:
            return SingleChunkStrategy(
                self.sf_manager, self.source_db, self.source_schema,
                self.source_table, self.table_config, self.batch_size
            ).create_chunks()
        
        sort_column = self.chunking_columns[0]
        quoted_col = quote_identifier(sort_column)
        
        # Get total row count
        query = f"""
            SELECT COUNT(*) as total_rows
            FROM {self.source_db}.{self.source_schema}.{self.source_table}
            WHERE {self.source_filter}
        """
        
        result = self.sf_manager.execute_query(query)
        if not result or result[0][0] == 0:
            self.logger.warning(f"No data found for {self.source_table}")
            return []
        
        total_rows = int(result[0][0])  # Convert to int in case Snowflake returns string
        num_chunks = math.ceil(total_rows / self.batch_size)
        
        self.logger.info(
            f"Offset-based strategy for {self.source_table}: "
            f"{format_number(total_rows)} total rows, {num_chunks} chunks of {format_number(self.batch_size)}"
        )
        
        # Create chunks using LIMIT/OFFSET
        chunks = []
        for chunk_id in range(num_chunks):
            offset = chunk_id * self.batch_size
            limit = self.batch_size
            
            # Calculate estimated rows for this chunk (last chunk might be smaller)
            estimated_rows = min(limit, total_rows - offset)
            
            # Build filter with ORDER BY, LIMIT, OFFSET
            # Note: We'll handle ORDER BY, LIMIT, OFFSET in the worker
            filter_sql = f"({self.source_filter}) ORDER BY {quoted_col} LIMIT {limit} OFFSET {offset}"
            
            chunks.append(ChunkInfo(
                chunk_id=chunk_id,
                filter_sql=filter_sql,
                estimated_rows=estimated_rows,
                metadata={
                    'strategy': 'offset_based',
                    'sort_column': sort_column,
                    'offset': offset,
                    'limit': limit
                }
            ))
        
        self.logger.info(f"Created {len(chunks)} chunks for {self.source_table}")
        return chunks


class ChunkingStrategyFactory:
    """Factory to create appropriate chunking strategy based on configuration"""
    
    @staticmethod
    def create_strategy(
        sf_manager: SnowflakeConnectionManager,
        source_db: str,
        source_schema: str,
        source_table: str,
        table_config: Dict[str, Any],
        batch_size: int,
        max_target_watermark: Optional[str] = None,
        pg_manager=None  # Add pg_manager for smart mode
    ) -> ChunkingStrategy:
        """
        Create appropriate chunking strategy based on table configuration
        
        Args:
            sf_manager: Snowflake connection manager
            source_db: Source database name
            source_schema: Source schema name
            source_table: Source table name
            table_config: Table configuration from config.json
            batch_size: Batch size for chunking
            max_target_watermark: Maximum watermark value from target table (for incremental optimization)
            pg_manager: PostgreSQL connection manager (for smart COPY/UPSERT mode)
        
        Returns:
            Appropriate ChunkingStrategy instance
        """
        chunking_columns = table_config.get('chunking_columns')
        chunking_column_types = table_config.get('chunking_column_types')
        source_watermark = table_config.get('source_watermark')
        target_watermark = table_config.get('target_watermark')
        truncate_onstart = table_config.get('truncate_onstart', False)
        
        # No chunking columns - use single chunk
        if not chunking_columns or chunking_columns == [None] or chunking_columns == []:
            return SingleChunkStrategy(
                sf_manager, source_db, source_schema, source_table, table_config, batch_size, pg_manager
            )
        
        # Determine strategy based on column type
        if chunking_column_types and len(chunking_column_types) > 0:
            first_type = chunking_column_types[0].lower()
            
            if first_type in ['int', 'bigint', 'integer', 'number']:
                return NumericRangeStrategy(
                    sf_manager, source_db, source_schema, source_table, table_config, batch_size, pg_manager
                )
            elif first_type in ['date', 'timestamp', 'timestamp_ntz', 'timestamp_ltz', 'timestamp_tz']:
                return DateRangeStrategy(
                    sf_manager, source_db, source_schema, source_table, table_config, batch_size,
                    max_target_watermark, pg_manager
                )
            elif first_type in ['uuid', 'varchar', 'string', 'text']:
                # Smart strategy selection for high-cardinality columns:
                # - If incremental mode (watermark set) and there's a timestamp column available:
                #   Use DateRangeStrategy on watermark column (more efficient for filtering)
                # - Otherwise: Use OffsetBasedStrategy on the UUID/VARCHAR column
                if source_watermark and target_watermark and not truncate_onstart:
                    # Incremental mode: Use timestamp-based chunking if available
                    logger.info(
                        f"Incremental mode detected: Using date-based chunking on "
                        f"{source_watermark} instead of {chunking_columns[0]}"
                    )
                    # Override chunking to use watermark column
                    modified_config = table_config.copy()
                    modified_config['chunking_columns'] = [source_watermark]
                    modified_config['chunking_column_types'] = ['timestamp']
                    return DateRangeStrategy(
                        sf_manager, source_db, source_schema, source_table, modified_config, batch_size,
                        max_target_watermark, pg_manager
                    )
                else:
                    # Full load: Use offset-based strategy (efficient)
                    return OffsetBasedStrategy(
                        sf_manager, source_db, source_schema, source_table, table_config, batch_size, pg_manager
                    )
        
        # Default to offset-based strategy (most universal)
        return OffsetBasedStrategy(
            sf_manager, source_db, source_schema, source_table, table_config, batch_size, pg_manager
        )

