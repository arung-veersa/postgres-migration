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
        batch_size: int
    ):
        self.sf_manager = sf_manager
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
        return result[0][0] if result else 0
    
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
        
        # Get min, max, and count
        query = f"""
            SELECT 
                MIN({quoted_col}) as min_id,
                MAX({quoted_col}) as max_id,
                COUNT(*) as total_rows,
                COUNT(DISTINCT {quoted_col}) as distinct_ids
            FROM {self.source_db}.{self.source_schema}.{self.source_table}
            WHERE {self.source_filter}
        """
        
        result = self.sf_manager.execute_query(query)
        if not result or result[0][0] is None:
            self.logger.warning(f"No data found for {self.source_table}")
            return []
        
        min_id, max_id, total_rows, distinct_ids = result[0]
        
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
            
            filter_sql = f"({self.source_filter}) AND {quoted_col} >= {current_min} AND {quoted_col} <= {current_max}"
            
            chunks.append(ChunkInfo(
                chunk_id=chunk_id,
                filter_sql=filter_sql,
                estimated_rows=self.batch_size,
                metadata={
                    'strategy': 'numeric_range',
                    'id_column': id_column,
                    'min_id': current_min,
                    'max_id': current_max
                }
            ))
            
            current_min = current_max + 1
            chunk_id += 1
        
        self.logger.info(f"Created {len(chunks)} chunks for {self.source_table}")
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
    
    def create_chunks(self) -> List[ChunkInfo]:
        if not self.chunking_columns or len(self.chunking_columns) == 0:
            return SingleChunkStrategy(
                self.sf_manager, self.source_db, self.source_schema,
                self.source_table, self.table_config, self.batch_size
            ).create_chunks()
        
        date_column = self.chunking_columns[0]
        quoted_col = quote_identifier(date_column)
        
        # Get date range and row distribution
        query = f"""
            SELECT 
                {quoted_col}::DATE as date_val,
                COUNT(*) as row_count
            FROM {self.source_db}.{self.source_schema}.{self.source_table}
            WHERE {self.source_filter}
            GROUP BY {quoted_col}::DATE
            ORDER BY date_val
        """
        
        results = self.sf_manager.execute_query(query)
        if not results:
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
            # If a single date has too many rows, it needs sub-chunking
            if row_count > self.batch_size * 1.5:
                # If we have accumulated dates, create chunk first
                if current_dates:
                    chunks.append(self._create_date_chunk(
                        chunk_id, date_column, current_dates, current_row_count
                    ))
                    chunk_id += 1
                    current_dates = []
                    current_row_count = 0
                
                # Create separate chunk for this date (may need further sub-chunking in worker)
                chunks.append(self._create_date_chunk(
                    chunk_id, date_column, [date_val], row_count
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


class ChunkingStrategyFactory:
    """Factory to create appropriate chunking strategy based on configuration"""
    
    @staticmethod
    def create_strategy(
        sf_manager: SnowflakeConnectionManager,
        source_db: str,
        source_schema: str,
        source_table: str,
        table_config: Dict[str, Any],
        batch_size: int
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
        
        Returns:
            Appropriate ChunkingStrategy instance
        """
        chunking_columns = table_config.get('chunking_columns')
        chunking_column_types = table_config.get('chunking_column_types')
        
        # No chunking columns - use single chunk
        if not chunking_columns or chunking_columns == [None] or chunking_columns == []:
            return SingleChunkStrategy(
                sf_manager, source_db, source_schema, source_table, table_config, batch_size
            )
        
        # Determine strategy based on column type
        if chunking_column_types and len(chunking_column_types) > 0:
            first_type = chunking_column_types[0].lower()
            
            if first_type in ['int', 'bigint', 'integer', 'number']:
                return NumericRangeStrategy(
                    sf_manager, source_db, source_schema, source_table, table_config, batch_size
                )
            elif first_type in ['date', 'timestamp', 'timestamp_ntz', 'timestamp_ltz', 'timestamp_tz']:
                return DateRangeStrategy(
                    sf_manager, source_db, source_schema, source_table, table_config, batch_size
                )
            elif first_type in ['uuid', 'varchar', 'string', 'text']:
                return GroupedValuesStrategy(
                    sf_manager, source_db, source_schema, source_table, table_config, batch_size
                )
        
        # Default to grouped values strategy
        return GroupedValuesStrategy(
            sf_manager, source_db, source_schema, source_table, table_config, batch_size
        )

