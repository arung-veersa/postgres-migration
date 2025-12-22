"""
Snowflake Unloader Module
Handles Snowflake COPY INTO @stage operations for S3 unloading
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import re

from .connections import SnowflakeConnectionManager
from .utils import logger

class SnowflakeUnloader:
    """Handles Snowflake UNLOAD operations to S3"""
    
    def __init__(
        self,
        sf_manager: SnowflakeConnectionManager,
        config: Dict[str, Any]
    ):
        """
        Initialize Snowflake Unloader
        
        Args:
            sf_manager: Snowflake connection manager
            config: Configuration dictionary
        """
        self.sf_manager = sf_manager
        self.unload_config = config.get('snowflake_unload', {})
        self.s3_config = config.get('s3_staging', {})
        
        # Unload settings
        self.storage_integration = self.unload_config.get('storage_integration')
        self.stage_name = self.unload_config.get('stage_name')
        self.max_file_size_bytes = self.unload_config.get('max_file_size_bytes', 104857600)  # 100MB
        
        # S3 settings
        self.file_format = self.s3_config.get('file_format', 'parquet').upper()
        self.compression = self.s3_config.get('compression', 'snappy').upper()
        
        # Get database and schema for fully qualified stage name
        # Stage database/schema can be different from source data database/schema
        self.stage_database = self.unload_config.get('stage_database', 'CONFLICTREPORT_SANDBOX')
        self.stage_schema = self.unload_config.get('stage_schema', 'PUBLIC')
        
        # Build fully qualified stage name
        self.fully_qualified_stage = f"{self.stage_database}.{self.stage_schema}.{self.stage_name}"
        
        logger.info(
            f"SnowflakeUnloader initialized - Stage: {self.fully_qualified_stage}, "
            f"Format: {self.file_format}, Compression: {self.compression}"
        )
    
    def _get_timestamp_columns(
        self,
        database: str,
        schema: str,
        table: str
    ) -> List[str]:
        """
        Identify TIMESTAMP_TZ columns that need casting
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
        
        Returns:
            List of column names with TIMESTAMP_TZ type
        """
        query = f"""
        SELECT column_name, data_type
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
          AND data_type IN ('TIMESTAMP_TZ', 'TIMESTAMP_LTZ')
        """
        
        try:
            result = self.sf_manager.execute_query(query)
            # execute_query returns list of tuples directly
            tz_columns = [row[0] for row in result]
            
            if tz_columns:
                logger.info(f"Found {len(tz_columns)} TIMESTAMP_TZ/LTZ column(s) in {table}: {', '.join(tz_columns)}")
            
            return tz_columns
            
        except Exception as e:
            logger.warning(f"Failed to detect TIMESTAMP_TZ columns: {str(e)}")
            return []
    
    def _build_select_with_casts(
        self,
        database: str,
        schema: str,
        table: str,
        source_filter: Optional[str] = None
    ) -> str:
        """
        Build SELECT statement with TIMESTAMP_TZ → TIMESTAMP_NTZ casts
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            source_filter: Optional WHERE clause
        
        Returns:
            Complete SELECT statement with casts
        """
        # Get all columns
        columns_query = f"""
        SELECT column_name, data_type
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        
        columns_result = self.sf_manager.execute_query(columns_query)
        
        # Build column list with casts for TIMESTAMP_TZ/LTZ
        # execute_query returns list of tuples directly
        column_expressions = []
        for col_name, col_type in columns_result:
            if col_type in ('TIMESTAMP_TZ', 'TIMESTAMP_LTZ'):
                # Cast to TIMESTAMP_NTZ (no timezone)
                # IMPORTANT: Use double quotes for column names (may have spaces)
                column_expressions.append(
                    f'"{col_name}"::TIMESTAMP_NTZ AS "{col_name}"'
                )
            else:
                column_expressions.append(f'"{col_name}"')
        
        select_clause = ",\n       ".join(column_expressions)
        
        # Build complete query
        query = f"""
SELECT {select_clause}
FROM {database}.{schema}.{table}
"""
        
        if source_filter:
            query += f"\nWHERE {source_filter}"
        
        return query
    
    def unload_table(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        s3_path: str,
        source_filter: Optional[str] = None,
        overwrite: bool = True
    ) -> Dict[str, Any]:
        """
        Unload a Snowflake table to S3
        
        Args:
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            s3_path: S3 path (e.g., "s3://bucket/prefix/")
            source_filter: Optional WHERE clause
            overwrite: Whether to overwrite existing files
        
        Returns:
            Dictionary with unload results
        """
        try:
            logger.info(f"\n{'='*70}")
            logger.info(f"SNOWFLAKE UNLOAD: {source_database}.{source_schema}.{source_table}")
            logger.info(f"{'='*70}")
            logger.info(f"Target S3: {s3_path}")
            
            # Build SELECT query with TIMESTAMP casts
            select_query = self._build_select_with_casts(
                database=source_database,
                schema=source_schema,
                table=source_table,
                source_filter=source_filter
            )
            
            logger.debug(f"SELECT query:\n{select_query}")
            
            # Build COPY INTO statement
            copy_query = self._build_copy_into_statement(
                select_query=select_query,
                s3_path=s3_path,
                overwrite=overwrite
            )
            
            logger.info(f"\nExecuting UNLOAD...")
            logger.debug(f"COPY INTO statement:\n{copy_query}")
            
            # Execute COPY INTO
            start_time = datetime.now()
            result = self.sf_manager.execute_query(copy_query)
            duration = (datetime.now() - start_time).total_seconds()
            
            # Parse results
            # execute_query returns list of tuples directly
            files_created = []
            total_rows = 0
            total_size = 0
            
            for row in result:
                # Row format: (file_name, file_size, row_count)
                if len(row) >= 3:
                    file_name = row[0]
                    file_size = row[1]
                    row_count = row[2]
                    
                    files_created.append({
                        'file_name': file_name,
                        'file_size': file_size,
                        'row_count': row_count,
                        's3_url': f"{s3_path.rstrip('/')}/{file_name}"
                    })
                    
                    total_rows += row_count
                    total_size += file_size
            
            logger.info(f"\n✅ UNLOAD completed successfully!")
            logger.info(f"   Duration: {duration:.2f} seconds")
            logger.info(f"   Files created: {len(files_created)}")
            logger.info(f"   Total rows: {total_rows:,}")
            logger.info(f"   Total size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
            
            if files_created:
                logger.info(f"\n📦 Files:")
                for file_info in files_created:
                    logger.info(
                        f"   - {file_info['file_name']}: "
                        f"{file_info['row_count']:,} rows, "
                        f"{file_info['file_size'] / 1024 / 1024:.2f} MB"
                    )
            
            return {
                'success': True,
                'files': files_created,
                'total_rows': total_rows,
                'total_size_bytes': total_size,
                'duration_seconds': duration,
                's3_path': s3_path
            }
            
        except Exception as e:
            error_msg = f"UNLOAD failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            
            return {
                'success': False,
                'error': error_msg,
                'files': [],
                'total_rows': 0,
                'total_size_bytes': 0
            }
    
    def _build_copy_into_statement(
        self,
        select_query: str,
        s3_path: str,
        overwrite: bool = True
    ) -> str:
        """
        Build COPY INTO @stage statement
        
        Args:
            select_query: SELECT query to unload
            s3_path: S3 destination path
            overwrite: Whether to overwrite existing files
        
        Returns:
            Complete COPY INTO statement
        """
        # Format settings based on file format
        if self.file_format == 'PARQUET':
            format_options = f"""
    TYPE = PARQUET
    COMPRESSION = {self.compression}
"""
        else:  # CSV
            format_options = f"""
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
    NULL_IF = ()
"""
        
        # Build COPY INTO statement
        # IMPORTANT: Use fully qualified stage reference (@DATABASE.SCHEMA.STAGE/path)
        # Extract path after bucket name from s3://bucket/path/
        if s3_path.startswith('s3://'):
            # Extract: s3://bucket/path/ → path/
            parts = s3_path.replace('s3://', '').split('/', 1)
            if len(parts) > 1:
                stage_path = parts[1]  # Everything after bucket name
            else:
                stage_path = ''
            copy_target = f"@{self.fully_qualified_stage}/{stage_path}"
        else:
            # Already a stage reference
            copy_target = s3_path
        
        copy_statement = f"""
COPY INTO {copy_target}
FROM (
{select_query}
)
FILE_FORMAT = (
{format_options}
)
OVERWRITE = {str(overwrite).upper()}
MAX_FILE_SIZE = {self.max_file_size_bytes}
DETAILED_OUTPUT = TRUE
"""
        
        return copy_statement
    
    def estimate_unload_time(
        self,
        row_count: int,
        avg_row_size_bytes: int = 500
    ) -> Dict[str, Any]:
        """
        Estimate UNLOAD time and file count
        
        Args:
            row_count: Number of rows to unload
            avg_row_size_bytes: Average row size in bytes
        
        Returns:
            Dictionary with estimates
        """
        total_size_bytes = row_count * avg_row_size_bytes
        total_size_mb = total_size_bytes / 1024 / 1024
        total_size_gb = total_size_mb / 1024
        
        # Estimate file count
        file_count = max(1, int(total_size_bytes / self.max_file_size_bytes) + 1)
        
        # Estimate time (rough: 1GB per minute for UNLOAD)
        estimated_minutes = max(1, int(total_size_gb))
        
        return {
            'total_size_mb': round(total_size_mb, 2),
            'total_size_gb': round(total_size_gb, 2),
            'estimated_file_count': file_count,
            'estimated_minutes': estimated_minutes,
            'max_file_size_mb': self.max_file_size_bytes / 1024 / 1024
        }


