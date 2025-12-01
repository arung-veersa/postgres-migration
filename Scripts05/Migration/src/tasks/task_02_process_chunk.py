"""
Task 02: Process Single Chunk of Conflict Updates
Strategy: Idempotent processing with composite (VisitDate, SSN) filtering
"""

import time
import json
import os
from pathlib import Path
from typing import Dict, Any, List

from src.tasks.base_task import BaseTask
from src.connectors.postgres_connector import PostgresConnector
from config.settings import CONFLICT_SCHEMA, ANALYTICS_SCHEMA, PROJECT_ROOT


class Task02ProcessChunk(BaseTask):
    """
    Processes a single chunk of conflict updates.
    Idempotent: Can be safely run multiple times.
    """
    
    def __init__(self, postgres_connector: PostgresConnector):
        """
        Initialize Task 02 Chunk Processor.
        
        Args:
            postgres_connector: Connection to the Postgres database.
        """
        super().__init__('TASK_02_PROCESS_CHUNK')
        self.pg = postgres_connector
    
    def execute(self, chunk_id: int, keys: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Process a single chunk of conflict updates.
        
        Args:
            chunk_id: Unique identifier for this chunk
            keys: Optional list of {date, ssn, rows} dictionaries.
                  If not provided, will load from stored chunks file.
            
        Returns:
            Dictionary with processing results
        """
        # If keys not provided, load from file
        if keys is None:
            keys = self._load_chunk_keys(chunk_id)
        
        self.logger.info(f"Processing chunk {chunk_id} with {len(keys)} (date, ssn) keys")
        start_time = time.time()
        
        # Extract dates and SSNs
        dates = sorted(set(k['date'] for k in keys))
        ssns = sorted(set(k['ssn'] for k in keys))
        estimated_rows = sum(k['rows'] for k in keys)
        
        self.logger.info(f"Chunk {chunk_id}: {estimated_rows} estimated rows, "
                        f"{len(dates)} dates, {len(ssns)} SSNs")
        
        # Step 1: Mark rows for processing (idempotent)
        marked_rows = self._mark_chunk_rows(keys)
        self.logger.info(f"Chunk {chunk_id}: Marked {marked_rows} rows for processing")
        
        if marked_rows == 0:
            self.logger.info(f"Chunk {chunk_id}: No rows to process (already completed)")
            return {
                "status": "success",
                "chunk_id": chunk_id,
                "rows_marked": 0,
                "rows_updated": 0,
                "estimated_rows": estimated_rows,
                "num_keys": len(keys),
                "date_range": {
                    "start": dates[0],
                    "end": dates[-1]
                },
                "duration_seconds": time.time() - start_time,
                "throughput_rows_per_sec": 0,
                "message": "Chunk already processed (idempotent skip)"
            }
        
        # Step 2: Execute main update (idempotent)
        updated_rows = self._update_chunk(keys)
        
        end_time = time.time()
        duration = end_time - start_time
        
        self.logger.info(f"Chunk {chunk_id}: Updated {updated_rows} rows in {duration:.2f} seconds")
        
        return {
            "status": "success",
            "chunk_id": chunk_id,
            "rows_marked": marked_rows,
            "rows_updated": updated_rows,
            "estimated_rows": estimated_rows,
            "num_keys": len(keys),
            "date_range": {
                "start": dates[0],
                "end": dates[-1]
            },
            "duration_seconds": duration,
            "throughput_rows_per_sec": updated_rows / duration if duration > 0 else 0
        }
    
    def _mark_chunk_rows(self, keys: List[Dict[str, Any]]) -> int:
        """
        Mark rows for this chunk (Step 1).
        Idempotent: Sets UpdateFlag = 1 for rows that need processing.
        
        Args:
            keys: List of (date, ssn) dictionaries
            
        Returns:
            Number of rows marked
        """
        # Build WHERE clause for (date, ssn) pairs
        keys_filter = self._build_keys_filter(keys)
        
        mark_sql = f"""
            UPDATE {CONFLICT_SCHEMA}.conflictvisitmaps
            SET "UpdateFlag" = 1
            WHERE "CONFLICTID" IS NOT NULL
              AND {keys_filter}
              AND ("UpdateFlag" IS NULL OR "UpdateFlag" != 1)
        """
        
        try:
            affected_rows = self.pg.execute(mark_sql)
            return affected_rows
        except Exception as e:
            self.logger.error(f"Failed to mark rows: {str(e)}", exc_info=True)
            raise
    
    def _update_chunk(self, keys: List[Dict[str, Any]]) -> int:
        """
        Execute main conflict update for this chunk (Step 2).
        Idempotent: Sets values (not increments), clears UpdateFlag when done.
        
        Args:
            keys: List of (date, ssn) dictionaries
            
        Returns:
            Number of rows updated
        """
        # Load SQL template
        sql_file_path = PROJECT_ROOT / "sql" / "task_02_update_conflicts_chunked.sql"
        
        if not sql_file_path.exists():
            self.logger.error(f"SQL script not found at: {sql_file_path}")
            raise FileNotFoundError(f"SQL script not found: {sql_file_path}")
        
        self.logger.info(f"Loading SQL script from: {sql_file_path}")
        
        with open(sql_file_path, 'r') as f:
            sql_template = f.read()
        
        # Inject schema names
        formatted_sql = sql_template.replace('{conflict_schema}', CONFLICT_SCHEMA)
        formatted_sql = formatted_sql.replace('{analytics_schema}', ANALYTICS_SCHEMA)
        
        # Inject chunk filter
        keys_filter = self._build_keys_filter(keys)
        formatted_sql = formatted_sql.replace('{chunk_filter}', keys_filter)
        
        # Execute
        try:
            updated_rows = self.pg.execute(formatted_sql)
            return updated_rows
        except Exception as e:
            self.logger.error(f"Failed to update chunk: {str(e)}", exc_info=True)
            raise
    
    def _build_keys_filter(self, keys: List[Dict[str, Any]]) -> str:
        """
        Build SQL WHERE clause for (VisitDate, SSN) filtering.
        
        Args:
            keys: List of (date, ssn) dictionaries
            
        Returns:
            SQL WHERE clause string
        """
        # Option 1: Use IN clause with tuples (best for small number of keys)
        if len(keys) <= 100:
            pairs = ", ".join(
                f"('{k['date']}'::date, '{k['ssn']}')"
                for k in keys
            )
            return f'("VisitDate", "SSN") IN ({pairs})'
        
        # Option 2: Use date range + SSN list (better for large number of keys)
        dates = sorted(set(k['date'] for k in keys))
        ssns = sorted(set(k['ssn'] for k in keys))
        
        date_filter = f'"VisitDate" BETWEEN \'{dates[0]}\'::date AND \'{dates[-1]}\'::date'
        
        # Escape single quotes in SSNs
        ssn_list = "', '".join(ssns)
        ssn_filter = f'"SSN" IN (\'{ssn_list}\')'
        
        return f'({date_filter} AND {ssn_filter})'
    
    def _load_chunk_keys(self, chunk_id: int) -> List[Dict[str, Any]]:
        """
        Load chunk keys from stored chunks file.
        
        Args:
            chunk_id: Chunk identifier
            
        Returns:
            List of keys for this chunk
        """
        # Determine chunks file location
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            chunks_file = '/tmp/task02_chunks.json'
        else:
            chunks_file = os.path.join(
                os.path.dirname(__file__), '..', '..', 'temp', 'task02_chunks.json'
            )
        
        if not os.path.exists(chunks_file):
            raise FileNotFoundError(
                f"Chunks file not found: {chunks_file}. "
                "Run get_task02_chunks first to generate chunks."
            )
        
        with open(chunks_file, 'r') as f:
            all_chunks = json.load(f)
        
        # Find the chunk with this ID
        for chunk in all_chunks:
            if chunk['chunk_id'] == chunk_id:
                return chunk['keys']
        
        raise ValueError(f"Chunk {chunk_id} not found in chunks file")

