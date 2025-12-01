"""
Task 02: Get Chunks for Conflict Update
Strategy: Composite (VisitDate, SSN) chunking with dynamic balancing
"""

import time
import json
import os
from typing import Dict, Any, List, Tuple
from datetime import datetime

from src.tasks.base_task import BaseTask
from src.connectors.postgres_connector import PostgresConnector
from config.settings import CONFLICT_SCHEMA
from config.chunking_config import CHUNKING_CONFIG


class Task02GetChunks(BaseTask):
    """
    Analyzes data distribution and creates balanced chunks.
    Each chunk contains (VisitDate, SSN) pairs that are safe to process independently.
    """
    
    def __init__(self, postgres_connector: PostgresConnector):
        """
        Initialize Task 02 Chunk Generator.
        
        Args:
            postgres_connector: Connection to the Postgres database.
        """
        super().__init__('TASK_02_GET_CHUNKS')
        self.pg = postgres_connector
        self.config = CHUNKING_CONFIG
    
    def execute(self) -> Dict[str, Any]:
        """
        Query data distribution and create balanced chunks.
        Store chunks in /tmp and return lightweight metadata.
        
        Returns:
            Dictionary with chunk metadata (not full chunk data)
        """
        self.logger.info("Starting Task 02: Generating chunks for conflict update")
        start_time = time.time()
        
        # Step 1: Get distribution of (VisitDate, SSN) pairs
        self.logger.info("Querying (VisitDate, SSN) distribution...")
        distribution = self._get_distribution()
        
        if not distribution:
            self.logger.warning("No rows found to process")
            return {
                "status": "success",
                "total_rows": 0,
                "num_chunks": 0,
                "chunk_ids": []
            }
        
        total_rows = sum(row['row_count'] for row in distribution)
        self.logger.info(f"Found {total_rows} rows across {len(distribution)} (date, ssn) pairs")
        
        # Step 2: Create balanced chunks
        self.logger.info("Creating balanced chunks...")
        chunks = self._create_balanced_chunks(distribution)
        
        # Step 3: Store chunks in /tmp (for Lambda) or local directory
        chunks_dir = self._get_chunks_directory()
        chunk_file = os.path.join(chunks_dir, 'task02_chunks.json')
        
        with open(chunk_file, 'w') as f:
            json.dump(chunks, f)
        
        self.logger.info(f"Stored {len(chunks)} chunks to {chunk_file}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        self.logger.info(f"Generated {len(chunks)} chunks in {duration:.2f} seconds")
        self._log_chunk_summary(chunks)
        
        # Return lightweight metadata only (not full chunks)
        return {
            "status": "success",
            "total_rows": total_rows,
            "total_keys": len(distribution),
            "num_chunks": len(chunks),
            "chunk_ids": list(range(len(chunks))),  # Just IDs, not full data
            "chunks_file": chunk_file,
            "config": {
                "target_size": self.config['target_chunk_size'],
                "max_size": self.config['max_chunk_size'],
                "max_concurrency": self.config['max_concurrency']
            },
            "duration_seconds": duration
        }
    
    def _get_distribution(self) -> List[Dict[str, Any]]:
        """
        Query database for (VisitDate, SSN) distribution.
        
        Returns:
            List of dicts with visit_date, ssn, row_count
        """
        query = f"""
            SELECT 
                "VisitDate"::date as visit_date,
                "SSN" as ssn,
                COUNT(*) as row_count,
                MIN("ID") as min_id,
                MAX("ID") as max_id
            FROM {CONFLICT_SCHEMA}.conflictvisitmaps
            WHERE "CONFLICTID" IS NOT NULL
              AND "VisitDate"::date BETWEEN 
                  (NOW() - INTERVAL '{self.config['date_range']['lookback_years']} years')::date 
                  AND (NOW() + INTERVAL '{self.config['date_range']['lookahead_days']} days')::date
            GROUP BY "VisitDate"::date, "SSN"
            HAVING COUNT(*) > 0
            ORDER BY "VisitDate"::date, "SSN"
        """
        
        try:
            # Execute query and fetch results
            with self.pg.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    results = []
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                    return results
        except Exception as e:
            self.logger.error(f"Failed to get distribution: {str(e)}", exc_info=True)
            raise
    
    def _create_balanced_chunks(self, distribution: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Create balanced chunks by grouping (VisitDate, SSN) pairs.
        
        Args:
            distribution: List of (date, ssn, count) tuples
            
        Returns:
            List of chunk definitions
        """
        chunks = []
        current_chunk_keys = []
        current_chunk_rows = 0
        chunk_id = 0
        
        target_size = self.config['target_chunk_size']
        max_size = self.config['max_chunk_size']
        max_keys = self.config['max_keys_per_chunk']
        
        for row in distribution:
            date = str(row['visit_date'])  # Convert to string for JSON serialization
            ssn = row['ssn']
            count = row['row_count']
            
            # Check if adding this key would exceed limits
            would_exceed_size = current_chunk_rows + count > max_size
            would_exceed_keys = len(current_chunk_keys) >= max_keys
            
            if (would_exceed_size or would_exceed_keys) and current_chunk_keys:
                # Finalize current chunk
                chunks.append(self._finalize_chunk(chunk_id, current_chunk_keys, current_chunk_rows))
                chunk_id += 1
                
                # Start new chunk
                current_chunk_keys = []
                current_chunk_rows = 0
            
            # Add this (date, ssn) to current chunk
            current_chunk_keys.append({
                'date': date,
                'ssn': ssn,
                'rows': count
            })
            current_chunk_rows += count
        
        # Finalize last chunk
        if current_chunk_keys:
            chunks.append(self._finalize_chunk(chunk_id, current_chunk_keys, current_chunk_rows))
        
        return chunks
    
    def _finalize_chunk(self, chunk_id: int, keys: List[Dict], row_count: int) -> Dict[str, Any]:
        """
        Create final chunk definition.
        
        Args:
            chunk_id: Unique chunk identifier
            keys: List of (date, ssn) dictionaries
            row_count: Estimated total rows
            
        Returns:
            Chunk definition dictionary
        """
        dates = sorted(set(k['date'] for k in keys))
        
        return {
            'chunk_id': chunk_id,
            'keys': keys,
            'estimated_rows': row_count,
            'num_keys': len(keys),
            'date_range': {
                'start': dates[0],
                'end': dates[-1],
                'num_dates': len(dates)
            },
            'status': 'pending'
        }
    
    def _log_chunk_summary(self, chunks: List[Dict[str, Any]]) -> None:
        """
        Log summary statistics about chunks.
        
        Args:
            chunks: List of chunk definitions
        """
        if not chunks:
            return
        
        row_counts = [c['estimated_rows'] for c in chunks]
        key_counts = [c['num_keys'] for c in chunks]
        
        self.logger.info("Chunk Summary:")
        self.logger.info(f"  Total chunks: {len(chunks)}")
        self.logger.info(f"  Rows per chunk: min={min(row_counts)}, max={max(row_counts)}, avg={sum(row_counts)//len(row_counts)}")
        self.logger.info(f"  Keys per chunk: min={min(key_counts)}, max={max(key_counts)}, avg={sum(key_counts)//len(key_counts)}")
        
        # Log first few chunks as examples
        for i, chunk in enumerate(chunks[:3]):
            self.logger.info(f"  Chunk {i}: {chunk['estimated_rows']} rows, "
                           f"{chunk['num_keys']} keys, "
                           f"dates {chunk['date_range']['start']} to {chunk['date_range']['end']}")
    
    def _get_chunks_directory(self) -> str:
        """
        Get directory for storing chunks.
        Use /tmp in Lambda, otherwise use project temp directory.
        
        Returns:
            Directory path
        """
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            # Running in Lambda - use /tmp
            chunks_dir = '/tmp'
        else:
            # Running locally - use project directory
            chunks_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'temp')
            os.makedirs(chunks_dir, exist_ok=True)
        
        return chunks_dir


