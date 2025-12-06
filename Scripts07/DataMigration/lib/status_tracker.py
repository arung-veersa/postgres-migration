"""
Status Tracker Module
Manages migration status tracking in PostgreSQL
"""

import uuid
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

from .connections import PostgresConnectionManager
from .utils import logger


class StatusTracker:
    """Tracks migration progress in PostgreSQL status tables"""
    
    def __init__(self, pg_manager: PostgresConnectionManager, target_database: str):
        self.pg_manager = pg_manager
        self.target_database = target_database
        self.logger = logger
        self.current_run_id: Optional[uuid.UUID] = None
    
    def create_migration_run(self, config_hash: str, total_sources: int, 
                            total_tables: int, metadata: Optional[Dict] = None) -> uuid.UUID:
        """Create a new migration run record"""
        query = """
            INSERT INTO migration_status.migration_runs 
                (config_hash, total_sources, total_tables, metadata)
            VALUES (%s, %s, %s, %s)
            RETURNING run_id
        """
        
        metadata_json = json.dumps(metadata) if metadata else None
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (config_hash, total_sources, total_tables, metadata_json))
            run_id = cursor.fetchone()[0]
            conn.commit()
            
            self.current_run_id = run_id
            self.logger.info(f"✓ Created migration run: {run_id}")
            return run_id
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to create migration run: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def update_run_status(self, run_id: uuid.UUID, status: str, 
                         completed_tables: Optional[int] = None,
                         failed_tables: Optional[int] = None,
                         total_rows_copied: Optional[int] = None,
                         error_message: Optional[str] = None):
        """Update migration run status"""
        updates = ["status = %s"]
        params = [status]
        
        if status in ['completed', 'failed', 'partial']:
            updates.append("completed_at = CURRENT_TIMESTAMP")
        
        if completed_tables is not None:
            updates.append("completed_tables = %s")
            params.append(completed_tables)
        
        if failed_tables is not None:
            updates.append("failed_tables = %s")
            params.append(failed_tables)
        
        if total_rows_copied is not None:
            updates.append("total_rows_copied = %s")
            params.append(total_rows_copied)
        
        if error_message is not None:
            updates.append("error_message = %s")
            params.append(error_message)
        
        params.append(str(run_id))
        
        query = f"""
            UPDATE migration_status.migration_runs
            SET {', '.join(updates)}
            WHERE run_id = %s
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update run status: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def create_table_status(self, run_id: uuid.UUID, source_name: str,
                           source_database: str, source_schema: str, source_table: str,
                           target_database: str, target_schema: str, target_table: str,
                           total_chunks: int, metadata: Optional[Dict] = None):
        """Create table status record"""
        query = """
            INSERT INTO migration_status.migration_table_status
                (run_id, source_name, source_database, source_schema, source_table,
                 target_database, target_schema, target_table, total_chunks, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, source_database, source_schema, source_table)
            DO UPDATE SET total_chunks = EXCLUDED.total_chunks, metadata = EXCLUDED.metadata
        """
        
        metadata_json = json.dumps(metadata) if metadata else None
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (
                str(run_id), source_name, source_database, source_schema, source_table,
                target_database, target_schema, target_table, total_chunks, metadata_json
            ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to create table status: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def update_table_status(self, run_id: uuid.UUID, source_database: str,
                           source_schema: str, source_table: str, status: str,
                           completed_chunks: Optional[int] = None,
                           failed_chunks: Optional[int] = None,
                           total_rows_copied: Optional[int] = None,
                           error_message: Optional[str] = None):
        """Update table status"""
        updates = ["status = %s"]
        params = [status]
        
        if status == 'in_progress':
            updates.append("started_at = COALESCE(started_at, CURRENT_TIMESTAMP)")
        
        if status in ['completed', 'failed']:
            updates.append("completed_at = CURRENT_TIMESTAMP")
        
        if completed_chunks is not None:
            updates.append("completed_chunks = %s")
            params.append(completed_chunks)
        
        if failed_chunks is not None:
            updates.append("failed_chunks = %s")
            params.append(failed_chunks)
        
        if total_rows_copied is not None:
            updates.append("total_rows_copied = %s")
            params.append(total_rows_copied)
        
        if error_message is not None:
            updates.append("error_message = %s")
            params.append(error_message)
        
        params.extend([str(run_id), source_database, source_schema, source_table])
        
        query = f"""
            UPDATE migration_status.migration_table_status
            SET {', '.join(updates)}
            WHERE run_id = %s AND source_database = %s 
              AND source_schema = %s AND source_table = %s
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update table status: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def create_chunk_status(self, run_id: uuid.UUID, source_database: str,
                           source_schema: str, source_table: str,
                           chunk_id: int, chunk_range: Dict[str, Any]):
        """Create chunk status record"""
        query = """
            INSERT INTO migration_status.migration_chunk_status
                (run_id, source_database, source_schema, source_table, 
                 chunk_id, chunk_range, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'pending')
            ON CONFLICT (run_id, source_database, source_schema, source_table, chunk_id)
            DO NOTHING
        """
        
        # Handle both dict and string inputs
        if isinstance(chunk_range, str):
            chunk_range_json = chunk_range
        else:
            chunk_range_json = json.dumps(chunk_range)
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (
                str(run_id), source_database, source_schema, source_table,
                chunk_id, chunk_range_json
            ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to create chunk status: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def update_chunk_status(self, run_id: uuid.UUID, source_database: str,
                           source_schema: str, source_table: str, chunk_id: int,
                           status: str, rows_copied: Optional[int] = None,
                           error_message: Optional[str] = None,
                           increment_retry: bool = False):
        """Update chunk status"""
        updates = ["status = %s"]
        params = [status]
        
        if status == 'in_progress':
            updates.append("started_at = CURRENT_TIMESTAMP")
        
        if status in ['completed', 'failed']:
            updates.append("completed_at = CURRENT_TIMESTAMP")
        
        if rows_copied is not None:
            updates.append("rows_copied = %s")
            params.append(rows_copied)
        
        if error_message is not None:
            updates.append("error_message = %s")
            params.append(error_message)
        
        if increment_retry:
            updates.append("retry_count = retry_count + 1")
        
        params.extend([str(run_id), source_database, source_schema, source_table, chunk_id])
        
        query = f"""
            UPDATE migration_status.migration_chunk_status
            SET {', '.join(updates)}
            WHERE run_id = %s AND source_database = %s 
              AND source_schema = %s AND source_table = %s AND chunk_id = %s
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update chunk status: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def get_pending_chunks(self, run_id: uuid.UUID, source_database: str,
                          source_schema: str, source_table: str) -> List[Dict]:
        """Get list of pending or failed chunks for a table"""
        query = """
            SELECT chunk_id, chunk_range, retry_count
            FROM migration_status.migration_chunk_status
            WHERE run_id = %s AND source_database = %s 
              AND source_schema = %s AND source_table = %s
              AND status IN ('pending', 'failed', 'in_progress')
            ORDER BY chunk_id
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (str(run_id), source_database, source_schema, source_table))
            results = cursor.fetchall()
            
            return [
                {
                    'chunk_id': row[0],
                    'chunk_range': json.loads(row[1]) if isinstance(row[1], str) else row[1],
                    'retry_count': row[2]
                }
                for row in results
            ]
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def find_resumable_run(self, config_hash: str, max_age_hours: int = 12) -> Optional[Dict]:
        """
        Find the most recent incomplete run with same config hash within age limit.
        
        Args:
            config_hash: MD5 hash of configuration
            max_age_hours: Maximum age in hours for resumable runs (default: 12)
        
        Returns:
            Dict with run details or None if no resumable run found
        """
        query = """
            SELECT 
                run_id, 
                status, 
                started_at,
                total_tables,
                completed_tables,
                failed_tables,
                total_rows_copied
            FROM migration_status.migration_runs
            WHERE config_hash = %s
              AND status IN ('running', 'partial', 'failed')
              AND started_at > CURRENT_TIMESTAMP - INTERVAL '%s hours'
            ORDER BY started_at DESC
            LIMIT 1
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (config_hash, max_age_hours))
            result = cursor.fetchone()
            
            if result:
                return {
                    'run_id': result[0],
                    'status': result[1],
                    'started_at': result[2],
                    'total_tables': result[3],
                    'completed_tables': result[4],
                    'failed_tables': result[5],
                    'total_rows_copied': result[6]
                }
            return None
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def get_table_progress(self, run_id: uuid.UUID) -> List[Dict]:
        """Get progress summary for all tables in a run"""
        query = """
            SELECT 
                source_name,
                source_table,
                target_table,
                status,
                total_chunks,
                completed_chunks,
                failed_chunks,
                total_rows_copied,
                EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at)) as duration_seconds
            FROM migration_status.migration_table_status
            WHERE run_id = %s
            ORDER BY started_at
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (str(run_id),))
            results = cursor.fetchall()
            
            return [
                {
                    'source_name': row[0],
                    'source_table': row[1],
                    'target_table': row[2],
                    'status': row[3],
                    'total_chunks': row[4],
                    'completed_chunks': row[5],
                    'failed_chunks': row[6],
                    'total_rows_copied': row[7],
                    'duration_seconds': row[8]
                }
                for row in results
            ]
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def get_table_status(self, run_id: uuid.UUID, source_database: str,
                        source_schema: str, source_table: str) -> Optional[Dict]:
        """Get status for a specific table"""
        query = """
            SELECT 
                status,
                total_chunks,
                completed_chunks,
                failed_chunks,
                total_rows_copied,
                started_at,
                completed_at
            FROM migration_status.migration_table_status
            WHERE run_id = %s AND source_database = %s 
              AND source_schema = %s AND source_table = %s
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (str(run_id), source_database, source_schema, source_table))
            result = cursor.fetchone()
            
            if result:
                return {
                    'status': result[0],
                    'total_chunks': result[1],
                    'completed_chunks': result[2],
                    'failed_chunks': result[3],
                    'total_rows_copied': result[4],
                    'started_at': result[5],
                    'completed_at': result[6]
                }
            return None
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def mark_indexes_disabled(self, run_id: uuid.UUID, source_database: str,
                             source_schema: str, source_table: str):
        """Mark that indexes have been disabled for a table"""
        query = """
            UPDATE migration_status.migration_table_status
            SET indexes_disabled = TRUE
            WHERE run_id = %s AND source_database = %s 
              AND source_schema = %s AND source_table = %s
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (str(run_id), source_database, source_schema, source_table))
            conn.commit()
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def mark_indexes_restored(self, run_id: uuid.UUID, source_database: str,
                             source_schema: str, source_table: str):
        """Mark that indexes have been restored for a table"""
        query = """
            UPDATE migration_status.migration_table_status
            SET indexes_restored = TRUE
            WHERE run_id = %s AND source_database = %s 
              AND source_schema = %s AND source_table = %s
        """
        
        conn = self.pg_manager.get_connection(self.target_database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, (str(run_id), source_database, source_schema, source_table))
            conn.commit()
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)

