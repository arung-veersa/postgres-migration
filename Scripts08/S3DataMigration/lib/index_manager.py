"""
Index Manager Module
Handles disabling and restoring indexes and constraints
"""

import logging
from typing import Dict, List, Tuple, Optional

from .connections import PostgresConnectionManager
from .utils import logger


class IndexManager:
    """Manages PostgreSQL indexes and constraints during migration"""
    
    def __init__(self, pg_manager: PostgresConnectionManager, database: str):
        self.pg_manager = pg_manager
        self.database = database
        self.logger = logger
    
    def get_indexes(self, schema: str, table: str) -> List[Dict]:
        """Get all indexes for a table"""
        query = """
            SELECT 
                i.indexname,
                pg_get_indexdef(idx.indexrelid) as index_def,
                idx.indisprimary as is_primary,
                idx.indisunique as is_unique
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.tablename
            JOIN pg_namespace n ON n.nspname = i.schemaname AND c.relnamespace = n.oid
            JOIN pg_index idx ON idx.indexrelid = (
                SELECT oid FROM pg_class WHERE relname = i.indexname
                AND relnamespace = n.oid
            )
            WHERE i.schemaname = %s
              AND i.tablename = %s
              AND NOT idx.indisprimary  -- Exclude primary keys for now
            ORDER BY i.indexname
        """
        
        try:
            results = self.pg_manager.execute_query(self.database, query, (schema, table))
            return [
                {
                    'name': row[0],
                    'definition': row[1],
                    'is_primary': row[2],
                    'is_unique': row[3]
                }
                for row in results
            ]
        except Exception as e:
            self.logger.warning(f"Could not retrieve indexes for {schema}.{table}: {e}")
            return []
    
    def get_constraints(self, schema: str, table: str) -> List[Dict]:
        """Get all constraints for a table (excluding primary key)"""
        query = """
            SELECT 
                con.conname as constraint_name,
                con.contype as constraint_type,
                pg_get_constraintdef(con.oid) as constraint_def
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND con.contype != 'p'  -- Exclude primary key
            ORDER BY con.conname
        """
        
        try:
            results = self.pg_manager.execute_query(self.database, query, (schema, table))
            return [
                {
                    'name': row[0],
                    'type': row[1],  # 'f'=foreign key, 'u'=unique, 'c'=check
                    'definition': row[2]
                }
                for row in results
            ]
        except Exception as e:
            self.logger.warning(f"Could not retrieve constraints for {schema}.{table}: {e}")
            return []
    
    def disable_indexes(self, schema: str, table: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Drop indexes and constraints (except primary key) for faster bulk loading
        
        Returns:
            Tuple of (indexes, constraints) that were dropped
        """
        indexes = self.get_indexes(schema, table)
        constraints = self.get_constraints(schema, table)
        
        if not indexes and not constraints:
            self.logger.info(f"No indexes or constraints to disable for {schema}.{table}")
            return ([], [])
        
        conn = self.pg_manager.get_connection(self.database)
        cursor = conn.cursor()
        
        try:
            # Drop constraints first (they may depend on indexes)
            for constraint in constraints:
                drop_sql = f'ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS "{constraint["name"]}" CASCADE'
                self.logger.debug(f"Dropping constraint: {constraint['name']}")
                cursor.execute(drop_sql)
            
            # Drop indexes
            for index in indexes:
                if not index['is_primary']:  # Extra safety check
                    drop_sql = f'DROP INDEX IF EXISTS {schema}."{index["name"]}" CASCADE'
                    self.logger.debug(f"Dropping index: {index['name']}")
                    cursor.execute(drop_sql)
            
            conn.commit()
            
            self.logger.info(
                f"✓ Disabled {len(indexes)} indexes and {len(constraints)} constraints "
                f"on {schema}.{table}"
            )
            
            return (indexes, constraints)
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to disable indexes/constraints for {schema}.{table}: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
    
    def restore_indexes(self, schema: str, table: str, 
                       indexes: List[Dict], constraints: List[Dict]):
        """
        Restore indexes and constraints after bulk loading
        
        Args:
            schema: Schema name
            table: Table name
            indexes: List of index definitions to restore
            constraints: List of constraint definitions to restore
        """
        if not indexes and not constraints:
            self.logger.info(f"No indexes or constraints to restore for {schema}.{table}")
            return
        
        conn = self.pg_manager.get_connection(self.database)
        cursor = conn.cursor()
        
        errors = []
        
        try:
            # Restore indexes first
            for index in indexes:
                try:
                    self.logger.debug(f"Creating index: {index['name']}")
                    cursor.execute(index['definition'])
                except Exception as e:
                    error_msg = f"Failed to create index {index['name']}: {e}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)
            
            # Restore constraints
            for constraint in constraints:
                try:
                    alter_sql = f'ALTER TABLE {schema}.{table} ADD CONSTRAINT "{constraint["name"]}" {constraint["definition"]}'
                    self.logger.debug(f"Creating constraint: {constraint['name']}")
                    cursor.execute(alter_sql)
                except Exception as e:
                    error_msg = f"Failed to create constraint {constraint['name']}: {e}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)
            
            conn.commit()
            
            if errors:
                self.logger.warning(
                    f"⚠ Restored indexes/constraints for {schema}.{table} with {len(errors)} errors"
                )
            else:
                self.logger.info(
                    f"✓ Restored {len(indexes)} indexes and {len(constraints)} constraints "
                    f"on {schema}.{table}"
                )
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to restore indexes/constraints for {schema}.{table}: {e}")
            raise
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)
        
        if errors:
            raise Exception(f"Some indexes/constraints failed to restore: {'; '.join(errors[:3])}")
    
    def analyze_table(self, schema: str, table: str):
        """Run ANALYZE on table to update statistics after bulk load"""
        conn = self.pg_manager.get_connection(self.database)
        cursor = conn.cursor()
        try:
            analyze_sql = f'ANALYZE {schema}.{table}'
            self.logger.debug(f"Analyzing table: {schema}.{table}")
            cursor.execute(analyze_sql)
            conn.commit()
            self.logger.info(f"✓ Analyzed {schema}.{table}")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to analyze {schema}.{table}: {e}")
            # Don't raise - ANALYZE failure shouldn't stop migration
        finally:
            cursor.close()
            self.pg_manager.return_connection(conn)

