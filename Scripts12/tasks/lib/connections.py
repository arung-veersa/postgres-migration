"""
Database Connection Management for Task 02 Conflict Updater
Manages connections to Snowflake and PostgreSQL databases
"""

import os
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager

import snowflake.connector
from snowflake.connector import SnowflakeConnection
import psycopg2
from psycopg2.extras import execute_batch
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from .utils import get_logger

logger = get_logger(__name__)


class SnowflakeConnectionManager:
    """Manages Snowflake database connections for conflict detection queries"""
    
    def __init__(self, config: Dict[str, Any]):
        self.account = config['account']
        self.user = config['user']
        self.warehouse = config['warehouse']
        self.rsa_key = config['rsa_key']
        self.analytics_database = config.get('analytics_database', 'ANALYTICS_SANDBOX')
        self.analytics_schema = config.get('analytics_schema', 'BI')
        self.connection: Optional[SnowflakeConnection] = None
        self.logger = logger
    
    @staticmethod
    def get_private_key(rsa_key_path_or_content: str) -> bytes:
        """
        Convert RSA key (from file or string) to private key bytes
        
        Args:
            rsa_key_path_or_content: File path or raw key content
        
        Returns:
            Private key bytes in DER format
        """
        # Check if it's a file path
        if os.path.isfile(rsa_key_path_or_content):
            with open(rsa_key_path_or_content, 'r') as key_file:
                rsa_key = key_file.read()
        else:
            rsa_key = rsa_key_path_or_content
        
        # Handle literal \n characters (common copy-paste issue)
        rsa_key_cleaned = rsa_key.replace('\\n', '\n')
        
        # Add BEGIN/END wrappers if not present
        if not rsa_key_cleaned.strip().startswith('-----BEGIN'):
            private_key_prefix = '-----BEGIN PRIVATE KEY-----\n'
            private_key_suffix = '\n-----END PRIVATE KEY-----'
            rsa_key_cleaned = private_key_prefix + rsa_key_cleaned.strip() + private_key_suffix
        
        # Load and convert to DER format for Snowflake
        p_key = serialization.load_pem_private_key(
            rsa_key_cleaned.encode(),
            password=None,
            backend=default_backend()
        )
        
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    
    def connect(self) -> SnowflakeConnection:
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                private_key=self.get_private_key(self.rsa_key),
                warehouse=self.warehouse,
                database=self.analytics_database,
                schema=self.analytics_schema,
                # Disable OCSP checks for Lambda VPC environments
                insecure_mode=True,
                session_parameters={
                    'QUERY_TAG': 'task02-conflict-update',
                    # Optimize for streaming results
                    'CLIENT_RESULT_PREFETCH_SLOTS': 0,
                    'CLIENT_RESULT_PREFETCH_THREADS': 1,
                }
            )
            
            self.logger.info(f"✓ Connected to Snowflake account: {self.account}")
            return self.connection
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def get_connection(self) -> SnowflakeConnection:
        """Get existing or create new connection"""
        if self.connection is None or self.connection.is_closed():
            return self.connect()
        return self.connection
    
    def close(self):
        """Close Snowflake connection"""
        if self.connection and not self.connection.is_closed():
            self.connection.close()
            self.logger.info("✓ Snowflake connection closed")
    
    @contextmanager
    def streaming_cursor(self):
        """
        Context manager for streaming cursor
        Use this to iterate large result sets without loading all into memory
        """
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Tuple]:
        """
        Execute query and return results
        
        Args:
            query: SQL query
            params: Query parameters
        
        Returns:
            List of result tuples
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return results
        except Exception as e:
            self.logger.error(f"Error executing Snowflake query: {e}")
            raise
        finally:
            cursor.close()


class PostgresConnectionManager:
    """Manages PostgreSQL database connections for conflict updates"""
    
    def __init__(self, config: Dict[str, Any]):
        self.host = config['host']
        self.user = config['user']
        self.password = config['password']
        self.port = config.get('port', 5432)
        self.conflict_database = config.get('conflict_database', 'CONFLICTREPORT_SANDBOX')
        self.conflict_schema = config.get('conflict_schema', 'PUBLIC')
        self.logger = logger
    
    def get_connection(self, database: Optional[str] = None):
        """
        Get connection to specified database
        
        Args:
            database: Database name (defaults to conflict_database from config)
        
        Returns:
            psycopg2 connection
        """
        db_name = database or self.conflict_database
        
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=db_name,
                # Optimize for updates
                options='-c synchronous_commit=off'
            )
            
            # Apply session-level optimizations
            cursor = conn.cursor()
            try:
                cursor.execute("SET work_mem = '256MB'")
                cursor.execute("SET maintenance_work_mem = '512MB'")
                conn.commit()
            except Exception as e:
                self.logger.warning(f"Could not set all session parameters: {e}")
            finally:
                cursor.close()
            
            return conn
            
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[tuple] = None, 
                     database: Optional[str] = None) -> List[Tuple]:
        """
        Execute query and return results
        
        Args:
            query: SQL query
            params: Query parameters
            database: Database name
        
        Returns:
            List of result tuples
        """
        conn = self.get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            conn.commit()
            return results
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    
    def execute_batch_update(self, update_statements: List[Tuple[str, tuple]], 
                            database: Optional[str] = None, batch_size: int = 1000):
        """
        Execute batch UPDATE statements efficiently
        
        Args:
            update_statements: List of (sql, params) tuples
            database: Database name
            batch_size: Number of statements per batch
        
        Returns:
            Number of rows updated
        """
        if not update_statements:
            return 0
        
        conn = self.get_connection(database)
        cursor = conn.cursor()
        total_updated = 0
        
        try:
            # Group by SQL statement for efficient batching
            grouped = {}
            for sql, params in update_statements:
                if sql not in grouped:
                    grouped[sql] = []
                grouped[sql].append(params)
            
            # Execute each group in batches
            for sql, params_list in grouped.items():
                execute_batch(cursor, sql, params_list, page_size=batch_size)
                total_updated += cursor.rowcount
            
            conn.commit()
            self.logger.info(f"✓ Batch update completed: {total_updated} rows updated")
            return total_updated
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Batch update failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def fetch_reference_data(self, table_name: str, columns: str = '*', 
                           where_clause: str = '1=1', 
                           database: Optional[str] = None) -> List[Tuple]:
        """
        Fetch reference data from Postgres tables
        
        Args:
            table_name: Table name
            columns: Column list (default: *)
            where_clause: WHERE condition
            database: Database name
        
        Returns:
            List of result tuples
        """
        schema = self.conflict_schema
        query = f"""
            SELECT {columns}
            FROM {schema}.{table_name}
            WHERE {where_clause}
        """
        
        return self.execute_query(query, database=database)
    
    def close_all(self):
        """Close all connections"""
        self.logger.info("✓ PostgreSQL connection manager closed")


class ConnectionFactory:
    """Factory for creating database connection managers"""
    
    def __init__(self, snowflake_config: Dict[str, Any], postgres_config: Dict[str, Any]):
        self.snowflake_config = snowflake_config
        self.postgres_config = postgres_config
        self.snowflake_manager: Optional[SnowflakeConnectionManager] = None
        self.postgres_manager: Optional[PostgresConnectionManager] = None
    
    def get_snowflake_manager(self) -> SnowflakeConnectionManager:
        """Get or create Snowflake connection manager"""
        if self.snowflake_manager is None:
            self.snowflake_manager = SnowflakeConnectionManager(self.snowflake_config)
            self.snowflake_manager.connect()
        return self.snowflake_manager
    
    def get_postgres_manager(self) -> PostgresConnectionManager:
        """Get or create PostgreSQL connection manager"""
        if self.postgres_manager is None:
            self.postgres_manager = PostgresConnectionManager(self.postgres_config)
        return self.postgres_manager
    
    def close_all(self):
        """Close all connections"""
        if self.snowflake_manager:
            self.snowflake_manager.close()
        if self.postgres_manager:
            self.postgres_manager.close_all()
