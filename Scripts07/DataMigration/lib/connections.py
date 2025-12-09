"""
Database Connection Management Module
Manages connections to Snowflake and PostgreSQL databases
"""

import os
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager

import snowflake.connector
from snowflake.connector import SnowflakeConnection
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from .utils import quote_identifier, logger


class SnowflakeConnectionManager:
    """Manages Snowflake database connections"""
    
    def __init__(self, config: Dict[str, Any]):
        self.account = config['account']
        self.user = config['user']
        self.warehouse = config['warehouse']
        self.rsa_key = config['rsa_key']
        self.connection: Optional[SnowflakeConnection] = None
        self.logger = logger
    
    @staticmethod
    def get_private_key(rsa_key_path_or_content):
        """
        Convert RSA key (from file or string) to private key bytes.
        Handles both file paths and direct key content.
        """
        # Check if it's a file path
        if os.path.isfile(rsa_key_path_or_content):
            with open(rsa_key_path_or_content, 'r') as key_file:
                rsa_key = key_file.read()
        else:
            rsa_key = rsa_key_path_or_content
        
        # Handle literal \n characters in the key (common copy-paste issue)
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
                # Disable OCSP checks to avoid SSL certificate validation issues in Lambda
                # When Lambda is in a VPC, OCSP checks to external CRL servers may fail
                insecure_mode=True,  # Disable SSL certificate verification completely
                session_parameters={
                    'QUERY_TAG': 'postgres-migration',
                    # Force client-side result handling instead of S3 staging
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
    
    def get_connection_info(self) -> str:
        """Get connection information for logging/display"""
        return f"{self.account}"
    
    def close(self):
        """Close Snowflake connection"""
        if self.connection and not self.connection.is_closed():
            self.connection.close()
            self.logger.info("✓ Snowflake connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """Execute query and return results"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def fetch_dataframe(self, query: str):
        """Execute query and return results as list of tuples (no pandas needed)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            # Use fetchall() instead of fetch_pandas_all() to avoid pandas dependency
            rows = cursor.fetchall()
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            # Return as dict with data and columns for DataFrame-like usage
            return {'data': rows, 'columns': columns}
        finally:
            cursor.close()
    
    def get_row_count(self, database: str, schema: str, table: str, where_clause: str = "1=1") -> int:
        """Get row count for a table"""
        query = f"""
            SELECT COUNT(*) as cnt
            FROM {database}.{schema}.{table}
            WHERE {where_clause}
        """
        result = self.execute_query(query)
        return result[0][0] if result else 0
    
    def get_column_info(self, database: str, schema: str, table: str) -> list:
        """Get column information for a table"""
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query)
    
    @contextmanager
    def cursor(self):
        """Context manager for cursor"""
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()


class PostgresConnectionManager:
    """Manages PostgreSQL database connections with connection pooling"""
    
    def __init__(self, config: Dict[str, Any], min_connections: int = 2, max_connections: int = 10):
        self.host = config['host']
        self.user = config['user']
        self.password = config['password']
        self.port = config.get('port', 5432)
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connection_pool: Optional[pool.ThreadedConnectionPool] = None
        self.logger = logger
    
    def create_pool(self, database: Optional[str] = None):
        """Create connection pool for a specific database"""
        # Don't create a global pool - we'll create per-database connections
        # This is intentionally left simple to avoid pool management complexity
        self.logger.info(
            f"✓ PostgreSQL connection manager initialized "
            f"(will create connections per database as needed)"
        )
    
    def get_connection(self, database: Optional[str] = None):
        """Get connection to specified database"""
        if not database:
            raise ValueError("Database name is required for PostgreSQL connection")
        
        # Create direct connection (no pooling complexity)
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=database
        )
        return conn
    
    def return_connection(self, conn):
        """Close connection (no pooling)"""
        if conn and not conn.closed:
            conn.close()
    
    def close_all(self):
        """Close all connections (no pool to close)"""
        self.logger.info("✓ PostgreSQL connections will be closed per-use")
    
    def execute_query(self, database: str, query: str, params: Optional[tuple] = None, fetch: bool = True):
        """Execute query on specified database"""
        conn = self.get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            conn.commit()
            return None
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
            self.return_connection(conn)
    
    def get_row_count(self, database: str, schema: str, table: str, where_clause: str = "1=1") -> int:
        """Get row count for a table"""
        query = f"""
            SELECT COUNT(*) as cnt
            FROM {schema}.{table}
            WHERE {where_clause}
        """
        result = self.execute_query(database, query)
        return result[0][0] if result else 0
    
    def get_column_info(self, database: str, schema: str, table: str) -> list:
        """Get column information for a table"""
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
        """
        return self.execute_query(database, query, (schema, table))
    
    def table_exists(self, database: str, schema: str, table: str) -> bool:
        """Check if table exists"""
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
            )
        """
        result = self.execute_query(database, query, (schema, table))
        return result[0][0] if result else False
    
    def get_max_watermark(self, database: str, schema: str, table: str, watermark_column: str):
        """Get maximum watermark value from table"""
        query = f"""
            SELECT MAX({quote_identifier(watermark_column)})
            FROM {schema}.{table}
        """
        try:
            result = self.execute_query(database, query)
            return result[0][0] if result and result[0][0] is not None else None
        except Exception as e:
            self.logger.warning(f"Could not get max watermark for {schema}.{table}: {e}")
            return None
    
    def get_max_watermark_for_chunk(self, database: str, schema: str, table: str, 
                                     watermark_column: str, chunk_filter: str):
        """
        Get maximum watermark value for a specific chunk
        
        Args:
            database: Target database name
            schema: Target schema name
            table: Target table name
            watermark_column: Watermark column name
            chunk_filter: WHERE clause filter for the chunk (without 'WHERE' keyword)
        
        Returns:
            Maximum watermark value for the chunk, or None if no data
        """
        query = f"""
            SELECT MAX({quote_identifier(watermark_column)})
            FROM {schema}.{table}
            WHERE {chunk_filter}
        """
        try:
            result = self.execute_query(database, query)
            return result[0][0] if result and result[0][0] is not None else None
        except Exception as e:
            self.logger.warning(f"Could not get max watermark for {schema}.{table} with filter: {e}")
            return None
    
    def initialize_status_schema(self, database: str, schema_file: str = "schema.sql"):
        """Initialize migration status tracking tables"""
        try:
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            conn = self.get_connection(database)
            cursor = conn.cursor()
            try:
                cursor.execute(schema_sql)
                conn.commit()
                self.logger.info(f"✓ Migration status schema initialized in {database}")
            finally:
                cursor.close()
                self.return_connection(conn)
                
        except Exception as e:
            self.logger.error(f"Failed to initialize status schema: {e}")
            raise
    
    @contextmanager
    def transaction(self, database: str):
        """Context manager for transaction"""
        conn = self.get_connection(database)
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            self.return_connection(conn)


class ConnectionFactory:
    """Factory for creating database connection managers"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.snowflake_manager: Optional[SnowflakeConnectionManager] = None
        self.postgres_manager: Optional[PostgresConnectionManager] = None
    
    def get_snowflake_manager(self) -> SnowflakeConnectionManager:
        """Get or create Snowflake connection manager"""
        if self.snowflake_manager is None:
            self.snowflake_manager = SnowflakeConnectionManager(self.config['snowflake'])
            self.snowflake_manager.connect()
        return self.snowflake_manager
    
    def get_postgres_manager(self) -> PostgresConnectionManager:
        """Get or create PostgreSQL connection manager"""
        if self.postgres_manager is None:
            self.postgres_manager = PostgresConnectionManager(self.config['postgres'])
            self.postgres_manager.create_pool()
        return self.postgres_manager
    
    def close_all(self):
        """Close all connections"""
        if self.snowflake_manager:
            self.snowflake_manager.close()
        if self.postgres_manager:
            self.postgres_manager.close_all()

