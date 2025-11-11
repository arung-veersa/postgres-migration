"""
Postgres database connector.
Handles connections and data operations for ConflictReport database.
"""

import pandas as pd
from contextlib import contextmanager
from typing import Optional, Dict, Any
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PostgresConnector:
    """
    Manages Postgres connections with transaction support.
    Read/Write access to ConflictReport database.
    """
    
    def __init__(self, host: str, port: int, database: str,
                 user: str, password: str, schema: str):
        """
        Initialize Postgres connector.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Username
            password: Password
            schema: Database schema
        """
        self.config = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password,
            'options': f'-c search_path={schema}'
        }
        self.schema = schema
        
        logger.info(
            f"Postgres connector initialized: "
            f"{database} @ {host}:{port} (Schema: {schema})"
        )
    
    @contextmanager
    def get_connection(self, autocommit: bool = False):
        """
        Context manager for Postgres connection.
        
        Args:
            autocommit: If True, enable autocommit mode
        """
        conn = None
        try:
            logger.debug("Opening Postgres connection")
            conn = psycopg2.connect(**self.config)
            
            if autocommit:
                conn.autocommit = True
            
            yield conn
            
            if not autocommit:
                conn.commit()
                logger.debug("Transaction committed")
                
        except Exception as e:
            if conn and not autocommit:
                conn.rollback()
                logger.warning("Transaction rolled back")
            logger.error(f"Postgres error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Postgres connection closed")
    
    def execute(self, query: str, 
                params: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute a query and return rows affected.
        
        Args:
            query: SQL query
            params: Optional parameters
            
        Returns:
            Number of rows affected
        """
        if isinstance(query, sql.Composed):
            log_query = query.as_string(psycopg2.connect(**self.config))
            logger.debug(f"Executing: {log_query[:100]}...")
        else:
            logger.debug(f"Executing: {query[:100]}...")
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})
                rowcount = cursor.rowcount
                logger.info(f"Query affected {rowcount} rows")
                return rowcount
    
    def fetch_dataframe(self, query: Any,
                       params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Fetch query results as DataFrame.
        
        Args:
            query: SQL query (string or psycopg2.sql object)
            params: Optional parameters
            
        Returns:
            DataFrame with results
        """
        if isinstance(query, sql.Composed):
            log_query = query.as_string(psycopg2.connect(**self.config))
            logger.debug(f"Fetching DataFrame: {log_query[:150]}...")
        else:
            logger.debug(f"Fetching DataFrame: {query[:150]}...")
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(cursor.fetchall(), columns=columns)
                return pd.DataFrame()
    
    def bulk_insert_dataframe(self, df: pd.DataFrame, 
                             table_name: str) -> int:
        """
        Bulk insert DataFrame using COPY command for speed.
        
        Args:
            df: DataFrame to insert
            table_name: Target table
            
        Returns:
            Number of rows inserted
        """
        if df.empty:
            logger.warning(f"Empty DataFrame for {self.schema}.{table_name}")
            return 0
        
        logger.info(
            f"Bulk inserting {len(df)} rows to {self.schema}.{table_name}"
        )
        
        # Use pandas to_sql with multi method for better performance
        with self.get_connection() as conn:
            # Create engine from connection
            from sqlalchemy import create_engine
            engine = create_engine(
                f"postgresql://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            
            df.to_sql(
                table_name,
                engine,
                schema=self.schema,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=5000
            )
            
            logger.info(f"Inserted {len(df)} rows")
            return len(df)
    
    def truncate_table(self, table_name: str) -> None:
        """
        Truncate a table.
        
        Args:
            table_name: Table to truncate
        """
        logger.info(f"Truncating {self.schema}.{table_name}")
        
        query = sql.SQL('TRUNCATE TABLE {}.{}').format(
            sql.Identifier(self.schema),
            sql.Identifier(table_name)
        )
        self.execute(query)
    
    def table_exists(self, table_name: str, schema: str) -> bool:
        """
        Check if table exists.
        
        Args:
            table_name: Table name
            
        Returns:
            True if table exists
        """
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s
                AND lower(table_name) = lower(%s)
            )
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (schema, table_name))
                return cursor.fetchone()[0]
    
    def get_row_count(self, table_name: str, schema: str) -> int:
        """
        Get row count of a table.
        
        Args:
            table_name: Table name
            
        Returns:
            Number of rows
        """
        query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone()[0]
    
    def test_connection(self) -> bool:
        """
        Test the Postgres connection.
        
        Returns:
            True if connection successful
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                logger.info("Postgres connection successful")
                return True
        except Exception as e:
            logger.error(f"Postgres connection failed: {str(e)}")
            return False

