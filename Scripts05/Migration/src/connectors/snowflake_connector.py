"""
Snowflake database connector.
Handles connections and data fetching from Analytics database.
"""

import pandas as pd
from contextlib import contextmanager
from typing import Optional, Dict, Any, Iterator, Union
import snowflake.connector
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SnowflakeConnector:
    """
    Manages Snowflake connections with optimized fetching.
    Read-only access to Analytics database.
    """
    
    def __init__(self, account: str, user: str,
                 warehouse: str, database: str, schema: str,
                 password: Optional[str] = None,
                 private_key: Optional[bytes] = None,
                 role: Optional[str] = None):
        """
        Initialize Snowflake connector.
        
        Args:
            account: Snowflake account identifier
            user: Username
            warehouse: Warehouse name
            database: Database name
            schema: Schema name
            password: Password (if using password auth)
            private_key: RSA private key bytes (if using key-pair auth)
            role: Optional role name
        """
        self.config: Dict[str, Union[str, bool, bytes]] = {
            'account': account,
            'user': user,
            'warehouse': warehouse,
            'database': database,
            'schema': schema,
            'client_session_keep_alive': True,
        }
        
        if password:
            self.config['password'] = password
        elif private_key:
            self.config['private_key'] = private_key
        else:
            raise ValueError("Either password or private_key must be provided.")

        if role:
            self.config['role'] = role
        
        logger.info(
            f"Snowflake connector initialized: "
            f"{database}.{schema} @ {account}"
        )
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for Snowflake connection.
        Ensures connection is properly closed.
        """
        conn = None
        try:
            logger.debug("Opening Snowflake connection")
            conn = snowflake.connector.connect(**self.config)
            yield conn
        except Exception as e:
            logger.error(f"Snowflake connection error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Snowflake connection closed")
    
    def fetch_dataframe(self, query: str, 
                       params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Fetch query results as pandas DataFrame using Arrow format.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            DataFrame with query results
        """
        logger.debug(f"Executing query: {query[:100]}...")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                cursor.execute(query, params or {})
                
                # Use Arrow format for 5-10x faster data transfer
                df = cursor.fetch_pandas_all()
                
                logger.info(f"Fetched {len(df)} rows, {len(df.columns)} columns")
                return df
                
            finally:
                cursor.close()
    
    def fetch_batches(self, query: str, 
                     batch_size: int = 10000) -> Iterator[pd.DataFrame]:
        """
        Generator that yields batches for memory-efficient processing.
        
        Args:
            query: SQL query to execute
            batch_size: Number of rows per batch
            
        Yields:
            DataFrame batches
        """
        logger.debug(f"Fetching in batches of {batch_size}")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                cursor.execute(query)
                
                batch_num = 0
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    
                    # Convert to DataFrame
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(batch, columns=columns)
                    
                    batch_num += 1
                    logger.debug(f"Yielding batch {batch_num}: {len(df)} rows")
                    yield df
                    
            finally:
                cursor.close()
    
    def execute(self, query: str, 
                params: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute a query without fetching results.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Number of rows affected
        """
        logger.debug(f"Executing: {query[:100]}...")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                cursor.execute(query, params or {})
                rowcount = cursor.rowcount
                logger.info(f"Query affected {rowcount} rows")
                return rowcount
                
            finally:
                cursor.close()
    
    def test_connection(self) -> bool:
        """
        Test the Snowflake connection.
        
        Returns:
            True if connection successful
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                cursor.close()
                logger.info(f"Snowflake connection successful. Version: {version}")
                return True
        except Exception as e:
            logger.error(f"Snowflake connection failed: {str(e)}")
            return False

