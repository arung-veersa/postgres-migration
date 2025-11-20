"""
Mock PostgresConnector for fast unit testing without database.
Simulates database operations with realistic responses.
"""

from typing import Dict, Any, Optional
import pandas as pd


class MockPostgresConnector:
    """
    Mock connector that simulates PostgresConnector behavior.
    Used for fast unit tests without requiring database connection.
    """
    
    def __init__(self, **config):
        """Initialize mock connector with config."""
        self.config = config
        self.schema = config.get('schema', 'public')
        self._connected = True
    
    def test_connection(self) -> bool:
        """Simulate successful connection test."""
        return True
    
    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        """
        Simulate SQL execution.
        Returns realistic row counts based on SQL type.
        """
        sql_lower = sql.lower().strip()
        
        # Simulate different row counts for different operations
        if 'insert' in sql_lower:
            if 'payer_provider_reminders' in sql_lower:
                return 156  # Mock: new reminders inserted
            else:
                return 15234  # Mock: large insert
        
        elif 'update' in sql_lower:
            if 'payer_provider_reminders' in sql_lower:
                return 1203  # Mock: existing reminders updated
            elif 'conflictvisitmaps' in sql_lower and 'updateflag' in sql_lower:
                return 8432  # Mock: rows marked for update
            elif 'conflictvisitmaps' in sql_lower:
                return 8432  # Mock: main update
            elif 'settings' in sql_lower:
                return 1  # Mock: settings update
        
        elif 'truncate' in sql_lower:
            return 0  # TRUNCATE returns 0
        
        elif 'delete' in sql_lower:
            return 50  # Mock: some deletions
        
        else:
            return 0  # Default for other operations
    
    def fetch_dataframe(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Simulate fetching data as DataFrame.
        Returns sample data.
        """
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Sample 1', 'Sample 2', 'Sample 3'],
            'value': [100, 200, 300]
        })
    
    def bulk_insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                             if_exists: str = 'append') -> int:
        """Simulate bulk insert from DataFrame."""
        return len(df)
    
    def table_exists(self, table_name: str) -> bool:
        """Simulate table existence check."""
        return True
    
    def get_row_count(self, table_name: str) -> int:
        """Simulate row count query."""
        return 10000  # Mock count
    
    def truncate_table(self, table_name: str) -> None:
        """Simulate table truncation."""
        pass
    
    def get_connection(self):
        """
        Simulate connection context manager.
        Returns self as a mock connection.
        """
        class MockConnection:
            def __enter__(self):
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                return False
            
            def cursor(self):
                class MockCursor:
                    def __enter__(self):
                        return self
                    
                    def __exit__(self, exc_type, exc_val, exc_tb):
                        return False
                    
                    def execute(self, sql, params=None):
                        pass
                    
                    def fetchone(self):
                        return (1,)
                    
                    def fetchall(self):
                        return [(1, 'test')]
                
                return MockCursor()
        
        return MockConnection()
    
    def __repr__(self):
        return f"MockPostgresConnector(database='{self.config.get('database', 'mock')}')"

