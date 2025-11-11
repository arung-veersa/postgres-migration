"""
Unit tests for database connectors.

Tests connector initialization, connection handling, and basic operations.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from src.connectors.snowflake_connector import SnowflakeConnector
from src.connectors.postgres_connector import PostgresConnector


class TestSnowflakeConnector:
    """Test Snowflake connector."""
    
    def test_init(self):
        """Test connector initialization."""
        connector = SnowflakeConnector(
            account='test_account',
            user='test_user',
            password='test_pass',
            warehouse='TEST_WH',
            database='ANALYTICS',
            schema='BI',
            role='TEST_ROLE'
        )
        
        assert connector.config['account'] == 'test_account'
        assert connector.config['database'] == 'ANALYTICS'
        assert connector.config['schema'] == 'BI'
        assert connector.config['role'] == 'TEST_ROLE'
        assert connector.config['client_session_keep_alive'] is True
    
    @patch('src.connectors.snowflake_connector.snowflake.connector.connect')
    def test_fetch_dataframe(self, mock_connect):
        """Test fetching data as DataFrame."""
        # Setup mock
        mock_cursor = Mock()
        mock_cursor.fetch_pandas_all.return_value = pd.DataFrame({'col1': [1, 2, 3]})
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Test
        connector = SnowflakeConnector(
            account='test', user='test', password='test',
            warehouse='TEST', database='TEST', schema='TEST'
        )
        
        df = connector.fetch_dataframe("SELECT * FROM table")
        
        assert len(df) == 3
        assert 'col1' in df.columns
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('src.connectors.snowflake_connector.snowflake.connector.connect')
    def test_fetch_batches(self, mock_connect):
        """Test fetching data in batches."""
        # Setup mock
        mock_cursor = Mock()
        mock_cursor.fetchmany.side_effect = [
            [(1, 'a'), (2, 'b')],  # First batch
            [(3, 'c'), (4, 'd')],  # Second batch
            []  # End
        ]
        mock_cursor.description = [('id',), ('name',)]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Test
        connector = SnowflakeConnector(
            account='test', user='test', password='test',
            warehouse='TEST', database='TEST', schema='TEST'
        )
        
        batches = list(connector.fetch_batches("SELECT * FROM table", batch_size=2))
        
        assert len(batches) == 2
        assert len(batches[0]) == 2
        assert len(batches[1]) == 2


class TestPostgresConnector:
    """Test Postgres connector."""
    
    def test_init(self):
        """Test connector initialization."""
        connector = PostgresConnector(
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser',
            password='testpass'
        )
        
        assert connector.config['host'] == 'localhost'
        assert connector.config['port'] == 5432
        assert connector.config['database'] == 'testdb'
        assert connector.config['user'] == 'testuser'
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_execute(self, mock_connect):
        """Test executing a query."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Test
        connector = PostgresConnector(
            host='localhost', port=5432, database='test',
            user='test', password='test'
        )
        
        rowcount = connector.execute("DELETE FROM table WHERE id = 1")
        
        assert rowcount == 5
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_truncate_table(self, mock_connect):
        """Test table truncation."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Test
        connector = PostgresConnector(
            host='localhost', port=5432, database='test',
            user='test', password='test'
        )
        
        connector.truncate_table('test_table')
        
        # Verify TRUNCATE query was executed
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'TRUNCATE' in call_args
        assert 'test_table' in call_args
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_table_exists(self, mock_connect):
        """Test checking if table exists."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Test
        connector = PostgresConnector(
            host='localhost', port=5432, database='test',
            user='test', password='test'
        )
        
        exists = connector.table_exists('test_table')
        
        assert exists is True
        mock_cursor.execute.assert_called_once()
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_get_row_count(self, mock_connect):
        """Test getting row count."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (100,)
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Test
        connector = PostgresConnector(
            host='localhost', port=5432, database='test',
            user='test', password='test'
        )
        
        count = connector.get_row_count('test_table')
        
        assert count == 100

