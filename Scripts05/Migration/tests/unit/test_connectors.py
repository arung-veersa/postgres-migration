"""
Unit tests for database connectors.

Tests connector initialization, connection handling, and basic operations.
"""

import os
import sys
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

# Ensure project root is on sys.path for direct test runs
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

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
    
    def test_init_requires_auth(self):
        """Ensure either password or private_key is required."""
        with pytest.raises(ValueError):
            SnowflakeConnector(
                account='acct', user='user',
                warehouse='WH', database='DB', schema='SC'
            )
    
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
    
    @patch('src.connectors.snowflake_connector.snowflake.connector.connect')
    def test_execute(self, mock_connect):
        """Test execute returns affected rows."""
        mock_cursor = Mock()
        mock_cursor.rowcount = 7
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        connector = SnowflakeConnector(
            account='acct', user='user', password='pw',
            warehouse='WH', database='DB', schema='SC'
        )
        rc = connector.execute("DELETE FROM t")
        assert rc == 7
        mock_cursor.execute.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('src.connectors.snowflake_connector.snowflake.connector.connect')
    def test_test_connection_success(self, mock_connect):
        """Test test_connection returns True on success."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ('9.35.2',)
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        connector = SnowflakeConnector(
            account='acct', user='user', password='pw',
            warehouse='WH', database='DB', schema='SC'
        )
        ok = connector.test_connection()
        assert ok is True
        mock_cursor.execute.assert_called_once()
        mock_conn.close.assert_called_once()


class TestPostgresConnector:
    """Test Postgres connector."""
    
    def test_init(self):
        """Test connector initialization."""
        connector = PostgresConnector(
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser',
            password='testpass',
            schema='public'
        )
        
        assert connector.config['host'] == 'localhost'
        assert connector.config['port'] == 5432
        assert connector.config['database'] == 'testdb'
        assert connector.config['user'] == 'testuser'
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_execute(self, mock_connect):
        """Test executing a query."""
        # Setup mock
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value.rowcount = 5
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn
        
        # Test
        connector = PostgresConnector(
            host='localhost', port=5432, database='test',
            user='test', password='test', schema='public'
        )
        
        rowcount = connector.execute("DELETE FROM table WHERE id = 1")
        
        assert rowcount == 5
        cursor_cm.__enter__.return_value.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_truncate_table(self, mock_connect):
        """Test table truncation."""
        # Setup mock
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn
        
        # Test
        connector = PostgresConnector(
            host='localhost', port=5432, database='test',
            user='test', password='test', schema='public'
        )
        
        connector.truncate_table('test_table')
        
        # Verify TRUNCATE query was executed
        call_arg = cursor_cm.__enter__.return_value.execute.call_args[0][0]
        # call_arg is a psycopg2.sql.Composed; validate via string representation
        s = str(call_arg)
        assert 'TRUNCATE' in s
        assert 'test_table' in s
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_table_exists(self, mock_connect):
        """Test checking if table exists."""
        # Setup mock
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value.fetchone.return_value = (True,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn
        
        # Test
        connector = PostgresConnector(
            host='localhost', port=5432, database='test',
            user='test', password='test', schema='public'
        )
        
        exists = connector.table_exists('test_table', 'public')
        
        assert exists is True
        cursor_cm.__enter__.return_value.execute.assert_called_once()
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_get_row_count(self, mock_connect):
        """Test getting row count."""
        # Setup mock
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value.fetchone.return_value = (100,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn
        
        # Test
        connector = PostgresConnector(
            host='localhost', port=5432, database='test',
            user='test', password='test', schema='public'
        )
        
        count = connector.get_row_count('test_table', 'public')
        
        assert count == 100
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_fetch_dataframe(self, mock_connect):
        """Test fetch_dataframe returns a DataFrame with columns."""
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value.description = [('id',), ('name',)]
        cursor_cm.__enter__.return_value.fetchall.return_value = [(1, 'a'), (2, 'b')]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        connector = PostgresConnector(
            host='localhost', port=5432, database='db',
            user='u', password='p', schema='public'
        )
        df = connector.fetch_dataframe("SELECT id, name FROM t")
        assert list(df.columns) == ['id', 'name']
        assert len(df) == 2
    
    @patch('src.connectors.postgres_connector.create_engine')
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_bulk_insert_dataframe(self, mock_connect, mock_engine):
        """Test bulk_insert_dataframe uses to_sql and returns count."""
        # Prepare connection context manager
        cursor_cm = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        # Mock to_sql on DataFrame
        df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        with patch.object(pd.DataFrame, 'to_sql', autospec=True) as to_sql_mock:
            connector = PostgresConnector(
                host='localhost', port=5432, database='db',
                user='u', password='p', schema='public'
            )
            inserted = connector.bulk_insert_dataframe(df, 'tbl')
            assert inserted == 2
            to_sql_mock.assert_called_once()
    
    @patch('src.connectors.postgres_connector.psycopg2.connect')
    def test_test_connection_success(self, mock_connect):
        """Test Postgres test_connection returns True."""
        mock_conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = cursor
        mock_connect.return_value = mock_conn

        connector = PostgresConnector(
            host='localhost', port=5432, database='db',
            user='u', password='p', schema='public'
        )
        ok = connector.test_connection()
        assert ok is True
        mock_conn.close.assert_called_once()

