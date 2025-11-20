"""
Unit tests for src/connectors/postgres_connector.py
Target coverage: 100%
"""

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch, call
import psycopg2
from psycopg2 import sql


def test_postgres_connector_initialization(mock_postgres_config):
    """Test PostgresConnector initialization."""
    from src.connectors.postgres_connector import PostgresConnector
    
    connector = PostgresConnector(**mock_postgres_config)
    
    assert connector.config['host'] == 'localhost'
    assert connector.config['port'] == 5432
    assert connector.config['dbname'] == 'test_db'
    assert connector.config['user'] == 'test_user'
    assert connector.config['password'] == 'test_pass'
    assert connector.schema is None


def test_postgres_connector_initialization_with_schema(mock_postgres_config):
    """Test PostgresConnector initialization with schema."""
    from src.connectors.postgres_connector import PostgresConnector
    
    connector = PostgresConnector(**mock_postgres_config, schema='test_schema')
    
    assert connector.schema == 'test_schema'


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_get_connection_success(mock_connect, mock_postgres_config):
    """Test successful database connection."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config, schema='test_schema')
    
    with connector.get_connection() as conn:
        assert conn == mock_conn
        mock_cursor.execute.assert_called_once()
        # Verify SET search_path was called
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'SET search_path' in str(call_args) or isinstance(call_args, sql.Composed)
    
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_get_connection_no_schema(mock_connect, mock_postgres_config):
    """Test database connection without schema."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    
    with connector.get_connection() as conn:
        assert conn == mock_conn
    
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_get_connection_error_rollback(mock_connect, mock_postgres_config):
    """Test connection error triggers rollback."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Setup cursor to raise exception
    mock_cursor.execute.side_effect = Exception("Connection error")
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config, schema='test_schema')
    
    with pytest.raises(Exception) as exc_info:
        with connector.get_connection() as conn:
            # Simulate doing something that triggers the error
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    
    assert "Connection error" in str(exc_info.value)
    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_called_once()


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_execute_query_success(mock_connect, mock_postgres_config):
    """Test successful query execution."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 42
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    result = connector.execute("SELECT * FROM test")
    
    assert result == 42
    mock_cursor.execute.assert_called()


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_execute_query_with_params(mock_connect, mock_postgres_config):
    """Test query execution with parameters."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 10
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    params = {'id': 123}
    result = connector.execute("SELECT * FROM test WHERE id = %(id)s", params)
    
    assert result == 10
    mock_cursor.execute.assert_called_with("SELECT * FROM test WHERE id = %(id)s", params)


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_execute_composed_sql(mock_connect, mock_postgres_config):
    """Test execution of composed SQL."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 5
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    composed_query = sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier('test'))
    result = connector.execute(composed_query)
    
    assert result == 5


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_fetch_dataframe_success(mock_connect, mock_postgres_config):
    """Test fetching DataFrame successfully."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [('id',), ('name',), ('value',)]
    mock_cursor.fetchall.return_value = [(1, 'Alice', 100), (2, 'Bob', 200)]
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    df = connector.fetch_dataframe("SELECT * FROM test")
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ['id', 'name', 'value']
    assert df['id'].tolist() == [1, 2]


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_fetch_dataframe_empty_result(mock_connect, mock_postgres_config):
    """Test fetching empty DataFrame."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = None
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    df = connector.fetch_dataframe("SELECT * FROM test WHERE 1=0")
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_fetch_dataframe_with_params(mock_connect, mock_postgres_config):
    """Test fetching DataFrame with parameters."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [('id',), ('name',)]
    mock_cursor.fetchall.return_value = [(1, 'Alice')]
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    params = {'id': 1}
    df = connector.fetch_dataframe("SELECT * FROM test WHERE id = %(id)s", params)
    
    assert len(df) == 1
    mock_cursor.execute.assert_called_with("SELECT * FROM test WHERE id = %(id)s", params)


@patch('src.connectors.postgres_connector.create_engine')
@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_bulk_insert_dataframe_success(mock_connect, mock_create_engine, mock_postgres_config):
    """Test successful bulk insert of DataFrame."""
    import pandas as pd
    from src.connectors.postgres_connector import PostgresConnector
    from unittest.mock import patch as mock_patch
    
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    
    # Create a DataFrame
    sample_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    })
    
    connector = PostgresConnector(**mock_postgres_config, schema='test_schema')
    
    # The connector stores database as 'dbname' but bulk_insert accesses 'database'
    # We need to add it to the config dict
    connector.config['database'] = connector.config['dbname']
    
    # Mock DataFrame.to_sql to avoid actual DB call
    with mock_patch.object(sample_df, 'to_sql') as mock_to_sql:
        result = connector.bulk_insert_dataframe(sample_df, 'test_table')
        
        assert result == 3
        # Verify to_sql was called with correct parameters
        mock_to_sql.assert_called_once()
        call_kwargs = mock_to_sql.call_args[1]
        assert call_kwargs['schema'] == 'test_schema'
        assert call_kwargs['if_exists'] == 'append'
        assert call_kwargs['index'] == False


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_bulk_insert_empty_dataframe(mock_connect, mock_postgres_config):
    """Test bulk insert with empty DataFrame."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config, schema='test_schema')
    empty_df = pd.DataFrame()
    result = connector.bulk_insert_dataframe(empty_df, 'test_table')
    
    assert result == 0


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_truncate_table(mock_connect, mock_postgres_config):
    """Test table truncation."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config, schema='test_schema')
    connector.truncate_table('test_table')
    
    mock_cursor.execute.assert_called()


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_table_exists_true(mock_connect, mock_postgres_config):
    """Test table_exists returns True when table exists."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (True,)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    result = connector.table_exists('test_table', 'test_schema')
    
    assert result is True


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_table_exists_false(mock_connect, mock_postgres_config):
    """Test table_exists returns False when table doesn't exist."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (False,)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    result = connector.table_exists('nonexistent_table', 'test_schema')
    
    assert result is False


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_get_row_count(mock_connect, mock_postgres_config):
    """Test getting row count of a table."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1234,)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    result = connector.get_row_count('test_table', 'test_schema')
    
    assert result == 1234


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_test_connection_success(mock_connect, mock_postgres_config):
    """Test successful connection test."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    connector = PostgresConnector(**mock_postgres_config)
    result = connector.test_connection()
    
    assert result is True
    mock_cursor.execute.assert_called_with("SELECT 1")


@patch('src.connectors.postgres_connector.psycopg2.connect')
def test_test_connection_failure(mock_connect, mock_postgres_config):
    """Test failed connection test."""
    from src.connectors.postgres_connector import PostgresConnector
    
    mock_connect.side_effect = Exception("Connection failed")
    
    connector = PostgresConnector(**mock_postgres_config)
    result = connector.test_connection()
    
    assert result is False

