"""
Shared test fixtures for the test suite.
"""

import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock
import psycopg2


@pytest.fixture
def mock_postgres_config():
    """Provide mock Postgres configuration."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }


@pytest.fixture
def mock_connection():
    """Provide a mock Postgres connection."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = [(1, 'test')]
    cursor.description = [('id',), ('name',)]
    cursor.rowcount = 10
    conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    return conn


@pytest.fixture
def mock_postgres_connector(mock_postgres_config, mock_connection):
    """Provide a mock PostgresConnector instance."""
    from unittest.mock import MagicMock, Mock, PropertyMock
    
    # Create a fully mocked connector
    connector = MagicMock()
    
    # Setup config attribute
    type(connector).config = PropertyMock(return_value={
        'host': mock_postgres_config['host'],
        'port': mock_postgres_config['port'],
        'dbname': mock_postgres_config['database'],
        'database': mock_postgres_config['database'],
        'user': mock_postgres_config['user'],
        'password': mock_postgres_config['password']
    })
    
    connector.schema = None
    
    # Setup connection context manager
    connector.get_connection = MagicMock()
    connector.get_connection.return_value.__enter__ = Mock(return_value=mock_connection)
    connector.get_connection.return_value.__exit__ = Mock(return_value=False)
    
    # Setup methods
    connector.execute = MagicMock(return_value=10)
    connector.test_connection = MagicMock(return_value=True)
    connector.table_exists = MagicMock(return_value=True)
    connector.get_row_count = MagicMock(return_value=100)
    
    return connector


@pytest.fixture
def temp_log_dir():
    """Create a temporary log directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_dir = Path(tmpdir) / 'logs'
        log_dir.mkdir(exist_ok=True)
        yield log_dir


@pytest.fixture
def temp_sql_file():
    """Create a temporary SQL file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("""
            -- Test SQL script
            SELECT * FROM {conflict_schema}.test_table;
            UPDATE {analytics_schema}.analytics_table SET status = 'processed';
        """)
        f.flush()
        yield Path(f.name)
        
    # Cleanup
    os.unlink(f.name)


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables."""
    monkeypatch.setenv('POSTGRES_HOST', 'localhost')
    monkeypatch.setenv('POSTGRES_PORT', '5432')
    monkeypatch.setenv('POSTGRES_DATABASE', 'test_db')
    monkeypatch.setenv('POSTGRES_USER', 'test_user')
    monkeypatch.setenv('POSTGRES_PASSWORD', 'test_pass')
    monkeypatch.setenv('POSTGRES_CONFLICT_SCHEMA', 'conflict')
    monkeypatch.setenv('POSTGRES_ANALYTICS_SCHEMA', 'analytics')
    monkeypatch.setenv('ENVIRONMENT', 'test')
    monkeypatch.setenv('LOG_LEVEL', 'DEBUG')


@pytest.fixture
def sample_dataframe():
    """Provide a sample pandas DataFrame."""
    import pandas as pd
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    })


@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging configuration between tests."""
    import logging
    # Clear all handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Reset logger state
    logging.root.setLevel(logging.WARNING)
    
    yield
    
    # Cleanup after test
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)


@pytest.fixture
def clean_settings_module():
    """Reset the settings module to clear cached values.
    
    Use this fixture explicitly in tests that need fresh module imports.
    Not autouse to avoid interfering with other tests.
    """
    import sys
    # Remove settings from cache if it exists
    if 'config.settings' in sys.modules:
        del sys.modules['config.settings']
    
    yield
    
    # Clean up after test
    if 'config.settings' in sys.modules:
        del sys.modules['config.settings']

