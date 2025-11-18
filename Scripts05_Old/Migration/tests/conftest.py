"""
Pytest configuration for enforcing coverage thresholds.

Requirements:
- Overall coverage is enforced via pytest.ini (--cov-fail-under=90)
- Per-file coverage: require 100% for src/tasks/task_01_copy_to_temp.py
  Implemented by inspecting coverage.json after the test session.
"""

import os
import json
from typing import Optional

import pytest


def _normalize(path: str) -> str:
    return os.path.normcase(os.path.normpath(path))


def _find_target_file_entry(files_dict: dict, root: str) -> Optional[dict]:
    """
    Find the coverage JSON entry for src/tasks/task_01_copy_to_temp.py,
    tolerating absolute vs relative paths and Windows path separators.
    """
    target_suffix = _normalize(os.path.join("src", "tasks", "task_01_copy_to_temp.py"))
    best = None
    for key, value in files_dict.items():
        key_path = key
        # If key is relative, make it absolute to compare consistently
        if not os.path.isabs(key_path):
            key_path = os.path.join(root, key_path)
        if _normalize(key_path).endswith(target_suffix):
            best = value
            break
    return best


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """
    After the test session, enforce 100% coverage on task_01_copy_to_temp.py.
    """
    root = str(session.config.rootpath)
    cov_json_path = os.path.join(root, "coverage.json")
    if not os.path.exists(cov_json_path):
        # If JSON report is unavailable, do not fail here; overall coverage will still be enforced.
        return

    try:
        with open(cov_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return

    files = data.get("files", {})
    entry = _find_target_file_entry(files, root)
    if not entry:
        # File not included in coverage (e.g., not collected) — enforce failure.
        session.exitstatus = 1
        tr = session.config.pluginmanager.getplugin("terminalreporter")
        if tr:
            tr.write_line(
                "Coverage requirement failed: src/tasks/task_01_copy_to_temp.py was not measured."
            )
        return

    pct = float(entry.get("summary", {}).get("percent_covered", 0.0))
    if pct < 100.0:
        session.exitstatus = 1
        tr = session.config.pluginmanager.getplugin("terminalreporter")
        if tr:
            tr.write_line(
                f"Coverage requirement failed: src/tasks/task_01_copy_to_temp.py {pct:.2f}% < 100.00%"
            )

"""
Pytest configuration and fixtures for test suite.
Provides reusable test fixtures and setup.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
import tempfile
import os


# ============================================================================
# Mock Connector Fixtures
# ============================================================================

@pytest.fixture
def mock_snowflake_connector():
    """Mock Snowflake connector for testing."""
    connector = Mock()
    connector.fetch_dataframe = Mock(return_value=pd.DataFrame())
    connector.fetch_batches = Mock(return_value=iter([]))
    connector.execute = Mock(return_value=0)
    connector.test_connection = Mock(return_value=True)
    return connector


@pytest.fixture
def mock_postgres_connector():
    """Mock Postgres connector for testing."""
    connector = Mock()
    connector.fetch_dataframe = Mock(return_value=pd.DataFrame())
    connector.execute = Mock(return_value=0)
    connector.bulk_insert_dataframe = Mock(return_value=0)
    connector.truncate_table = Mock()
    connector.table_exists = Mock(return_value=True)
    connector.get_row_count = Mock(return_value=0)
    connector.test_connection = Mock(return_value=True)
    return connector


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_payer_provider_relationships():
    """Sample payer-provider relationships from Analytics."""
    return pd.DataFrame({
        'PayerID': ['P001', 'P002', 'P003'],
        'AppPayerID': ['APP001', 'APP002', 'APP003'],
        'Contract': ['Contract A', 'Contract B', 'Contract C'],
        'ProviderID': ['PROV001', 'PROV002', 'PROV003'],
        'AppProviderID': ['APPPROV001', 'APPPROV002', 'APPPROV003'],
        'ProviderName': ['Provider One', 'Provider Two', 'Provider Three']
    })


@pytest.fixture
def sample_existing_reminders():
    """Sample existing reminders in Postgres."""
    return pd.DataFrame({
        'PayerID': ['P001', 'P002'],
        'ProviderID': ['PROV001', 'PROV002'],
    })


@pytest.fixture
def sample_conflict_visit_maps():
    """Sample conflict visit maps data."""
    today = datetime.now().date()
    
    return pd.DataFrame({
        'ID': [1, 2, 3],
        'CONFLICTID': [100, 101, 102],
        'SSN': ['123-45-6789', '987-65-4321', '111-22-3333'],
        'ProviderID': ['PROV001', 'PROV002', 'PROV001'],
        'AppProviderID': ['APP001', 'APP002', 'APP001'],
        'ProviderName': ['Provider One', 'Provider Two', 'Provider One'],
        'VisitID': ['V001', 'V002', 'V003'],
        'AppVisitID': ['AV001', 'AV002', 'AV003'],
        'ConVisitID': ['V002', 'V003', 'V004'],
        'ConAppVisitID': ['AV002', 'AV003', 'AV004'],
        'VisitDate': [today, today - timedelta(days=30), today + timedelta(days=15)],
        'SchStartTime': [
            datetime.combine(today, datetime.min.time().replace(hour=9)),
            datetime.combine(today - timedelta(days=30), datetime.min.time().replace(hour=10)),
            datetime.combine(today + timedelta(days=15), datetime.min.time().replace(hour=14))
        ],
        'SchEndTime': [
            datetime.combine(today, datetime.min.time().replace(hour=17)),
            datetime.combine(today - timedelta(days=30), datetime.min.time().replace(hour=18)),
            datetime.combine(today + timedelta(days=15), datetime.min.time().replace(hour=22))
        ]
    })


@pytest.fixture
def sample_conflicts():
    """Sample conflicts data."""
    return pd.DataFrame({
        'CONFLICTID': [100, 101, 102],
        'StatusFlag': ['U', 'R', 'U'],
        'FlagForReview': ['N', 'Y', 'N'],
        'FlagForReviewDate': [None, datetime.now(), None],
        'ResolveDate': [None, datetime.now(), None],
    })


# ============================================================================
# Utility Fixtures
# ============================================================================

@pytest.fixture
def temp_directory():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def mock_logger():
    """Mock logger to prevent log pollution during tests."""
    return Mock()


@pytest.fixture
def freeze_time():
    """Fixture to freeze time for testing."""
    frozen_time = datetime(2024, 1, 15, 12, 0, 0)
    
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = frozen_time
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        yield frozen_time


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing."""
    env_vars = {
        'ENVIRONMENT': 'test',
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_PASSWORD': 'test_pass',
        'SNOWFLAKE_DATABASE': 'ANALYTICS',
        'SNOWFLAKE_SCHEMA': 'BI',
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DATABASE': 'conflictreport_test',
        'POSTGRES_USER': 'test_user',
        'POSTGRES_PASSWORD': 'test_pass',
        'LOG_LEVEL': 'ERROR',
    }
    
    with patch.dict(os.environ, env_vars, clear=True):
        yield env_vars


# ============================================================================
# Integration Test Fixtures (if needed later)
# ============================================================================

@pytest.fixture(scope='session')
def integration_snowflake_connector():
    """
    Real Snowflake connector for integration tests.
    Only created if SF credentials are available.
    """
    from config.settings import SNOWFLAKE_CONFIG
    from src.connectors.snowflake_connector import SnowflakeConnector
    
    try:
        if all(SNOWFLAKE_CONFIG.values()):
            connector = SnowflakeConnector(**SNOWFLAKE_CONFIG)
            if connector.test_connection():
                yield connector
            else:
                pytest.skip("Snowflake connection test failed")
        else:
            pytest.skip("Snowflake credentials not configured")
    except Exception as e:
        pytest.skip(f"Snowflake not available: {str(e)}")


@pytest.fixture(scope='session')
def integration_postgres_connector():
    """
    Real Postgres connector for integration tests.
    Only created if PG credentials are available.
    """
    from config.settings import POSTGRES_CONFIG
    from src.connectors.postgres_connector import PostgresConnector
    
    try:
        if all(POSTGRES_CONFIG.values()):
            connector = PostgresConnector(**POSTGRES_CONFIG)
            if connector.test_connection():
                yield connector
            else:
                pytest.skip("Postgres connection test failed")
        else:
            pytest.skip("Postgres credentials not configured")
    except Exception as e:
        pytest.skip(f"Postgres not available: {str(e)}")

