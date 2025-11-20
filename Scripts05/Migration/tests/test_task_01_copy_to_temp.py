"""
Unit tests for src/tasks/task_01_copy_to_temp.py
Target coverage: 100% for business logic
"""

import pytest
import time
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, mock_open


def test_task01_initialization(mock_postgres_connector):
    """Test Task01CopyToTemp initialization."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    
    task = Task01CopyToTemp(mock_postgres_connector)
    
    assert task.task_name == 'TASK_01'
    assert task.pg == mock_postgres_connector


def test_task01_inherits_from_base_task(mock_postgres_connector):
    """Test that Task01CopyToTemp inherits from BaseTask."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from src.tasks.base_task import BaseTask
    
    task = Task01CopyToTemp(mock_postgres_connector)
    
    assert isinstance(task, BaseTask)


def test_task01_execute_success(mock_postgres_connector, tmp_path):
    """Test successful execution of Task 01."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    # Create a test SQL file
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("""
        -- Test SQL script
        SELECT * FROM {conflict_schema}.test_table;
        UPDATE {analytics_schema}.analytics_table SET status = 'processed';
    """)
    
    # Configure mock to return a specific value
    mock_postgres_connector.execute.return_value = 150
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'analytics'):
                task = Task01CopyToTemp(mock_postgres_connector)
                result = task.execute()
    
    assert result['status'] == 'success'
    assert result['affected_rows'] == 150
    assert 'duration_seconds' in result
    assert result['duration_seconds'] >= 0
    
    # Verify execute was called with formatted SQL
    mock_postgres_connector.execute.assert_called_once()
    call_args = mock_postgres_connector.execute.call_args[0][0]
    assert 'conflict.test_table' in call_args
    assert 'analytics.analytics_table' in call_args
    assert '{conflict_schema}' not in call_args
    assert '{analytics_schema}' not in call_args


def test_task01_execute_sql_file_not_found(mock_postgres_connector, tmp_path):
    """Test Task 01 raises error when SQL file is missing."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    # Create project root but no SQL file
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        task = Task01CopyToTemp(mock_postgres_connector)
        
        with pytest.raises(FileNotFoundError) as exc_info:
            task.execute()
        
        assert 'SQL script not found' in str(exc_info.value)


def test_task01_sql_template_substitution(mock_postgres_connector, tmp_path):
    """Test that SQL template correctly substitutes schema names."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    # Create a test SQL file with placeholders
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("""
        INSERT INTO {conflict_schema}.temp_table
        SELECT * FROM {analytics_schema}.source_table
        WHERE {conflict_schema}.id = {analytics_schema}.id;
    """)
    
    mock_postgres_connector.execute.return_value = 100
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'myconflict'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'myanalytics'):
                task = Task01CopyToTemp(mock_postgres_connector)
                result = task.execute()
    
    # Verify the SQL was formatted correctly
    call_args = mock_postgres_connector.execute.call_args[0][0]
    assert 'myconflict.temp_table' in call_args
    assert 'myanalytics.source_table' in call_args
    assert '{conflict_schema}' not in call_args
    assert '{analytics_schema}' not in call_args


def test_task01_timing_measurement(mock_postgres_connector, tmp_path):
    """Test that Task 01 correctly measures execution time."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    # Create a test SQL file
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("SELECT 1;")
    
    # Make execute take some time
    def slow_execute(*args, **kwargs):
        time.sleep(0.1)
        return 50
    
    mock_postgres_connector.execute.side_effect = slow_execute
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'analytics'):
                task = Task01CopyToTemp(mock_postgres_connector)
                result = task.execute()
    
    assert result['duration_seconds'] >= 0.1


def test_task01_returns_affected_rows(mock_postgres_connector, tmp_path):
    """Test that Task 01 returns the number of affected rows."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("INSERT INTO test VALUES (1);")
    
    mock_postgres_connector.execute.return_value = 999
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'analytics'):
                task = Task01CopyToTemp(mock_postgres_connector)
                result = task.execute()
    
    assert result['affected_rows'] == 999


def test_task01_sql_file_path_construction(mock_postgres_connector, tmp_path):
    """Test that Task 01 constructs the correct SQL file path."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("SELECT 1;")
    
    mock_postgres_connector.execute.return_value = 1
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'analytics'):
                task = Task01CopyToTemp(mock_postgres_connector)
                result = task.execute()
    
    # If we got here without FileNotFoundError, path construction was correct
    assert result['status'] == 'success'


def test_task01_multiple_schema_replacements(mock_postgres_connector, tmp_path):
    """Test that multiple occurrences of schema placeholders are replaced."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("""
        SELECT {conflict_schema}.a, {conflict_schema}.b
        FROM {conflict_schema}.table1
        JOIN {analytics_schema}.table2 ON {conflict_schema}.id = {analytics_schema}.id
        WHERE {conflict_schema}.status = 'active'
        AND {analytics_schema}.type = 'valid';
    """)
    
    mock_postgres_connector.execute.return_value = 10
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'conf'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'anal'):
                task = Task01CopyToTemp(mock_postgres_connector)
                result = task.execute()
    
    call_args = mock_postgres_connector.execute.call_args[0][0]
    # Count replacements
    assert call_args.count('conf') >= 4
    assert call_args.count('anal') >= 2
    assert '{conflict_schema}' not in call_args
    assert '{analytics_schema}' not in call_args


def test_task01_execute_error_handling(mock_postgres_connector, tmp_path):
    """Test that Task 01 properly handles execution errors."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("SELECT 1;")
    
    mock_postgres_connector.execute.side_effect = Exception("Database error")
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'analytics'):
                task = Task01CopyToTemp(mock_postgres_connector)
                
                with pytest.raises(Exception) as exc_info:
                    task.execute()
                
                assert "Database error" in str(exc_info.value)


def test_task01_empty_sql_file(mock_postgres_connector, tmp_path):
    """Test Task 01 with an empty SQL file."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("")
    
    mock_postgres_connector.execute.return_value = 0
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'analytics'):
                task = Task01CopyToTemp(mock_postgres_connector)
                result = task.execute()
    
    # Should still execute successfully, just with empty SQL
    assert result['status'] == 'success'
    assert result['affected_rows'] == 0


def test_task01_result_structure(mock_postgres_connector, tmp_path):
    """Test that Task 01 returns properly structured result."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_01_copy_to_temp.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("SELECT 1;")
    
    mock_postgres_connector.execute.return_value = 42
    
    with patch('src.tasks.task_01_copy_to_temp.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_01_copy_to_temp.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_01_copy_to_temp.ANALYTICS_SCHEMA', 'analytics'):
                task = Task01CopyToTemp(mock_postgres_connector)
                result = task.execute()
    
    # Verify result structure
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert isinstance(result['affected_rows'], int)
    assert isinstance(result['duration_seconds'], float)


def test_task01_connector_reference(mock_postgres_connector):
    """Test that Task 01 stores the connector reference."""
    from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
    
    task = Task01CopyToTemp(mock_postgres_connector)
    
    assert task.pg is mock_postgres_connector

