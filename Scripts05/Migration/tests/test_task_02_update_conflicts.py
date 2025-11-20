"""
Unit tests for src/tasks/task_02_update_conflicts.py
Target coverage: 100% for business logic
"""

import pytest
import time
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch


def test_task02_initialization(mock_postgres_connector):
    """Test Task02UpdateConflictVisitMaps initialization."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    
    task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
    
    assert task.task_name == 'TASK_02'
    assert task.pg == mock_postgres_connector


def test_task02_inherits_from_base_task(mock_postgres_connector):
    """Test that Task02UpdateConflictVisitMaps inherits from BaseTask."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from src.tasks.base_task import BaseTask
    
    task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
    
    assert isinstance(task, BaseTask)


def test_task02_execute_success(mock_postgres_connector, tmp_path):
    """Test successful execution of Task 02."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    # Create a test SQL file
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("""
        -- Test SQL script
        UPDATE {conflict_schema}.conflictvisitmaps
        SET status = 'updated'
        WHERE id IN (
            SELECT id FROM {analytics_schema}.analytics_data
        );
    """)
    
    mock_postgres_connector.execute.return_value = 250
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    assert result['status'] == 'success'
    assert result['updated_rows'] == 250
    assert 'duration_seconds' in result
    assert result['duration_seconds'] >= 0
    
    # Verify execute was called with formatted SQL
    mock_postgres_connector.execute.assert_called_once()
    call_args = mock_postgres_connector.execute.call_args[0][0]
    assert 'conflict.conflictvisitmaps' in call_args
    assert 'analytics.analytics_data' in call_args
    assert '{conflict_schema}' not in call_args
    assert '{analytics_schema}' not in call_args


def test_task02_execute_sql_file_not_found(mock_postgres_connector, tmp_path):
    """Test Task 02 raises error when SQL file is missing."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    # Create project root but no SQL file
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
        
        with pytest.raises(FileNotFoundError) as exc_info:
            task.execute()
        
        assert 'SQL script not found' in str(exc_info.value)


def test_task02_sql_template_substitution(mock_postgres_connector, tmp_path):
    """Test that SQL template correctly substitutes schema names."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    # Create a test SQL file with placeholders
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("""
        UPDATE {conflict_schema}.conflictvisitmaps c
        SET c.value = a.value
        FROM {analytics_schema}.analytics a
        WHERE c.id = a.id
        AND {conflict_schema}.status = 'pending'
        AND {analytics_schema}.type = 'valid';
    """)
    
    mock_postgres_connector.execute.return_value = 100
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'myconflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'myanalytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    # Verify the SQL was formatted correctly
    call_args = mock_postgres_connector.execute.call_args[0][0]
    assert 'myconflict.conflictvisitmaps' in call_args
    assert 'myanalytics.analytics' in call_args
    assert 'myconflict.status' in call_args
    assert 'myanalytics.type' in call_args
    assert '{conflict_schema}' not in call_args
    assert '{analytics_schema}' not in call_args


def test_task02_timing_measurement(mock_postgres_connector, tmp_path):
    """Test that Task 02 correctly measures execution time."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    # Create a test SQL file
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("UPDATE test SET value = 1;")
    
    # Make execute take some time
    def slow_execute(*args, **kwargs):
        time.sleep(0.1)
        return 75
    
    mock_postgres_connector.execute.side_effect = slow_execute
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    assert result['duration_seconds'] >= 0.1


def test_task02_returns_updated_rows(mock_postgres_connector, tmp_path):
    """Test that Task 02 returns the number of updated rows."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("UPDATE test SET value = 1;")
    
    mock_postgres_connector.execute.return_value = 888
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    assert result['updated_rows'] == 888


def test_task02_sql_file_path_construction(mock_postgres_connector, tmp_path):
    """Test that Task 02 constructs the correct SQL file path."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("UPDATE test SET value = 1;")
    
    mock_postgres_connector.execute.return_value = 1
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    # If we got here without FileNotFoundError, path construction was correct
    assert result['status'] == 'success'


def test_task02_multiple_schema_replacements(mock_postgres_connector, tmp_path):
    """Test that multiple occurrences of schema placeholders are replaced."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("""
        UPDATE {conflict_schema}.conflictvisitmaps
        SET {conflict_schema}.field1 = {analytics_schema}.field1,
            {conflict_schema}.field2 = {analytics_schema}.field2
        FROM {analytics_schema}.source
        WHERE {conflict_schema}.id = {analytics_schema}.id
        AND {conflict_schema}.status = 'pending'
        AND {analytics_schema}.valid = true;
    """)
    
    mock_postgres_connector.execute.return_value = 10
    
    with patch('config.settings.PROJECT_ROOT', tmp_path):
        with patch('config.settings.CONFLICT_SCHEMA', 'conf'):
            with patch('config.settings.ANALYTICS_SCHEMA', 'anal'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    call_args = mock_postgres_connector.execute.call_args[0][0]
    # Count replacements - should have multiple of each
    assert call_args.count('conf') >= 4
    assert call_args.count('anal') >= 4
    assert '{conflict_schema}' not in call_args
    assert '{analytics_schema}' not in call_args


def test_task02_execute_error_handling(mock_postgres_connector, tmp_path):
    """Test that Task 02 properly handles execution errors."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("UPDATE test SET value = 1;")
    
    mock_postgres_connector.execute.side_effect = Exception("Database connection lost")
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                
                with pytest.raises(Exception) as exc_info:
                    task.execute()
                
                assert "Database connection lost" in str(exc_info.value)


def test_task02_empty_sql_file(mock_postgres_connector, tmp_path):
    """Test Task 02 with an empty SQL file."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("")
    
    mock_postgres_connector.execute.return_value = 0
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    # Should still execute successfully, just with empty SQL
    assert result['status'] == 'success'
    assert result['updated_rows'] == 0


def test_task02_result_structure(mock_postgres_connector, tmp_path):
    """Test that Task 02 returns properly structured result."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("UPDATE test SET value = 1;")
    
    mock_postgres_connector.execute.return_value = 123
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    # Verify result structure
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert isinstance(result['updated_rows'], int)
    assert isinstance(result['duration_seconds'], float)


def test_task02_connector_reference(mock_postgres_connector):
    """Test that Task 02 stores the connector reference."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    
    task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
    
    assert task.pg is mock_postgres_connector


def test_task02_uses_replace_not_format(mock_postgres_connector, tmp_path):
    """Test that Task 02 uses replace() to avoid conflicts with SQL's curly braces."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    # SQL with curly braces that are not placeholders (like in Postgres JSON operations)
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("""
        UPDATE {conflict_schema}.table
        SET data = data || '{"key": "value"}'::jsonb
        FROM {analytics_schema}.source;
    """)
    
    mock_postgres_connector.execute.return_value = 5
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    # Should not raise an error about unmatched braces
    assert result['status'] == 'success'
    
    # Verify JSON braces are preserved
    call_args = mock_postgres_connector.execute.call_args[0][0]
    assert '{"key": "value"}' in call_args


def test_task02_large_row_count(mock_postgres_connector, tmp_path):
    """Test Task 02 with a large number of updated rows."""
    from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
    from unittest.mock import patch
    
    sql_file = tmp_path / 'sql' / 'task_02_update_conflicts.sql'
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("UPDATE test SET value = 1;")
    
    mock_postgres_connector.execute.return_value = 1_000_000
    
    with patch('src.tasks.task_02_update_conflicts.PROJECT_ROOT', tmp_path):
        with patch('src.tasks.task_02_update_conflicts.CONFLICT_SCHEMA', 'conflict'):
            with patch('src.tasks.task_02_update_conflicts.ANALYTICS_SCHEMA', 'analytics'):
                task = Task02UpdateConflictVisitMaps(mock_postgres_connector)
                result = task.execute()
    
    assert result['updated_rows'] == 1_000_000

