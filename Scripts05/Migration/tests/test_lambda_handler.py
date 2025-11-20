"""
Unit tests for Lambda handler.
Fast tests using mocks - no database required.

Target coverage: 100% of lambda_handler.py
"""

import pytest
from unittest.mock import Mock, MagicMock
from scripts.lambda_handler import lambda_handler


def test_lambda_handler_validate_config_success(mocker):
    """Test validate_config action succeeds."""
    # Mock the validate_config function
    mock_validate = mocker.patch('scripts.lambda_handler.validate_config')
    
    event = {'action': 'validate_config'}
    result = lambda_handler(event, None)
    
    # Assertions
    assert result['statusCode'] == 200
    assert result['body']['status'] == 'success'
    assert result['body']['action'] == 'validate_config'
    mock_validate.assert_called_once()


def test_lambda_handler_validate_config_failure(mocker):
    """Test validate_config action fails when config is invalid."""
    # Mock validate_config to raise ValueError
    mock_validate = mocker.patch(
        'scripts.lambda_handler.validate_config',
        side_effect=ValueError('Missing POSTGRES_HOST')
    )
    
    event = {'action': 'validate_config'}
    result = lambda_handler(event, None)
    
    # Should return error response
    assert result['statusCode'] == 500
    assert result['body']['status'] == 'error'
    assert 'POSTGRES_HOST' in result['body']['error']


def test_lambda_handler_task_01_success_mock_mode(mocker):
    """Test Task 01 execution with mock database."""
    # Mock the MockPostgresConnector (imported dynamically in lambda_handler)
    mock_connector_class = mocker.patch('scripts.mock_postgres_connector.MockPostgresConnector')
    mock_connector = MagicMock()
    mock_connector_class.return_value = mock_connector
    
    # Mock Task01CopyToTemp
    mock_task_class = mocker.patch('scripts.lambda_handler.Task01CopyToTemp')
    mock_task = MagicMock()
    mock_task.run.return_value = {
        'status': 'success',
        'duration_seconds': 45.2,
        'result': {'affected_rows': 15234}
    }
    mock_task_class.return_value = mock_task
    
    event = {'action': 'task_01', 'use_mock': True}
    result = lambda_handler(event, None)
    
    # Assertions
    assert result['statusCode'] == 200
    assert result['body']['status'] == 'success'
    assert result['body']['duration_seconds'] == 45.2
    assert result['body']['result']['affected_rows'] == 15234
    
    mock_connector_class.assert_called_once()
    mock_task_class.assert_called_once_with(mock_connector)
    mock_task.run.assert_called_once()


def test_lambda_handler_task_01_success_real_db(mocker):
    """Test Task 01 execution with real database (mocked)."""
    # Mock PostgresConnector
    mock_connector_class = mocker.patch('scripts.lambda_handler.PostgresConnector')
    mock_connector = MagicMock()
    mock_connector_class.return_value = mock_connector
    
    # Mock Task01CopyToTemp
    mock_task_class = mocker.patch('scripts.lambda_handler.Task01CopyToTemp')
    mock_task = MagicMock()
    mock_task.run.return_value = {
        'status': 'success',
        'duration_seconds': 52.1,
        'result': {'affected_rows': 18432}
    }
    mock_task_class.return_value = mock_task
    
    event = {'action': 'task_01', 'use_mock': False}
    result = lambda_handler(event, None)
    
    # Assertions
    assert result['statusCode'] == 200
    assert result['body']['status'] == 'success'
    mock_connector_class.assert_called_once()


def test_lambda_handler_task_01_failure(mocker):
    """Test Task 01 execution fails."""
    # Mock PostgresConnector
    mock_connector_class = mocker.patch('scripts.lambda_handler.PostgresConnector')
    mock_connector = MagicMock()
    mock_connector_class.return_value = mock_connector
    
    # Mock Task01CopyToTemp to return failure
    mock_task_class = mocker.patch('scripts.lambda_handler.Task01CopyToTemp')
    mock_task = MagicMock()
    mock_task.run.return_value = {
        'status': 'failed',
        'duration_seconds': 5.2,
        'error': 'SQL file not found'
    }
    mock_task_class.return_value = mock_task
    
    event = {'action': 'task_01'}
    result = lambda_handler(event, None)
    
    # Should return 500 error
    assert result['statusCode'] == 500
    assert result['body']['status'] == 'failed'
    assert 'SQL file not found' in result['body']['error']


def test_lambda_handler_task_02_success_mock_mode(mocker):
    """Test Task 02 execution with mock database."""
    # Mock the MockPostgresConnector (imported dynamically in lambda_handler)
    mock_connector_class = mocker.patch('scripts.mock_postgres_connector.MockPostgresConnector')
    mock_connector = MagicMock()
    mock_connector_class.return_value = mock_connector
    
    # Mock Task02UpdateConflictVisitMaps
    mock_task_class = mocker.patch('scripts.lambda_handler.Task02UpdateConflictVisitMaps')
    mock_task = MagicMock()
    mock_task.run.return_value = {
        'status': 'success',
        'duration_seconds': 187.3,
        'result': {'updated_rows': 8432}
    }
    mock_task_class.return_value = mock_task
    
    event = {'action': 'task_02', 'use_mock': True}
    result = lambda_handler(event, None)
    
    # Assertions
    assert result['statusCode'] == 200
    assert result['body']['status'] == 'success'
    assert result['body']['duration_seconds'] == 187.3
    assert result['body']['result']['updated_rows'] == 8432
    
    mock_connector_class.assert_called_once()
    mock_task_class.assert_called_once_with(mock_connector)
    mock_task.run.assert_called_once()


def test_lambda_handler_task_02_success_real_db(mocker):
    """Test Task 02 execution with real database (mocked)."""
    # Mock PostgresConnector
    mock_connector_class = mocker.patch('scripts.lambda_handler.PostgresConnector')
    mock_connector = MagicMock()
    mock_connector_class.return_value = mock_connector
    
    # Mock Task02UpdateConflictVisitMaps
    mock_task_class = mocker.patch('scripts.lambda_handler.Task02UpdateConflictVisitMaps')
    mock_task = MagicMock()
    mock_task.run.return_value = {
        'status': 'success',
        'duration_seconds': 210.5,
        'result': {'updated_rows': 9123}
    }
    mock_task_class.return_value = mock_task
    
    event = {'action': 'task_02', 'use_mock': False}
    result = lambda_handler(event, None)
    
    # Assertions
    assert result['statusCode'] == 200
    assert result['body']['status'] == 'success'
    mock_connector_class.assert_called_once()


def test_lambda_handler_task_02_failure(mocker):
    """Test Task 02 execution fails."""
    # Mock PostgresConnector
    mock_connector_class = mocker.patch('scripts.lambda_handler.PostgresConnector')
    mock_connector = MagicMock()
    mock_connector_class.return_value = mock_connector
    
    # Mock Task02UpdateConflictVisitMaps to return failure
    mock_task_class = mocker.patch('scripts.lambda_handler.Task02UpdateConflictVisitMaps')
    mock_task = MagicMock()
    mock_task.run.return_value = {
        'status': 'failed',
        'duration_seconds': 12.3,
        'error': 'Database connection lost'
    }
    mock_task_class.return_value = mock_task
    
    event = {'action': 'task_02'}
    result = lambda_handler(event, None)
    
    # Should return 500 error
    assert result['statusCode'] == 500
    assert result['body']['status'] == 'failed'
    assert 'Database connection lost' in result['body']['error']


def test_lambda_handler_unknown_action():
    """Test lambda handler with unknown action."""
    event = {'action': 'unknown_action'}
    result = lambda_handler(event, None)
    
    # Should return 400 bad request
    assert result['statusCode'] == 400
    assert result['body']['status'] == 'error'
    assert 'Unknown action' in result['body']['error']
    assert 'unknown_action' in result['body']['error']


def test_lambda_handler_missing_action():
    """Test lambda handler with missing action."""
    event = {}
    result = lambda_handler(event, None)
    
    # Should return 400 bad request
    assert result['statusCode'] == 400
    assert result['body']['status'] == 'error'


def test_lambda_handler_exception_handling(mocker):
    """Test lambda handler handles unexpected exceptions."""
    # Mock validate_config to raise unexpected exception
    mocker.patch(
        'scripts.lambda_handler.validate_config',
        side_effect=RuntimeError('Unexpected error')
    )
    
    event = {'action': 'validate_config'}
    result = lambda_handler(event, None)
    
    # Should return 500 with error details
    assert result['statusCode'] == 500
    assert result['body']['status'] == 'error'
    assert 'Unexpected error' in result['body']['error']


def test_lambda_handler_task_01_with_exception(mocker):
    """Test Task 01 raises exception during execution."""
    # Mock PostgresConnector to raise exception
    mocker.patch(
        'scripts.lambda_handler.PostgresConnector',
        side_effect=Exception('Connection failed')
    )
    
    event = {'action': 'task_01'}
    result = lambda_handler(event, None)
    
    # Should return 500 with error
    assert result['statusCode'] == 500
    assert result['body']['status'] == 'error'
    assert 'Connection failed' in result['body']['error']

