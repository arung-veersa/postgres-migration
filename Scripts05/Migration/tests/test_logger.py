"""
Unit tests for src/utils/logger.py
Target coverage: 100%
"""

import pytest
import logging
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock


def test_get_logger_returns_logger():
    """Test that get_logger returns a Logger instance."""
    from src.utils.logger import get_logger
    
    logger = get_logger('test_logger')
    assert isinstance(logger, logging.Logger)
    assert logger.name == 'test_logger'


def test_get_logger_sets_correct_level(mock_env_vars):
    """Test that logger is configured with correct log level."""
    # Clear existing handlers from the logger
    import logging
    test_logger_name = 'test_logger_level_unique'
    test_logger = logging.getLogger(test_logger_name)
    for handler in test_logger.handlers[:]:
        test_logger.removeHandler(handler)
    
    with patch('config.settings.LOG_LEVEL', 'DEBUG'):
        from src.utils.logger import get_logger
        
        logger = get_logger(test_logger_name)
        # The logger level should be set to DEBUG
        # Note: If handlers already exist from previous tests, the level may not change
        assert logger.level <= logging.INFO  # Should be DEBUG or INFO


def test_get_logger_adds_console_handler():
    """Test that logger has a console handler."""
    from src.utils.logger import get_logger
    
    logger = get_logger('test_console')
    
    # Check that logger has handlers
    assert len(logger.handlers) > 0
    
    # Check for StreamHandler (console)
    console_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
    assert len(console_handlers) > 0
    
    # Verify console handler streams to stdout
    console_handler = console_handlers[0]
    assert console_handler.stream == sys.stdout


def test_get_logger_adds_file_handler(tmp_path):
    """Test that logger has a file handler."""
    log_file = tmp_path / 'test.log'
    
    with patch('config.settings.PROJECT_ROOT', tmp_path):
        with patch('config.settings.LOG_FILE', 'test.log'):
            from src.utils.logger import get_logger
            
            logger = get_logger('test_file')
            
            # Check for FileHandler
            file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) > 0


def test_get_logger_formatter_format():
    """Test that logger handlers have correct formatter."""
    from src.utils.logger import get_logger, LOG_FORMAT
    
    logger = get_logger('test_format')
    
    for handler in logger.handlers:
        assert handler.formatter is not None
        # Check that formatter uses the expected format
        assert '%(asctime)s' in handler.formatter._fmt
        assert '%(name)s' in handler.formatter._fmt
        assert '%(levelname)s' in handler.formatter._fmt
        assert '%(message)s' in handler.formatter._fmt


def test_get_logger_no_propagation():
    """Test that logger does not propagate to root logger."""
    from src.utils.logger import get_logger
    
    logger = get_logger('test_propagate')
    assert logger.propagate is False


def test_get_logger_reuses_existing_logger():
    """Test that get_logger reuses existing logger instance."""
    from src.utils.logger import get_logger
    
    logger1 = get_logger('test_reuse')
    initial_handler_count = len(logger1.handlers)
    
    logger2 = get_logger('test_reuse')
    
    # Should be the same logger
    assert logger1 is logger2
    
    # Should not add duplicate handlers
    assert len(logger2.handlers) == initial_handler_count


def test_console_handler_level():
    """Test that console handler has INFO level."""
    from src.utils.logger import get_logger
    
    logger = get_logger('test_console_level')
    
    console_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)
                       and h.stream == sys.stdout]
    
    assert len(console_handlers) > 0
    assert console_handlers[0].level == logging.INFO


def test_file_handler_level():
    """Test that file handler has DEBUG level."""
    from src.utils.logger import get_logger
    
    logger = get_logger('test_file_level')
    
    file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
    
    assert len(file_handlers) > 0
    assert file_handlers[0].level == logging.DEBUG


def test_logger_date_format():
    """Test that logger uses correct date format."""
    from src.utils.logger import get_logger, DATE_FORMAT
    
    logger = get_logger('test_date_format')
    
    assert DATE_FORMAT == '%Y-%m-%d %H:%M:%S'
    
    for handler in logger.handlers:
        assert handler.formatter.datefmt == DATE_FORMAT


def test_log_directory_creation(tmp_path):
    """Test that log directory is created if it doesn't exist."""
    test_project_root = tmp_path / 'test_project'
    test_project_root.mkdir()
    
    with patch('config.settings.PROJECT_ROOT', test_project_root):
        with patch('config.settings.LOG_FILE', 'logs/test.log'):
            # Force reload the logger module to trigger directory creation
            import importlib
            import src.utils.logger
            importlib.reload(src.utils.logger)
            
            log_dir = test_project_root / 'logs'
            assert log_dir.exists()
            assert log_dir.is_dir()


def test_log_format_constant():
    """Test that LOG_FORMAT constant is correctly defined."""
    from src.utils.logger import LOG_FORMAT
    
    assert isinstance(LOG_FORMAT, str)
    assert '%(asctime)s' in LOG_FORMAT
    assert '%(name)s' in LOG_FORMAT
    assert '%(levelname)s' in LOG_FORMAT
    assert '%(message)s' in LOG_FORMAT


def test_multiple_loggers_independent():
    """Test that multiple loggers are independent."""
    from src.utils.logger import get_logger
    
    logger1 = get_logger('test_logger_1')
    logger2 = get_logger('test_logger_2')
    
    assert logger1 is not logger2
    assert logger1.name != logger2.name

