"""
Unit tests for config/settings.py
Target coverage: 100% for configuration validation logic
"""

import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch


def test_project_root_is_set():
    """Test that PROJECT_ROOT is correctly set."""
    from config.settings import PROJECT_ROOT
    
    assert PROJECT_ROOT is not None
    assert isinstance(PROJECT_ROOT, Path)
    assert PROJECT_ROOT.exists()


def test_environment_default_value():
    """Test ENVIRONMENT has a default value."""
    with patch.dict(os.environ, {}, clear=True):
        # Force module reload
        if 'config.settings' in sys.modules:
            del sys.modules['config.settings']
        import config.settings as settings
        
        assert settings.ENVIRONMENT == 'dev'


def test_environment_from_env_var(mock_env_vars):
    """Test ENVIRONMENT can be set from environment variable."""
    # Force module reload
    if 'config.settings' in sys.modules:
        del sys.modules['config.settings']
    import config.settings as settings
    
    assert settings.ENVIRONMENT == 'test'


def test_postgres_config_structure(mock_env_vars):
    """Test POSTGRES_CONFIG has all required fields."""
    # Force module reload
    if 'config.settings' in sys.modules:
        del sys.modules['config.settings']
    import config.settings as settings
    
    assert 'host' in settings.POSTGRES_CONFIG
    assert 'port' in settings.POSTGRES_CONFIG
    assert 'database' in settings.POSTGRES_CONFIG
    assert 'user' in settings.POSTGRES_CONFIG
    assert 'password' in settings.POSTGRES_CONFIG
    
    # Check types
    assert isinstance(settings.POSTGRES_CONFIG['port'], int)
    assert settings.POSTGRES_CONFIG['host'] == 'localhost'
    assert settings.POSTGRES_CONFIG['database'] == 'test_db'


def test_postgres_config_port_is_integer(mock_env_vars):
    """Test that POSTGRES_PORT is converted to integer."""
    # Force module reload
    if 'config.settings' in sys.modules:
        del sys.modules['config.settings']
    import config.settings as settings
    
    assert isinstance(settings.POSTGRES_CONFIG['port'], int)
    assert settings.POSTGRES_CONFIG['port'] == 5432


def test_schema_configuration(mock_env_vars):
    """Test schema configuration variables."""
    # Force module reload
    if 'config.settings' in sys.modules:
        del sys.modules['config.settings']
    import config.settings as settings
    
    assert settings.CONFLICT_SCHEMA == 'conflict'
    assert settings.ANALYTICS_SCHEMA == 'analytics'


def test_log_level_default():
    """Test LOG_LEVEL has a default value."""
    with patch.dict(os.environ, {}, clear=True):
        # Force module reload
        if 'config.settings' in sys.modules:
            del sys.modules['config.settings']
        import config.settings as settings
        
        assert settings.LOG_LEVEL == 'INFO'


def test_log_file_default():
    """Test LOG_FILE has a default value."""
    with patch.dict(os.environ, {}, clear=True):
        # Force module reload
        if 'config.settings' in sys.modules:
            del sys.modules['config.settings']
        import config.settings as settings
        
        assert settings.LOG_FILE == 'logs/etl_pipeline.log'


def test_date_range_configuration():
    """Test date range configuration constants."""
    from config.settings import DATE_RANGE_YEARS_BACK, DATE_RANGE_DAYS_FORWARD
    
    assert DATE_RANGE_YEARS_BACK == 2
    assert DATE_RANGE_DAYS_FORWARD == 45


def test_batch_processing_configuration():
    """Test batch processing configuration constants."""
    from config.settings import DEFAULT_BATCH_SIZE, MAX_WORKERS
    
    assert DEFAULT_BATCH_SIZE == 10000
    assert MAX_WORKERS == 6


def test_validate_config_success(mock_env_vars):
    """Test validate_config passes with all required config."""
    # Force module reload
    if 'config.settings' in sys.modules:
        del sys.modules['config.settings']
    import config.settings as settings
    
    # Should not raise any exception
    settings.validate_config()


def test_validate_config_missing_host(clean_settings_module):
    """Test validate_config fails when POSTGRES_HOST is missing."""
    import config.settings as settings
    
    # Temporarily modify POSTGRES_CONFIG to simulate missing host
    original_config = settings.POSTGRES_CONFIG.copy()
    try:
        settings.POSTGRES_CONFIG['host'] = None
        
        with pytest.raises(ValueError) as exc_info:
            settings.validate_config()
        
        assert 'POSTGRES_HOST' in str(exc_info.value)
    finally:
        settings.POSTGRES_CONFIG = original_config


def test_validate_config_missing_database(clean_settings_module):
    """Test validate_config fails when POSTGRES_DATABASE is missing."""
    import config.settings as settings
    
    # Temporarily modify POSTGRES_CONFIG to simulate missing database
    original_config = settings.POSTGRES_CONFIG.copy()
    try:
        settings.POSTGRES_CONFIG['database'] = None
        
        with pytest.raises(ValueError) as exc_info:
            settings.validate_config()
        
        assert 'POSTGRES_DATABASE' in str(exc_info.value)
    finally:
        settings.POSTGRES_CONFIG = original_config


def test_validate_config_missing_user(clean_settings_module):
    """Test validate_config fails when POSTGRES_USER is missing."""
    import config.settings as settings
    
    # Temporarily modify POSTGRES_CONFIG to simulate missing user
    original_config = settings.POSTGRES_CONFIG.copy()
    try:
        settings.POSTGRES_CONFIG['user'] = None
        
        with pytest.raises(ValueError) as exc_info:
            settings.validate_config()
        
        assert 'POSTGRES_USER' in str(exc_info.value)
    finally:
        settings.POSTGRES_CONFIG = original_config


def test_validate_config_missing_password(clean_settings_module):
    """Test validate_config fails when POSTGRES_PASSWORD is missing."""
    import config.settings as settings
    
    # Temporarily modify POSTGRES_CONFIG to simulate missing password
    original_config = settings.POSTGRES_CONFIG.copy()
    try:
        settings.POSTGRES_CONFIG['password'] = None
        
        with pytest.raises(ValueError) as exc_info:
            settings.validate_config()
        
        assert 'POSTGRES_PASSWORD' in str(exc_info.value)
    finally:
        settings.POSTGRES_CONFIG = original_config


def test_validate_config_missing_multiple(clean_settings_module):
    """Test validate_config shows all missing configuration."""
    import config.settings as settings
    
    # Temporarily modify POSTGRES_CONFIG to simulate multiple missing fields
    original_config = settings.POSTGRES_CONFIG.copy()
    try:
        settings.POSTGRES_CONFIG['database'] = None
        settings.POSTGRES_CONFIG['user'] = None
        settings.POSTGRES_CONFIG['password'] = None
        
        with pytest.raises(ValueError) as exc_info:
            settings.validate_config()
        
        error_message = str(exc_info.value)
        assert 'POSTGRES_DATABASE' in error_message
        assert 'POSTGRES_USER' in error_message
        assert 'POSTGRES_PASSWORD' in error_message
    finally:
        settings.POSTGRES_CONFIG = original_config
