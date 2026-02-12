"""
Configuration management for Task 02 Conflict Updater
Loads settings from config.json with environment variable substitution
"""

import json
import os
import re
from typing import Dict, Any


class Settings:
    """Configuration loader with environment variable substitution"""
    
    def __init__(self, config_file: str = 'config/config.json'):
        self.config_file = config_file
        self.config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from JSON file"""
        config_path = self._resolve_path(self.config_file)
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            raw_config = f.read()
        
        # Substitute environment variables
        config_with_env = self._substitute_env_vars(raw_config)
        self.config = json.loads(config_with_env)
    
    def _resolve_path(self, path: str) -> str:
        """Resolve relative path from script location"""
        if os.path.isabs(path):
            return path
        
        # Get the directory of this settings.py file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to tasks/ directory
        tasks_dir = os.path.dirname(script_dir)
        
        return os.path.join(tasks_dir, path)
    
    def _substitute_env_vars(self, text: str) -> str:
        """
        Replace ${VAR_NAME} patterns with environment variable values.
        If environment variable doesn't exist, keeps the placeholder.

        Values are JSON-escaped so that characters like newlines, tabs,
        and backslashes in environment variables (e.g. RSA private keys)
        don't break the JSON structure.
        """
        pattern = re.compile(r'\$\{([^}]+)\}')
        
        def replacer(match):
            var_name = match.group(1)
            value = os.environ.get(var_name)
            if value is None:
                return match.group(0)
            # JSON-encode the value to escape newlines, tabs, backslashes, etc.
            # json.dumps adds surrounding quotes -- strip them since the
            # placeholder is already inside a quoted JSON string.
            return json.dumps(value)[1:-1]
        
        return pattern.sub(replacer, text)
    
    def get_snowflake_config(self) -> Dict[str, Any]:
        """Get Snowflake connection configuration"""
        return self.config.get('snowflake', {})
    
    def get_postgres_config(self) -> Dict[str, Any]:
        """Get PostgreSQL connection configuration"""
        return self.config.get('postgres', {})
    
    def get_task02_parameters(self) -> Dict[str, Any]:
        """Get Task 02 specific parameters"""
        return self.config.get('task02_parameters', {})
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration (pg_cron job name, materialized view, required tables)"""
        return self.config.get('pipeline', {})

    def get_email_config(self) -> Dict[str, Any]:
        """Get email configuration (SES settings, recipients, etc.)"""
        return self.config.get('email', {})

    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.config.get('logging', {})
    
    def get(self, key: str, default=None):
        """Get configuration value by key"""
        return self.config.get(key, default)
    
    def get_database_names(self) -> Dict[str, str]:
        """Get all database and schema names for SQL parameterization"""
        sf_config = self.get_snowflake_config()
        pg_config = self.get_postgres_config()
        
        return {
            'sf_database': sf_config.get('analytics_database', 'ANALYTICS_SANDBOX'),
            'sf_schema': sf_config.get('analytics_schema', 'BI'),
            'pg_database': pg_config.get('conflict_database', 'CONFLICTREPORT_SANDBOX'),
            'pg_schema': pg_config.get('conflict_schema', 'PUBLIC')
        }
