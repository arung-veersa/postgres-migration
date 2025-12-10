"""
Configuration Loader Module
Loads and validates the migration configuration from config.json
Handles environment variable substitution
"""

import json
import os
import re
import hashlib
from typing import Dict, Any, List
from pathlib import Path


class ConfigLoader:
    """Loads and validates migration configuration"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = {}
        
    def load(self) -> Dict[str, Any]:
        """Load configuration from file with environment variable substitution"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config_text = f.read()
        
        # Substitute environment variables
        config_text = self._substitute_env_vars(config_text)
        
        # Parse JSON
        self.config = json.loads(config_text)
        
        # Validate
        self._validate_config()
        
        return self.config
    
    def _substitute_env_vars(self, text: str) -> str:
        """Replace ${VAR_NAME} with environment variable values"""
        pattern = r'\$\{([^}]+)\}'
        
        def replace_var(match):
            var_name = match.group(1)
            value = os.getenv(var_name)
            if value is None:
                raise ValueError(f"Environment variable '{var_name}' is not set")
            return value
        
        return re.sub(pattern, replace_var, text)
    
    def _validate_config(self):
        """Validate configuration structure"""
        required_top_level = ['snowflake', 'postgres', 'sources']
        for key in required_top_level:
            if key not in self.config:
                raise ValueError(f"Missing required configuration key: {key}")
        
        # Validate Snowflake config
        required_sf = ['account', 'user', 'warehouse', 'rsa_key']
        for key in required_sf:
            if key not in self.config['snowflake']:
                raise ValueError(f"Missing required Snowflake configuration: {key}")
        
        # Validate PostgreSQL config
        required_pg = ['host', 'user', 'password']
        for key in required_pg:
            if key not in self.config['postgres']:
                raise ValueError(f"Missing required PostgreSQL configuration: {key}")
        
        # Validate sources
        if not isinstance(self.config['sources'], list):
            raise ValueError("'sources' must be a list")
        
        for idx, source in enumerate(self.config['sources']):
            self._validate_source(source, idx)
    
    def _validate_source(self, source: Dict[str, Any], idx: int):
        """Validate a single source configuration"""
        required = ['source_sf_database', 'source_sf_schema', 
                   'target_pg_database', 'target_pg_schema', 'tables']
        
        for key in required:
            if key not in source:
                raise ValueError(f"Source {idx}: Missing required key '{key}'")
        
        if not isinstance(source['tables'], list):
            raise ValueError(f"Source {idx}: 'tables' must be a list")
        
        for tidx, table in enumerate(source['tables']):
            self._validate_table(table, idx, tidx)
    
    def _validate_table(self, table: Dict[str, Any], source_idx: int, table_idx: int):
        """Validate a single table configuration"""
        required = ['source', 'target']
        
        for key in required:
            if key not in table:
                raise ValueError(
                    f"Source {source_idx}, Table {table_idx}: Missing required key '{key}'"
                )
        
        # Validate chunking configuration
        chunking_cols = table.get('chunking_columns')
        chunking_types = table.get('chunking_column_types')
        
        if chunking_cols and chunking_types:
            if len(chunking_cols) != len(chunking_types):
                raise ValueError(
                    f"Source {source_idx}, Table {table_idx}: "
                    f"chunking_columns and chunking_column_types must have same length"
                )
    
    def get_enabled_sources(self) -> List[Dict[str, Any]]:
        """Get list of enabled sources"""
        return [s for s in self.config['sources'] if s.get('enabled', True)]
    
    def get_enabled_tables(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get list of enabled tables for a source"""
        return [t for t in source['tables'] if t.get('enabled', True)]
    
    def get_global_config(self) -> Dict[str, Any]:
        """Get global configuration (batch_size, threads, etc.)"""
        return {
            'parallel_threads': self.config.get('parallel_threads', 4),
            'batch_size': self.config.get('batch_size', 10000),
            'max_retry_attempts': self.config.get('max_retry_attempts', 3),
            'lambda_timeout_buffer_seconds': self.config.get('lambda_timeout_buffer_seconds', 120),
        }
    
    def get_config_hash(self) -> str:
        """Generate MD5 hash of configuration for tracking"""
        config_str = json.dumps(self.config, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()
    
    def get_execution_hash(self, source_names: List[str]) -> str:
        """
        Generate execution context hash (config + source selection).
        Used for safe concurrent execution and resume matching.
        
        Args:
            source_names: List of source names being migrated
                         - For single source: ["analytics"]
                         - For multiple: ["analytics", "conflict"]
                         - For all enabled: ["aggregator", "analytics", "conflict"]
        
        Returns:
            MD5 hash combining config_hash and sorted source_names
            
        Note:
            source_names are sorted before hashing to ensure consistency
            regardless of input order (e.g., "analytics,conflict" vs "conflict,analytics")
        """
        context = {
            'config_hash': self.get_config_hash(),
            'source_names': sorted(source_names)  # Sort for consistency
        }
        context_str = json.dumps(context, sort_keys=True)
        return hashlib.md5(context_str.encode()).hexdigest()
    
    @staticmethod
    def resolve_filter(source_filter: Any) -> str:
        """Resolve source filter to SQL WHERE clause"""
        if source_filter is None:
            return "1=1"
        if isinstance(source_filter, str):
            if source_filter.strip() == "":
                return "1=1"
            return source_filter
        return "1=1"


if __name__ == "__main__":
    # Test configuration loading
    from dotenv import load_dotenv
    load_dotenv()
    
    loader = ConfigLoader("config.json")
    config = loader.load()
    
    print(f"✓ Configuration loaded successfully")
    print(f"  Config hash: {loader.get_config_hash()}")
    print(f"  Enabled sources: {len(loader.get_enabled_sources())}")
    
    for source in loader.get_enabled_sources():
        enabled_tables = loader.get_enabled_tables(source)
        print(f"  Source '{source.get('source_name', 'unnamed')}': {len(enabled_tables)} enabled tables")

