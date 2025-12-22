"""
Configuration validator for migration tool.
Validates config.json structure and values before starting migration.
"""

from typing import Dict, List, Any
import logging

logger = logging.getLogger("migration")


class ConfigValidator:
    """Validates migration configuration"""
    
    REQUIRED_GLOBAL_KEYS = [
        'parallel_threads',
        'batch_size',
        'sources'
    ]
    
    REQUIRED_SOURCE_KEYS = [
        'source_name',
        'enabled',
        'source_sf_database',
        'source_sf_schema',
        'target_pg_database',
        'target_pg_schema',
        'tables'
    ]
    
    REQUIRED_TABLE_KEYS = [
        'enabled',
        'source',
        'target'
    ]
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.errors = []
        self.warnings = []
    
    def validate(self) -> bool:
        """
        Validate configuration and return True if valid.
        Logs errors and warnings.
        """
        self._validate_global_config()
        self._validate_sources()
        
        if self.errors:
            logger.error("Configuration validation failed:")
            for error in self.errors:
                logger.error(f"  ❌ {error}")
        
        if self.warnings:
            logger.warning("Configuration warnings:")
            for warning in self.warnings:
                logger.warning(f"  ⚠️  {warning}")
        
        return len(self.errors) == 0
    
    def _validate_global_config(self):
        """Validate global configuration keys"""
        for key in self.REQUIRED_GLOBAL_KEYS:
            if key not in self.config:
                self.errors.append(f"Missing required global key: {key}")
        
        # Validate thread count
        threads = self.config.get('parallel_threads', 0)
        if not isinstance(threads, int) or threads < 1:
            self.errors.append(f"parallel_threads must be >= 1, got: {threads}")
        elif threads > 20:
            self.warnings.append(f"parallel_threads is very high ({threads}), may cause connection issues")
        
        # Validate batch size
        batch_size = self.config.get('batch_size', 0)
        if not isinstance(batch_size, int) or batch_size < 100:
            self.errors.append(f"batch_size must be >= 100, got: {batch_size}")
        elif batch_size > 100000:
            self.warnings.append(f"batch_size is very large ({batch_size}), may cause memory issues")
    
    def _validate_sources(self):
        """Validate sources configuration"""
        sources = self.config.get('sources', [])
        
        if not sources:
            self.errors.append("No sources defined")
            return
        
        if not isinstance(sources, list):
            self.errors.append("sources must be a list")
            return
        
        enabled_sources = [s for s in sources if s.get('enabled', False)]
        if not enabled_sources:
            self.warnings.append("No enabled sources found")
        
        for idx, source in enumerate(sources):
            self._validate_source(idx, source)
    
    def _validate_source(self, idx: int, source: Dict[str, Any]):
        """Validate a single source configuration"""
        source_name = source.get('source_name', f'source[{idx}]')
        
        # Check required keys
        for key in self.REQUIRED_SOURCE_KEYS:
            if key not in source:
                self.errors.append(f"Source '{source_name}': Missing required key '{key}'")
        
        # Validate tables
        tables = source.get('tables', [])
        if not isinstance(tables, list):
            self.errors.append(f"Source '{source_name}': tables must be a list")
            return
        
        if not tables:
            self.warnings.append(f"Source '{source_name}': No tables defined")
            return
        
        enabled_tables = [t for t in tables if t.get('enabled', False)]
        if source.get('enabled') and not enabled_tables:
            self.warnings.append(f"Source '{source_name}': Enabled but no enabled tables")
        
        for table_idx, table in enumerate(tables):
            self._validate_table(source_name, table_idx, table)
    
    def _validate_table(self, source_name: str, idx: int, table: Dict[str, Any]):
        """Validate a single table configuration"""
        table_name = table.get('source', f'table[{idx}]')
        
        # Check required keys
        for key in self.REQUIRED_TABLE_KEYS:
            if key not in table:
                self.errors.append(
                    f"Source '{source_name}', Table '{table_name}': Missing required key '{key}'"
                )
        
        # Validate uniqueness_columns and chunking_columns
        uniqueness_cols = table.get('uniqueness_columns')
        if uniqueness_cols is not None:
            if not isinstance(uniqueness_cols, list):
                self.errors.append(
                    f"Source '{source_name}', Table '{table_name}': "
                    f"uniqueness_columns must be a list or null"
                )
            elif len(uniqueness_cols) == 0:
                self.warnings.append(
                    f"Source '{source_name}', Table '{table_name}': "
                    f"uniqueness_columns is empty (consider setting to null)"
                )
        
        chunking_cols = table.get('chunking_columns')
        chunking_types = table.get('chunking_column_types')
        
        if chunking_cols is not None:
            if not isinstance(chunking_cols, list):
                self.errors.append(
                    f"Source '{source_name}', Table '{table_name}': "
                    f"chunking_columns must be a list or null"
                )
            elif chunking_types is not None:
                if not isinstance(chunking_types, list):
                    self.errors.append(
                        f"Source '{source_name}', Table '{table_name}': "
                        f"chunking_column_types must be a list or null"
                    )
                elif len(chunking_cols) != len(chunking_types):
                    self.errors.append(
                        f"Source '{source_name}', Table '{table_name}': "
                        f"chunking_columns and chunking_column_types must have same length"
                    )
        
        # Validate watermark columns
        source_watermark = table.get('source_watermark')
        target_watermark = table.get('target_watermark')
        
        if (source_watermark is None) != (target_watermark is None):
            self.warnings.append(
                f"Source '{source_name}', Table '{table_name}': "
                f"Both source_watermark and target_watermark should be set or both null"
            )


def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate configuration and return True if valid.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if configuration is valid, False otherwise
    """
    validator = ConfigValidator(config)
    return validator.validate()

