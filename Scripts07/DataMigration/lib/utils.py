"""
Utilities Module
Logging setup, helper functions, and common utilities
"""

import logging
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional
from functools import wraps


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """
    Setup structured logging for the migration process
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional file path for logging
    
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger("migration")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers = []
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(detailed_formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
    
    return logger


def format_number(num: int) -> str:
    """Format number with thousand separators"""
    return f"{num:,}"


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_bytes(bytes_count: int) -> str:
    """Format bytes in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.2f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.2f} PB"


def timing_decorator(func):
    """Decorator to measure function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        
        logger = logging.getLogger("migration")
        logger.debug(f"{func.__name__} took {format_duration(duration)}")
        
        return result
    return wrapper


def quote_identifier(identifier: str) -> str:
    """
    Quote SQL identifier with double quotes for case preservation
    
    Args:
        identifier: Column or table name
    
    Returns:
        Quoted identifier
    """
    # Escape any existing double quotes
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def build_where_clause(filters: list) -> str:
    """
    Build WHERE clause from list of filter conditions
    
    Args:
        filters: List of SQL filter conditions
    
    Returns:
        Complete WHERE clause
    """
    if not filters:
        return ""
    
    valid_filters = [f for f in filters if f and f.strip()]
    if not valid_filters:
        return ""
    
    return "WHERE " + " AND ".join(f"({f})" for f in valid_filters)


def safe_table_name(schema: str, table: str, uppercase: bool = False) -> str:
    """
    Build safe fully-qualified table name
    
    Args:
        schema: Schema name
        table: Table name
        uppercase: Whether to uppercase table name (for Snowflake)
    
    Returns:
        Fully qualified table name (schema.table)
    """
    if uppercase:
        return f"{schema}.{table.upper()}"
    return f"{schema}.{table.lower()}"


def get_column_list_sql(columns: list, quote: bool = True) -> str:
    """
    Build comma-separated column list for SQL
    
    Args:
        columns: List of column names
        quote: Whether to quote identifiers
    
    Returns:
        Comma-separated quoted column list
    """
    if quote:
        return ", ".join(quote_identifier(col) for col in columns)
    return ", ".join(columns)


def chunk_list(items: list, chunk_size: int) -> list:
    """
    Split list into chunks
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
    
    Returns:
        List of chunks
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


class Timer:
    """Context manager for timing operations"""
    
    def __init__(self, description: str = "Operation", logger: Optional[logging.Logger] = None):
        self.description = description
        self.logger = logger or logging.getLogger("migration")
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.info(f"Starting: {self.description}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if exc_type is None:
            self.logger.info(f"✓ Completed: {self.description} in {format_duration(duration)}")
        else:
            self.logger.error(f"✗ Failed: {self.description} after {format_duration(duration)}")
        
        return False  # Don't suppress exceptions
    
    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds"""
        if self.start_time is None:
            return 0
        end = self.end_time if self.end_time else time.time()
        return end - self.start_time


class ProgressTracker:
    """Track and display progress of long-running operations"""
    
    def __init__(self, total: int, description: str = "Progress", logger: Optional[logging.Logger] = None):
        self.total = total
        self.description = description
        self.logger = logger or logging.getLogger("migration")
        self.current = 0
        self.start_time = time.time()
        self.last_log_time = self.start_time
        self.log_interval = 5  # Log every 5 seconds
    
    def update(self, amount: int = 1):
        """Update progress by amount"""
        self.current += amount
        current_time = time.time()
        
        # Log if enough time has passed or if complete
        if (current_time - self.last_log_time >= self.log_interval) or (self.current >= self.total):
            self._log_progress()
            self.last_log_time = current_time
    
    def _log_progress(self):
        """Log current progress"""
        if self.total == 0:
            percentage = 100
        else:
            percentage = (self.current / self.total) * 100
        
        elapsed = time.time() - self.start_time
        
        if self.current > 0 and elapsed > 0:
            rate = self.current / elapsed
            remaining = (self.total - self.current) / rate if rate > 0 else 0
            eta_str = f", ETA: {format_duration(remaining)}" if remaining > 0 else ""
        else:
            eta_str = ""
        
        self.logger.info(
            f"{self.description}: {self.current}/{self.total} "
            f"({percentage:.1f}%) - {format_duration(elapsed)} elapsed{eta_str}"
        )
    
    def complete(self):
        """Mark as complete and log final stats"""
        self.current = self.total
        elapsed = time.time() - self.start_time
        rate = self.total / elapsed if elapsed > 0 else 0
        
        self.logger.info(
            f"✓ {self.description} completed: {format_number(self.total)} items "
            f"in {format_duration(elapsed)} ({rate:.1f} items/sec)"
        )


def sanitize_for_logging(value: Any, max_length: int = 100) -> str:
    """
    Sanitize value for safe logging (truncate long strings, hide sensitive data)
    
    Args:
        value: Value to sanitize
        max_length: Maximum length for string values
    
    Returns:
        Sanitized string representation
    """
    if value is None:
        return "NULL"
    
    str_value = str(value)
    
    # Truncate long strings
    if len(str_value) > max_length:
        return str_value[:max_length] + "..."
    
    return str_value


# Global logger instance
logger = setup_logging()

