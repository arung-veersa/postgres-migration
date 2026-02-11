"""
Utility functions and helpers for Task 02 Conflict Updater
"""

import logging
import sys
from typing import Optional


def get_logger(name: str, level: str = 'INFO') -> logging.Logger:
    """
    Get or create a logger with specified name and level
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(getattr(logging, level.upper()))
        
        # Console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, level.upper()))
        
        # Format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    
    return logger


def format_exclusion_list(items: list, quote_char: str = "'") -> str:
    """
    Format a list of items for SQL IN clause
    
    Args:
        items: List of items to format
        quote_char: Quote character to use (default: single quote)
    
    Returns:
        Comma-separated quoted string: 'item1','item2','item3'
    
    Example:
        >>> format_exclusion_list(['123', '456'])
        "'123','456'"
    """
    if not items:
        return "''"
    
    # Escape any quotes in the items
    escaped_items = [str(item).replace(quote_char, quote_char + quote_char) for item in items]
    
    return ','.join([f"{quote_char}{item}{quote_char}" for item in escaped_items])


def format_sql_identifier(identifier: str, quote_char: str = '"') -> str:
    """
    Format SQL identifier with quotes
    
    Args:
        identifier: SQL identifier (table, column, schema name)
        quote_char: Quote character (default: double quote for Snowflake/Postgres)
    
    Returns:
        Quoted identifier
    
    Example:
        >>> format_sql_identifier('Visit Date')
        '"Visit Date"'
    """
    # Escape any existing quotes
    escaped = identifier.replace(quote_char, quote_char + quote_char)
    return f"{quote_char}{escaped}{quote_char}"


def chunk_list(items: list, chunk_size: int):
    """
    Split list into chunks of specified size
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
    
    Yields:
        List chunks
    
    Example:
        >>> list(chunk_list([1,2,3,4,5], 2))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string
    
    Args:
        seconds: Duration in seconds
    
    Returns:
        Formatted string (e.g., "2m 30s", "1h 5m 15s")
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return ' '.join(parts)


def estimate_memory_mb(row_count: int, column_count: int, avg_bytes_per_cell: int = 50) -> float:
    """
    Estimate memory usage for result set
    
    Args:
        row_count: Number of rows
        column_count: Number of columns
        avg_bytes_per_cell: Average bytes per cell (default: 50)
    
    Returns:
        Estimated memory in MB
    """
    total_bytes = row_count * column_count * avg_bytes_per_cell
    # Add 50% overhead for Python objects
    total_bytes = total_bytes * 1.5
    return total_bytes / (1024 * 1024)
