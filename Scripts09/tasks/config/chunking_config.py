"""
Chunking Configuration for Task 02
Strategy: Composite (VisitDate, SSN) with Idempotent Processing
"""

# Chunking parameters
CHUNKING_CONFIG = {
    # Target rows per chunk (aim for this size)
    'target_chunk_size': 10000,
    
    # Maximum rows per chunk (don't exceed this)
    'max_chunk_size': 15000,
    
    # Minimum rows per chunk (combine small chunks)
    'min_chunk_size': 1000,
    
    # Maximum number of (date, ssn) keys per chunk
    # Prevents SQL statement from becoming too large
    'max_keys_per_chunk': 5000,
    
    # Date range for processing (relative to NOW())
    'date_range': {
        'lookback_years': 2,
        'lookahead_days': 45
    },
    
    # Parallel execution settings (for Step Functions)
    'max_concurrency': 5,
    
    # Retry settings per chunk
    'retry': {
        'max_attempts': 2,
        'backoff_rate': 2.0,
        'interval_seconds': 10
    }
}

