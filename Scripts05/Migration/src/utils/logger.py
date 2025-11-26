"""
Logging configuration for the ETL pipeline.
"""

import logging
import sys
import os
from pathlib import Path
from config.settings import LOG_LEVEL, LOG_FILE, PROJECT_ROOT

# Detect if running in Lambda
IS_LAMBDA = os.getenv('AWS_LAMBDA_FUNCTION_NAME') is not None

# Create logs directory if it doesn't exist (only for local/non-Lambda)
if IS_LAMBDA:
    # Use /tmp in Lambda (only writable directory)
    log_dir = Path('/tmp')
    log_file_path = log_dir / 'etl_pipeline.log'
else:
    # Use project logs directory locally
    log_dir = PROJECT_ROOT / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file_path = PROJECT_ROOT / LOG_FILE

# Configure logging format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(getattr(logging, LOG_LEVEL))
        
        # Console handler (always enabled - goes to CloudWatch in Lambda)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler (optional - only if writable)
        try:
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except (OSError, PermissionError):
            # Skip file handler if can't write (shouldn't happen with /tmp)
            pass
        
        # Prevent propagation to root logger
        logger.propagate = False
    
    return logger

