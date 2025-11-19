"""
Configuration settings for the ETL pipeline.
Loads from environment variables.

Future: Can be easily swapped to load from AWS Secrets Manager
by only changing this file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
load_dotenv()

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# Environment
ENVIRONMENT = os.getenv('ENVIRONMENT', 'dev')

# Postgres Configuration (ConflictReport Database - Read/Write)
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE', 'conflictreport'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}

# Schema Configuration
CONFLICT_SCHEMA = os.getenv('POSTGRES_CONFLICT_SCHEMA', 'conflict')
ANALYTICS_SCHEMA = os.getenv('POSTGRES_ANALYTICS_SCHEMA', 'analytics')

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'logs/etl_pipeline.log')

# Date Range for Data Processing (from TASK_01)
DATE_RANGE_YEARS_BACK = 2
DATE_RANGE_DAYS_FORWARD = 45

# Batch Processing
DEFAULT_BATCH_SIZE = 10000
MAX_WORKERS = 6


def validate_config():
    """Validate that all required configuration is present."""
    required_postgres = ['host', 'database', 'user', 'password']
    
    missing = []
    
    for key in required_postgres:
        if not POSTGRES_CONFIG.get(key):
            missing.append(f'POSTGRES_{key.upper()}')
    
    if missing:
        raise ValueError(
            f"Missing required configuration: {', '.join(missing)}\n"
            f"Please set these in your .env file or environment variables."
        )


if __name__ == '__main__':
    # Test configuration
    try:
        validate_config()
        print("Configuration valid")
        print(f"Environment: {ENVIRONMENT}")
        print(f"Postgres DB: {POSTGRES_CONFIG['database']}")
        print(f"Conflict Schema: {CONFLICT_SCHEMA}")
        print(f"Analytics Schema: {ANALYTICS_SCHEMA}")
    except ValueError as e:
        print(f"Configuration error: {e}")

