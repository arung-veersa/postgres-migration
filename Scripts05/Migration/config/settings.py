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


def get_private_key(rsa_key: str) -> Optional[bytes]:
    """
    Deserializes a PEM-formatted RSA private key string into DER format
    for the Snowflake connector.
    
    Args:
        rsa_key: PEM-formatted RSA private key string
        
    Returns:
        DER-encoded private key bytes or None
    """
    if not rsa_key:
        return None
    
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        
        private_key_prefix = '-----BEGIN PRIVATE KEY-----\n'
        private_key_suffix = '\n-----END PRIVATE KEY-----'
        
        # The key from .env might have literal \\n, so replace them with a real newline
        full_rsa_key = private_key_prefix + rsa_key.replace('\\n', '\n') + private_key_suffix
        
        p_key = serialization.load_pem_private_key(
            full_rsa_key.encode(),
            password=None,
            backend=default_backend()
        )
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    except ImportError:
        # cryptography not installed - will use password auth instead
        return None
    except Exception as e:
        print(f"Warning: Failed to parse RSA key: {e}")
        return None

# Postgres Configuration (ConflictReport Database - Read/Write)
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}

# Snowflake Configuration (Analytics Database - Read Only)
# Note: Lazy initialization to avoid loading cryptography when not needed
def get_snowflake_config() -> dict:
    """
    Get Snowflake configuration with lazy initialization.
    Only loads when actually needed, avoiding unnecessary imports.
    """
    return {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        #'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'private_key': get_private_key(os.getenv('SNOWFLAKE_PRIVATE_KEY', '')),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'ANALYTICS'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'BI'),
        #'role': os.getenv('SNOWFLAKE_ROLE'),
    }


# For backwards compatibility, provide SNOWFLAKE_CONFIG but with basic values
# The private_key will only be loaded when get_snowflake_config() is called
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    #'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'private_key': None,  # Will be set when needed
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'ANALYTICS'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'BI'),
    #'role': os.getenv('SNOWFLAKE_ROLE'),
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
            f"Missing required Postgres configuration: {', '.join(missing)}\n"
            f"Please set these in your .env file or environment variables."
        )


def validate_snowflake_config():
    """Validate that Snowflake configuration is present."""
    required_snowflake = ['account', 'user', 'warehouse', 'database', 'schema']
    
    missing = []
    
    for key in required_snowflake:
        if not SNOWFLAKE_CONFIG.get(key):
            missing.append(f'SNOWFLAKE_{key.upper()}')
    
    # Check for either password or private_key in environment variables (not in config dict)
    password = os.getenv('SNOWFLAKE_PASSWORD')
    private_key = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    
    if not password and not private_key:
        missing.append('SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY')
    
    if missing:
        raise ValueError(
            f"Missing required Snowflake configuration: {', '.join(missing)}\n"
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

