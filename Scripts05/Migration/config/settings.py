"""
Configuration settings for the ETL pipeline.
Loads from environment variables.

Future: Can be easily swapped to load from AWS Secrets Manager
by only changing this file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
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
    """
    if not rsa_key:
        return None
        
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

# Snowflake Configuration (Analytics Database - Read Only)
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'private_key': get_private_key(os.getenv('SNOWFLAKE_PRIVATE_KEY')),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'ANALYTICS'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'BI'),
    'role': os.getenv('SNOWFLAKE_ROLE'),
}

# Postgres Configuration (ConflictReport Database - Read/Write)
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE', 'conflictreport'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'schema': os.getenv('POSTGRES_SCHEMA', 'public'),
}

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'logs/etl_pipeline.log')

# Date Range for Data Processing (from TASK_01)
DATE_RANGE_YEARS_BACK = 2
DATE_RANGE_DAYS_FORWARD = 45

# Batch Processing
DEFAULT_BATCH_SIZE = 10000
MAX_WORKERS = 4


def validate_config():
    """Validate that all required configuration is present."""
    required_snowflake = ['account', 'user', 'database']
    required_postgres = ['host', 'database', 'user', 'password']
    
    missing = []
    
    for key in required_snowflake:
        if not SNOWFLAKE_CONFIG.get(key):
            missing.append(f'SNOWFLAKE_{key.upper()}')

    # Require password OR private_key, but not both
    password = SNOWFLAKE_CONFIG.get('password')
    private_key = SNOWFLAKE_CONFIG.get('private_key')

    if not password and not private_key:
        missing.append('SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY')
    elif password and private_key:
        raise ValueError("Provide either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY, not both.")
    
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
        print("✅ Configuration valid")
        print(f"Environment: {ENVIRONMENT}")
        print(f"Snowflake DB: {SNOWFLAKE_CONFIG['database']}")
        print(f"Postgres DB: {POSTGRES_CONFIG['database']}")
    except ValueError as e:
        print(f"❌ Configuration error: {e}")

