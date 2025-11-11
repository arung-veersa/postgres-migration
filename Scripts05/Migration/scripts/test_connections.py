"""
Test database connections.

Simple script to verify that both Snowflake and Postgres
connections are working correctly.

Usage:
    python scripts/test_connections.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import SNOWFLAKE_CONFIG, POSTGRES_CONFIG, validate_config
from src.connectors.snowflake_connector import SnowflakeConnector
from src.connectors.postgres_connector import PostgresConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """Main function to test connections."""
    logger.info("=" * 60)
    logger.info("Testing Database Connections")
    logger.info("=" * 60)
    logger.info("\n1. Validating configuration...")

    try:
        validate_config()
        logger.info("Configuration valid\n")
        config_ok = True
    except ValueError as e:
        logger.error(f"Configuration error: {e}\n")
        config_ok = False

    if not config_ok:
        logger.error("Aborting due to invalid configuration.")
        return 1

    # Test Snowflake
    logger.info("2. Testing Snowflake connection...")
    logger.info(f"   Account: {SNOWFLAKE_CONFIG['account']}")
    logger.info(f"   Database: {SNOWFLAKE_CONFIG['database']}")
    logger.info(f"   Schema: {SNOWFLAKE_CONFIG['schema']}")
    
    sf_ok = False  # Default to False
    try:
        sf_connector = SnowflakeConnector(**SNOWFLAKE_CONFIG)
        if sf_connector.test_connection():
            # Try a simple query
            df = sf_connector.fetch_dataframe("SELECT CURRENT_VERSION()")
            version = df.iloc[0][0]
            logger.info(f"Snowflake connected (Version: {version})\n")
            sf_ok = True
        else:
            logger.error("Snowflake connection test failed\n")
    except Exception as e:
        logger.error(f"Snowflake error: {e}\n")
    
    # Test Postgres
    logger.info("3. Testing Postgres connection...")
    logger.info(f"   Host: {POSTGRES_CONFIG['host']}")
    logger.info(f"   Database: {POSTGRES_CONFIG['database']}")
    
    pg_ok = False  # Default to False
    try:
        pg_connector = PostgresConnector(**POSTGRES_CONFIG)
        if pg_connector.test_connection():
            # Try a simple query
            df = pg_connector.fetch_dataframe("SELECT version()")
            version_line = df.iloc[0][0].split('\n')[0]
            logger.info(f"   {version_line}")
            logger.info("Postgres connected\n")
            pg_ok = True
        else:
            logger.error("Postgres connection test failed\n")
    except Exception as e:
        logger.error(f"Postgres error: {e}\n")
    
    # Summary
    logger.info("=" * 60)
    if sf_ok and pg_ok:
        logger.info("ALL CONNECTIONS SUCCESSFUL")
        logger.info("=" * 60)
        return 0
    else:
        logger.error("SOME CONNECTIONS FAILED")
        logger.info("=" * 60)
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

