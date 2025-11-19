"""
Test database connection.

Simple script to verify that Postgres connection is working correctly.

Usage:
    python scripts/test_connections.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import POSTGRES_CONFIG, CONFLICT_SCHEMA, ANALYTICS_SCHEMA, validate_config
from src.connectors.postgres_connector import PostgresConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """Main function to test Postgres connection."""
    logger.info("=" * 60)
    logger.info("Testing Postgres Connection")
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

    # Test Postgres
    logger.info("2. Testing Postgres connection...")
    logger.info(f"   Host: {POSTGRES_CONFIG['host']}")
    logger.info(f"   Database: {POSTGRES_CONFIG['database']}")
    logger.info(f"   Conflict Schema: {CONFLICT_SCHEMA}")
    logger.info(f"   Analytics Schema: {ANALYTICS_SCHEMA}")
    
    pg_ok = False
    try:
        pg_connector = PostgresConnector(**POSTGRES_CONFIG)
        if pg_connector.test_connection():
            # Try a simple query
            df = pg_connector.fetch_dataframe("SELECT version()")
            version_line = str(df.iloc[0, 0]).split('\n')[0]
            logger.info(f"   {version_line}")
            logger.info("Postgres connected\n")
            
            # Verify schemas exist
            logger.info("3. Verifying schemas...")
            schema_query = f"""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('{CONFLICT_SCHEMA}', '{ANALYTICS_SCHEMA}')
            """
            schema_df = pg_connector.fetch_dataframe(schema_query)
            
            if len(schema_df) == 2:
                logger.info(f"   PASS: Schema '{CONFLICT_SCHEMA}' exists")
                logger.info(f"   PASS: Schema '{ANALYTICS_SCHEMA}' exists\n")
                pg_ok = True
            else:
                existing_schemas = schema_df['schema_name'].tolist()
                logger.warning(f"   Found schemas: {existing_schemas}")
                if CONFLICT_SCHEMA not in existing_schemas:
                    logger.error(f"   FAIL: Schema '{CONFLICT_SCHEMA}' not found")
                if ANALYTICS_SCHEMA not in existing_schemas:
                    logger.error(f"   FAIL: Schema '{ANALYTICS_SCHEMA}' not found\n")
        else:
            logger.error("Postgres connection test failed\n")
    except Exception as e:
        logger.error(f"Postgres error: {e}\n")
    
    # Summary
    logger.info("=" * 60)
    if pg_ok:
        logger.info("CONNECTION SUCCESSFUL")
        logger.info("=" * 60)
        return 0
    else:
        logger.error("CONNECTION FAILED")
        logger.info("=" * 60)
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

