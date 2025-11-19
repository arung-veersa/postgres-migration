"""
Script to run TASK_01: Copy Data to Temp.

Usage:
    python scripts/run_task_01.py

This script:
1. Validates configuration
2. Tests Postgres connection
3. Executes TASK_01 (SQL-based)
4. Reports results
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import POSTGRES_CONFIG, validate_config
from src.connectors.postgres_connector import PostgresConnector
from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("TASK_01 Runner")
    logger.info("=" * 80)
    
    # Step 1: Validate configuration
    logger.info("Step 1: Validating configuration...")
    try:
        validate_config()
        logger.info("Configuration valid")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    
    # Step 2: Initialize Postgres connector
    logger.info("Step 2: Initializing Postgres connector...")
    try:
        pg_connector = PostgresConnector(**POSTGRES_CONFIG)
        logger.info(f"Connected to Postgres: {POSTGRES_CONFIG['database']}")
    except Exception as e:
        logger.error(f"Failed to initialize connector: {e}")
        return 1
    
    # Step 3: Test Postgres connection
    logger.info("Step 3: Testing Postgres connection...")
    
    pg_ok = pg_connector.test_connection()
    
    if not pg_ok:
        logger.error("Postgres connection test failed")
        return 1
    
    logger.info("Postgres connection successful")
    
    # Step 4: Execute TASK_01
    logger.info("Step 4: Executing TASK_01...")
    try:
        task = Task01CopyToTemp(pg_connector)
        result = task.run()
        
        if result['status'] == 'success':
            logger.info("=" * 60)
            logger.info(f"TASK_01 completed successfully")
            logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
            logger.info(f"Total rows affected: {result['result']['affected_rows']}")
            logger.info("=" * 60)
            
            return 0
        else:
            logger.error("=" * 80)
            logger.error("TASK_01 failed")
            logger.error("=" * 80)
            logger.error(f"Error: {result.get('error', 'Unknown error')}")
            logger.error("=" * 80)
            
            return 1
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

