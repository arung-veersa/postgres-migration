"""
Script to run TASK_02: Update ConflictVisitMaps

Usage:
    python scripts/run_task_02.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.connectors.postgres_connector import PostgresConnector
from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
from src.utils.logger import get_logger
from config.settings import validate_config, POSTGRES_CONFIG

logger = get_logger(__name__)


def main():
    """Run TASK_02."""
    logger.info("="*60)
    logger.info("TASK_02: Update ConflictVisitMaps")
    logger.info("="*60)
    
    # Validate configuration
    try:
        validate_config()
        logger.info("Configuration validated")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    # Initialize connectors
    pg_connector = None
    
    try:
        logger.info("Initializing database connection...")
        
        pg_connector = PostgresConnector(**POSTGRES_CONFIG)
        logger.info(f"Connected to Postgres: {POSTGRES_CONFIG['database']}")
        
        # Initialize and run task
        task = Task02UpdateConflictVisitMaps(pg_connector)
        result = task.run()
        
        # Display results
        logger.info("\n" + "="*60)
        logger.info("TASK_02 Results")
        logger.info("="*60)
        
        if result['status'] == 'success':
            logger.info("Task completed successfully")
            logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
            logger.info("\nDetails:")
            for key, value in result['result'].items():
                logger.info(f"  {key}: {value}")
            sys.exit(0)
        else:
            logger.error("Task failed")
            logger.error(f"Error: {result.get('error', 'Unknown error')}")
            logger.error(f"Duration: {result['duration_seconds']:.2f} seconds")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("\nTask interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)
        

if __name__ == '__main__':
    main()

