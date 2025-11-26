"""
AWS Lambda handler for ETL Pipeline.
Can run both in AWS Lambda and locally for testing.

Usage in Lambda:
    Event: {"action": "validate_config" | "task_01" | "task_02"}

Usage locally:
    python scripts/lambda_handler.py task_01
"""

import sys
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import POSTGRES_CONFIG, SNOWFLAKE_CONFIG, get_snowflake_config, validate_config, validate_snowflake_config
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.snowflake_connector import SnowflakeConnector
from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
from src.utils.logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    """
    Main AWS Lambda handler.
    
    Args:
        event: Lambda event containing action to perform
            {
                "action": "validate_config" | "test_postgres" | "test_snowflake" | "task_01" | "task_02",
                "use_mock": false  # Optional: for testing
            }
        context: Lambda context (unused in local testing)
    
    Returns:
        Response dictionary with status and results
    """
    action = event.get('action')
    use_mock = event.get('use_mock', False)
    
    logger.info(f"Lambda invoked with action: {action}")
    
    try:
        # Action: Validate Configuration
        if action == 'validate_config':
            logger.info("Validating configuration...")
            validate_config()
            logger.info("Configuration validation successful")
            
            return {
                'statusCode': 200,
                'body': {
                    'status': 'success',
                    'message': 'Configuration validated successfully',
                    'action': action,
                    'config': {
                        'host': POSTGRES_CONFIG.get('host'),
                        'port': POSTGRES_CONFIG.get('port'),
                        'database': POSTGRES_CONFIG.get('database'),
                        'user': POSTGRES_CONFIG.get('user')
                    }
                }
            }
        
        # Action: Test Postgres Connection
        elif action == 'test_postgres':
            logger.info("Testing Postgres connection...")
            
            try:
                connector = PostgresConnector(**POSTGRES_CONFIG)
                connection_successful = connector.test_connection()
                
                if connection_successful:
                    # Get additional info
                    with connector.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT version()")
                            db_version = cursor.fetchone()[0]
                            
                            cursor.execute("SELECT current_database()")
                            current_db = cursor.fetchone()[0]
                            
                            cursor.execute("SELECT current_user")
                            current_user = cursor.fetchone()[0]
                    
                    logger.info("Postgres connection test successful")
                    return {
                        'statusCode': 200,
                        'body': {
                            'status': 'success',
                            'message': 'Postgres connection successful',
                            'action': action,
                            'details': {
                                'host': POSTGRES_CONFIG['host'],
                                'port': POSTGRES_CONFIG['port'],
                                'database': current_db,
                                'user': current_user,
                                'version': db_version[:100]  # Truncate long version string
                            }
                        }
                    }
                else:
                    logger.error("Postgres connection test failed")
                    return {
                        'statusCode': 500,
                        'body': {
                            'status': 'error',
                            'message': 'Postgres connection failed',
                            'action': action
                        }
                    }
            
            except Exception as e:
                logger.error(f"Postgres connection test failed: {str(e)}", exc_info=True)
                return {
                    'statusCode': 500,
                    'body': {
                        'status': 'error',
                        'message': f'Postgres connection failed: {str(e)}',
                        'action': action,
                        'error_type': type(e).__name__
                    }
                }
        
        # Action: Test Snowflake Connection
        elif action == 'test_snowflake':
            logger.info("Testing Snowflake connection...")
            
            try:
                # Validate Snowflake config first
                validate_snowflake_config()
                
                # Get Snowflake config with lazy initialization (includes private_key if needed)
                sf_config = get_snowflake_config()
                
                # Build connector config (remove None values)
                sf_config = {k: v for k, v in sf_config.items() if v is not None}
                connector = SnowflakeConnector(**sf_config)
                connection_successful = connector.test_connection()
                
                if connection_successful:
                    # Get additional info
                    with connector.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT CURRENT_VERSION()")
                            sf_version = cursor.fetchone()[0]
                            
                            cursor.execute("SELECT CURRENT_DATABASE()")
                            current_db = cursor.fetchone()[0]
                            
                            cursor.execute("SELECT CURRENT_USER()")
                            current_user = cursor.fetchone()[0]
                            
                            cursor.execute("SELECT CURRENT_WAREHOUSE()")
                            current_warehouse = cursor.fetchone()[0]
                            
                            cursor.execute("SELECT CURRENT_ROLE()")
                            current_role = cursor.fetchone()[0]
                    
                    logger.info("Snowflake connection test successful")
                    return {
                        'statusCode': 200,
                        'body': {
                            'status': 'success',
                            'message': 'Snowflake connection successful',
                            'action': action,
                            'details': {
                                'account': SNOWFLAKE_CONFIG['account'],
                                'warehouse': current_warehouse,
                                'database': current_db,
                                'schema': SNOWFLAKE_CONFIG['schema'],
                                'user': current_user,
                                'role': current_role,
                                'version': sf_version
                            }
                        }
                    }
                else:
                    logger.error("Snowflake connection test failed")
                    return {
                        'statusCode': 500,
                        'body': {
                            'status': 'error',
                            'message': 'Snowflake connection failed',
                            'action': action
                        }
                    }
            
            except Exception as e:
                logger.error(f"Snowflake connection test failed: {str(e)}", exc_info=True)
                return {
                    'statusCode': 500,
                    'body': {
                        'status': 'error',
                        'message': f'Snowflake connection failed: {str(e)}',
                        'action': action,
                        'error_type': type(e).__name__
                    }
                }
        
        # Action: Execute Task 01
        elif action == 'task_01':
            logger.info("Executing Task 01: Copy to Temp")
            
            # Use mock connector for testing if requested
            if use_mock:
                from scripts.mock_postgres_connector import MockPostgresConnector
                connector = MockPostgresConnector(**POSTGRES_CONFIG)
                logger.info("Using MOCK connector (no real database)")
            else:
                connector = PostgresConnector(**POSTGRES_CONFIG)
                logger.info(f"Connected to database: {POSTGRES_CONFIG['database']}")
            
            # Create and run task
            task = Task01CopyToTemp(connector)
            result = task.run()
            
            # Prepare response
            if result['status'] == 'success':
                logger.info(f"Task 01 completed successfully in {result['duration_seconds']:.2f}s")
                return {
                    'statusCode': 200,
                    'body': result
                }
            else:
                logger.error(f"Task 01 failed: {result.get('error')}")
                return {
                    'statusCode': 500,
                    'body': result
                }
        
        # Action: Execute Task 02
        elif action == 'task_02':
            logger.info("Executing Task 02: Update Conflicts")
            
            # Use mock connector for testing if requested
            if use_mock:
                from scripts.mock_postgres_connector import MockPostgresConnector
                connector = MockPostgresConnector(**POSTGRES_CONFIG)
                logger.info("Using MOCK connector (no real database)")
            else:
                connector = PostgresConnector(**POSTGRES_CONFIG)
                logger.info(f"Connected to database: {POSTGRES_CONFIG['database']}")
            
            # Create and run task
            task = Task02UpdateConflictVisitMaps(connector)
            result = task.run()
            
            # Prepare response
            if result['status'] == 'success':
                logger.info(f"Task 02 completed successfully in {result['duration_seconds']:.2f}s")
                return {
                    'statusCode': 200,
                    'body': result
                }
            else:
                logger.error(f"Task 02 failed: {result.get('error')}")
                return {
                    'statusCode': 500,
                    'body': result
                }
        
        # Unknown action
        else:
            error_msg = f"Unknown action: {action}"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': {
                    'status': 'error',
                    'error': error_msg,
                    'message': 'Valid actions: validate_config, test_postgres, test_snowflake, task_01, task_02'
                }
            }
    
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'status': 'error',
                'error': str(e),
                'action': action
            }
        }


def main():
    """
    Main function for local testing.
    Allows running Lambda handler from command line.
    """
    # Parse command line arguments
    if len(sys.argv) > 1:
        action = sys.argv[1]
    else:
        print("Usage: python scripts/lambda_handler.py <action>")
        print("Actions: validate_config, test_postgres, test_snowflake, task_01, task_02")
        sys.exit(1)
    
    # Check for mock flag
    use_mock = '--mock' in sys.argv
    
    # Create event
    event = {
        'action': action,
        'use_mock': use_mock
    }
    
    print("=" * 70)
    print("LAMBDA HANDLER - LOCAL EXECUTION")
    print("=" * 70)
    print(f"Action: {action}")
    print(f"Mock Mode: {use_mock}")
    print("=" * 70)
    print()
    
    # Execute handler
    start_time = time.time()
    result = lambda_handler(event, None)
    duration = time.time() - start_time
    
    # Display results
    print()
    print("=" * 70)
    print("EXECUTION RESULT")
    print("=" * 70)
    print(f"Status Code: {result['statusCode']}")
    print(f"Duration: {duration:.2f}s")
    print()
    print("Response Body:")
    print(json.dumps(result['body'], indent=2, default=str))
    print("=" * 70)
    
    # Exit with appropriate code
    sys.exit(0 if result['statusCode'] == 200 else 1)


if __name__ == '__main__':
    main()

