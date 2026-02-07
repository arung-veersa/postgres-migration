"""
AWS Lambda Handler for Task 02 Conflict Detection and Update
Cross-database operation between Snowflake and PostgreSQL

Entry point for AWS Lambda or local execution
"""

import sys
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.query_builder import QueryBuilder
from lib.conflict_processor import ConflictProcessor
from lib.utils import get_logger, format_duration

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Optional[Any] = None) -> Dict[str, Any]:
    """
    Main AWS Lambda handler for Task 02 conflict detection and update
    
    Args:
        event: Lambda event containing:
            {
                "action": "task02_00_run_conflict_update" | "test_connections" | "validate_config",
                "lookback_hours": 36 (optional override),
                "lookback_years": 2 (optional override),
                "lookforward_days": 45 (optional override),
                "batch_size": 5000 (optional override)
            }
        context: Lambda context (for timeout detection)
    
    Returns:
        Response dictionary with status and results
    """
    start_time = time.time()
    action = event.get('action', 'task02_00_run_conflict_update')
    
    logger.info("=" * 70)
    logger.info("TASK 02 CONFLICT UPDATER - LAMBDA INVOCATION")
    logger.info("=" * 70)
    logger.info(f"Action: {action}")
    logger.info(f"Event: {json.dumps(event, default=str)}")
    
    # Check remaining time if Lambda context provided
    remaining_time_ms = None
    if context:
        remaining_time_ms = context.get_remaining_time_in_millis()
        logger.info(f"Lambda timeout: {remaining_time_ms / 1000:.1f}s remaining")
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        settings = Settings()
        sf_config = settings.get_snowflake_config()
        pg_config = settings.get_postgres_config()
        task_params = settings.get_task02_parameters()
        db_names = settings.get_database_names()
        
        logger.info(f"  Snowflake: {sf_config['account']} / {db_names['sf_database']}.{db_names['sf_schema']}")
        logger.info(f"  Postgres: {pg_config['host']} / {db_names['pg_database']}.{db_names['pg_schema']}")
        
        # Action: Validate Configuration
        if action == 'validate_config':
            logger.info("Validating configuration...")
            
            return {
                'statusCode': 200,
                'body': {
                    'status': 'success',
                    'message': 'Configuration validated successfully',
                    'action': action,
                    'databases': db_names,
                    'task_parameters': task_params
                }
            }
        
        # Action: Test Connections
        elif action == 'test_connections':
            logger.info("Testing database connections...")
            
            # Initialize connection factory
            conn_factory = ConnectionFactory(sf_config, pg_config)
            
            try:
                # Test Snowflake with simple query
                sf_manager = conn_factory.get_snowflake_manager()
                sf_conn = sf_manager.get_connection()
                cursor = sf_conn.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                sf_version = cursor.fetchone()[0]
                cursor.close()
                logger.info(f"✓ Snowflake connection successful: {sf_version}")
                
                # Test PostgreSQL
                pg_manager = conn_factory.get_postgres_manager()
                pg_conn = pg_manager.get_connection(db_names['pg_database'])
                cursor = pg_conn.cursor()
                cursor.execute("SELECT version()")
                pg_version = cursor.fetchone()[0]
                cursor.close()
                pg_conn.close()
                logger.info(f"✓ PostgreSQL connection successful")
                
                # Test reference tables accessibility
                logger.info("Testing reference table access...")
                
                # List available tables in the schema
                pg_conn = pg_manager.get_connection(db_names['pg_database'])
                cursor = pg_conn.cursor()
                
                # Check what tables exist in the schema
                list_tables_query = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """
                cursor.execute(list_tables_query, (db_names['pg_schema'],))
                available_tables = [row[0] for row in cursor.fetchall()]
                logger.info(f"Available tables in schema '{db_names['pg_schema']}': {', '.join(available_tables) if available_tables else 'None'}")
                
                # Try to find and test reference tables
                reference_tables = {}
                # Expected table names in lowercase (PostgreSQL convention)
                test_tables = {
                    'SETTINGS': 'settings',
                    'EXCLUDED_AGENCY': 'excluded_agency', 
                    'EXCLUDED_SSN': 'excluded_ssn',
                    'MPH': 'mph',
                    'CONFLICTVISITMAPS': 'conflictvisitmaps'
                }
                
                for display_name, table_name in test_tables.items():
                    if table_name in available_tables:
                        try:
                            # Use unquoted table names (PostgreSQL will treat as lowercase)
                            count_query = f'SELECT COUNT(*) FROM {db_names["pg_schema"]}.{table_name}'
                            cursor.execute(count_query)
                            count = cursor.fetchone()[0]
                            reference_tables[display_name] = {'name': table_name, 'count': count}
                            logger.info(f"✓ {display_name} table accessible as '{table_name}': {count} row(s)")
                        except Exception as table_err:
                            logger.warning(f"Found {table_name} but couldn't query it: {table_err}")
                    else:
                        logger.warning(f"✗ {display_name} table (expected as '{table_name}') not found")
                
                cursor.close()
                pg_conn.close()
                
                conn_factory.close_all()
                
                return {
                    'statusCode': 200,
                    'body': {
                        'status': 'success',
                        'message': 'All connections tested successfully',
                        'action': action,
                        'snowflake': {
                            'connected': True,
                            'version': sf_version,
                            'database': db_names['sf_database'],
                            'schema': db_names['sf_schema']
                        },
                        'postgres': {
                            'connected': True,
                            'version': pg_version[:80],
                            'database': db_names['pg_database'],
                            'schema': db_names['pg_schema'],
                            'available_tables': available_tables
                        },
                        'reference_tables': reference_tables
                    }
                }
            
            except Exception as e:
                logger.error(f"Connection test failed: {e}", exc_info=True)
                conn_factory.close_all()
                return {
                    'statusCode': 500,
                    'body': {
                        'status': 'error',
                        'message': f'Connection test failed: {str(e)}',
                        'action': action,
                        'error_type': type(e).__name__
                    }
                }
        
        # Action: Execute Conflict Update
        elif action == 'task02_00_run_conflict_update':
            logger.info("Executing conflict detection and update...")
            
            # Get parameters (allow event overrides)
            lookback_hours = event.get('lookback_hours', task_params.get('lookback_hours', 36))
            lookback_years = event.get('lookback_years', task_params.get('lookback_years', 2))
            lookforward_days = event.get('lookforward_days', task_params.get('lookforward_days', 45))
            batch_size = event.get('batch_size', task_params.get('batch_size', 5000))
            timeout_buffer = task_params.get('timeout_buffer_seconds', 90)
            skip_unchanged_records = event.get('skip_unchanged_records', task_params.get('skip_unchanged_records', True))
            enable_asymmetric_join = event.get('enable_asymmetric_join', task_params.get('enable_asymmetric_join', True))
            enable_stale_cleanup = event.get('enable_stale_cleanup', task_params.get('enable_stale_cleanup', True))
            
            logger.info("Configuration settings:")
            logger.info(f"  Lookback: {lookback_years} years, +{lookforward_days} days")
            logger.info(f"  Updates: last {lookback_hours} hours")
            logger.info(f"  Batch size: {batch_size}")
            logger.info(f"  Skip unchanged records: {'YES' if skip_unchanged_records else 'NO'}")
            logger.info(f"  Asymmetric join: {'ENABLED' if enable_asymmetric_join else 'DISABLED'}")
            logger.info(f"  Stale cleanup: {'ENABLED' if enable_stale_cleanup else 'DISABLED'}")
            
            # Initialize connections
            conn_factory = ConnectionFactory(sf_config, pg_config)
            sf_manager = conn_factory.get_snowflake_manager()
            pg_manager = conn_factory.get_postgres_manager()
            
            # Initialize query builder and processor
            query_builder = QueryBuilder()
            processor = ConflictProcessor(
                sf_manager, pg_manager, query_builder, db_names, batch_size,
                skip_unchanged_records=skip_unchanged_records,
                enable_asymmetric_join=enable_asymmetric_join,
                enable_stale_cleanup=enable_stale_cleanup
            )
            
            # Step 1: Fetch reference data from Postgres
            ref_data = processor.fetch_reference_data()
            
            # Step 2: Build conflict detection query
            conflict_query = query_builder.build_conflict_detection_query(
                db_names=db_names,
                excluded_agencies=ref_data['excluded_agencies'],
                excluded_ssns=ref_data['excluded_ssns'],
                settings_data=ref_data['settings'],
                mph_data=ref_data['mph'],
                lookback_years=lookback_years,
                lookforward_days=lookforward_days,
                lookback_hours=lookback_hours,
                enable_asymmetric_join=enable_asymmetric_join
            )
            
            # Step 3: Stream and process conflicts with timeout callback
            def check_timeout():
                if context:
                    remaining = context.get_remaining_time_in_millis() / 1000
                    return remaining < timeout_buffer
                return False
            
            stats = processor.stream_and_process_conflicts(
                conflict_query,
                timeout_callback=check_timeout
            )
            
            # Close connections
            conn_factory.close_all()
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Log summary with duration
            processor.log_summary(duration)
            logger.info(f"Total execution time: {format_duration(duration)}")
            
            # Determine status
            status = 'completed' if stats['errors'] == 0 else 'partial'
            
            return {
                'statusCode': 200 if status == 'completed' else 500,
                'body': {
                    'status': status,
                    'action': action,
                    'statistics': stats,
                    'duration_seconds': duration,
                    'parameters': {
                        'lookback_hours': lookback_hours,
                        'lookback_years': lookback_years,
                        'lookforward_days': lookforward_days,
                        'batch_size': batch_size,
                        'skip_unchanged_records': skip_unchanged_records,
                        'enable_asymmetric_join': enable_asymmetric_join,
                        'enable_stale_cleanup': enable_stale_cleanup
                    }
                }
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
                    'message': 'Valid actions: task02_00_run_conflict_update, test_connections, validate_config'
                }
            }
    
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        
        return {
            'statusCode': 500,
            'body': {
                'status': 'error',
                'error': str(e),
                'action': action,
                'error_type': type(e).__name__,
                'duration_seconds': duration
            }
        }


def main():
    """
    Main function for local testing
    Allows running Lambda handler from command line
    """
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python scripts/lambda_handler.py <action> [lookback_hours]")
        print("Actions: task02_00_run_conflict_update, test_connections, validate_config")
        print("Example: python scripts/lambda_handler.py task02_00_run_conflict_update 36")
        sys.exit(1)
    
    action = sys.argv[1]
    
    # Build event
    event = {'action': action}
    
    if len(sys.argv) > 2:
        event['lookback_hours'] = int(sys.argv[2])
    
    print("=" * 70)
    print("LAMBDA HANDLER - LOCAL EXECUTION")
    print("=" * 70)
    print(f"Action: {action}")
    print(f"Event: {json.dumps(event, indent=2)}")
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
