"""
AWS Lambda handler for Snowflake to PostgreSQL Migration.
Can run both in AWS Lambda and locally for testing.

Usage in Lambda:
    Event: {
        "action": "migrate",
        "source_name": "analytics",              # Single source
        OR
        "source_name": "analytics,aggregator",   # Multiple sources (comma-separated)
        "no_resume": false,
        "resume_max_age": 24,
        "resume_run_id": "uuid"
    }

Usage locally:
    python scripts/lambda_handler.py migrate analytics
"""

import sys
import json
import time
import os
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.migration_orchestrator import run_migration
from lib.utils import get_logger

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    """
    Main AWS Lambda handler.
    
    Args:
        event: Lambda event containing migration parameters
            {
                "action": "migrate" | "validate_config" | "test_connections",
                "source_name": "analytics",              # Single source
                OR
                "source_name": "analytics,aggregator",   # Multiple (comma-separated)
                "no_resume": false,          # Optional
                "resume_max_age": 24,        # Optional (hours)
                "resume_run_id": "uuid"      # Optional (specific run to resume)
            }
        context: Lambda context (for timeout detection)
    
    Returns:
        Response dictionary with status and results
    """
    # Handle Step Functions passing nested input structure
    # Extract from either 'input' subobject (Step Functions) or directly (direct invocation)
    if 'input' in event and isinstance(event['input'], dict):
        # Called from Step Functions with nested structure
        input_data = event['input']
    else:
        # Direct Lambda invocation
        input_data = event
    
    action = event.get('action', 'migrate')
    source_name = input_data.get('source_name')
    
    # Extract optional parameters, checking defaults if not in input
    defaults = event.get('defaults', {})
    resume_max_age = input_data.get('resume_max_age') or defaults.get('resume_max_age', 12)
    
    logger.info(f"Lambda invoked with action: {action}, source: {source_name}")
    logger.info(f"Event structure - has 'input': {'input' in event}, keys: {list(event.keys())}")  # Debug
    
    logger.info(f"Lambda invoked with action: {action}, source: {source_name}")
    
    # Check remaining time (Lambda timeout detection)
    remaining_time_ms = None
    if context:
        remaining_time_ms = context.get_remaining_time_in_millis()
        logger.info(f"Lambda timeout: {remaining_time_ms / 1000:.1f}s remaining")
    
    try:
        # Action: Validate Configuration
        if action == 'validate_config':
            logger.info("Validating configuration...")
            from lib.config_loader import ConfigLoader
            
            config_loader = ConfigLoader('config.json')
            config = config_loader.load()
            
            # Basic validation
            enabled_sources = config_loader.get_enabled_sources()
            if not enabled_sources:
                raise ValueError("No enabled sources")
            
            logger.info(f"Configuration validated: {len(enabled_sources)} enabled sources")
            
            return {
                'statusCode': 200,
                'body': {
                    'status': 'success',
                    'message': 'Configuration validated successfully',
                    'action': action,
                    'sources': [s['source_name'] for s in enabled_sources]
                }
            }
        
        # Action: Test Connections
        elif action == 'test_connections':
            logger.info("Testing database connections...")
            
            try:
                # Load config and initialize connections
                from lib.config_loader import ConfigLoader
                from lib.connections import ConnectionFactory
                
                config_loader = ConfigLoader('config.json')
                config = config_loader.load()  # Get the dict, not the loader
                
                # Initialize connection factory with config dict
                conn_factory = ConnectionFactory(config)
                sf_manager = conn_factory.get_snowflake_manager()
                pg_manager = conn_factory.get_postgres_manager()
                
                # Test Snowflake
                sf_info = sf_manager.get_connection_info()
                logger.info(f"✓ Snowflake: {sf_info}")
                
                # Test PostgreSQL (get first target database)
                sources = config_loader.get_enabled_sources()
                if sources:
                    first_target_db = sources[0]['target_pg_database']
                    pg_conn = pg_manager.get_connection(first_target_db)
                    pg_conn.close()
                    logger.info("✓ PostgreSQL: Connected")
                
                return {
                    'statusCode': 200,
                    'body': {
                        'status': 'success',
                        'message': 'All connections successful',
                        'action': action,
                        'snowflake': sf_info
                    }
                }
            except Exception as e:
                logger.error(f"Connection test failed: {str(e)}", exc_info=True)
                return {
                    'statusCode': 500,
                    'body': {
                        'status': 'error',
                        'message': f'Connection test failed: {str(e)}',
                        'action': action,
                        'error_type': type(e).__name__
                    }
                }
        
        # Action: Execute Migration
        elif action == 'migrate':
            # If source_name not provided, default to all enabled sources
            if not source_name:
                logger.info("No source_name provided, defaulting to all enabled sources")
                from lib.config_loader import ConfigLoader
                
                config_loader = ConfigLoader('config.json')
                config_loader.load()  # Must load config before using it
                enabled_sources = config_loader.get_enabled_sources()
                
                if not enabled_sources:
                    error_msg = "No enabled sources found in config.json"
                    logger.error(error_msg)
                    return {
                        'statusCode': 400,
                        'body': {
                            'status': 'error',
                            'error': error_msg,
                            'message': 'No enabled sources in config.json. Enable at least one source.'
                        }
                    }
                
                source_names = [s['source_name'] for s in enabled_sources]
                logger.info(f"Found {len(source_names)} enabled source(s): {', '.join(source_names)}")
                # Convert list to comma-separated string for run_migration
                source_name = ','.join(source_names)
            else:
                # Parse source_name (supports comma-separated: "analytics,aggregator")
                if isinstance(source_name, list):
                    source_names = source_name
                elif ',' in source_name:
                    source_names = [s.strip() for s in source_name.split(',')]
                else:
                    source_names = [source_name]
                
                logger.info(f"Executing migration for {len(source_names)} source(s): {', '.join(source_names)}")
            
            # Extract migration parameters from input_data (already extracted resume_max_age above)
            # Check both input_data and defaults for no_resume (defaults to False)
            # Explicitly check presence to handle False correctly
            if 'no_resume' in input_data:
                no_resume = input_data['no_resume']
            else:
                no_resume = defaults.get('no_resume', False)
            resume_run_id = input_data.get('resume_run_id')
            
            # Run migration with timeout detection
            result = run_migration(
                source_name=source_name,
                no_resume=no_resume,
                resume_max_age=resume_max_age,  # Use the one extracted at the top
                resume_run_id=resume_run_id,
                lambda_context=context
            )
            
            # Prepare response based on status
            status_code = 200 if result['status'] in ['completed', 'partial'] else 500
            
            if result['status'] == 'completed':
                summary = result.get('summary', f"{result.get('sources_completed', 0)} source(s) completed")
                logger.info(f"Migration completed successfully: {summary}, {result['total_rows_migrated']} rows in {result['duration_seconds']:.1f}s")
            elif result['status'] == 'partial':
                summary = result.get('summary', 'Migration partial')
                logger.warning(f"Migration partial: {summary}")
            else:
                logger.error(f"Migration failed: {result.get('error', 'Unknown error')}")
            
            return {
                'statusCode': status_code,
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
                    'message': 'Valid actions: validate_config, test_connections, migrate'
                }
            }
    
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'status': 'error',
                'error': str(e),
                'action': action,
                'error_type': type(e).__name__
            }
        }


def main():
    """
    Main function for local testing.
    Allows running Lambda handler from command line.
    """
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python scripts/lambda_handler.py <action> [source_name] [--no-resume]")
        print("Actions: validate_config, test_connections, migrate")
        print("Example: python scripts/lambda_handler.py migrate analytics")
        print("         python scripts/lambda_handler.py migrate analytics,aggregator")
        print("         python scripts/lambda_handler.py migrate analytics --no-resume")
        sys.exit(1)
    
    action = sys.argv[1]
    source_name = sys.argv[2] if len(sys.argv) > 2 else None
    no_resume = '--no-resume' in sys.argv
    
    # Create event
    event = {
        'action': action,
        'source_name': source_name,
        'no_resume': no_resume
    }
    
    print("=" * 70)
    print("LAMBDA HANDLER - LOCAL EXECUTION")
    print("=" * 70)
    print(f"Action: {action}")
    print(f"Source: {source_name}")
    print(f"No Resume: {no_resume}")
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

