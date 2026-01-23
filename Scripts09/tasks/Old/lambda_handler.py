"""
AWS Lambda handler for ETL Pipeline.
Can run both in AWS Lambda and locally for testing.

Usage in Lambda:
    Event: {"action": "validate_config" | "task_01_prepare" | "task_0201_chunked" | "get_task0200_chunks" | "process_task0200_chunk"}

Usage locally:
    python lambda_handler.py task_01_prepare
    python lambda_handler.py task_0201_chunked
    python lambda_handler.py get_task0200_chunks
"""

import sys
import json
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import POSTGRES_CONFIG, validate_config
from connectors.postgres_connector import PostgresConnector
from src.task_01_step01_prepare import Task01Step01Prepare
from src.task_01_step02_get_chunks import Task01Step02GetChunks
from src.task_01_step03_process_chunk import Task01Step03ProcessChunk
from src.task_02_00_step01_get_chunks import Task02Step01GetChunks
from src.task_02_00_step02_process_chunk import Task02Step02ProcessChunk
from src.task_02_01_step01_get_chunks import Task0201Step01GetChunks
from src.task_02_01_step02_process_chunk import Task0201Step02ProcessChunk
from src.task_02_03_finalize_conflicts import Task0203FinalizeConflicts
from src.task_03_00_step01_get_chunks import Task03Step01GetChunks
from src.task_03_00_step02_process_chunk import Task03Step02ProcessChunk
from utils.logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    """
    Main AWS Lambda handler.
    
    Args:
        event: Lambda event containing action to perform
            {
                "action": "validate_config" | "test_postgres" | "task_01" | "task_02" | "task_03_chunked",
                "use_mock": false  # Optional: for testing
            }
        context: Lambda context (unused in local testing)
    
    Returns:
        Response dictionary with status and results
    """
    # Regular Lambda invocation (action-based)
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
        
        # Action: Task 01 Prepare (Steps 1-3: Sync reminders, truncate temp)
        elif action == 'task_01_prepare':
            logger.info("Executing Task 01: Prepare (Sync reminders, truncate temp)")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task01Step01Prepare(connector)
            result = task.execute_prepare()
            
            if result['status'] == 'success':
                logger.info(f"Task 01 prepare completed successfully")
                return {
                    'statusCode': 200,
                    'body': result
                }
            else:
                logger.error(f"Task 01 prepare failed: {result.get('error')}")
                return {
                    'statusCode': 500,
                    'body': result
                }
        
        # Action: Task 01 Finalize (Step 5: Update settings)
        elif action == 'task_01_finalize':
            logger.info("Executing Task 01: Finalize (Update settings)")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task01Step01Prepare(connector)
            result = task.execute_finalize()
            
            if result['status'] == 'success':
                logger.info(f"Task 01 finalize completed successfully")
                return {
                    'statusCode': 200,
                    'body': result
                }
            else:
                logger.error(f"Task 01 finalize failed: {result.get('error')}")
                return {
                    'statusCode': 500,
                    'body': result
                }
        
        # Action: Get Task 01 Chunks (Phase 2)
        elif action == 'get_task01_chunks':
            logger.info("Getting chunks for Task 01")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task01Step02GetChunks(connector)
            result = task.run()
            
            if result['status'] == 'success':
                chunk_data = result['result']
                logger.info(f"Generated {chunk_data['num_chunks']} chunks for {chunk_data['total_rows']} rows")
                # Return data directly for Step Functions (not wrapped in statusCode/body)
                return chunk_data
            else:
                logger.error(f"Failed to get chunks: {result.get('error')}")
                raise Exception(f"Chunk generation failed: {result.get('error')}")
        
        # Action: Process Task 01 Chunk (Phase 2 - Called from Step Functions)
        elif action == 'process_task01_chunk':
            chunk_id = event.get('chunk_id')
            min_id = event.get('min_id')
            max_id = event.get('max_id')
            target_ids_per_chunk = event.get('target_ids_per_chunk')
            
            if chunk_id is None:
                error_msg = "Missing required parameter: chunk_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            if min_id is None:
                error_msg = "Missing required parameter: min_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            if max_id is None:
                error_msg = "Missing required parameter: max_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            if target_ids_per_chunk is None:
                error_msg = "Missing required parameter: target_ids_per_chunk"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            # Compute start_id and end_id from chunk_id and parameters
            start_id = min_id + (chunk_id * target_ids_per_chunk)
            end_id = min(start_id + target_ids_per_chunk - 1, max_id)
            
            logger.info(f"Processing Task 01 chunk {chunk_id}: IDs {start_id} to {end_id}")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task01Step03ProcessChunk(connector)
            result = task.execute(chunk_id=chunk_id, start_id=start_id, end_id=end_id)
            
            if result['status'] == 'success':
                logger.info(f"Chunk {chunk_id} completed: {result['rows_copied']} rows in {result['duration_seconds']:.2f}s")
                # Return minimal response to avoid Step Functions DataLimitExceeded error
                # (Map state combines all iteration results, which can exceed 256KB limit)
                return {"status": "completed"}
            else:
                logger.error(f"Chunk {chunk_id} failed: {result.get('error')}")
                raise Exception(f"Chunk {chunk_id} processing failed: {result.get('error')}")
        
        # Action: Get Task 02_01 Chunks (InService Conflicts)
        elif action == 'get_task0201_chunks':
            logger.info("Getting chunks for Task 02_01 (InService Conflicts)")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task0201Step01GetChunks(connector)
            result = task.run()
            
            if result['status'] == 'success':
                chunk_data = result['result']
                logger.info(f"Generated {chunk_data['num_chunks']} chunks for {chunk_data['total_rows']} rows")
                # Return data directly for Step Functions (not wrapped in statusCode/body)
                return chunk_data
            else:
                logger.error(f"Failed to get chunks: {result.get('error')}")
                raise Exception(f"Chunk generation failed: {result.get('error')}")
        
        # Action: Process Task 02_01 Chunk (InService Conflicts - Load from task0201_chunk_keys table using run_id)
        elif action == 'process_task0201_chunk':
            chunk_id = event.get('chunk_id')
            run_id = event.get('run_id')
            
            if chunk_id is None:
                error_msg = "Missing required parameter: chunk_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            if run_id is None:
                error_msg = "Missing required parameter: run_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            logger.info(f"Processing Task 02_01 chunk {chunk_id} for run_id: {run_id}")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task0201Step02ProcessChunk(connector)
            
            try:
                result = task.execute(chunk_id=chunk_id, run_id=run_id)
                
                if result['status'] == 'success':
                    logger.info(f"Chunk {chunk_id} completed: {result.get('rows_updated', 0):,} rows in {result.get('duration_seconds', 0):.2f}s")
                    # Return minimal response to avoid Step Functions DataLimitExceeded error
                    # (Map state combines all iteration results, which can exceed 256KB limit)
                    return {"status": "completed"}
                else:
                    logger.error(f"Chunk {chunk_id} failed: {result.get('error', 'Unknown error')}")
                    raise Exception(f"Chunk {chunk_id} processing failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                logger.error(f"Chunk {chunk_id} processing exception: {str(e)}", exc_info=True)
                raise
        
        # Action: Task 02_01 Chunked (Combined - Get Chunks + Process All Chunks)
        elif action == 'task_0201_chunked':
            logger.info("Executing Task 02_01: InService Conflicts Chunked Processing (All-in-One)")
            start_time = time.time()
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            
            # Step 1: Get chunks (stores keys in database, returns chunk_ids)
            logger.info("Step 1: Getting chunks for Task 02_01")
            get_chunks_task = Task0201Step01GetChunks(connector)
            chunks_result = get_chunks_task.run()
            
            if chunks_result['status'] != 'success':
                error_msg = f"Failed to get chunks: {chunks_result.get('error')}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            chunk_data = chunks_result['result']
            
            if chunk_data['num_chunks'] == 0:
                logger.info("No chunks to process - all rows already completed")
                return {
                    'statusCode': 200,
                    'body': {
                        'status': 'success',
                        'message': 'No rows to process',
                        'total_rows': 0,
                        'chunks_processed': 0,
                        'total_rows_updated': 0,
                        'duration_seconds': time.time() - start_time
                    }
                }
            
            run_id = chunk_data['run_id']
            chunk_ids = chunk_data.get('chunk_ids', [])
            
            logger.info(f"Step 2: Processing {len(chunk_ids)} chunks sequentially (run_id: {run_id})")
            
            if not chunk_ids:
                error_msg = "No chunk_ids found in result."
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Step 3: Process each chunk sequentially
            process_task = Task0201Step02ProcessChunk(connector)
            total_updated = 0
            chunk_results = []
            
            for i, chunk_id in enumerate(chunk_ids):
                logger.info(f"Processing chunk {i + 1}/{len(chunk_ids)} (chunk_id: {chunk_id})")
                
                # Load keys from database using run_id + chunk_id
                result = process_task.execute(chunk_id=chunk_id, run_id=run_id)
                
                if result['status'] == 'success':
                    total_updated += result['rows_updated']
                    chunk_results.append({
                        'chunk_id': chunk_id,
                        'rows_updated': result['rows_updated'],
                        'duration_seconds': result['duration_seconds']
                    })
                    logger.info(f"Chunk {chunk_id} completed: {result['rows_updated']} rows updated "
                              f"in {result['duration_seconds']:.2f}s")
                else:
                    error_msg = f"Chunk {chunk_id} failed: {result.get('error')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            
            total_duration = time.time() - start_time
            
            logger.info(f"Task 02_01 chunked processing completed: {total_updated} total rows updated "
                       f"across {len(chunk_ids)} chunks in {total_duration:.2f}s")
            
            return {
                'statusCode': 200,
                'body': {
                    'status': 'success',
                    'run_id': run_id,
                    'total_rows': chunk_data['total_rows'],
                    'chunks_processed': len(chunk_ids),
                    'total_rows_updated': total_updated,
                    'duration_seconds': total_duration,
                    'summary': {
                        'avg_rows_per_chunk': total_updated / len(chunk_ids) if chunk_ids else 0,
                        'avg_duration_per_chunk': sum(r['duration_seconds'] for r in chunk_results) / len(chunk_results) if chunk_results else 0
                    }
                }
            }
        
        # Action: Get Task 02_00 Chunks (Update Conflicts - High Parallelism)
        elif action == 'get_task0200_chunks':
            logger.info("Getting chunks for Task 02_00 (Update Conflicts - high-parallelism mode)")
            
            try:
                connector = PostgresConnector(**POSTGRES_CONFIG)
                task = Task02Step01GetChunks(connector)
                result = task.run()  # Use run() for proper error handling
                
                if result['status'] == 'success':
                    chunk_data = result['result']  # Access result from run() wrapper
                    logger.info(f"Generated {chunk_data['num_chunks']} chunks for {chunk_data['total_rows']:,} rows")
                    # Return data directly for Step Functions (not wrapped in statusCode/body)
                    return chunk_data
                else:
                    error_msg = result.get('error', 'Unknown error')
                    logger.error(f"Failed to get chunks: {error_msg}")
                    raise Exception(f"Chunk generation failed: {error_msg}")
            except Exception as e:
                logger.error(f"Exception in get_task02_chunks: {str(e)}", exc_info=True)
                raise
        
        # Action: Process Task 02_00 Chunk (Update Conflicts - Load from task0200_chunk_keys table using run_id)
        elif action == 'process_task0200_chunk':
            chunk_id = event.get('chunk_id')
            run_id = event.get('run_id')
            
            if chunk_id is None:
                error_msg = "Missing required parameter: chunk_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            if run_id is None:
                error_msg = "Missing required parameter: run_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            logger.info(f"Processing Task 02_00 chunk {chunk_id} for run_id: {run_id}")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task02Step02ProcessChunk(connector)
            
            try:
                result = task.execute(chunk_id=chunk_id, run_id=run_id)
                
                if result['status'] == 'success':
                    logger.info(f"Chunk {chunk_id} completed: {result.get('rows_updated', 0):,} rows in {result.get('duration_seconds', 0):.2f}s")
                    # Return minimal response to avoid Step Functions DataLimitExceeded error
                    # (Map state combines all iteration results, which can exceed 256KB limit)
                    return {"status": "completed"}
                else:
                    logger.error(f"Chunk {chunk_id} failed: {result.get('error', 'Unknown error')}")
                    raise Exception(f"Chunk {chunk_id} processing failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                logger.error(f"Chunk {chunk_id} processing exception: {str(e)}", exc_info=True)
                raise
        
        # Action: Task 02_03 Finalize Conflicts (Post-processing after Task 02_00/02_01)
        # This runs ALL 7 steps in sequence - may timeout for large datasets
        elif action == 'task_0203_finalize':
            logger.info("Executing Task 02_03: Finalize Conflicts (all steps)")
            start_time = time.time()
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task0203FinalizeConflicts(connector)
            
            try:
                result = task.execute()
                
                total_duration = time.time() - start_time
                
                if result['status'] == 'success':
                    logger.info(f"Task 02_03 completed: {result['total_rows_affected']:,} rows affected "
                               f"in {total_duration:.2f}s ({total_duration/60:.2f} min)")
                    return {
                        'statusCode': 200,
                        'body': {
                            'status': 'success',
                            'total_rows_affected': result['total_rows_affected'],
                            'steps': result['steps'],
                            'duration_seconds': total_duration,
                            'duration_minutes': total_duration / 60
                        }
                    }
                else:
                    logger.error(f"Task 02_03 failed: {result.get('error')}")
                    return {
                        'statusCode': 500,
                        'body': result
                    }
            except Exception as e:
                logger.error(f"Task 02_03 exception: {str(e)}", exc_info=True)
                raise
        
        # Action: Task 02_03 Individual Steps (for Step Functions - runs one step at a time)
        # Use actions: task_0203_step1, task_0203_step2a, task_0203_step3, ..., task_0203_step7
        # Note: Step 2b is now handled via chunked parallel processing (get_task0203_step2b_chunks / process_task0203_step2b_chunk)
        elif action and action.startswith('task_0203_step'):
            # Extract step identifier from action (e.g., 'task_0203_step1' -> '1', 'task_0203_step2a' -> '2a')
            step_id = action.replace('task_0203_step', '')
            
            # Validate step identifier (2b and 2c removed - use chunked processing instead)
            valid_steps = {'1', '2a', '3a', '3b', '3c', '3d', '3e', '3f', '3g', '4', '5', '6', '7'}
            if step_id not in valid_steps:
                error_msg = f"Invalid step identifier: {step_id}. Valid steps are: {', '.join(sorted(valid_steps, key=lambda x: (x[0], x)))}. Note: Step 2b uses get_task0203_step2b_chunks and process_task0203_step2b_chunk for parallel processing."
                logger.error(error_msg)
                raise Exception(error_msg)
            
            logger.info(f"Executing Task 02_03 Step {step_id}")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task0203FinalizeConflicts(connector)
            
            try:
                result = task.execute_step(step_id)
                
                if result['status'] == 'success':
                    logger.info(f"Task 02_03 Step {step_id} completed: {result['rows_affected']:,} rows "
                               f"in {result['duration_seconds']:.2f}s")
                    # Return data for Step Functions
                    return {
                        "status": "success",
                        "step_id": step_id,
                        "rows_affected": result['rows_affected'],
                        "duration_seconds": result['duration_seconds']
                    }
                else:
                    logger.error(f"Task 02_03 Step {step_id} failed")
                    raise Exception(f"Step {step_id} failed")
            except Exception as e:
                logger.error(f"Task 02_03 Step {step_id} exception: {str(e)}", exc_info=True)
                raise
        
        # Action: Get Task 02_03 Step 2B Chunks (Date Range Parallel Processing)
        # Generates date range chunks for parallel processing of step 2B
        elif action == 'get_task0203_step2b_chunks':
            logger.info("Getting date range chunks for Task 02_03 Step 2B")
            
            try:
                connector = PostgresConnector(**POSTGRES_CONFIG)
                task = Task0203FinalizeConflicts(connector)
                
                # Get chunk size from event (default 30 days)
                chunk_size_days = event.get('chunk_size_days', 30)
                
                result = task.get_step2b_chunks(chunk_size_days=chunk_size_days)
                
                logger.info(f"Generated {result['num_chunks']} chunks for {result['total_days']} days")
                
                # Return data directly for Step Functions Map state
                return result
                
            except Exception as e:
                logger.error(f"Failed to get step2b chunks: {str(e)}", exc_info=True)
                raise
        
        # Action: Process Task 02_03 Step 2B Chunk (Date Range)
        # Processes a single date range chunk for step 2B
        elif action == 'process_task0203_step2b_chunk':
            start_date = event.get('start_date')
            end_date = event.get('end_date')
            chunk_id = event.get('chunk_id')
            
            if not start_date or not end_date:
                error_msg = "Missing required parameters: start_date and end_date"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            logger.info(f"Processing Task 02_03 Step 2B chunk {chunk_id}: {start_date} to {end_date}")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task0203FinalizeConflicts(connector)
            
            try:
                result = task.process_step2b_chunk(
                    start_date=start_date,
                    end_date=end_date,
                    chunk_id=chunk_id
                )
                
                if result['status'] == 'success':
                    logger.info(f"Chunk {chunk_id} completed: {result['rows_affected']:,} rows "
                               f"in {result['duration_seconds']:.2f}s")
                    # Return minimal response to avoid Step Functions DataLimitExceeded error
                    return {"status": "completed", "rows_affected": result['rows_affected']}
                else:
                    logger.error(f"Chunk {chunk_id} failed")
                    raise Exception(f"Chunk {chunk_id} processing failed")
            except Exception as e:
                logger.error(f"Chunk {chunk_id} processing exception: {str(e)}", exc_info=True)
                raise
        
        # Action: Get Task 02_03 Step 3 Chunks (Date Range Parallel Processing for 3a-3g)
        # Generates date range chunks for parallel processing of step 3 sub-steps
        elif action == 'get_task0203_step3_chunks':
            step_id = event.get('step_id')
            
            if not step_id:
                error_msg = "Missing required parameter: step_id (should be 3a, 3b, 3c, 3d, 3e, 3f, or 3g)"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            logger.info(f"Getting date range chunks for Task 02_03 Step {step_id}")
            
            try:
                connector = PostgresConnector(**POSTGRES_CONFIG)
                task = Task0203FinalizeConflicts(connector)
                
                # Get chunk size from event (default 30 days)
                chunk_size_days = event.get('chunk_size_days', 30)
                
                result = task.get_step3_chunks(step_id=step_id, chunk_size_days=chunk_size_days)
                
                logger.info(f"Generated {result['num_chunks']} chunks for {result['total_days']} days (Step {step_id})")
                
                # Return data directly for Step Functions Map state
                return result
                
            except Exception as e:
                logger.error(f"Failed to get step3 chunks: {str(e)}", exc_info=True)
                raise
        
        # Action: Process Task 02_03 Step 3 Chunk (Date Range for 3a-3g)
        # Processes a single date range chunk for step 3 sub-steps
        elif action == 'process_task0203_step3_chunk':
            step_id = event.get('step_id')
            start_date = event.get('start_date')
            end_date = event.get('end_date')
            chunk_id = event.get('chunk_id')
            
            if not step_id:
                error_msg = "Missing required parameter: step_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            if not start_date or not end_date:
                error_msg = "Missing required parameters: start_date and end_date"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            logger.info(f"Processing Task 02_03 Step {step_id} chunk {chunk_id}: {start_date} to {end_date}")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task0203FinalizeConflicts(connector)
            
            try:
                result = task.process_step3_chunk(
                    step_id=step_id,
                    start_date=start_date,
                    end_date=end_date,
                    chunk_id=chunk_id
                )
                
                if result['status'] == 'success':
                    logger.info(f"Step {step_id} chunk {chunk_id} completed: {result['rows_affected']:,} rows "
                               f"in {result['duration_seconds']:.2f}s")
                    # Return minimal response to avoid Step Functions DataLimitExceeded error
                    return {"status": "completed", "rows_affected": result['rows_affected'], "step_id": step_id}
                else:
                    logger.error(f"Step {step_id} chunk {chunk_id} failed")
                    raise Exception(f"Step {step_id} chunk {chunk_id} processing failed")
            except Exception as e:
                logger.error(f"Step {step_id} chunk {chunk_id} processing exception: {str(e)}", exc_info=True)
                raise
        
        # ======================================================================
        # TASK 03_00: INSERT NEW CONFLICTS
        # ======================================================================
        
        # Action: Get Task 03_00 Chunks (High Parallelism - Generate all chunks at once)
        elif action == 'get_task0300_chunks':
            logger.info("Getting chunks for Task 03_00 (Insert New Conflicts - high-parallelism mode)")
            
            try:
                connector = PostgresConnector(**POSTGRES_CONFIG)
                task = Task03Step01GetChunks(connector)
                result = task.run()  # Use run() for proper error handling
                
                if result['status'] == 'success':
                    chunk_data = result['result']  # Access result from run() wrapper
                    logger.info(f"Generated {chunk_data['num_chunks']} chunks for {chunk_data['total_rows']:,} potential rows")
                    # Return data directly for Step Functions (not wrapped in statusCode/body)
                    return chunk_data
                else:
                    error_msg = result.get('error', 'Unknown error')
                    logger.error(f"Failed to get chunks: {error_msg}")
                    raise Exception(f"Chunk generation failed: {error_msg}")
            except Exception as e:
                logger.error(f"Exception in get_task0300_chunks: {str(e)}", exc_info=True)
                raise
        
        # Action: Process Task 03_00 Chunk (Insert New Conflicts - Load from task03_chunk_keys table using run_id)
        elif action == 'process_task0300_chunk':
            chunk_id = event.get('chunk_id')
            run_id = event.get('run_id')
            
            if chunk_id is None:
                error_msg = "Missing required parameter: chunk_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            if run_id is None:
                error_msg = "Missing required parameter: run_id"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            logger.info(f"Processing Task 03_00 chunk {chunk_id} for run_id: {run_id}")
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            task = Task03Step02ProcessChunk(connector)
            
            try:
                result = task.execute(chunk_id=chunk_id, run_id=run_id)
                
                if result['status'] == 'success':
                    logger.info(f"Chunk {chunk_id} completed: {result.get('rows_inserted', 0):,} rows in {result.get('duration_seconds', 0):.2f}s")
                    # Return minimal response to avoid Step Functions DataLimitExceeded error
                    return {"status": "completed"}
                else:
                    logger.error(f"Chunk {chunk_id} failed: {result.get('error', 'Unknown error')}")
                    raise Exception(f"Chunk {chunk_id} processing failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                logger.error(f"Chunk {chunk_id} processing exception: {str(e)}", exc_info=True)
                raise
        
        # Action: Task 03_00 Chunked (Combined - Get Chunks + Process All Chunks Sequentially)
        elif action == 'task_0300_chunked':
            logger.info("Executing Task 03_00: Insert New Conflicts Chunked Processing (All-in-One)")
            start_time = time.time()
            
            connector = PostgresConnector(**POSTGRES_CONFIG)
            
            # Step 1: Get chunks (stores keys in database, returns chunk_ids)
            logger.info("Step 1: Getting chunks for Task 03_00")
            get_chunks_task = Task03Step01GetChunks(connector)
            chunks_result = get_chunks_task.run()
            
            if chunks_result['status'] != 'success':
                error_msg = f"Failed to get chunks: {chunks_result.get('error')}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            chunk_data = chunks_result['result']
            
            if chunk_data['num_chunks'] == 0:
                logger.info("No chunks to process - no new conflicts to insert")
                return {
                    'statusCode': 200,
                    'body': {
                        'status': 'success',
                        'message': 'No rows to process',
                        'total_rows': 0,
                        'chunks_processed': 0,
                        'total_rows_inserted': 0,
                        'duration_seconds': time.time() - start_time
                    }
                }
            
            run_id = chunk_data['run_id']
            chunk_ids = chunk_data.get('chunk_ids', [])
            
            logger.info(f"Step 2: Processing {len(chunk_ids)} chunks sequentially (run_id: {run_id})")
            
            if not chunk_ids:
                error_msg = "No chunk_ids found in result."
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Step 3: Process each chunk sequentially
            process_task = Task03Step02ProcessChunk(connector)
            total_inserted = 0
            chunk_results = []
            
            for i, chunk_id in enumerate(chunk_ids):
                logger.info(f"Processing chunk {i + 1}/{len(chunk_ids)} (chunk_id: {chunk_id})")
                
                # Load keys from database using run_id + chunk_id
                result = process_task.execute(chunk_id=chunk_id, run_id=run_id)
                
                if result['status'] == 'success':
                    total_inserted += result.get('rows_inserted', 0)
                    chunk_results.append({
                        'chunk_id': chunk_id,
                        'rows_inserted': result.get('rows_inserted', 0),
                        'duration_seconds': result['duration_seconds']
                    })
                    logger.info(f"Chunk {chunk_id} completed: {result.get('rows_inserted', 0)} rows inserted "
                              f"in {result['duration_seconds']:.2f}s")
                else:
                    error_msg = f"Chunk {chunk_id} failed: {result.get('error')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            
            total_duration = time.time() - start_time
            
            logger.info(f"Task 03_00 chunked processing completed: {total_inserted} total rows inserted "
                       f"across {len(chunk_ids)} chunks in {total_duration:.2f}s")
            
            return {
                'statusCode': 200,
                'body': {
                    'status': 'success',
                    'run_id': run_id,
                    'total_rows': chunk_data['total_rows'],
                    'chunks_processed': len(chunk_ids),
                    'total_rows_inserted': total_inserted,
                    'duration_seconds': total_duration,
                    'summary': {
                        'avg_rows_per_chunk': total_inserted / len(chunk_ids) if chunk_ids else 0,
                        'avg_duration_per_chunk': sum(r['duration_seconds'] for r in chunk_results) / len(chunk_results) if chunk_results else 0
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
                    'message': 'Valid actions: validate_config, test_postgres, task_01_prepare, task_01_finalize, task_0201_chunked, task_0203_finalize, task_0203_step1-7, get_task01_chunks, process_task01_chunk, get_task0200_chunks, process_task0200_chunk, get_task0201_chunks, process_task0201_chunk, get_task0203_step2b_chunks, process_task0203_step2b_chunk, get_task0203_step3_chunks, process_task0203_step3_chunk, get_task0300_chunks, process_task0300_chunk, task_0300_chunked'
                }
            }
    
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        
        # Step Functions actions should propagate exceptions (not return statusCode/body)
        # This allows Step Functions Catch blocks to properly handle errors
        # IMPORTANT: All actions called from Step Functions must be listed here!
        step_functions_actions = [
            # Config validation
            'validate_config',
            # Task 01 actions
            'task_01_prepare', 'task_01_finalize',
            'get_task01_chunks', 'process_task01_chunk',
            # Task 02_00 actions (Update Conflicts)
            'get_task0200_chunks', 'process_task0200_chunk',
            # Task 02_01 actions (InService Conflicts)
            'get_task0201_chunks', 'process_task0201_chunk',
            # Task 02_03 actions (Finalize - all individual steps + finalize + chunked step2b)
            'task_0203_finalize',
            'task_0203_step1', 'task_0203_step2a',
            'task_0203_step3a', 'task_0203_step3b', 'task_0203_step3c', 
            'task_0203_step3d', 'task_0203_step3e', 'task_0203_step3f', 'task_0203_step3g',
            'task_0203_step4', 'task_0203_step5', 'task_0203_step6', 'task_0203_step7',
            'get_task0203_step2b_chunks', 'process_task0203_step2b_chunk',
            'get_task0203_step3_chunks', 'process_task0203_step3_chunk',
            # Task 03_00 actions (Insert New Conflicts)
            'get_task0300_chunks', 'process_task0300_chunk', 'task_0300_chunked',
        ]
        
        if action in step_functions_actions or (action and action.startswith('task_0203_step')):
            # Re-raise for Step Functions to catch via its Catch block
            raise
        
        # For API Gateway style invocations, return formatted error response
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
        print("Usage: python lambda_handler.py <action>")
        print("Actions: validate_config, test_postgres, task_01_prepare, task_01_finalize")
        print("        task_0201_chunked, task_0203_finalize, task_0203_step1 through task_0203_step7")
        print("        get_task01_chunks, process_task01_chunk")
        print("        get_task0200_chunks, process_task0200_chunk")
        print("        get_task0201_chunks, process_task0201_chunk")
        print("        get_task0203_step3_chunks, process_task0203_step3_chunk")
        print("        get_task0300_chunks, process_task0300_chunk, task_0300_chunked")
        print("Note: process_task0200_chunk and process_task0201_chunk require additional parameters (use JSON event)")
        print("Note: task_0201_chunked combines get_task0201_chunks + process all chunks in a single invocation")
        print("Note: task_0203_finalize runs ALL 7 steps in sequence (may timeout for large datasets)")
        print("Note: task_0203_step1-7 runs individual steps separately (recommended for production)")
        print("Note: get_task0203_step3_chunks + process_task0203_step3_chunk for parallel step 3a-3g processing")
        print("Note: task_0300_chunked combines get_task0300_chunks + process all chunks (Insert New Conflicts)")


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
    
    # Handle both wrapped responses (statusCode/body) and direct responses (chunking actions)
    if isinstance(result, dict) and 'statusCode' in result:
        # Standard Lambda response format
        print(f"Status Code: {result['statusCode']}")
        print(f"Duration: {duration:.2f}s")
        print()
        print("Response Body:")
        print(json.dumps(result['body'], indent=2, default=str))
        status_code = result['statusCode']
    else:
        # Direct response (chunking actions)
        print(f"Status: Success")
        print(f"Duration: {duration:.2f}s")
        print()
        print("Response Data:")
        print(json.dumps(result, indent=2, default=str))
        status_code = 200  # Assume success if no statusCode
    
    print("=" * 70)
    
    # Exit with appropriate code
    sys.exit(0 if status_code == 200 else 1)


if __name__ == '__main__':
    main()

