"""
Migration orchestrator wrapper for Lambda execution.
Makes migrate.py logic callable programmatically with timeout detection.
"""

import time
from typing import Dict, Any, Optional, List
from lib.utils import get_logger

logger = get_logger(__name__)


def run_migration(
    source_name: str,
    no_resume: bool = False,
    resume_max_age: int = 12,
    resume_run_id: Optional[str] = None,
    lambda_context: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Run migration programmatically for Lambda or local execution.
    
    Args:
        source_name: Name of the source(s) to migrate
                    - Single: "analytics"
                    - Multiple (comma-separated): "analytics,aggregator,conflict"
                    - Multiple (list): ["analytics", "aggregator"]
        no_resume: If True, force fresh start (ignore incomplete runs)
        resume_max_age: Maximum age (hours) for resumable runs
        resume_run_id: Specific run ID to resume (optional)
        lambda_context: AWS Lambda context for timeout detection (optional)
    
    Returns:
        Dictionary with migration results:
        {
            "status": "completed" | "partial" | "failed",
            "run_id": "uuid",
            "sources": {
                "analytics": {
                    "status": "completed",
                    "tables_completed": 1,
                    "tables_failed": 0,
                    "rows_migrated": 9275451
                },
                "aggregator": {
                    "status": "partial",
                    "tables_completed": 0,
                    "tables_failed": 0,
                    "rows_migrated": 0
                }
            },
            "total_sources": 2,
            "sources_completed": 1,
            "sources_partial": 1,
            "sources_failed": 0,
            "total_rows_migrated": 9275451,
            "duration_seconds": 850,
            "remaining_time_seconds": 50,  # Lambda only
            "needs_retry": false,
            "progress_percent": 50,
            "error": "error message"  # Only if failed
        }
    """
    from lib.config_loader import ConfigLoader
    from lib.connections import ConnectionFactory
    from lib.status_tracker import StatusTracker
    from migrate import MigrationOrchestrator
    
    start_time = time.time()
    
    # Parse source_name(s) - support comma-separated string or list
    if isinstance(source_name, list):
        source_names = source_name
    elif ',' in source_name:
        source_names = [s.strip() for s in source_name.split(',')]
    else:
        source_names = [source_name]
    
    logger.info(f"Starting migration orchestrator for {len(source_names)} source(s): {', '.join(source_names)}")
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config.json')
        config = config_loader.load()
        
        # Find all requested sources
        sources_to_migrate = []
        missing_sources = []
        
        # Get all sources from config (both enabled and disabled)
        all_sources = config.get('sources', [])
        
        for source_name_item in source_names:
            source = None
            for s in all_sources:
                if s.get('source_name') == source_name_item and s.get('enabled', True):
                    source = s
                    break
            
            if source:
                # Check if source has enabled tables
                enabled_tables = config_loader.get_enabled_tables(source)
                if enabled_tables:
                    sources_to_migrate.append(source)
                else:
                    logger.warning(f"Source '{source_name_item}' has no enabled tables, skipping")
            else:
                missing_sources.append(source_name_item)
                logger.warning(f"Source '{source_name_item}' not found or not enabled in config.json")
        
        # If no valid sources found, return error
        if not sources_to_migrate and missing_sources:
            error_msg = f"Source(s) not found or not enabled: {', '.join(missing_sources)}"
            logger.error(error_msg)
            return {
                'status': 'failed',
                'error': error_msg,
                'source_names': source_names,
                'duration_seconds': time.time() - start_time
            }
        
        # If all sources have no enabled tables, return completed
        if not sources_to_migrate:
            logger.warning("All requested sources have no enabled tables")
            return {
                'status': 'completed',
                'message': 'No enabled tables to migrate in any source',
                'source_names': source_names,
                'total_sources': 0,
                'sources_completed': 0,
                'total_rows_migrated': 0,
                'duration_seconds': time.time() - start_time,
                'progress_percent': 100
            }
        
        # Initialize managers
        conn_factory = ConnectionFactory(config)  # Pass config dict, not loader
        sf_manager = conn_factory.get_snowflake_manager()
        pg_manager = conn_factory.get_postgres_manager()
        
        # Initialize status tracker
        global_config = config_loader.get_global_config()
        status_db = global_config.get('status_tracking_database', 'conflict_management')
        # Note: status schema is hardcoded as 'migration_status' in StatusTracker
        status_tracker = StatusTracker(pg_manager, status_db)
        
        # Create orchestrator with Lambda-specific parameters
        class Args:
            """Mock args object for compatibility"""
            def __init__(self):
                self.no_resume = no_resume
                self.resume_max_age = resume_max_age
                self.resume_run_id = resume_run_id
        
        args = Args()
        
        orchestrator = MigrationOrchestrator(
            config=config,  # Pass config dict
            sf_manager=sf_manager,
            pg_manager=pg_manager,
            status_tracker=status_tracker,
            args=args,
            lambda_context=lambda_context  # Pass Lambda context for timeout detection
        )
        
        # Initialize status schema
        logger.info("Initializing migration status schema...")
        pg_manager.initialize_status_schema(status_db, "sql/migration_status_schema.sql")
        logger.info(f"✓ Migration status schema initialized in {status_db}")
        
        # Handle resume logic
        config_hash = config_loader.get_config_hash()
        
        # Calculate execution hash (same logic as in status_tracker.find_resumable_run)
        import hashlib
        import json
        context = {
            'config_hash': config_hash,
            'source_names': sorted(source_names)
        }
        execution_hash = hashlib.md5(
            json.dumps(context, sort_keys=True).encode()
        ).hexdigest()
        
        logger.info("=" * 80)
        logger.info("RESUME DETECTION PHASE")
        logger.info("=" * 80)
        logger.info(f"Config hash: {config_hash}")
        logger.info(f"Execution hash: {execution_hash}")
        logger.info(f"Resume settings: no_resume={no_resume}, resume_max_age={resume_max_age}h, resume_run_id={resume_run_id}")
        logger.info(f"Requested sources: {', '.join(source_names)}")
        
        if resume_run_id:
            # User specified run ID to resume - validate it exists and is not completed
            import uuid
            run_uuid = uuid.UUID(resume_run_id)
            
            # Query the run status
            query = """
                SELECT status, started_at, total_tables, completed_tables, failed_tables
                FROM migration_status.migration_runs
                WHERE run_id = %s
            """
            conn = pg_manager.get_connection(status_db)
            with conn.cursor() as cursor:
                cursor.execute(query, (str(run_uuid),))
                result = cursor.fetchone()
            
            if not result:
                logger.error(f"❌ Resume run ID {resume_run_id} not found in database")
                logger.info("→ Creating new migration run instead")
            elif result[0] == 'completed':
                logger.warning(f"❌ Resume run ID {resume_run_id} is already completed")
                logger.warning(f"   Started: {result[1]}, Tables: {result[3]}/{result[2]} completed")
                logger.info("→ Creating new migration run instead")
            else:
                # Valid resumable run
                orchestrator.run_id = run_uuid
                orchestrator.resuming = True
                logger.info(f"✅ RESUME DETECTED: Explicit run_id provided")
                logger.info(f"   Run ID: {orchestrator.run_id}")
                logger.info(f"   Status: {result[0]}")
                logger.info(f"   Started: {result[1]}")
                logger.info(f"   Progress: {result[3]}/{result[2]} tables completed, {result[4]} failed")
                logger.info(f"   Will preserve existing data in tables")
        elif not no_resume:
            # Auto-detect resumable run
            logger.info(f"🔍 Searching for resumable run...")
            logger.info(f"   Config hash: {config_hash[:8]}...")
            logger.info(f"   Execution hash: {execution_hash}")
            logger.info(f"   Sources: {source_names}")
            logger.info(f"   Max age: {resume_max_age}h")
            
            resumable_run = status_tracker.find_resumable_run(config_hash, source_names, resume_max_age)
            
            if resumable_run:
                orchestrator.run_id = resumable_run['run_id']
                orchestrator.resuming = True
                logger.info(f"✅ RESUME DETECTED: Auto-detected resumable run")
                logger.info(f"   Run ID: {orchestrator.run_id}")
                logger.info(f"   Status: {resumable_run['status']}")
                logger.info(f"   Started: {resumable_run['started_at']}")
                logger.info(f"   Progress: {resumable_run['completed_tables']}/{resumable_run['total_tables']} tables")
                logger.info(f"   Will preserve existing data in tables")
            else:
                logger.warning(f"❌ NO RESUMABLE RUN FOUND")
                logger.warning(f"   Searched for config_hash={config_hash[:8]}...")
                logger.warning(f"   Searched for execution_hash={execution_hash}")
                logger.warning(f"   Max age: {resume_max_age}h")
                logger.warning(f"   → Will create NEW run_id (may cause truncation if tables have data)")
        else:
            logger.info(f"⚠️  Resume disabled (no_resume={no_resume}), will create fresh run")
        
        # Create new run if not resuming
        if not orchestrator.resuming:
            total_tables = sum(len(config_loader.get_enabled_tables(s)) for s in sources_to_migrate)
            orchestrator.run_id = status_tracker.create_migration_run(
                config_hash=config_hash,
                source_names=source_names,
                total_sources=len(sources_to_migrate),
                total_tables=total_tables,
                metadata={'lambda': True}
            )
            logger.info(f"✅ NEW RUN CREATED")
            logger.info(f"   Run ID: {orchestrator.run_id}")
            logger.info(f"   Total tables: {total_tables}")
            logger.info(f"   Reason: {'Resume disabled' if no_resume else 'No resumable run found'}")
        
        logger.info("=" * 80)
        logger.info(f"FINAL DECISION: {'RESUMING' if orchestrator.resuming else 'NEW RUN'}")
        logger.info(f"Run ID: {orchestrator.run_id}")
        logger.info(f"Resuming Flag: {orchestrator.resuming}")
        logger.info("=" * 80)
        
        # Helper function to check if a source is fully completed
        def is_source_completed(run_id, source_name: str, table_names: List[str]) -> bool:
            """
            Check if all tables in a source are completed.
            Returns True only if all tables have status='completed'.
            """
            if not table_names:
                return False
            
            try:
                # Query status for all tables in this source
                query = """
                    SELECT COUNT(*) as total_tables,
                           SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tables
                    FROM migration_status.migration_table_status
                    WHERE run_id = %s
                      AND source_table = ANY(%s)
                """
                conn = pg_manager.get_connection(status_db)
                with conn.cursor() as cursor:
                    cursor.execute(query, (str(run_id), table_names))
                    result = cursor.fetchone()
                    
                    total_tables = result[0]
                    completed_tables = result[1] if result[1] else 0
                    
                    # Source is completed only if:
                    # 1. We have records for all tables (total_tables == len(table_names))
                    # 2. All of them are completed (completed_tables == len(table_names))
                    if total_tables == 0:
                        return False  # No records yet (not started)
                    
                    return completed_tables == len(table_names)
            except Exception as e:
                logger.warning(f"Could not check source completion for {source_name}: {e}")
                return False  # Safe fallback - don't skip on error
        
        # Run migration for each source
        source_results = {}
        total_rows_all_sources = 0
        timed_out = False
        
        for source in sources_to_migrate:
            source_name_item = source['source_name']
            table_names = [t['source'] for t in config_loader.get_enabled_tables(source)]
            
            # OPTIMIZATION: If resuming, check if source is already fully completed
            if orchestrator.resuming and orchestrator.run_id:
                if is_source_completed(orchestrator.run_id, source_name_item, table_names):
                    logger.info(f"✓ Source '{source_name_item}' already completed in previous run, skipping entirely")
                    
                    # Get statistics from status tables for reporting
                    try:
                        query = """
                            SELECT 
                                COUNT(*) as tables_completed,
                                SUM(total_rows_copied) as rows_migrated
                            FROM migration_status.migration_table_status
                            WHERE run_id = %s
                              AND source_table = ANY(%s)
                              AND status = 'completed'
                        """
                        conn = pg_manager.get_connection(status_db)
                        with conn.cursor() as cursor:
                            cursor.execute(query, (str(orchestrator.run_id), table_names))
                            result = cursor.fetchone()
                            tables_completed = result[0] if result[0] else 0
                            rows_migrated = result[1] if result[1] else 0
                            total_rows_all_sources += rows_migrated
                    except Exception as e:
                        logger.warning(f"Could not fetch stats for {source_name_item}: {e}")
                        tables_completed = len(table_names)
                        rows_migrated = 0
                    
                    source_results[source_name_item] = {
                        'status': 'completed',
                        'tables_completed': tables_completed,
                        'tables_failed': 0,
                        'rows_migrated': rows_migrated,
                        'message': 'Previously completed (skipped)'
                    }
                    continue  # Skip to next source
            
            # Check timeout before starting each source
            if lambda_context and orchestrator._check_lambda_timeout():
                logger.warning(f"Lambda timeout approaching, stopping before source: {source_name_item}")
                timed_out = True
                # Mark remaining sources as not started
                source_results[source_name_item] = {
                    'status': 'not_started',
                    'tables_completed': 0,
                    'tables_failed': 0,
                    'rows_migrated': 0,
                    'message': 'Not started due to Lambda timeout'
                }
                continue
            
            logger.info(f"Starting migration for source: {source_name_item}")
            orchestrator.table_stats = {}  # Reset for each source
            orchestrator.total_rows_migrated = 0
            
            try:
                orchestrator.run_single_source(source)
                
                # Get statistics for this source
                tables_completed = sum(1 for s in orchestrator.table_stats.values() if s['status'] == 'completed')
                tables_failed = sum(1 for s in orchestrator.table_stats.values() if s['status'] == 'failed')
                rows_migrated = orchestrator.total_rows_migrated
                total_rows_all_sources += rows_migrated
                
                # Determine source status
                if orchestrator.timed_out:
                    source_status = 'partial'
                    timed_out = True
                elif tables_failed > 0:
                    source_status = 'failed'
                elif tables_completed > 0:
                    source_status = 'completed'
                else:
                    source_status = 'partial'
                
                source_results[source_name_item] = {
                    'status': source_status,
                    'tables_completed': tables_completed,
                    'tables_failed': tables_failed,
                    'rows_migrated': rows_migrated
                }
                
                if orchestrator.timed_out:
                    source_results[source_name_item]['message'] = 'Paused due to Lambda timeout'
                    logger.warning(f"Source {source_name_item} paused due to timeout")
                    break  # Stop processing more sources
                
            except Exception as e:
                logger.error(f"Source {source_name_item} failed: {e}", exc_info=True)
                source_results[source_name_item] = {
                    'status': 'failed',
                    'tables_completed': 0,
                    'tables_failed': 0,
                    'rows_migrated': 0,
                    'error': str(e)
                }
        
        # Calculate overall statistics
        duration = time.time() - start_time
        remaining_time = None
        if lambda_context:
            remaining_time = lambda_context.get_remaining_time_in_millis() / 1000
        
        sources_completed = sum(1 for r in source_results.values() if r['status'] == 'completed')
        sources_partial = sum(1 for r in source_results.values() if r['status'] == 'partial')
        sources_failed = sum(1 for r in source_results.values() if r['status'] == 'failed')
        sources_not_started = sum(1 for r in source_results.values() if r['status'] == 'not_started')
        
        # Determine overall status
        if timed_out or sources_partial > 0 or sources_not_started > 0:
            overall_status = 'partial'
            needs_retry = True
        elif sources_failed > 0 and sources_completed == 0:
            overall_status = 'failed'
            needs_retry = True
        elif sources_failed > 0:
            overall_status = 'partial'
            needs_retry = True
        else:
            overall_status = 'completed'
            needs_retry = False
        
        total_sources = len(sources_to_migrate)
        progress = (sources_completed / total_sources * 100) if total_sources > 0 else 100
        
        result = {
            'status': overall_status,
            'run_id': str(orchestrator.run_id) if orchestrator.run_id else None,
            'sources': source_results,
            'total_sources': total_sources,
            'sources_completed': sources_completed,
            'sources_partial': sources_partial,
            'sources_failed': sources_failed,
            'total_rows_migrated': total_rows_all_sources,
            'duration_seconds': round(duration, 2),
            'progress_percent': round(progress, 1),
            'needs_retry': needs_retry
        }
        
        if remaining_time:
            result['remaining_time_seconds'] = round(remaining_time, 1)
        
        if timed_out:
            result['message'] = 'Migration paused due to approaching Lambda timeout. Will auto-resume on retry.'
        
        # Add summary message
        if overall_status == 'completed':
            result['summary'] = f"All {sources_completed} source(s) completed successfully"
        elif overall_status == 'partial':
            result['summary'] = f"{sources_completed} completed, {sources_partial} partial, {sources_failed} failed, {sources_not_started} not started"
        else:
            result['summary'] = f"{sources_failed} source(s) failed"
        
        logger.info(f"Migration orchestrator completed: status={overall_status}, sources={total_sources}, rows={total_rows_all_sources}, duration={duration:.1f}s")
        
        # Update the run status in the database
        if orchestrator.run_id:
            try:
                # Calculate total completed and failed tables across all sources
                total_completed = sum(r.get('tables_completed', 0) for r in source_results.values())
                total_failed = sum(r.get('tables_failed', 0) for r in source_results.values())
                
                status_tracker.update_run_status(
                    orchestrator.run_id,
                    status=overall_status,
                    completed_tables=total_completed,
                    failed_tables=total_failed,
                    total_rows_copied=total_rows_all_sources
                )
                logger.info(f"✓ Updated run status in database: {overall_status}")
            except Exception as e:
                logger.warning(f"Failed to update run status in database: {e}")
                # Don't fail the entire migration just because status update failed
        
        return result
    
    except Exception as e:
        logger.error(f"Migration orchestrator failed: {str(e)}", exc_info=True)
        return {
            'status': 'failed',
            'error': str(e),
            'source_name': source_name,
            'duration_seconds': time.time() - start_time,
            'error_type': type(e).__name__
        }

