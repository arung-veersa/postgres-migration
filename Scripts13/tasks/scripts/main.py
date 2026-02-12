"""
ECS Container Entry Point for Conflict Management Pipeline
Cross-database operation between Snowflake and PostgreSQL

Container entry point for ECS/Fargate execution.
All parameters are read from environment variables or config.json defaults.

ACTION environment variable supports:
  - Single action:   ACTION=test_connections
  - Comma-separated: ACTION=validate_config,test_connections,task02_00_run_conflict_update
  - Default:         ACTION not set  →  runs full DEFAULT_ACTIONS pipeline

Actions are executed sequentially. If any action fails, execution stops
and the container exits with code 1.

Special actions:
  - task00_preflight:  Pre-run validation, disables pg_cron, sets InProgressFlag=1
  - task99_postflight: Post-run cleanup, VACUUM/ANALYZE, re-enables pg_cron, sends email
    Postflight receives all previous action results for the status email.
"""

import sys
import os
import json
import time
import signal
from pathlib import Path
from typing import Optional, Dict, Callable, List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.query_builder import QueryBuilder
from lib.conflict_processor import ConflictProcessor
from lib.utils import get_logger, format_duration
from scripts.actions.preflight import run_preflight
from scripts.actions.postflight import run_postflight

logger = get_logger(__name__)

# --- Graceful shutdown handling ---
# ECS sends SIGTERM 30 seconds before SIGKILL when stopping a task.
# We use this to log final state and close database connections cleanly.

_shutdown_requested = False
_conn_factory: Optional[ConnectionFactory] = None

# Default pipeline: runs all tasks sequentially when ACTION is not set.
# Add new tasks to this list as they are developed.
# Note: validate_config and test_connections are included in preflight,
# so they don't appear in the default pipeline. They remain in
# ACTION_REGISTRY for standalone use (e.g. ACTION=validate_config).
DEFAULT_ACTIONS = [
    'task00_preflight',
    'task02_00_run_conflict_update',
    'task99_postflight',
]


def _handle_sigterm(signum, frame):
    """Handle SIGTERM from ECS for graceful shutdown."""
    global _shutdown_requested
    _shutdown_requested = True
    logger.warning("SIGTERM received -- initiating graceful shutdown")


signal.signal(signal.SIGTERM, _handle_sigterm)


def _get_env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    """Read an environment variable as an integer, or return default."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer for env var {name}={value!r}, using default={default}")
        return default


def _get_env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
    """Read an environment variable as a boolean, or return default."""
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in ('true', '1', 'yes')


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

def validate_config(settings: Settings) -> dict:
    """Validate configuration and print summary."""
    task_params = settings.get_task02_parameters()
    db_names = settings.get_database_names()

    logger.info("Configuration validated successfully")
    logger.info(f"  Databases: {json.dumps(db_names, indent=2)}")
    logger.info(f"  Task parameters: {json.dumps(task_params, indent=2)}")

    return {
        'status': 'success',
        'message': 'Configuration validated successfully',
        'databases': db_names,
        'task_parameters': task_params,
    }


def test_connections(settings: Settings) -> dict:
    """Test Snowflake and PostgreSQL connectivity."""
    global _conn_factory

    sf_config = settings.get_snowflake_config()
    pg_config = settings.get_postgres_config()
    db_names = settings.get_database_names()

    _conn_factory = ConnectionFactory(sf_config, pg_config)

    try:
        # Snowflake
        sf_manager = _conn_factory.get_snowflake_manager()
        sf_conn = sf_manager.get_connection()
        cursor = sf_conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        sf_version = cursor.fetchone()[0]
        cursor.close()
        logger.info(f"Snowflake connection successful: {sf_version}")

        # PostgreSQL
        pg_manager = _conn_factory.get_postgres_manager()
        pg_conn = pg_manager.get_connection(db_names['pg_database'])
        cursor = pg_conn.cursor()
        cursor.execute("SELECT version()")
        pg_version = cursor.fetchone()[0]
        cursor.close()
        pg_conn.close()
        logger.info("PostgreSQL connection successful")

        # Reference tables
        logger.info("Testing reference table access...")
        pg_conn = pg_manager.get_connection(db_names['pg_database'])
        cursor = pg_conn.cursor()

        list_tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        cursor.execute(list_tables_query, (db_names['pg_schema'],))
        available_tables = [row[0] for row in cursor.fetchall()]
        logger.info(
            f"Available tables in schema '{db_names['pg_schema']}': "
            f"{', '.join(available_tables) if available_tables else 'None'}"
        )

        reference_tables = {}
        test_tables = {
            'SETTINGS': 'settings',
            'EXCLUDED_AGENCY': 'excluded_agency',
            'EXCLUDED_SSN': 'excluded_ssn',
            'MPH': 'mph',
            'CONFLICTVISITMAPS': 'conflictvisitmaps',
        }

        for display_name, table_name in test_tables.items():
            if table_name in available_tables:
                try:
                    count_query = f'SELECT COUNT(*) FROM {db_names["pg_schema"]}.{table_name}'
                    cursor.execute(count_query)
                    count = cursor.fetchone()[0]
                    reference_tables[display_name] = {'name': table_name, 'count': count}
                    logger.info(f"  {display_name} table accessible as '{table_name}': {count} row(s)")
                except Exception as table_err:
                    logger.warning(f"  Found {table_name} but couldn't query it: {table_err}")
            else:
                logger.warning(f"  {display_name} table (expected as '{table_name}') not found")

        cursor.close()
        pg_conn.close()

        return {
            'status': 'success',
            'message': 'All connections tested successfully',
            'snowflake': {
                'connected': True,
                'version': sf_version,
                'database': db_names['sf_database'],
                'schema': db_names['sf_schema'],
            },
            'postgres': {
                'connected': True,
                'version': pg_version[:80],
                'database': db_names['pg_database'],
                'schema': db_names['pg_schema'],
                'available_tables': available_tables,
            },
            'reference_tables': reference_tables,
        }

    finally:
        _conn_factory.close_all()


def run_conflict_update(settings: Settings) -> dict:
    """Execute the full v3 conflict detection and update pipeline."""
    global _conn_factory

    sf_config = settings.get_snowflake_config()
    pg_config = settings.get_postgres_config()
    task_params = settings.get_task02_parameters()
    db_names = settings.get_database_names()

    # Parameters: env-var overrides > config.json values > hardcoded defaults
    lookback_hours = _get_env_int('LOOKBACK_HOURS', task_params.get('lookback_hours', 36))
    lookback_years = _get_env_int('LOOKBACK_YEARS', task_params.get('lookback_years', 2))
    lookforward_days = _get_env_int('LOOKFORWARD_DAYS', task_params.get('lookforward_days', 45))
    batch_size = _get_env_int('BATCH_SIZE', task_params.get('batch_size', 5000))
    skip_unchanged_records = _get_env_bool(
        'SKIP_UNCHANGED_RECORDS', task_params.get('skip_unchanged_records', True)
    )
    enable_asymmetric_join = _get_env_bool(
        'ENABLE_ASYMMETRIC_JOIN', task_params.get('enable_asymmetric_join', True)
    )
    enable_stale_cleanup = _get_env_bool(
        'ENABLE_STALE_CLEANUP', task_params.get('enable_stale_cleanup', True)
    )

    logger.info("Configuration settings:")
    logger.info(f"  Lookback: {lookback_years} years, +{lookforward_days} days")
    logger.info(f"  Updates: last {lookback_hours} hours")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  Skip unchanged records: {'YES' if skip_unchanged_records else 'NO'}")
    logger.info(f"  Asymmetric join: {'ENABLED' if enable_asymmetric_join else 'DISABLED'}")
    logger.info(f"  Stale cleanup: {'ENABLED' if enable_stale_cleanup else 'DISABLED'}")

    # Initialize connections
    _conn_factory = ConnectionFactory(sf_config, pg_config)
    sf_manager = _conn_factory.get_snowflake_manager()
    pg_manager = _conn_factory.get_postgres_manager()

    # Initialize query builder and processor
    query_builder = QueryBuilder()
    processor = ConflictProcessor(
        sf_manager, pg_manager, query_builder, db_names, batch_size,
        skip_unchanged_records=skip_unchanged_records,
        enable_asymmetric_join=enable_asymmetric_join,
        enable_stale_cleanup=enable_stale_cleanup,
    )

    # Step 1: Fetch reference data from Postgres
    ref_data = processor.fetch_reference_data()

    # Step 2: Build conflict detection query (v3 with temp tables)
    queries = query_builder.build_conflict_detection_query_v3(
        db_names=db_names,
        excluded_agencies=ref_data['excluded_agencies'],
        excluded_ssns=ref_data['excluded_ssns'],
        settings_data=ref_data['settings'],
        mph_data=ref_data['mph'],
        lookback_years=lookback_years,
        lookforward_days=lookforward_days,
        lookback_hours=lookback_hours,
        enable_asymmetric_join=enable_asymmetric_join,
    )

    # Step 3: Stream and process conflicts
    # No timeout callback -- ECS has no 15-minute limit.
    # Instead, check the _shutdown_requested flag for graceful SIGTERM handling.
    def check_shutdown():
        return _shutdown_requested

    stats = processor.stream_and_process_conflicts_v3(
        queries,
        timeout_callback=check_shutdown,
    )

    # Close connections
    _conn_factory.close_all()
    _conn_factory = None

    return {
        'status': 'completed' if stats['errors'] == 0 else 'partial',
        'statistics': stats,
        'parameters': {
            'lookback_hours': lookback_hours,
            'lookback_years': lookback_years,
            'lookforward_days': lookforward_days,
            'batch_size': batch_size,
            'skip_unchanged_records': skip_unchanged_records,
            'enable_asymmetric_join': enable_asymmetric_join,
            'enable_stale_cleanup': enable_stale_cleanup,
        },
    }


# ---------------------------------------------------------------------------
# Action Registry
# ---------------------------------------------------------------------------
# Maps action names to handler functions.
# Add new actions here as they are implemented.

# Pipeline results collected during execution.
# Postflight reads this to generate the status email and row-count deltas.
_pipeline_results: List[dict] = []


def _run_postflight_wrapper(settings: Settings) -> dict:
    """Wrapper that passes accumulated pipeline results to postflight."""
    return run_postflight(settings, pipeline_results=list(_pipeline_results))


ACTION_REGISTRY: Dict[str, Callable[[Settings], dict]] = {
    'task00_preflight': run_preflight,
    'validate_config': validate_config,
    'test_connections': test_connections,
    'task02_00_run_conflict_update': run_conflict_update,
    'task99_postflight': _run_postflight_wrapper,
    # Future actions:
    # 'task01_copy_to_staging': run_task01,
    # 'task02_01_run_conflict_insert': run_conflict_insert,
}


def _parse_actions(action_str: str) -> List[str]:
    """
    Parse the ACTION env var into a list of action names.

    Supports:
      - Single action:   "test_connections"
      - Comma-separated: "validate_config, test_connections, task02_00_run_conflict_update"
      - Empty/unset:     falls back to DEFAULT_ACTIONS (full pipeline)
    """
    actions = [a.strip() for a in action_str.split(',') if a.strip()]
    return actions if actions else list(DEFAULT_ACTIONS)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """
    Container entry point for ECS Fargate execution.

    Reads ACTION env var (comma-separated list supported), executes each
    action sequentially, and exits with code 0 if all succeed or 1 if
    any action fails.
    """
    start_time = time.time()
    action_str = os.environ.get('ACTION', '').strip()
    actions = _parse_actions(action_str)

    logger.info("=" * 70)
    logger.info("CONFLICT MANAGEMENT - ECS CONTAINER")
    logger.info("=" * 70)
    logger.info(f"Actions: {' -> '.join(actions)}" if len(actions) > 1
                else f"Action: {actions[0]}")
    logger.info(f"Environment: {os.environ.get('ENVIRONMENT', 'unknown')}")

    # Validate all action names upfront before running any
    available = ', '.join(sorted(ACTION_REGISTRY.keys()))
    for action in actions:
        if action not in ACTION_REGISTRY:
            logger.error(f"Unknown action: {action}")
            logger.error(f"Available actions: {available}")
            sys.exit(1)

    try:
        settings = Settings()
        sf_config = settings.get_snowflake_config()
        pg_config = settings.get_postgres_config()
        db_names = settings.get_database_names()

        logger.info("Loading configuration...")
        logger.info(f"  Snowflake: {sf_config['account']} / {db_names['sf_database']}.{db_names['sf_schema']}")
        logger.info(f"  Postgres: {pg_config['host']} / {db_names['pg_database']}.{db_names['pg_schema']}")

        # Execute actions sequentially
        results = []
        _pipeline_results.clear()  # Reset for this run

        for i, action in enumerate(actions, 1):
            action_start = time.time()
            label = f"[{i}/{len(actions)}] " if len(actions) > 1 else ""

            logger.info("")
            logger.info("-" * 70)
            logger.info(f"{label}RUNNING: {action}")
            logger.info("-" * 70)

            # Check for SIGTERM between actions
            if _shutdown_requested:
                logger.warning(f"Shutdown requested -- skipping {action} and remaining actions")
                break

            result = ACTION_REGISTRY[action](settings)
            action_duration = time.time() - action_start
            result['action'] = action
            result['duration_seconds'] = round(action_duration, 2)
            results.append(result)
            _pipeline_results.append(result)  # Accumulate for postflight

            status = result.get('status', 'unknown')
            logger.info(f"{label}FINISHED: {action} -- {status} ({format_duration(action_duration)})")

            # Stop on failure -- but still run postflight if it's in the pipeline
            if status not in ('success', 'completed'):
                logger.error(f"Action '{action}' failed with status '{status}' -- stopping pipeline")

                # Run postflight for cleanup even on failure (re-enable pg_cron,
                # clear InProgressFlag) if it's in the remaining actions
                remaining = actions[i:]  # actions after the failed one (i is 1-based, already incremented)
                if 'task99_postflight' in remaining:
                    logger.info("")
                    logger.info("-" * 70)
                    logger.info("RUNNING: task99_postflight (cleanup after failure)")
                    logger.info("-" * 70)
                    try:
                        pf_start = time.time()
                        pf_result = ACTION_REGISTRY['task99_postflight'](settings)
                        pf_duration = time.time() - pf_start
                        pf_result['action'] = 'task99_postflight'
                        pf_result['duration_seconds'] = round(pf_duration, 2)
                        results.append(pf_result)
                        _pipeline_results.append(pf_result)
                        logger.info(f"FINISHED: task99_postflight -- {pf_result.get('status')} "
                                    f"({format_duration(pf_duration)})")
                    except Exception as pf_err:
                        logger.error(f"Postflight cleanup failed: {pf_err}")

                break

        # Final summary
        total_duration = time.time() - start_time
        all_ok = all(r.get('status') in ('success', 'completed') for r in results)
        overall_status = 'completed' if (all_ok and len(results) == len(actions)) else 'failed'

        logger.info("")
        logger.info("=" * 70)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 70)
        for r in results:
            logger.info(f"  {r['action']}: {r['status']} ({r['duration_seconds']}s)")
        logger.info(f"Overall: {overall_status}")
        logger.info(f"Total duration: {format_duration(total_duration)}")
        logger.info("=" * 70)

        sys.exit(0 if overall_status == 'completed' else 1)

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Container execution failed after {format_duration(duration)}: {e}", exc_info=True)

        # Best-effort connection cleanup
        if _conn_factory is not None:
            try:
                _conn_factory.close_all()
            except Exception:
                pass

        sys.exit(1)


if __name__ == '__main__':
    main()
