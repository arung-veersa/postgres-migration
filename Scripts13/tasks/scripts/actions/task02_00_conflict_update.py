"""
Task 02.00 - Conflict Update Action

Executes the v3 conflict detection and update pipeline:
  1. Fetch reference data from PostgreSQL (excluded agencies/SSNs, settings, mph)
  2. Build Snowflake SQL using v3 multi-step templates
  3. Stream and process conflicts (batch updates with change detection)
  4. Pair-precise stale cleanup
"""

import os
from typing import Optional, Callable

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.query_builder import QueryBuilder
from lib.conflict_processor import ConflictProcessor
from lib.utils import get_logger

logger = get_logger(__name__)


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


def run_task02_00_conflict_update(
    settings: Settings,
    shutdown_check: Optional[Callable[[], bool]] = None,
) -> dict:
    """
    Execute the full v3 conflict detection and update pipeline.

    Args:
        settings: Configuration settings
        shutdown_check: Optional callback that returns True if shutdown requested
    """
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
    enable_insert = _get_env_bool(
        'ENABLE_INSERT', task_params.get('enable_insert', True)
    )

    logger.info("Configuration settings:")
    logger.info(f"  Lookback: {lookback_years} years, +{lookforward_days} days")
    logger.info(f"  Updates: last {lookback_hours} hours")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  Skip unchanged records: {'YES' if skip_unchanged_records else 'NO'}")
    logger.info(f"  Asymmetric join: {'ENABLED' if enable_asymmetric_join else 'DISABLED'}")
    logger.info(f"  Stale cleanup: {'ENABLED' if enable_stale_cleanup else 'DISABLED'}")
    logger.info(f"  Insert new conflicts: {'ENABLED' if enable_insert else 'DISABLED'}")

    # Initialize connections
    conn_factory = ConnectionFactory(sf_config, pg_config)

    try:
        sf_manager = conn_factory.get_snowflake_manager()
        pg_manager = conn_factory.get_postgres_manager()

        # Initialize query builder and processor
        query_builder = QueryBuilder()
        processor = ConflictProcessor(
            sf_manager, pg_manager, query_builder, db_names, batch_size,
            skip_unchanged_records=skip_unchanged_records,
            enable_asymmetric_join=enable_asymmetric_join,
            enable_stale_cleanup=enable_stale_cleanup,
            enable_insert=enable_insert,
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
        timeout_callback = shutdown_check if shutdown_check else lambda: False

        stats = processor.stream_and_process_conflicts_v3(
            queries,
            timeout_callback=timeout_callback,
        )

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
                'enable_insert': enable_insert,
            },
        }

    finally:
        # Close connections
        conn_factory.close_all()
