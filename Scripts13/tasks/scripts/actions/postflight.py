"""
Task 99 - Postflight Cleanup

Post-run cleanup and reporting after the pipeline tasks complete:
  1. VACUUM all tables in the conflict_dev schema
  2. ANALYZE all tables in the conflict_dev schema
  3. Set InProgressFlag = 0 in the settings table
  4. Refresh the materialized view (CONCURRENTLY)
  5. Re-enable the pg_cron job with the schedule saved during preflight
  6. Capture post-run row counts and generate summary
  7. Send status email via AWS SES (if configured)
"""

import os
import time
from typing import Dict, Any, List, Optional

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.email_sender import send_pipeline_email
from lib.utils import get_logger, format_duration

logger = get_logger(__name__)


def _refresh_materialized_view(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
    view_name: str,
) -> Dict[str, Any]:
    """
    Refresh a materialized view using REFRESH MATERIALIZED VIEW CONCURRENTLY.

    CONCURRENTLY requires a UNIQUE index on the view but allows reads during refresh.
    Requires autocommit mode (cannot run inside a transaction block).
    """
    pg_manager = conn_factory.get_postgres_manager()
    result = {'refreshed': False, 'duration_seconds': 0}

    try:
        pg_conn = pg_manager.get_connection(db_name, autocommit=True)
        cursor = pg_conn.cursor()

        # Check if the materialized view exists
        cursor.execute(
            "SELECT 1 FROM pg_matviews WHERE schemaname = %s AND matviewname = %s",
            (schema, view_name),
        )
        if cursor.fetchone() is None:
            logger.warning(f"  Materialized view '{schema}.{view_name}' not found -- skipping refresh")
            cursor.close()
            pg_conn.close()
            return result

        start = time.time()
        logger.info(f"  Refreshing materialized view {schema}.{view_name} (CONCURRENTLY)...")

        try:
            cursor.execute(
                f'REFRESH MATERIALIZED VIEW CONCURRENTLY {schema}."{view_name}"'
            )
        except Exception as e:
            # If CONCURRENTLY fails (e.g., no unique index), fall back to regular refresh
            err_str = str(e).lower()
            if 'unique index' in err_str or 'concurrently' in err_str:
                logger.warning(f"  CONCURRENTLY failed ({e}), falling back to non-concurrent refresh")
                cursor.execute(
                    f'REFRESH MATERIALIZED VIEW {schema}."{view_name}"'
                )
            else:
                raise

        duration = time.time() - start
        result['refreshed'] = True
        result['duration_seconds'] = round(duration, 2)
        logger.info(f"  Materialized view refreshed in {format_duration(duration)}")

        cursor.close()
        pg_conn.close()

    except Exception as e:
        logger.error(f"  Failed to refresh materialized view: {e}")

    return result


def _enable_pg_cron_job(
    conn_factory: ConnectionFactory,
    job_name: str,
    jobid: Optional[int],
    schedule: Optional[str],
) -> bool:
    """
    Re-enable the pg_cron job with its original schedule.

    Uses the jobid and schedule captured during preflight.
    """
    if not jobid:
        logger.info(f"  No pg_cron job to re-enable (jobid not captured)")
        return False

    pg_manager = conn_factory.get_postgres_manager()
    try:
        pg_conn = pg_manager.get_connection('postgres')
        cursor = pg_conn.cursor()

        cursor.execute(
            "UPDATE cron.job SET active = true WHERE jobid = %s",
            (jobid,),
        )
        pg_conn.commit()
        cursor.close()
        pg_conn.close()

        logger.info(f"  pg_cron job '{job_name}' (id={jobid}) re-enabled (schedule: {schedule})")
        return True

    except Exception as e:
        logger.error(f"  Failed to re-enable pg_cron job '{job_name}': {e}")
        return False


def _vacuum_schema_tables(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
) -> Dict[str, Any]:
    """
    Run VACUUM on all tables in the schema.

    VACUUM cannot run inside a transaction, so we use autocommit mode.
    """
    pg_manager = conn_factory.get_postgres_manager()
    result = {'tables_vacuumed': 0, 'errors': [], 'duration_seconds': 0}

    try:
        # First, get the list of tables in normal mode
        pg_conn = pg_manager.get_connection(db_name)
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema,),
        )
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        pg_conn.close()

        if not tables:
            logger.info(f"  No tables found in {schema} -- skipping VACUUM")
            return result

        # Now run VACUUM in autocommit mode
        start = time.time()
        pg_conn = pg_manager.get_connection(db_name, autocommit=True)
        cursor = pg_conn.cursor()

        for table in tables:
            try:
                cursor.execute(f'VACUUM {schema}."{table}"')
                result['tables_vacuumed'] += 1
            except Exception as e:
                result['errors'].append(f"{table}: {e}")
                logger.warning(f"    VACUUM {schema}.{table} failed: {e}")

        cursor.close()
        pg_conn.close()

        duration = time.time() - start
        result['duration_seconds'] = round(duration, 2)
        logger.info(
            f"  VACUUM completed: {result['tables_vacuumed']}/{len(tables)} tables "
            f"in {format_duration(duration)}"
        )

    except Exception as e:
        logger.error(f"  VACUUM failed: {e}")
        result['errors'].append(str(e))

    return result


def _analyze_schema_tables(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
) -> Dict[str, Any]:
    """
    Run ANALYZE on all tables in the schema to update query planner statistics.

    ANALYZE can run inside a transaction but we use autocommit for consistency.
    """
    pg_manager = conn_factory.get_postgres_manager()
    result = {'tables_analyzed': 0, 'errors': [], 'duration_seconds': 0}

    try:
        pg_conn = pg_manager.get_connection(db_name)
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema,),
        )
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        pg_conn.close()

        if not tables:
            logger.info(f"  No tables found in {schema} -- skipping ANALYZE")
            return result

        start = time.time()
        pg_conn = pg_manager.get_connection(db_name, autocommit=True)
        cursor = pg_conn.cursor()

        for table in tables:
            try:
                cursor.execute(f'ANALYZE {schema}."{table}"')
                result['tables_analyzed'] += 1
            except Exception as e:
                result['errors'].append(f"{table}: {e}")
                logger.warning(f"    ANALYZE {schema}.{table} failed: {e}")

        cursor.close()
        pg_conn.close()

        duration = time.time() - start
        result['duration_seconds'] = round(duration, 2)
        logger.info(
            f"  ANALYZE completed: {result['tables_analyzed']}/{len(tables)} tables "
            f"in {format_duration(duration)}"
        )

    except Exception as e:
        logger.error(f"  ANALYZE failed: {e}")
        result['errors'].append(str(e))

    return result


def _set_in_progress_flag(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
    value: int = 0,
) -> bool:
    """Set InProgressFlag in the settings table."""
    pg_manager = conn_factory.get_postgres_manager()
    try:
        pg_conn = pg_manager.get_connection(db_name)
        cursor = pg_conn.cursor()
        cursor.execute(
            f'UPDATE {schema}."settings" SET "InProgressFlag" = %s',
            (value,),
        )
        pg_conn.commit()
        cursor.close()
        pg_conn.close()
        logger.info(f"  InProgressFlag set to {value}")
        return True
    except Exception as e:
        logger.warning(f"  Could not set InProgressFlag to {value}: {e}")
        return False


def _capture_row_counts(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
    tables: list,
) -> Dict[str, int]:
    """Capture row counts for key tables."""
    pg_manager = conn_factory.get_postgres_manager()
    pg_conn = pg_manager.get_connection(db_name)
    cursor = pg_conn.cursor()

    counts = {}
    for table in tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {schema}."{table}"')
            count = cursor.fetchone()[0]
            counts[table] = count
            logger.info(f"    {table}: {count:,} rows")
        except Exception as e:
            logger.warning(f"    {table}: error counting -- {e}")
            pg_conn.rollback()
            counts[table] = -1

    cursor.close()
    pg_conn.close()
    return counts


# ---------------------------------------------------------------------------
# Main action entry point
# ---------------------------------------------------------------------------

def run_postflight(settings: Settings, pipeline_results: Optional[List[dict]] = None) -> dict:
    """
    Execute all postflight cleanup tasks.

    Args:
        settings: Application settings
        pipeline_results: List of result dicts from previous pipeline actions.
            Used for the email summary and to extract preflight_data.

    Returns:
        Result dict with status and all postflight data.
    """
    start = time.time()
    sf_config = settings.get_snowflake_config()
    pg_config = settings.get_postgres_config()
    db_names = settings.get_database_names()
    pipeline_config = settings.get_pipeline_config()
    email_config = settings.get_email_config()

    pipeline_results = pipeline_results or []

    # Extract preflight data if available
    preflight_data = {}
    for r in pipeline_results:
        if r.get('action') == 'task00_preflight' and 'preflight_data' in r:
            preflight_data = r['preflight_data']
            break

    conn_factory = ConnectionFactory(sf_config, pg_config)
    warnings = []

    # --- 1. VACUUM ---
    logger.info("Postflight: Running VACUUM on all tables...")
    vacuum_result = _vacuum_schema_tables(
        conn_factory, db_names['pg_database'], db_names['pg_schema']
    )
    if vacuum_result['errors']:
        warnings.extend(vacuum_result['errors'])

    # --- 2. ANALYZE ---
    logger.info("Postflight: Running ANALYZE on all tables...")
    analyze_result = _analyze_schema_tables(
        conn_factory, db_names['pg_database'], db_names['pg_schema']
    )
    if analyze_result['errors']:
        warnings.extend(analyze_result['errors'])

    # --- 3. Set InProgressFlag = 0 ---
    logger.info("Postflight: Setting InProgressFlag = 0...")
    flag_set = _set_in_progress_flag(
        conn_factory, db_names['pg_database'], db_names['pg_schema'], value=0
    )

    # --- 4. Refresh materialized view ---
    mv_name = pipeline_config.get('materialized_view_name', '')
    mv_result = {'refreshed': False}
    if mv_name:
        logger.info(f"Postflight: Refreshing materialized view '{mv_name}'...")
        mv_result = _refresh_materialized_view(
            conn_factory, db_names['pg_database'], db_names['pg_schema'], mv_name
        )
    else:
        logger.info("Postflight: No materialized view configured -- skipping refresh")

    # --- 5. Re-enable pg_cron job ---
    job_name = pipeline_config.get('pg_cron_job_name', '')
    cron_enabled = False
    if job_name and preflight_data.get('pg_cron_jobid'):
        logger.info(f"Postflight: Re-enabling pg_cron job '{job_name}'...")
        cron_enabled = _enable_pg_cron_job(
            conn_factory,
            job_name,
            preflight_data.get('pg_cron_jobid'),
            preflight_data.get('pg_cron_schedule'),
        )
    elif job_name:
        logger.info(f"Postflight: pg_cron job '{job_name}' -- no jobid from preflight, skipping re-enable")
    else:
        logger.info("Postflight: No pg_cron job configured -- skipping re-enable")

    # --- 6. Capture post-run row counts ---
    count_tables = ['conflicts', 'conflictvisitmaps', 'conflictlog_staging', 'settings']
    logger.info("Postflight: Capturing post-run row counts...")
    post_counts = _capture_row_counts(
        conn_factory, db_names['pg_database'], db_names['pg_schema'], count_tables
    )

    pre_counts = preflight_data.get('pre_run_counts', {})

    # Log row count deltas
    if pre_counts:
        logger.info("Postflight: Row count deltas:")
        for table in count_tables:
            pre = pre_counts.get(table, -1)
            post = post_counts.get(table, -1)
            if pre >= 0 and post >= 0:
                delta = post - pre
                sign = '+' if delta >= 0 else ''
                logger.info(f"    {table}: {pre:,} -> {post:,} ({sign}{delta:,})")

    conn_factory.close_all()

    # --- 7. Send status email ---
    total_duration = time.time() - preflight_data.get('start_time', start)
    all_ok = all(r.get('status') in ('success', 'completed') for r in pipeline_results)
    overall_status = 'completed' if all_ok else 'failed'

    email_sent = False
    if email_config.get('enabled', False):
        logger.info("Postflight: Sending status email...")
        environment = os.environ.get('ENVIRONMENT', 'dev')
        email_sent = send_pipeline_email(
            email_config=email_config,
            pipeline_results=pipeline_results,
            pre_counts=pre_counts,
            post_counts=post_counts,
            total_duration=total_duration,
            overall_status=overall_status,
            environment=environment,
        )
    else:
        logger.info("Postflight: Email notifications disabled -- skipping")

    duration = time.time() - start
    logger.info(f"Postflight: completed in {format_duration(duration)}")
    if warnings:
        logger.warning(f"Postflight: {len(warnings)} warning(s) during cleanup")

    return {
        'status': 'success',
        'materialized_view': mv_result,
        'pg_cron_reenabled': cron_enabled,
        'vacuum': vacuum_result,
        'analyze': analyze_result,
        'in_progress_flag_cleared': flag_set,
        'post_run_counts': post_counts,
        'pre_run_counts': pre_counts,
        'email_sent': email_sent,
        'warnings': warnings,
    }
