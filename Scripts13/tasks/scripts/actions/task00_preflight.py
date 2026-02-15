"""
Task 00 - Preflight Checks

Pre-run validation and setup before the heavy pipeline tasks execute:
  1. Validate config parameters (lookback_hours > 0, batch_size > 0, etc.)
  2. Verify database connectivity (Snowflake and PostgreSQL)
  3. Check required PostgreSQL tables exist
  4. Disable the pg_cron job that refreshes the materialized view
  5. Set InProgressFlag = 1 in the settings table
  6. Sync identity sequences (advance to MAX(ID) if behind -- prevents PK collisions)
  7. VACUUM all tables (reclaim dead-tuple space so subsequent tasks start clean)
  8. ANALYZE all tables (refresh planner statistics for optimal query plans)
  9. Capture pre-run row counts for key tables (stored in result for postflight)
"""

import time
from typing import Dict, Any

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.utils import get_logger, format_duration

logger = get_logger(__name__)


def _verify_snowflake(conn_factory: ConnectionFactory) -> Dict[str, Any]:
    """Test Snowflake connectivity and return version."""
    sf_manager = conn_factory.get_snowflake_manager()
    sf_conn = sf_manager.get_connection()
    cursor = sf_conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()[0]
    cursor.close()
    logger.info(f"  Snowflake connected: v{version}")
    return {'connected': True, 'version': version}


def _verify_postgres(conn_factory: ConnectionFactory, db_name: str) -> Dict[str, Any]:
    """Test PostgreSQL connectivity and return version."""
    pg_manager = conn_factory.get_postgres_manager()
    pg_conn = pg_manager.get_connection(db_name)
    cursor = pg_conn.cursor()
    cursor.execute("SELECT version()")
    version = cursor.fetchone()[0]
    cursor.close()
    pg_conn.close()
    logger.info(f"  PostgreSQL connected: {version[:80]}")
    return {'connected': True, 'version': version[:80]}


def _check_required_tables(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
    required_tables: list,
) -> Dict[str, Any]:
    """Check that all required tables exist in the schema."""
    pg_manager = conn_factory.get_postgres_manager()
    pg_conn = pg_manager.get_connection(db_name)
    cursor = pg_conn.cursor()

    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    available = {row[0] for row in cursor.fetchall()}

    # Also check materialized views
    cursor.execute(
        """
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = %s
        ORDER BY matviewname
        """,
        (schema,),
    )
    mat_views = {row[0] for row in cursor.fetchall()}

    cursor.close()
    pg_conn.close()

    missing = []
    present = []
    for table in required_tables:
        if table in available:
            present.append(table)
            logger.info(f"    {table}: OK")
        else:
            missing.append(table)
            logger.warning(f"    {table}: MISSING")

    return {
        'available_tables': sorted(available),
        'materialized_views': sorted(mat_views),
        'required_present': present,
        'required_missing': missing,
    }


def _validate_config_params(settings: Settings) -> Dict[str, Any]:
    """Validate that key config parameters are sane."""
    common_params = settings.get_common_parameters()
    task02_params = settings.get_task02_parameters()
    issues = []

    # Common parameters
    lookback_years = common_params.get('lookback_years', 0)
    if lookback_years <= 0:
        issues.append(f"common_parameters.lookback_years must be > 0 (got {lookback_years})")

    lookforward_days = common_params.get('lookforward_days', 0)
    if lookforward_days <= 0:
        issues.append(f"common_parameters.lookforward_days must be > 0 (got {lookforward_days})")

    batch_size = common_params.get('batch_size', 0)
    if batch_size <= 0:
        issues.append(f"common_parameters.batch_size must be > 0 (got {batch_size})")

    lookback_hours = common_params.get('lookback_hours', 0)
    if lookback_hours <= 0:
        issues.append(f"common_parameters.lookback_hours must be > 0 (got {lookback_hours})")

    if issues:
        for issue in issues:
            logger.warning(f"  Config issue: {issue}")
    else:
        logger.info("  All config parameters validated OK")

    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'common_parameters': common_params,
        'task02_parameters': task02_params,
    }


def _disable_pg_cron_job(
    conn_factory: ConnectionFactory,
    job_name: str,
) -> Dict[str, Any]:
    """
    Disable the pg_cron job by name. Returns the job's current schedule
    so postflight can re-enable it with the same schedule.

    pg_cron jobs are managed in the 'postgres' database, so we connect there.
    If pg_cron is not installed or the job doesn't exist, we log a warning
    and continue (non-fatal).
    """
    pg_manager = conn_factory.get_postgres_manager()
    result = {'disabled': False, 'schedule': None, 'jobid': None}

    try:
        # pg_cron runs in the 'postgres' database
        pg_conn = pg_manager.get_connection('postgres')
        cursor = pg_conn.cursor()

        # Check if cron schema exists
        cursor.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'cron'"
        )
        if cursor.fetchone() is None:
            logger.warning("  pg_cron not installed (cron schema not found) -- skipping")
            cursor.close()
            pg_conn.close()
            return result

        # Find the job by name
        cursor.execute(
            "SELECT jobid, schedule, active FROM cron.job WHERE jobname = %s",
            (job_name,),
        )
        row = cursor.fetchone()
        if row is None:
            logger.warning(f"  pg_cron job '{job_name}' not found -- skipping")
            cursor.close()
            pg_conn.close()
            return result

        jobid, schedule, active = row
        result['jobid'] = jobid
        result['schedule'] = schedule

        if not active:
            logger.info(f"  pg_cron job '{job_name}' (id={jobid}) already inactive")
            result['disabled'] = True
        else:
            cursor.execute(
                "UPDATE cron.job SET active = false WHERE jobid = %s",
                (jobid,),
            )
            pg_conn.commit()
            result['disabled'] = True
            logger.info(
                f"  pg_cron job '{job_name}' (id={jobid}) disabled "
                f"(was schedule: {schedule})"
            )

        cursor.close()
        pg_conn.close()

    except Exception as e:
        logger.warning(f"  Could not disable pg_cron job '{job_name}': {e}")

    return result


def _set_in_progress_flag(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
    value: int = 1,
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


def _sync_identity_sequences(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
    tables: list,
) -> Dict[str, Any]:
    """
    Ensure identity column sequences are ahead of MAX(ID) for each table.

    This is necessary when tables were bulk-loaded with explicit ID values
    (e.g. during migration) without advancing the underlying sequence.
    Without this fix, INSERTs that rely on the identity default will
    generate values that collide with existing rows.

    Args:
        tables: list of table names whose "ID" column is GENERATED BY DEFAULT
    """
    pg_manager = conn_factory.get_postgres_manager()
    pg_conn = pg_manager.get_connection(db_name)
    cursor = pg_conn.cursor()

    results: Dict[str, Any] = {}
    for table in tables:
        try:
            # Get the sequence backing the identity column
            cursor.execute(
                "SELECT pg_get_serial_sequence(%s, 'ID')",
                (f'{schema}.{table}',),
            )
            row = cursor.fetchone()
            if not row or row[0] is None:
                logger.info(f"    {table}: no identity sequence found -- skipping")
                results[table] = {'status': 'no_sequence'}
                continue

            seq_name = row[0]

            # Compare current sequence value with MAX(ID)
            cursor.execute(f'SELECT MAX("ID") FROM {schema}."{table}"')
            max_id = cursor.fetchone()[0] or 0

            cursor.execute(f"SELECT last_value FROM {seq_name}")
            seq_value = cursor.fetchone()[0] or 0

            if seq_value < max_id:
                cursor.execute(
                    f"SELECT setval(%s, %s)",
                    (seq_name, max_id),
                )
                pg_conn.commit()
                logger.warning(
                    f"    {table}: sequence {seq_name} was {seq_value:,}, "
                    f"advanced to {max_id:,} (MAX ID)"
                )
                results[table] = {
                    'status': 'fixed',
                    'was': seq_value,
                    'now': max_id,
                    'sequence': seq_name,
                }
            else:
                logger.info(
                    f"    {table}: sequence OK ({seq_value:,} >= MAX ID {max_id:,})"
                )
                results[table] = {
                    'status': 'ok',
                    'seq_value': seq_value,
                    'max_id': max_id,
                }
        except Exception as e:
            logger.warning(f"    {table}: sequence check failed -- {e}")
            pg_conn.rollback()
            results[table] = {'status': 'error', 'error': str(e)}

    cursor.close()
    pg_conn.close()
    return results


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
    result: Dict[str, Any] = {'tables_vacuumed': 0, 'errors': [], 'duration_seconds': 0}

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
            logger.info(f"  No tables found in {schema} -- skipping VACUUM")
            return result

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
    result: Dict[str, Any] = {'tables_analyzed': 0, 'errors': [], 'duration_seconds': 0}

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

def run_task00_preflight(settings: Settings) -> dict:
    """
    Execute all preflight checks.

    Returns a dict with status and all collected preflight data.
    The 'preflight_data' key is intended to be passed through to postflight
    via the pipeline results.
    """
    start = time.time()
    sf_config = settings.get_snowflake_config()
    pg_config = settings.get_postgres_config()
    db_names = settings.get_database_names()
    pipeline_config = settings.get_pipeline_config()

    conn_factory = ConnectionFactory(sf_config, pg_config)
    errors = []

    # --- 1. Validate config ---
    logger.info("Preflight: Validating configuration parameters...")
    config_check = _validate_config_params(settings)
    if not config_check['valid']:
        errors.append(f"Config validation issues: {'; '.join(config_check['issues'])}")

    # --- 2. Verify connectivity ---
    logger.info("Preflight: Verifying database connectivity...")
    try:
        sf_info = _verify_snowflake(conn_factory)
    except Exception as e:
        sf_info = {'connected': False, 'error': str(e)}
        errors.append(f"Snowflake connection failed: {e}")

    try:
        pg_info = _verify_postgres(conn_factory, db_names['pg_database'])
    except Exception as e:
        pg_info = {'connected': False, 'error': str(e)}
        errors.append(f"PostgreSQL connection failed: {e}")

    # If we can't connect to PG, stop early -- remaining checks need PG
    if not pg_info.get('connected'):
        conn_factory.close_all()
        return {
            'status': 'failed',
            'errors': errors,
            'snowflake': sf_info,
            'postgres': pg_info,
            'config_check': config_check,
        }

    # --- 3. Check required tables ---
    logger.info("Preflight: Checking required tables...")
    required_tables = pipeline_config.get('required_tables', [])
    table_check = _check_required_tables(
        conn_factory, db_names['pg_database'], db_names['pg_schema'], required_tables
    )
    if table_check['required_missing']:
        errors.append(
            f"Missing required tables: {', '.join(table_check['required_missing'])}"
        )

    # --- 4. Disable pg_cron job ---
    job_name = pipeline_config.get('pg_cron_job_name', '')
    cron_result = {'disabled': False, 'schedule': None, 'jobid': None}
    if job_name:
        logger.info(f"Preflight: Disabling pg_cron job '{job_name}'...")
        cron_result = _disable_pg_cron_job(conn_factory, job_name)
    else:
        logger.info("Preflight: No pg_cron job configured -- skipping")

    # --- 5. Set InProgressFlag ---
    logger.info("Preflight: Setting InProgressFlag = 1...")
    flag_set = _set_in_progress_flag(
        conn_factory, db_names['pg_database'], db_names['pg_schema'], value=1
    )

    # --- 6. Sync identity sequences ---
    # Only tables that receive INSERTs from the pipeline
    identity_tables = ['conflictvisitmaps']
    logger.info("Preflight: Syncing identity sequences...")
    seq_sync = _sync_identity_sequences(
        conn_factory, db_names['pg_database'], db_names['pg_schema'], identity_tables
    )

    # --- 7. VACUUM ---
    logger.info("Preflight: Running VACUUM on all tables...")
    vacuum_result = _vacuum_schema_tables(
        conn_factory, db_names['pg_database'], db_names['pg_schema']
    )
    if vacuum_result['errors']:
        errors.extend(vacuum_result['errors'])

    # --- 8. ANALYZE ---
    logger.info("Preflight: Running ANALYZE on all tables...")
    analyze_result = _analyze_schema_tables(
        conn_factory, db_names['pg_database'], db_names['pg_schema']
    )
    if analyze_result['errors']:
        errors.extend(analyze_result['errors'])

    # --- 9. Capture pre-run row counts ---
    count_tables = ['conflicts', 'conflictvisitmaps', 'conflictlog_staging', 'settings']
    logger.info("Preflight: Capturing pre-run row counts...")
    pre_counts = _capture_row_counts(
        conn_factory, db_names['pg_database'], db_names['pg_schema'], count_tables
    )

    conn_factory.close_all()

    status = 'success' if not errors else 'failed'
    duration = time.time() - start

    preflight_data = {
        'pg_cron_job_name': job_name,
        'pg_cron_schedule': cron_result.get('schedule'),
        'pg_cron_jobid': cron_result.get('jobid'),
        'pre_run_counts': pre_counts,
        'start_time': start,
    }

    logger.info(f"Preflight: {status.upper()} ({duration:.1f}s)")
    if errors:
        for err in errors:
            logger.error(f"  {err}")

    return {
        'status': status,
        'errors': errors,
        'snowflake': sf_info,
        'postgres': pg_info,
        'table_check': table_check,
        'config_check': config_check,
        'pg_cron': cron_result,
        'in_progress_flag_set': flag_set,
        'sequence_sync': seq_sync,
        'vacuum': vacuum_result,
        'analyze': analyze_result,
        'pre_run_counts': pre_counts,
        'preflight_data': preflight_data,
    }
