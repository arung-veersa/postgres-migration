"""
Task 02.01 - InService Conflict Detection (Update + Insert)

Detects conflicts between regular visits and InService events for the same
caregiver at different providers.  Processes both directions:
  - Visit (primary) vs InService event (conflicting)
  - InService event (primary) vs Visit (conflicting)

This is a self-contained streaming processor -- simpler than task02_00's
ConflictProcessor because InService conflicts:
  - Have all 7 rule flags hardcoded to 'N' (no distance/schedule computation)
  - Have InServiceFlag = 'Y'
  - Use a temporal overlap join instead of same-VisitDate join
  - Use synthetic VisitIDs (MD5 hash) for InService events
  - Do not require stale cleanup or asymmetric join optimisation

Pipeline:
  1. Fetch reference data from PostgreSQL (excluded agencies / SSNs)
  2. Build Snowflake SQL (3 steps: visits temp, events temp, UNION ALL pairs)
  3. Stream and process results in batches (update existing, insert new)
"""

import os
import time
from typing import Optional, Callable, Dict, Any, List, Tuple

import psycopg2
import psycopg2.extras
import psycopg2.errors

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.query_builder import QueryBuilder, INSERVICE_INSERT_COLUMN_MAP
from lib.utils import get_logger, format_duration

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm_key(visit_id, con_visit_id) -> tuple:
    """Normalise a (VisitID, ConVisitID) pair for dict-key comparison.

    Snowflake MD5() returns 32-char hex WITHOUT dashes, but PostgreSQL
    stores VisitID as UUID type which auto-formats WITH dashes.  Stripping
    dashes and lower-casing ensures consistent matching regardless of source.
    """
    v = str(visit_id or '').replace('-', '').lower()
    c = str(con_visit_id or '').replace('-', '').lower()
    return (v, c)


def _get_env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer for env var {name}={value!r}, using default={default}")
        return default


def _get_env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in ('true', '1', 'yes')


# ---------------------------------------------------------------------------
# InService UPDATE builder (simplified -- no flag computation)
# ---------------------------------------------------------------------------

# Columns that are updated unconditionally from Snowflake data.
# This covers all data columns from INSERVICE_INSERT_COLUMN_MAP except the
# identifiers used in the WHERE clause (VisitID, ConVisitID).
_UPDATE_EXCLUDE_KEYS = frozenset(['VisitID', 'ConVisitID'])

# Column name translation: Snowflake name -> PostgreSQL name
_COLUMN_NAME_MAP = {
    'ETATravleMinutes': 'ETATravelMinutes',
    'SchVisitTimeSame': 'SchAndVisitTimeSameFlag',
}


def _build_update_params(
    row: Dict[str, Any],
    existing: Dict[str, Any],
    schema: str,
) -> Optional[Tuple[str, tuple]]:
    """
    Build an UPDATE statement for a single InService conflict record.

    InService updates are simpler than regular task02_00 because:
      - All 7 conflict flags are always 'N' (no conditional N->Y logic)
      - InServiceFlag: conditional (only N -> Y, preserve Y)
      - StatusFlag: conditional (not W/I -> U, preserve W/I)
      - All other columns: unconditional update
    """
    set_clauses: list = []
    params: list = []

    for sf_col, pg_col in INSERVICE_INSERT_COLUMN_MAP:
        if sf_col in _UPDATE_EXCLUDE_KEYS:
            continue
        if sf_col not in row:
            continue

        pg_col = _COLUMN_NAME_MAP.get(sf_col, pg_col)

        # StatusFlag: preserve W/I
        if sf_col == 'StatusFlag':
            existing_status = existing.get('StatusFlag', '')
            if existing_status not in ('W', 'I'):
                set_clauses.append('"StatusFlag" = %s')
                params.append('U')
            continue

        # InServiceFlag: only N -> Y
        if pg_col == 'InServiceFlag':
            existing_flag = existing.get('InServiceFlag', 'N')
            if existing_flag == 'N':
                set_clauses.append('"InServiceFlag" = %s')
                params.append('Y')
            continue

        # Everything else: unconditional
        set_clauses.append(f'"{pg_col}" = %s')
        params.append(row[sf_col])

    # Fixed-value columns
    set_clauses.append('"UpdateFlag" = NULL')
    set_clauses.append('"UpdatedDate" = CURRENT_TIMESTAMP')
    set_clauses.append('"ResolveDate" = NULL')

    if not set_clauses:
        return None

    visit_id = row['VisitID']
    con_visit_id = row.get('ConVisitID')

    sql = f"""
        UPDATE {schema}.conflictvisitmaps
        SET {', '.join(set_clauses)}
        WHERE (
            ("VisitID" = %s AND "ConVisitID" = %s)
            OR ("VisitID" = %s AND "ConVisitID" IS NULL AND %s IS NULL)
        )
    """
    params.extend([visit_id, con_visit_id, visit_id, con_visit_id])
    return sql, tuple(params)


# ---------------------------------------------------------------------------
# Main action
# ---------------------------------------------------------------------------

def run_task02_01_inservice_conflict(
    settings: Settings,
    shutdown_check: Optional[Callable[[], bool]] = None,
) -> dict:
    """
    Execute the InService conflict detection and update+insert pipeline.
    """
    sf_config = settings.get_snowflake_config()
    pg_config = settings.get_postgres_config()
    task_params = settings.get_task02_parameters()
    db_names = settings.get_database_names()

    lookback_years = _get_env_int('LOOKBACK_YEARS', task_params.get('lookback_years', 2))
    lookforward_days = _get_env_int('LOOKFORWARD_DAYS', task_params.get('lookforward_days', 45))
    batch_size = _get_env_int('BATCH_SIZE', task_params.get('batch_size', 5000))
    enable_inservice = _get_env_bool(
        'ENABLE_INSERVICE', task_params.get('enable_inservice', True)
    )

    if not enable_inservice:
        logger.info("InService conflict processing is DISABLED via config")
        return {'status': 'completed', 'statistics': {'skipped': True}}

    logger.info("=" * 60)
    logger.info("TASK 02.01 - InService Conflict Detection")
    logger.info("=" * 60)
    logger.info("Configuration:")
    logger.info(f"  Date range: -{lookback_years}Y to +{lookforward_days}D")
    logger.info(f"  Batch size: {batch_size}")

    conn_factory = ConnectionFactory(sf_config, pg_config)

    stats = {
        'rows_fetched': 0,
        'rows_updated': 0,
        'rows_inserted': 0,
        'rows_skipped_existing': 0,
        'batches_processed': 0,
        'errors': 0,
        'unique_visit_ids': set(),
    }

    try:
        sf_manager = conn_factory.get_snowflake_manager()
        pg_manager = conn_factory.get_postgres_manager()
        query_builder = QueryBuilder()

        # ── Step A: Fetch reference data from Postgres ──────────────────
        logger.info("Fetching reference data from PostgreSQL...")

        query = query_builder.build_reference_query(
            'pg_fetch_excluded_agencies.sql', db_names
        )
        agencies = pg_manager.execute_query(query, database=db_names['pg_database'])
        excluded_agencies = [r[0] for r in agencies if r[0]]
        logger.info(f"  Excluded agencies: {len(excluded_agencies)}")

        query = query_builder.build_reference_query(
            'pg_fetch_excluded_ssns.sql', db_names
        )
        ssns = pg_manager.execute_query(query, database=db_names['pg_database'])
        excluded_ssns = [r[0] for r in ssns if r[0]]
        logger.info(f"  Excluded SSNs: {len(excluded_ssns)}")

        # ── Step B: Build Snowflake queries ─────────────────────────────
        queries = query_builder.build_inservice_queries(
            db_names=db_names,
            excluded_agencies=excluded_agencies,
            excluded_ssns=excluded_ssns,
            lookback_years=lookback_years,
            lookforward_days=lookforward_days,
        )

        # ── Step C: Build INSERT template (once) ────────────────────────
        insert_sql, insert_sf_columns = query_builder.build_inservice_insert_template(
            db_names
        )

        # ── Step D: Execute Snowflake queries and stream results ────────
        pg_conn = pg_manager.get_connection(database=db_names['pg_database'])
        schema = db_names['pg_schema']

        with sf_manager.streaming_cursor() as cursor:
            # Step 0: excluded SSNs
            if queries.get('step0_create'):
                logger.info("STEP 0: Creating excluded_ssns temp table...")
                cursor.execute(queries['step0_create'])
                for stmt in queries.get('step0_inserts', []):
                    cursor.execute(stmt)
                logger.info(f"  Loaded {len(queries.get('step0_inserts', []))} batch(es)")

            # Step 1: inservice_visits temp table
            step1_start = time.time()
            logger.info("STEP 1: Creating inservice_visits temp table...")
            cursor.execute(queries['step1'])
            step1_dur = time.time() - step1_start
            logger.info(f"  Done ({step1_dur:.1f}s)")

            # Step 2: inservice_events temp table
            step2_start = time.time()
            logger.info("STEP 2: Creating inservice_events temp table...")
            cursor.execute(queries['step2'])
            step2_dur = time.time() - step2_start
            logger.info(f"  Done ({step2_dur:.1f}s)")

            # Step 3: streaming pairs
            step3_start = time.time()
            logger.info("STEP 3: Executing final InService pairs query...")
            cursor.execute(queries['step3'])
            column_names = [desc[0] for desc in cursor.description]
            logger.info(f"  Result columns: {len(column_names)}")
            logger.info("  Streaming results...")

            batch: List[Dict[str, Any]] = []
            batch_number = 0

            for sf_row in cursor:
                # Check for graceful shutdown
                if shutdown_check and shutdown_check():
                    logger.warning("Shutdown requested -- stopping InService processing")
                    break

                row = dict(zip(column_names, sf_row))
                batch.append(row)
                stats['rows_fetched'] += 1

                vid = row.get('VisitID')
                con_vid = row.get('ConVisitID')
                if vid:
                    stats['unique_visit_ids'].add(vid)
                if con_vid:
                    stats['unique_visit_ids'].add(con_vid)

                if len(batch) >= batch_size:
                    batch_number += 1
                    _process_batch(
                        batch, batch_number, pg_conn, schema,
                        insert_sql, insert_sf_columns, stats,
                    )
                    batch = []

            # Flush remaining
            if batch:
                batch_number += 1
                _process_batch(
                    batch, batch_number, pg_conn, schema,
                    insert_sql, insert_sf_columns, stats,
                )

            step3_dur = time.time() - step3_start

        # ── Summary ─────────────────────────────────────────────────────
        stats['unique_visit_ids'] = len(stats['unique_visit_ids'])
        _log_summary(stats, step1_dur, step2_dur, step3_dur)

        return {
            'status': 'completed' if stats['errors'] == 0 else 'partial',
            'statistics': stats,
            'parameters': {
                'lookback_years': lookback_years,
                'lookforward_days': lookforward_days,
                'batch_size': batch_size,
            },
        }

    finally:
        conn_factory.close_all()


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def _process_batch(
    batch: List[Dict[str, Any]],
    batch_number: int,
    pg_conn,
    schema: str,
    insert_sql: str,
    insert_sf_columns: List[str],
    stats: Dict[str, Any],
) -> None:
    """Process a batch of InService conflict pairs: update existing, insert new."""
    batch_start = time.time()

    # Collect all VisitIDs in this batch for PG lookup
    visit_ids = set()
    for row in batch:
        vid = row.get('VisitID')
        con_vid = row.get('ConVisitID')
        if vid:
            visit_ids.add(vid)
        if con_vid:
            visit_ids.add(con_vid)

    # Fetch existing PG records matching any VisitID in this batch
    existing_records = _fetch_existing_records(pg_conn, schema, visit_ids)
    matched = len(existing_records)

    # Partition rows into updates vs inserts
    updates: List[Tuple[str, tuple]] = []
    new_rows: List[Dict[str, Any]] = []

    for row in batch:
        key = _norm_key(row.get('VisitID', ''), row.get('ConVisitID', ''))
        existing = existing_records.get(key)
        if existing:
            result = _build_update_params(row, existing, schema)
            if result:
                updates.append(result)
            else:
                stats['rows_skipped_existing'] += 1
        else:
            new_rows.append(row)

    # Execute updates
    updated = 0
    if updates:
        try:
            pg_cursor = pg_conn.cursor()
            for sql, params in updates:
                pg_cursor.execute(sql, params)
            pg_conn.commit()
            updated = len(updates)
            pg_cursor.close()
        except Exception as e:
            pg_conn.rollback()
            logger.error(f"  Batch {batch_number}: UPDATE error: {e}")
            stats['errors'] += 1

    # Execute inserts
    inserted = 0
    if new_rows:
        inserted = _execute_inserts(
            new_rows, pg_conn, schema, insert_sql, insert_sf_columns,
            batch_number, stats,
        )

    stats['rows_updated'] += updated
    stats['rows_inserted'] += inserted
    stats['batches_processed'] += 1

    batch_dur = time.time() - batch_start
    logger.info(
        f"  Batch {batch_number}: {len(batch)} rows "
        f"(Existing: {matched}, Updated: {updated}, New: {len(new_rows)}, "
        f"Inserted: {inserted}) [{batch_dur:.1f}s]"
    )


def _fetch_existing_records(
    pg_conn,
    schema: str,
    visit_ids: set,
) -> Dict[tuple, Dict[str, Any]]:
    """
    Fetch ALL existing conflict records from PostgreSQL that match any
    VisitID in the batch.  No InService-specific filter -- we need to find
    any record with a matching (VisitID, ConVisitID) to avoid unique-constraint
    collisions on idx_cvm_visitids_unique, regardless of InServiceFlag value.

    Returns dict keyed by (VisitID, ConVisitID) -> row dict.
    """
    if not visit_ids:
        return {}

    placeholders = ', '.join(['%s'] * len(visit_ids))
    ids_list = list(visit_ids)

    sql = f"""
        SELECT *
        FROM {schema}.conflictvisitmaps
        WHERE "VisitID" IN ({placeholders}) OR "ConVisitID" IN ({placeholders})
    """
    params = ids_list + ids_list

    cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()

    records: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        key = _norm_key(row.get('VisitID', ''), row.get('ConVisitID', ''))
        records[key] = dict(row)

    return records


def _execute_inserts(
    new_rows: List[Dict[str, Any]],
    pg_conn,
    schema: str,
    insert_sql: str,
    insert_sf_columns: List[str],
    batch_number: int,
    stats: Dict[str, Any],
) -> int:
    """
    Execute batch INSERTs for new InService conflict records.

    Strategy:
      1. Try execute_batch for the whole batch (fast path).
      2. On UniqueViolation: fix ID sequence and retry once.
      3. If retry also fails with UniqueViolation: fall back to row-by-row
         insert, skipping individual collisions and counting successes.
    """
    params_list = []
    for row in new_rows:
        params_list.append(tuple(row.get(col) for col in insert_sf_columns))

    if not params_list:
        return 0

    cursor = pg_conn.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, insert_sql, params_list, page_size=100)
        pg_conn.commit()
        inserted = len(params_list)
        logger.info(f"    Batch {batch_number}: {inserted} InService rows inserted (COMMITTED)")
        return inserted
    except psycopg2.errors.UniqueViolation as e:
        pg_conn.rollback()
        detail = e.diag.message_detail or ''

        # Check if it's a PK (ID) sequence issue vs (VisitID, ConVisitID) collision
        if 'ID' in detail and 'VisitID' not in detail:
            logger.warning(f"    Batch {batch_number}: PK sequence collision -- fixing and retrying")
            try:
                seq_sql = f"""
                    SELECT setval(
                        pg_get_serial_sequence('{schema}.conflictvisitmaps', 'ID'),
                        COALESCE((SELECT MAX("ID") FROM {schema}.conflictvisitmaps), 1)
                    )
                """
                cursor.execute(seq_sql)
                pg_conn.commit()
                logger.info("    Sequence advanced -- retrying batch")

                psycopg2.extras.execute_batch(cursor, insert_sql, params_list, page_size=100)
                pg_conn.commit()
                inserted = len(params_list)
                logger.info(f"    Batch {batch_number}: {inserted} InService rows inserted (RETRY OK)")
                return inserted
            except psycopg2.errors.UniqueViolation:
                pg_conn.rollback()
                logger.warning(f"    Batch {batch_number}: Batch retry hit collision -- falling back to row-by-row")
                return _insert_row_by_row(cursor, pg_conn, insert_sql, params_list, batch_number, stats)
            except Exception as retry_err:
                pg_conn.rollback()
                logger.error(f"    Batch {batch_number}: Retry failed: {retry_err}")
                stats['errors'] += 1
                return 0
        else:
            # (VisitID, ConVisitID) collision -- fall back to row-by-row
            logger.warning(
                f"    Batch {batch_number}: VisitID collision detected -- falling back to row-by-row"
            )
            return _insert_row_by_row(cursor, pg_conn, insert_sql, params_list, batch_number, stats)
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"    Batch {batch_number}: INSERT error: {e}")
        stats['errors'] += 1
        return 0
    finally:
        cursor.close()


def _insert_row_by_row(
    cursor,
    pg_conn,
    insert_sql: str,
    params_list: list,
    batch_number: int,
    stats: Dict[str, Any],
) -> int:
    """Insert rows one at a time, skipping any that violate unique constraints."""
    inserted = 0
    skipped = 0
    for params in params_list:
        try:
            cursor.execute(insert_sql, params)
            pg_conn.commit()
            inserted += 1
        except psycopg2.errors.UniqueViolation:
            pg_conn.rollback()
            skipped += 1
        except Exception as e:
            pg_conn.rollback()
            logger.error(f"    Batch {batch_number}: Row insert error: {e}")
            stats['errors'] += 1
    if skipped:
        logger.info(f"    Batch {batch_number}: Row-by-row: {inserted} inserted, {skipped} skipped (existing)")
    else:
        logger.info(f"    Batch {batch_number}: {inserted} InService rows inserted (row-by-row, COMMITTED)")
    return inserted


# ---------------------------------------------------------------------------
# Summary logging
# ---------------------------------------------------------------------------

def _log_summary(
    stats: Dict[str, Any],
    step1_dur: float,
    step2_dur: float,
    step3_dur: float,
) -> None:
    """Log a human-readable summary of the InService processing run."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("INSERVICE CONFLICT PROCESSING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Snowflake Step 1 (visits temp table): {step1_dur:.1f}s")
    logger.info(f"  Snowflake Step 2 (events temp table): {step2_dur:.1f}s")
    logger.info(f"  Snowflake Step 3 (pairs + processing): {step3_dur:.1f}s")
    logger.info("")
    logger.info(f"  Rows fetched from Snowflake: {stats['rows_fetched']:,}")
    logger.info(f"  Unique VisitIDs: {stats['unique_visit_ids']:,}")
    logger.info(f"  Batches processed: {stats['batches_processed']}")
    logger.info("")
    logger.info(f"  Rows updated: {stats['rows_updated']:,}")
    logger.info(f"  Rows inserted: {stats['rows_inserted']:,}")
    logger.info(f"  Rows skipped (no changes): {stats['rows_skipped_existing']:,}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("=" * 60)
