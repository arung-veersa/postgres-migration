"""
Task 01 - Copy to Staging

Syncs payer-provider reminders and populates the staging table:
  1. Sync payer_provider_reminders from Snowflake dimension tables
     a. INSERT new payer-provider pairs not already in PostgreSQL
     b. UPDATE existing pairs with latest Contract/ProviderName
  2. Truncate conflictlog_staging
  3. Populate conflictlog_staging from conflictvisitmaps + conflicts (date-filtered)

Based on Snowflake procedure: COPY_DATA_FROM_CONFLICTVISITMAPS_TO_TEMP
"""

import time
from typing import Dict, Any, List

from psycopg2.extras import execute_values

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.query_builder import QueryBuilder
from lib.utils import get_logger

logger = get_logger(__name__)

# Batch size for INSERT/UPDATE operations
PPR_BATCH_SIZE = 5000

# Name of the staging table (PostgreSQL equivalent of Snowflake's CONFLICTVISITMAPS_TEMP)
STAGING_TABLE = 'conflictlog_staging'


# ---------------------------------------------------------------------------
# Step 1: Sync payer_provider_reminders
# ---------------------------------------------------------------------------

def _fetch_snowflake_dim_data(
    conn_factory: ConnectionFactory,
    db_names: Dict[str, str],
    query_builder: QueryBuilder,
) -> List[Dict[str, Any]]:
    """
    Fetch payer-provider dimension data from Snowflake.

    Joins DIMPROVIDER, DIMPAYERPROVIDER, DIMPAYER to get all distinct
    payer-provider relationships with names.
    """
    sf_manager = conn_factory.get_snowflake_manager()

    sql = query_builder.load_sql_file('sf_task01_dim_payer_provider.sql')
    sql = sql.format(
        sf_database=db_names['sf_database'],
        sf_schema=db_names['sf_schema'],
    )

    logger.info("  Querying Snowflake dimension tables (DIMPROVIDER + DIMPAYERPROVIDER + DIMPAYER)...")
    results = sf_manager.execute_query(sql)

    # Convert to list of dicts
    columns = ['PayerID', 'AppPayerID', 'Contract', 'ProviderID', 'AppProviderID', 'ProviderName']
    records = [dict(zip(columns, row)) for row in results]

    logger.info(f"  Fetched {len(records):,} payer-provider dimension records from Snowflake")
    return records


def _sync_payer_provider_reminders(
    conn_factory: ConnectionFactory,
    db_names: Dict[str, str],
    query_builder: QueryBuilder,
) -> Dict[str, Any]:
    """
    Sync payer_provider_reminders from Snowflake dimension tables.

    Loads all Snowflake dimension data into a PostgreSQL temp table, then:
      Step 1a: INSERT new pairs via NOT EXISTS (server-side set operation)
      Step 1b: UPDATE existing pairs via UPDATE FROM JOIN (server-side set operation)

    This replaces the previous row-by-row executemany approach (~65s for 20K
    updates) with two single-statement set operations against a temp table.
    """
    start = time.time()

    # Fetch dimension data from Snowflake
    dim_records = _fetch_snowflake_dim_data(conn_factory, db_names, query_builder)
    if not dim_records:
        logger.warning("  No dimension records found in Snowflake -- skipping PPR sync")
        return {
            'sf_dim_records': 0, 'inserted': 0, 'updated': 0,
            'duration_seconds': round(time.time() - start, 2),
        }

    schema = db_names['pg_schema']
    pg_manager = conn_factory.get_postgres_manager()
    pg_conn = pg_manager.get_connection(db_names['pg_database'])
    cursor = pg_conn.cursor()

    # --- Load all dim data into a temp table ---
    # ON COMMIT DROP ensures cleanup even if we forget to drop explicitly.
    # The temp table has no indexes -- bulk loading is fast.
    logger.info(f"  Loading {len(dim_records):,} dimension records into temp table...")

    cursor.execute("""
        CREATE TEMP TABLE _tmp_ppr_dim (
            "PayerID" text NOT NULL,
            "AppPayerID" bigint,
            "Contract" varchar(100),
            "ProviderID" text NOT NULL,
            "AppProviderID" bigint,
            "ProviderName" varchar(100)
        ) ON COMMIT DROP
    """)

    execute_values(
        cursor,
        """INSERT INTO _tmp_ppr_dim
           ("PayerID", "AppPayerID", "Contract",
            "ProviderID", "AppProviderID", "ProviderName")
           VALUES %s""",
        [(str(rec['PayerID']), rec['AppPayerID'], rec['Contract'],
          str(rec['ProviderID']), rec['AppProviderID'], rec['ProviderName'])
         for rec in dim_records],
        page_size=PPR_BATCH_SIZE,
    )
    logger.info(f"  Loaded {len(dim_records):,} records into _tmp_ppr_dim")

    # --- INSERT new pairs (server-side NOT EXISTS) ---
    # Replaces the Python-side diff + executemany INSERT.
    # Mirrors Snowflake's table_command2 (INSERT WHERE NOT EXISTS).
    cursor.execute(f"""
        INSERT INTO {schema}.payer_provider_reminders
        ("PayerID", "AppPayerID", "Contract", "ProviderID", "AppProviderID",
         "ProviderName", "CreatedDateTime", "NumberOfDays")
        SELECT
            d."PayerID"::uuid, d."AppPayerID", d."Contract",
            d."ProviderID"::uuid, d."AppProviderID", d."ProviderName",
            CURRENT_TIMESTAMP, NULL
        FROM _tmp_ppr_dim d
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.payer_provider_reminders p
            WHERE p."PayerID" = d."PayerID"::uuid
            AND p."ProviderID" = d."ProviderID"::uuid
        )
    """)
    inserted = cursor.rowcount
    logger.info(f"  Inserted {inserted:,} new payer-provider reminders")

    # --- UPDATE existing pairs (server-side UPDATE FROM JOIN) ---
    # Replaces the row-by-row executemany UPDATE.
    # Mirrors Snowflake's table_command3 (UPDATE ... FROM ... WHERE).
    cursor.execute(f"""
        UPDATE {schema}.payer_provider_reminders p
        SET "Contract" = d."Contract",
            "ProviderName" = d."ProviderName"
        FROM _tmp_ppr_dim d
        WHERE p."PayerID" = d."PayerID"::uuid
        AND p."ProviderID" = d."ProviderID"::uuid
    """)
    updated = cursor.rowcount
    logger.info(f"  Updated {updated:,} existing payer-provider reminders")

    pg_conn.commit()  # Commits and drops _tmp_ppr_dim (ON COMMIT DROP)
    cursor.close()
    pg_conn.close()

    duration = time.time() - start
    return {
        'sf_dim_records': len(dim_records),
        'inserted': inserted,
        'updated': updated,
        'duration_seconds': round(duration, 2),
    }


# ---------------------------------------------------------------------------
# Step 2: Populate staging table
# ---------------------------------------------------------------------------

def _get_staging_columns(
    conn_factory: ConnectionFactory,
    db_name: str,
    schema: str,
    table_name: str,
) -> List[str]:
    """Get column names for the staging table from information_schema."""
    pg_manager = conn_factory.get_postgres_manager()
    pg_conn = pg_manager.get_connection(db_name)
    cursor = pg_conn.cursor()

    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table_name),
    )
    columns = [row[0] for row in cursor.fetchall()]

    cursor.close()
    pg_conn.close()
    return columns


def _build_staging_insert_sql(
    schema: str,
    staging_table: str,
    staging_columns: List[str],
    lookback_years: int,
    lookforward_days: int,
) -> str:
    """
    Build the INSERT SELECT query for populating the staging table.

    Maps columns from conflictvisitmaps (CVM) and conflicts (C):
      - "StatusFlag" in staging  ->  C."StatusFlag"  (from parent conflicts table)
      - "ConStatusFlag" in staging -> CVM."StatusFlag" (child's own flag)
      - All other columns -> CVM."<column>"

    Based on the Snowflake original which JOINs CVM with C to pull the
    parent's StatusFlag and FlagForReview into the staging/temp table.
    """
    has_con_status_flag = 'ConStatusFlag' in staging_columns

    # Columns whose staging value comes from the parent conflicts table
    # rather than from conflictvisitmaps.  Only activate the mapping when
    # the staging table also carries ConStatusFlag / ConFlagForReview so we
    # know the schema expects the parent/child split.
    conflicts_overrides = set()
    if has_con_status_flag:
        conflicts_overrides.add('StatusFlag')
    if 'ConFlagForReview' in staging_columns:
        conflicts_overrides.add('FlagForReview')
    if 'ConFlagForReviewDate' in staging_columns:
        conflicts_overrides.add('FlagForReviewDate')

    # Build target column list and source expressions
    target_cols = []
    source_exprs = []

    for col in staging_columns:
        quoted_col = f'"{col}"'
        target_cols.append(quoted_col)

        if col in conflicts_overrides:
            # Value comes from the parent conflicts table
            source_exprs.append(f'C.{quoted_col}')
        elif col == 'ConStatusFlag':
            # ConStatusFlag = child's own StatusFlag from conflictvisitmaps
            source_exprs.append('CVM."StatusFlag"')
        elif col == 'ConFlagForReview':
            source_exprs.append('CVM."FlagForReview"')
        elif col == 'ConFlagForReviewDate':
            source_exprs.append('CVM."FlagForReviewDate"')
        elif col == 'CreatedDate':
            # Use current timestamp for the staging copy date
            source_exprs.append('CURRENT_TIMESTAMP')
        else:
            source_exprs.append(f'CVM.{quoted_col}')

    target_list = ',\n    '.join(target_cols)
    source_list = ',\n    '.join(source_exprs)

    sql = f"""INSERT INTO {schema}."{staging_table}" (
    {target_list}
)
SELECT
    {source_list}
FROM {schema}.conflictvisitmaps CVM
INNER JOIN {schema}.conflicts C ON C."CONFLICTID" = CVM."CONFLICTID"
WHERE CVM."VisitDate" BETWEEN
    (CURRENT_DATE - INTERVAL '{lookback_years} years')
    AND (CURRENT_DATE + INTERVAL '{lookforward_days} days')
"""
    return sql


def _populate_staging(
    conn_factory: ConnectionFactory,
    db_names: Dict[str, str],
    lookback_years: int,
    lookforward_days: int,
    staging_table: str = STAGING_TABLE,
) -> Dict[str, Any]:
    """
    Truncate and populate the staging table from conflictvisitmaps + conflicts.

    Date filter: VisitDate BETWEEN (NOW - lookback_years) AND (NOW + lookforward_days)
    """
    start = time.time()
    schema = db_names['pg_schema']
    db_name = db_names['pg_database']
    pg_manager = conn_factory.get_postgres_manager()

    # --- Discover staging table columns ---
    staging_columns = _get_staging_columns(conn_factory, db_name, schema, staging_table)
    if not staging_columns:
        raise RuntimeError(
            f"Staging table '{schema}.{staging_table}' has no columns or does not exist"
        )
    logger.info(f"  Staging table '{staging_table}' has {len(staging_columns)} columns")

    # --- TRUNCATE ---
    logger.info(f"  Truncating {schema}.{staging_table}...")
    pg_conn = pg_manager.get_connection(db_name)
    cursor = pg_conn.cursor()
    cursor.execute(f'TRUNCATE TABLE {schema}."{staging_table}"')
    pg_conn.commit()
    cursor.close()
    pg_conn.close()
    logger.info(f"  Truncated {schema}.{staging_table}")

    # --- POPULATE ---
    insert_sql = _build_staging_insert_sql(
        schema, staging_table, staging_columns,
        lookback_years, lookforward_days,
    )

    logger.info(
        f"  Populating {schema}.{staging_table} from conflictvisitmaps + conflicts "
        f"(VisitDate: -{lookback_years}y to +{lookforward_days}d)..."
    )

    pg_conn = pg_manager.get_connection(db_name)
    cursor = pg_conn.cursor()
    cursor.execute(insert_sql)
    rows_inserted = cursor.rowcount
    pg_conn.commit()
    cursor.close()
    pg_conn.close()

    duration = time.time() - start
    logger.info(f"  Populated {rows_inserted:,} rows into {staging_table} ({duration:.1f}s)")

    return {
        'staging_table': staging_table,
        'columns': len(staging_columns),
        'rows_inserted': rows_inserted,
        'date_range': f"-{lookback_years}y to +{lookforward_days}d",
        'duration_seconds': round(duration, 2),
    }


# ---------------------------------------------------------------------------
# Main action entry point
# ---------------------------------------------------------------------------

def run_task01_copy_to_staging(settings: Settings) -> dict:
    """
    Execute Task 01 - Copy to Staging.

    Steps:
      1. Sync payer_provider_reminders from Snowflake dimension tables
         (INSERT new pairs, UPDATE existing names)
      2. Truncate + populate conflictlog_staging from conflictvisitmaps + conflicts

    Returns a dict with status and step results.
    """
    start = time.time()
    sf_config = settings.get_snowflake_config()
    pg_config = settings.get_postgres_config()
    db_names = settings.get_database_names()
    common_params = settings.get_common_parameters()

    lookback_years = common_params.get('lookback_years', 2)
    lookforward_days = common_params.get('lookforward_days', 45)

    conn_factory = ConnectionFactory(sf_config, pg_config)
    query_builder = QueryBuilder()
    errors = []

    # --- Step 1: Sync payer_provider_reminders ---
    logger.info("Task 01 Step 1: Syncing payer_provider_reminders from Snowflake...")
    ppr_result: Dict[str, Any] = {}
    try:
        ppr_result = _sync_payer_provider_reminders(conn_factory, db_names, query_builder)
    except Exception as e:
        logger.error(f"  Failed to sync payer_provider_reminders: {e}")
        errors.append(f"PPR sync failed: {e}")
        ppr_result = {'error': str(e)}

    # --- Step 2: Populate staging ---
    logger.info("Task 01 Step 2: Populating conflictlog_staging...")
    staging_result: Dict[str, Any] = {}
    try:
        staging_result = _populate_staging(
            conn_factory, db_names,
            lookback_years=lookback_years,
            lookforward_days=lookforward_days,
        )
    except Exception as e:
        logger.error(f"  Failed to populate staging: {e}")
        errors.append(f"Staging populate failed: {e}")
        staging_result = {'error': str(e)}

    conn_factory.close_all()

    duration = time.time() - start
    status = 'success' if not errors else 'failed'

    logger.info(f"Task 01: {status.upper()} ({duration:.1f}s)")
    if errors:
        for err in errors:
            logger.error(f"  {err}")

    return {
        'status': status,
        'errors': errors,
        'payer_provider_reminders': ppr_result,
        'staging': staging_result,
        'duration_seconds': round(duration, 2),
    }
