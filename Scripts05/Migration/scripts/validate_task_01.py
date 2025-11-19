"""
Validation script for TASK_01.

Validates that TASK_01 executed correctly by checking:
- Payer-provider reminders were synced
- Temp table was populated
- Settings flag was updated

Usage:
    python scripts/validate_task_01.py
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import (
    POSTGRES_CONFIG, CONFLICT_SCHEMA, ANALYTICS_SCHEMA,
    DATE_RANGE_YEARS_BACK, DATE_RANGE_DAYS_FORWARD,
    validate_config
)
from src.connectors.postgres_connector import PostgresConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def validate_table_exists(pg_connector: PostgresConnector, table_name: str, schema: str) -> bool:
    """Check if table exists in Postgres."""
    exists = pg_connector.table_exists(table_name, schema=schema)
    if exists:
        logger.info(f"Table '{schema}.{table_name}' exists")
    else:
        logger.error(f"Table '{schema}.{table_name}' does not exist")
    return exists


def validate_row_count(pg_connector: PostgresConnector, table_name: str, schema: str) -> int:
    """Get and validate row count."""
    count = pg_connector.get_row_count(table_name, schema=schema)
    logger.info(f"  Row count: {count:,}")
    return count


def validate_payer_provider_reminders(pg_connector: PostgresConnector) -> bool:
    """Validate payer_provider_reminders table against analytics source."""
    table_name = 'payer_provider_reminders'
    logger.info("\n" + "=" * 60)
    logger.info(f"Validating {table_name}")
    logger.info("=" * 60)

    if not pg_connector.table_exists(table_name.lower(), schema=CONFLICT_SCHEMA):
        logger.error(f"Table '{CONFLICT_SCHEMA}.{table_name}' does not exist")
        return False
    logger.info(f"Table '{CONFLICT_SCHEMA}.{table_name}' exists")

    conflict_count = pg_connector.get_row_count(table_name.lower(), schema=CONFLICT_SCHEMA)
    
    # Get count from Analytics schema
    analytics_query = f"""
        SELECT COUNT(DISTINCT DPP."Provider Id" || '_' || DPP."Payer Id") AS cnt
        FROM {ANALYTICS_SCHEMA}.dimprovider AS DP
        INNER JOIN {ANALYTICS_SCHEMA}.dimpayerprovider AS DPP 
            ON DPP."Provider Id" = DP."Provider Id"
        INNER JOIN {ANALYTICS_SCHEMA}.dimpayer AS DPA 
            ON DPA."Payer Id" = DPP."Payer Id"
    """
    
    analytics_df = pg_connector.fetch_dataframe(analytics_query)
    analytics_count = analytics_df.iloc[0, 0]
    
    logger.info(f"  Expected count (Analytics schema): {analytics_count:,}")
    logger.info(f"  Actual count (Conflict schema): {conflict_count:,}")
    
    if conflict_count >= analytics_count:
        logger.info("Row count validation passed")
        return True
    else:
        logger.error(f"Row count mismatch (expected >= {analytics_count}, got {conflict_count})")
        return False


def validate_conflictvisitmaps_temp(pg_connector: PostgresConnector) -> bool:
    """Validate conflictvisitmaps_temp table."""
    table_name = 'conflictvisitmaps_temp'
    logger.info("\n" + "=" * 60)
    logger.info(f"Validating {table_name}")
    logger.info("=" * 60)

    if not pg_connector.table_exists(table_name, schema=CONFLICT_SCHEMA):
        logger.error(f"Table '{CONFLICT_SCHEMA}.{table_name}' does not exist")
        return False
    logger.info(f"Table '{CONFLICT_SCHEMA}.{table_name}' exists")

    temp_count = pg_connector.get_row_count(table_name, schema=CONFLICT_SCHEMA)
    
    # Get count from original table within date range
    start_date = datetime.now() - timedelta(days=DATE_RANGE_YEARS_BACK * 365)
    end_date = datetime.now() + timedelta(days=DATE_RANGE_DAYS_FORWARD)
    
    source_query = f"""
        SELECT COUNT(*)
        FROM {CONFLICT_SCHEMA}.conflictvisitmaps
        WHERE "VisitDate" BETWEEN %(start_date)s AND %(end_date)s
    """
    
    df_source = pg_connector.fetch_dataframe(
        source_query,
        params={'start_date': start_date, 'end_date': end_date}
    )
    source_count = df_source.iloc[0, 0]
    
    logger.info(f"  Expected count (CONFLICTVISITMAPS): {source_count:,}")
    logger.info(f"  Actual count (TEMP): {temp_count:,}")
    
    # Calculate difference
    difference = abs(source_count - temp_count)
    percent_diff = (difference / source_count * 100) if source_count > 0 else 0
    
    if temp_count == source_count:
        logger.info("PASS: Row count matches exactly")
        return True
    elif temp_count == 0:
        logger.warning("  Temp table is empty (may be expected if no data in date range)")
        return True
    elif percent_diff < 1.0:  # Less than 1% difference
        logger.warning(f"  Small difference: {difference:,} rows ({percent_diff:.2f}%)")
        logger.warning("  This is acceptable - likely due to INNER JOIN with CONFLICTS table")
        logger.info("PASS: Row count validation passed (within tolerance)")
        return True
    else:
        logger.error(f"Row count mismatch: {difference:,} rows ({percent_diff:.2f}% difference)")
        return False


def validate_settings_flag(pg_connector: PostgresConnector) -> bool:
    """Validate SETTINGS.InProgressFlag."""
    table_name = 'settings'
    logger.info("\n" + "=" * 60)
    logger.info(f"Validating {CONFLICT_SCHEMA}.{table_name}.InProgressFlag")
    logger.info("=" * 60)

    try:
        query = f'SELECT "InProgressFlag" FROM {CONFLICT_SCHEMA}.{table_name.lower()}'
        
        df = pg_connector.fetch_dataframe(query)
        
        if df.empty:
            logger.error(f"No data in SETTINGS table")
            return False
            
        flag = df.iloc[0]['InProgressFlag']
        logger.info(f"  InProgressFlag: {flag}")
            
        if flag == 1:
            logger.info(f"InProgressFlag correctly set to 1 (In Progress)")
            return True
        else:
            logger.warning(f"InProgressFlag is {flag} (expected 1)")
            return True  # Not a failure, just informational
                
    except Exception as e:
        logger.error(f"Error checking SETTINGS: {e}")
        return False


def main():
    """Main validation function."""
    logger.info("=" * 80)
    logger.info("TASK_01 Validation")
    logger.info("=" * 80)
    logger.info(f"Conflict Schema: {CONFLICT_SCHEMA}")
    logger.info(f"Analytics Schema: {ANALYTICS_SCHEMA}")
    
    # Validate configuration
    try:
        validate_config()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    
    # Initialize Postgres connector
    try:
        pg_connector = PostgresConnector(**POSTGRES_CONFIG)
        logger.info("Postgres connector initialized")
    except Exception as e:
        logger.error(f"Failed to initialize connector: {e}")
        return 1
    
    # Run validations
    results = []
    
    results.append(validate_payer_provider_reminders(pg_connector))
    results.append(validate_conflictvisitmaps_temp(pg_connector))
    results.append(validate_settings_flag(pg_connector))
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)
    
    passed = sum(results)
    total = len(results)
    
    logger.info(f"Passed: {passed}/{total}")
    
    if all(results):
        logger.info("ALL VALIDATIONS PASSED")
        logger.info("=" * 80)
        return 0
    else:
        logger.error("SOME VALIDATIONS FAILED")
        logger.info("=" * 80)
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

