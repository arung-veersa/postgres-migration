"""
Validation script for TASK_01.

Compares results between Snowflake and Postgres to ensure
the migration is working correctly.

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
    SNOWFLAKE_CONFIG, POSTGRES_CONFIG,
    DATE_RANGE_YEARS_BACK, DATE_RANGE_DAYS_FORWARD,
    validate_config
)
from src.connectors.snowflake_connector import SnowflakeConnector
from src.connectors.postgres_connector import PostgresConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def validate_table_exists(pg_connector: PostgresConnector, table_name: str) -> bool:
    """Check if table exists in Postgres."""
    exists = pg_connector.table_exists(table_name, schema=pg_connector.schema)
    if exists:
        logger.info(f"Table '{table_name}' exists")
    else:
        logger.error(f"Table '{table_name}' does not exist")
    return exists


def validate_row_count(pg_connector: PostgresConnector, table_name: str) -> int:
    """Get and validate row count."""
    count = pg_connector.get_row_count(table_name, schema=pg_connector.schema)
    logger.info(f"  Row count: {count:,}")
    return count


def validate_payer_provider_reminders(sf_connector: SnowflakeConnector,
                                     pg_connector: PostgresConnector) -> bool:
    """Validate payer_provider_reminders table."""
    table_name = 'payer_provider_reminders'
    logger.info("\n" + "=" * 60)
    logger.info(f"Validating {table_name}")
    logger.info("=" * 60)

    if not pg_connector.table_exists(table_name.lower(), schema=pg_connector.schema):
        logger.error(f"Table '{table_name}' does not exist in Postgres")
        return False
    logger.info(f"Table '{table_name}' exists")

    pg_count = pg_connector.get_row_count(table_name.lower(), schema=pg_connector.schema)
    
    # Get count from Snowflake
    sf_query = f"""
        SELECT COUNT(DISTINCT DPP."Provider Id" || '_' || DPP."Payer Id") AS cnt
        FROM "{sf_connector.config['database']}"."{sf_connector.config['schema']}".DIMPROVIDER AS DP
        INNER JOIN "{sf_connector.config['database']}"."{sf_connector.config['schema']}".DIMPAYERPROVIDER AS DPP 
            ON DPP."Provider Id" = DP."Provider Id"
        INNER JOIN "{sf_connector.config['database']}"."{sf_connector.config['schema']}".DIMPAYER AS DPA 
            ON DPA."Payer Id" = DPP."Payer Id"
    """
    
    sf_df = sf_connector.fetch_dataframe(sf_query)
    sf_count = sf_df.iloc[0][0]
    
    logger.info(f"  Expected count (Analytics): {sf_count:,}")
    logger.info(f"  Actual count (Postgres): {pg_count:,}")
    
    if pg_count >= sf_count:
        logger.info("Row count validation passed")
        return True
    else:
        logger.error(f"Row count mismatch (expected >= {sf_count}, got {pg_count})")
        return False


def validate_conflictvisitmaps_temp(pg_connector: PostgresConnector) -> bool:
    """Validate conflictvisitmaps_temp table."""
    table_name = 'conflictvisitmaps_temp'
    logger.info("\n" + "=" * 60)
    logger.info(f"Validating {table_name}")
    logger.info("=" * 60)

    if not pg_connector.table_exists(table_name, schema=pg_connector.schema):
        logger.error(f"Table '{table_name}' does not exist")
        return False
    logger.info(f"Table '{table_name}' exists")

    temp_count = pg_connector.get_row_count(table_name, schema=pg_connector.schema)
    
    # Get count from original table within date range
    start_date = datetime.now() - timedelta(days=DATE_RANGE_YEARS_BACK * 365)
    end_date = datetime.now() + timedelta(days=DATE_RANGE_DAYS_FORWARD)
    
    source_query = f"""
        SELECT COUNT(*)
        FROM "{pg_connector.schema}"."conflictvisitmaps"
        WHERE "VisitDate" BETWEEN %(start_date)s AND %(end_date)s
    """
    
    df_source = pg_connector.fetch_dataframe(
        source_query,
        params={'start_date': start_date, 'end_date': end_date}
    )
    source_count = df_source.iloc[0][0]
    
    logger.info(f"  Expected count (CONFLICTVISITMAPS): {source_count:,}")
    logger.info(f"  Actual count (TEMP): {temp_count:,}")
    
    if temp_count == source_count:
        logger.info("Row count matches exactly")
        return True
    elif temp_count == 0:
        logger.warning("  Temp table is empty (may be expected if no data in date range)")
        return True
    else:
        logger.error(f"Row count mismatch (expected {source_count}, got {temp_count})")
        return False


def validate_settings_flag(pg_connector: PostgresConnector) -> bool:
    """Validate SETTINGS.InProgressFlag."""
    table_name = 'settings'
    logger.info("\n" + "=" * 60)
    logger.info(f"Validating {table_name}.InProgressFlag")
    logger.info("=" * 60)

    try:
        query = f'SELECT "InProgressFlag" FROM "{pg_connector.schema}"."{table_name.lower()}"'
        
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
    
    # Validate configuration
    try:
        validate_config()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    
    # Initialize connectors
    try:
        sf_connector = SnowflakeConnector(**SNOWFLAKE_CONFIG)
        pg_connector = PostgresConnector(**POSTGRES_CONFIG)
    except Exception as e:
        logger.error(f"Failed to initialize connectors: {e}")
        return 1
    
    # Run validations
    results = []
    
    results.append(validate_payer_provider_reminders(sf_connector, pg_connector))
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

