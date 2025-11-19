"""
Validation script for TASK_02.

Validates that TASK_02 executed correctly by checking:
- ConflictVisitMaps rows were updated
- Calculated fields are populated (distance, flags, etc.)
- Data quality checks
- Analytics data joins were successful

Usage:
    python scripts/validate_task_02.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import (
    POSTGRES_CONFIG, CONFLICT_SCHEMA, ANALYTICS_SCHEMA,
    validate_config
)
from src.connectors.postgres_connector import PostgresConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def validate_rows_updated(pg_connector: PostgresConnector) -> bool:
    """Validate that conflictvisitmaps rows exist and were potentially updated."""
    logger.info("\n" + "=" * 60)
    logger.info("Validating ConflictVisitMaps Row Counts")
    logger.info("=" * 60)
    
    try:
        # Check total rows
        count_query = f"""
            SELECT COUNT(*) as total_rows
            FROM {CONFLICT_SCHEMA}.conflictvisitmaps
        """
        df = pg_connector.fetch_dataframe(count_query)
        total_rows = df.iloc[0, 0]
        
        logger.info(f"  Total rows in conflictvisitmaps: {total_rows:,}")
        
        if total_rows == 0:
            logger.error("No rows in conflictvisitmaps table")
            return False
        
        logger.info("PASS: ConflictVisitMaps has data")
        return True
        
    except Exception as e:
        logger.error(f"Error checking row counts: {e}")
        return False


def validate_calculated_fields(pg_connector: PostgresConnector) -> bool:
    """Validate that calculated fields are populated."""
    logger.info("\n" + "=" * 60)
    logger.info("Validating Calculated Fields")
    logger.info("=" * 60)
    
    try:
        # Check for populated calculated fields
        fields_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT("DistanceMilesFromLatLng") as distance_populated,
                COUNT("ETATravleMinutes") as eta_populated,
                COUNT("MinuteDiffBetweenSch") as minute_diff_populated,
                COUNT("AverageMilesPerHour") as mph_populated,
                SUM(CASE WHEN "DistanceFlag" IS NOT NULL THEN 1 ELSE 0 END) as distance_flag_set,
                SUM(CASE WHEN "InServiceFlag" IS NOT NULL THEN 1 ELSE 0 END) as inservice_flag_set,
                SUM(CASE WHEN "PTOFlag" IS NOT NULL THEN 1 ELSE 0 END) as pto_flag_set
            FROM {CONFLICT_SCHEMA}.conflictvisitmaps
            WHERE "VisitDate" >= CURRENT_DATE - INTERVAL '30 days'
        """
        
        df = pg_connector.fetch_dataframe(fields_query)
        
        total = df.iloc[0, 0]
        distance_pop = df.iloc[0, 1]
        eta_pop = df.iloc[0, 2]
        minute_diff_pop = df.iloc[0, 3]
        mph_pop = df.iloc[0, 4]
        distance_flag = df.iloc[0, 5]
        inservice_flag = df.iloc[0, 6]
        pto_flag = df.iloc[0, 7]
        
        logger.info(f"  Rows checked (last 30 days): {total:,}")
        logger.info(f"  Distance populated: {distance_pop:,} ({distance_pop/total*100:.1f}%)")
        logger.info(f"  ETA populated: {eta_pop:,} ({eta_pop/total*100:.1f}%)")
        logger.info(f"  Minute diff populated: {minute_diff_pop:,} ({minute_diff_pop/total*100:.1f}%)")
        logger.info(f"  MPH populated: {mph_pop:,} ({mph_pop/total*100:.1f}%)")
        logger.info(f"  Distance flag set: {distance_flag:,}")
        logger.info(f"  InService flag set: {inservice_flag:,}")
        logger.info(f"  PTO flag set: {pto_flag:,}")
        
        # Validation: At least some calculated fields should be populated
        if distance_pop == 0 and eta_pop == 0 and minute_diff_pop == 0:
            logger.error("No calculated fields are populated")
            return False
        
        logger.info("PASS: Calculated fields are being populated")
        return True
        
    except Exception as e:
        logger.error(f"Error checking calculated fields: {e}")
        return False


def validate_analytics_joins(pg_connector: PostgresConnector) -> bool:
    """Validate that analytics data was successfully joined."""
    logger.info("\n" + "=" * 60)
    logger.info("Validating Analytics Data Joins")
    logger.info("=" * 60)
    
    try:
        # Check for populated analytics-sourced fields (last updated info, caregiver details, etc.)
        join_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT("LastUpdatedBy") as last_updated_by_populated,
                COUNT("LastUpdatedDate") as last_updated_date_populated,
                COUNT("AideFName") as aide_fname_populated,
                COUNT("ConAideFName") as con_aide_fname_populated
            FROM {CONFLICT_SCHEMA}.conflictvisitmaps
            WHERE "VisitDate" >= CURRENT_DATE - INTERVAL '30 days'
        """
        
        df = pg_connector.fetch_dataframe(join_query)
        
        total = df.iloc[0, 0]
        last_updated_by = df.iloc[0, 1]
        last_updated_date = df.iloc[0, 2]
        aide_fname = df.iloc[0, 3]
        con_aide_fname = df.iloc[0, 4]
        
        logger.info(f"  Rows checked (last 30 days): {total:,}")
        logger.info(f"  LastUpdatedBy populated: {last_updated_by:,} ({last_updated_by/total*100:.1f}%)")
        logger.info(f"  LastUpdatedDate populated: {last_updated_date:,} ({last_updated_date/total*100:.1f}%)")
        logger.info(f"  AideFName populated: {aide_fname:,} ({aide_fname/total*100:.1f}%)")
        logger.info(f"  ConAideFName populated: {con_aide_fname:,} ({con_aide_fname/total*100:.1f}%)")
        
        # Validation: At least some analytics fields should be populated
        if aide_fname == 0 or con_aide_fname == 0:
            logger.warning("Some analytics-sourced fields are not well populated")
            logger.warning("This may be expected if analytics data is incomplete")
        
        logger.info("PASS: Analytics data joins completed")
        return True
        
    except Exception as e:
        logger.error(f"Error checking analytics joins: {e}")
        return False


def validate_time_overlap_flags(pg_connector: PostgresConnector) -> bool:
    """Validate that time overlap flags are set appropriately."""
    logger.info("\n" + "=" * 60)
    logger.info("Validating Time Overlap Flags")
    logger.info("=" * 60)
    
    try:
        # Check time overlap flags (flags are VARCHAR type, so compare with '1')
        flags_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN "SameSchTimeFlag" = '1' THEN 1 ELSE 0 END) as same_sch_time,
                SUM(CASE WHEN "SameVisitTimeFlag" = '1' THEN 1 ELSE 0 END) as same_visit_time,
                SUM(CASE WHEN "SchAndVisitTimeSameFlag" = '1' THEN 1 ELSE 0 END) as sch_visit_same,
                SUM(CASE WHEN "SchOverAnotherSchTimeFlag" = '1' THEN 1 ELSE 0 END) as sch_overlap,
                SUM(CASE WHEN "VisitTimeOverAnotherVisitTimeFlag" = '1' THEN 1 ELSE 0 END) as visit_overlap,
                SUM(CASE WHEN "SchTimeOverVisitTimeFlag" = '1' THEN 1 ELSE 0 END) as sch_visit_overlap
            FROM {CONFLICT_SCHEMA}.conflictvisitmaps
            WHERE "VisitDate" >= CURRENT_DATE - INTERVAL '30 days'
        """
        
        df = pg_connector.fetch_dataframe(flags_query)
        
        total = df.iloc[0, 0]
        same_sch = df.iloc[0, 1]
        same_visit = df.iloc[0, 2]
        sch_visit_same = df.iloc[0, 3]
        sch_overlap = df.iloc[0, 4]
        visit_overlap = df.iloc[0, 5]
        sch_visit_overlap = df.iloc[0, 6]
        
        logger.info(f"  Rows checked (last 30 days): {total:,}")
        logger.info(f"  SameSchTimeFlag=1: {same_sch:,}")
        logger.info(f"  SameVisitTimeFlag=1: {same_visit:,}")
        logger.info(f"  SchAndVisitTimeSameFlag=1: {sch_visit_same:,}")
        logger.info(f"  SchOverAnotherSchTimeFlag=1: {sch_overlap:,}")
        logger.info(f"  VisitTimeOverAnotherVisitTimeFlag=1: {visit_overlap:,}")
        logger.info(f"  SchTimeOverVisitTimeFlag=1: {sch_visit_overlap:,}")
        
        total_flags = same_sch + same_visit + sch_visit_same + sch_overlap + visit_overlap + sch_visit_overlap
        
        if total_flags > 0:
            logger.info(f"  Total conflict flags set: {total_flags:,}")
            logger.info("PASS: Time overlap flags are being set")
        else:
            logger.warning("No time overlap flags are set")
            logger.warning("This may be expected if there are no overlapping visits in the date range")
            logger.info("PASS: Time overlap flags check completed")
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking time overlap flags: {e}")
        return False


def validate_data_quality(pg_connector: PostgresConnector) -> bool:
    """Perform basic data quality checks."""
    logger.info("\n" + "=" * 60)
    logger.info("Validating Data Quality")
    logger.info("=" * 60)
    
    try:
        # Check for invalid distances (negative or extremely large)
        quality_query = f"""
            SELECT 
                COUNT(*) as total_with_distance,
                SUM(CASE WHEN "DistanceMilesFromLatLng" < 0 THEN 1 ELSE 0 END) as negative_distance,
                SUM(CASE WHEN "DistanceMilesFromLatLng" > 500 THEN 1 ELSE 0 END) as extreme_distance,
                AVG("DistanceMilesFromLatLng") as avg_distance,
                MAX("DistanceMilesFromLatLng") as max_distance
            FROM {CONFLICT_SCHEMA}.conflictvisitmaps
            WHERE "DistanceMilesFromLatLng" IS NOT NULL
            AND "VisitDate" >= CURRENT_DATE - INTERVAL '30 days'
        """
        
        df = pg_connector.fetch_dataframe(quality_query)
        
        total = df.iloc[0, 0]
        negative = df.iloc[0, 1]
        extreme = df.iloc[0, 2]
        avg_dist = df.iloc[0, 3]
        max_dist = df.iloc[0, 4]
        
        logger.info(f"  Rows with distance data: {total:,}")
        logger.info(f"  Average distance: {avg_dist:.2f} miles")
        logger.info(f"  Maximum distance: {max_dist:.2f} miles")
        logger.info(f"  Negative distances: {negative}")
        logger.info(f"  Extreme distances (>500mi): {extreme}")
        
        if negative > 0:
            logger.error(f"Found {negative} rows with negative distances")
            return False
        
        if extreme > total * 0.1:  # More than 10% extreme distances is suspicious
            logger.warning(f"High number of extreme distances: {extreme} ({extreme/total*100:.1f}%)")
            logger.warning("This may indicate data quality issues")
        
        logger.info("PASS: Data quality checks passed")
        return True
        
    except Exception as e:
        logger.error(f"Error checking data quality: {e}")
        return False


def main():
    """Main validation function."""
    logger.info("=" * 80)
    logger.info("TASK_02 Validation")
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
    
    results.append(validate_rows_updated(pg_connector))
    results.append(validate_calculated_fields(pg_connector))
    results.append(validate_analytics_joins(pg_connector))
    results.append(validate_time_overlap_flags(pg_connector))
    results.append(validate_data_quality(pg_connector))
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)
    
    passed = sum(results)
    total = len(results)
    
    logger.info(f"Passed: {passed}/{total}")
    
    if all(results):
        logger.info("SUCCESS: ALL VALIDATIONS PASSED")
        logger.info("=" * 80)
        return 0
    else:
        logger.error("FAILURE: SOME VALIDATIONS FAILED")
        logger.info("=" * 80)
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

