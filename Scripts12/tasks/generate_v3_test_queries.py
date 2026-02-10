"""
Generate v3 test queries with default values for direct Snowflake execution
Creates both symmetric and asymmetric versions with concrete parameters
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from lib.query_builder import QueryBuilder

def generate_test_queries():
    """Generate test queries with default values"""
    
    # Default configuration values (from config.json)
    db_names = {
        'sf_database': 'ANALYTICS',
        'sf_schema': 'BI'
    }
    
    # Default task parameters
    lookback_years = 2
    lookforward_days = 45
    lookback_hours = 36
    
    # Reference data (simplified - actual values from database)
    excluded_agencies = ['10039', '10040', '10041', '10083', '10107', '10123', '10126', 
                        '10137', '10138', '10140', '10148', '10158']
    
    settings_data = {'ExtraDistancePer': 100}
    
    # MPH lookup data (sample - actual has more rows)
    mph_data = [
        {'From': 0, 'To': 2, 'AverageMilesPerHour': 2},
        {'From': 2, 'To': 5, 'AverageMilesPerHour': 6},
        {'From': 5, 'To': 10, 'AverageMilesPerHour': 15},
        {'From': 10, 'To': 25, 'AverageMilesPerHour': 25},
        {'From': 25, 'To': 50, 'AverageMilesPerHour': 45},
        {'From': 50, 'To': 100, 'AverageMilesPerHour': 55},
        {'From': 100, 'To': 500, 'AverageMilesPerHour': 60}
    ]
    
    # Initialize query builder
    query_builder = QueryBuilder(sql_dir='sql')
    
    print("Generating v3 test queries...")
    print("=" * 70)
    
    # SYMMETRIC VERSION
    print("\n1. Generating SYMMETRIC query (v3-sym-defaults.sql)...")
    sym_queries = query_builder.build_conflict_detection_query_v3(
        db_names=db_names,
        excluded_agencies=excluded_agencies,
        excluded_ssns=[],
        settings_data=settings_data,
        mph_data=mph_data,
        lookback_years=lookback_years,
        lookforward_days=lookforward_days,
        lookback_hours=lookback_hours,
        enable_asymmetric_join=False
    )
    
    # Combine symmetric steps (step1 is None for symmetric)
    sym_full_query = f"""-- ============================================================================
-- Task 02 v3: SYMMETRIC MODE - Test Query with Default Values
-- ============================================================================
-- Generated for direct Snowflake execution via DBeaver or similar client
-- 
-- MODE: SYMMETRIC (enable_asymmetric_join = false)
-- DESCRIPTION: Only processes visits updated in last {lookback_hours} hours
-- EXPECTED ROWS IN base_visits: ~70,000
-- EXPECTED RUNTIME: 30-40 seconds
-- ============================================================================

{sym_queries['step2']}

-- STEP 3: Final conflict detection query
{sym_queries['step3']}
"""
    
    with open('sql/sf_task02_v3-sym-defaults.sql', 'w', encoding='utf-8') as f:
        f.write(sym_full_query)
    print("   Created: sql/sf_task02_v3-sym-defaults.sql")
    print(f"   Query length: {len(sym_full_query):,} characters")
    
    # ASYMMETRIC VERSION
    print("\n2. Generating ASYMMETRIC query (v3-asym-defaults.sql)...")
    asym_queries = query_builder.build_conflict_detection_query_v3(
        db_names=db_names,
        excluded_agencies=excluded_agencies,
        excluded_ssns=[],
        settings_data=settings_data,
        mph_data=mph_data,
        lookback_years=lookback_years,
        lookforward_days=lookforward_days,
        lookback_hours=lookback_hours,
        enable_asymmetric_join=True
    )
    
    # Combine asymmetric steps
    asym_full_query = f"""-- ============================================================================
-- Task 02 v3: ASYMMETRIC MODE - Test Query with Default Values
-- ============================================================================
-- Generated for direct Snowflake execution via DBeaver or similar client
-- 
-- MODE: ASYMMETRIC (enable_asymmetric_join = true)
-- DESCRIPTION: Processes visits updated in last {lookback_hours} hours + related visits
-- EXPECTED ROWS IN base_visits: ~9.6 million
-- EXPECTED RUNTIME: 3-5 minutes (estimated)
-- 
-- NOTE: This query uses temp tables and must be run in a single session.
--       All three steps below must be executed sequentially.
-- ============================================================================

-- STEP 1: Create delta_keys temp table
{asym_queries['step1']}

-- STEP 2: Create base_visits temp table (with expanded scope)
{asym_queries['step2']}

-- STEP 3: Final conflict detection query
{asym_queries['step3']}
"""
    
    with open('sql/sf_task02_v3-asym-defaults.sql', 'w', encoding='utf-8') as f:
        f.write(asym_full_query)
    print("   Created: sql/sf_task02_v3-asym-defaults.sql")
    print(f"   Query length: {len(asym_full_query):,} characters")
    
    print("\n" + "=" * 70)
    print("Test queries generated successfully!")
    print("\nUsage:")
    print("  1. Open DBeaver or Snowflake web console")
    print("  2. Connect to your Snowflake instance")
    print("  3. Open the generated SQL file")
    print("  4. Execute the entire script (all steps must run in same session)")
    print("  5. Review execution time and row counts")
    print("\nTo test symmetric version first (recommended):")
    print("  - Run sql/sf_task02_v3-sym-defaults.sql")
    print("  - Expected: 30-40 seconds, similar to previous results")
    print("\nTo test asymmetric version:")
    print("  - Run sql/sf_task02_v3-asym-defaults.sql")
    print("  - Expected: 3-5 minutes (much faster than v2's timeout)")


if __name__ == '__main__':
    try:
        generate_test_queries()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
