-- ============================================================================
-- PERFORMANCE OPTIMIZATION INDEXES
-- For: task02_03_conflict_detection_merge_OPTIMIZED_V2.sql
-- ============================================================================
-- Purpose: Create indexes to optimize conflict detection query performance
-- Expected Impact: 50-70% reduction in query runtime (from 30-60 min to 10-20 min)
--
-- Execution Order:
--   1. Run Phase 1 indexes (critical, ~1-2 hours build time)
--   2. Test query performance
--   3. Run Phase 2 indexes if needed (medium priority, ~2-3 hours)
--   4. Enable autovacuum and run ANALYZE
--
-- Disk Space Required: ~20-25 GB free space
-- Build Time: ~45-80 minutes total
-- ============================================================================

-- ============================================================================
-- PHASE 1: CRITICAL INDEXES (Run These First)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Index 1A: MOST CRITICAL - Primary covering index (time-related columns)
-- ----------------------------------------------------------------------------
-- Provides the most frequently accessed columns for conflict detection
-- Split into 2 indexes due to PostgreSQL's 32-column limit
-- Expected Impact: 50-60% improvement
-- Build Time: 20-40 minutes
-- Size: 8-10 GB (full index without date filter)
-- Note: No WHERE clause (CURRENT_DATE is not IMMUTABLE)
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fvcp_conflict_time_covering 
ON analytics_dev2.factvisitcallperformance_cr 
USING btree (
    "Visit Date",
    "Caregiver Id",
    "Provider Id"
)
INCLUDE (
    "Visit Id",
    "Application Visit Id",
    "Scheduled Start Time",
    "Scheduled End Time",
    "Visit Start Time",
    "Visit End Time",
    "Is Missed",
    "Call In Time",
    "Call Out Time",
    "Call In GPS Coordinates",
    "Call Out GPS Coordinates",
    "Office Id",
    "Application Office Id",
    "Patient Id",
    "Application Patient Id",
    "Provider Patient Id",
    "Application Provider Patient Id",
    "Payer Patient Id",
    "Application Payer Patient Id",
    "Application Caregiver Id",
    "Call Out Device Type"
    -- 3 indexed + 23 included = 26 total columns (under 32 limit)
);

-- ----------------------------------------------------------------------------
-- Index 1B: Secondary covering index (billing and service columns)
-- ----------------------------------------------------------------------------
-- Complements Index 1A with billing and reference data
-- Expected Impact: 10-15% additional improvement
-- Build Time: 15-25 minutes
-- Size: 4-6 GB (full index without date filter)
-- Note: No WHERE clause (CURRENT_DATE is not IMMUTABLE)
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fvcp_conflict_billing_covering 
ON analytics_dev2.factvisitcallperformance_cr 
USING btree (
    "Visit Date",
    "Visit Id"
)
INCLUDE (
    "Payer Id",
    "Application Payer Id",
    "Contract Id",
    "Application Contract Id",
    "Service Code Id",
    "Application Service Code Id",
    "Billed Hours",
    "Billed Rate",
    "Total Billed Amount",
    "Billed",
    "Invoice Date",
    "Bill Rate Non-Billed",
    "Bill Type",
    "Missed Visit Reason",
    "Visit Updated Timestamp",
    "Visit Updated User Id",
    "Application Visit Updated User Id"
    -- 2 indexed + 17 included = 19 total columns (under 32 limit)
);

-- ----------------------------------------------------------------------------
-- Index 2: Self-Join Optimization (SSN + VisitDate + ProviderID)
-- ----------------------------------------------------------------------------
-- Optimizes the V1 LEFT JOIN V2 self-join
-- Expected Impact: 40-50% improvement
-- Build Time: 15-25 minutes
-- Size: 3-4 GB (full index without filter)
-- Note: Removed WHERE clause with CURRENT_DATE (not IMMUTABLE)
--       Query will still benefit from index on "Visit Date" column
--       "Visit Date" is timestamptz, no explicit cast needed
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fvcp_ssn_visitdate_providerid 
ON analytics_dev2.factvisitcallperformance_cr 
USING btree (
    "Caregiver Id",
    "Visit Date",
    "Provider Id"
)
INCLUDE (
    "Visit Id",
    "Scheduled Start Time",
    "Scheduled End Time",
    "Visit Start Time",
    "Visit End Time"
);

-- ----------------------------------------------------------------------------
-- Index 3: Provider Exclusion Optimization
-- ----------------------------------------------------------------------------
-- Speeds up "NOT IN (SELECT ProviderID FROM excluded_agency)" check
-- Expected Impact: 10-15% improvement
-- Build Time: 8-12 minutes
-- Size: 2-3 GB (full index without date filter)
-- Note: Removed WHERE clause with CURRENT_DATE (not IMMUTABLE)
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fvcp_providerid_visitdate 
ON analytics_dev2.factvisitcallperformance_cr 
USING btree ("Provider Id", "Visit Date");

-- ----------------------------------------------------------------------------
-- Index 10: Excluded Agency - Provider Lookup
-- ----------------------------------------------------------------------------
-- Optimizes NOT IN subquery
-- Expected Impact: 5-10% improvement
-- Build Time: <1 minute
-- Size: <10 MB
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excluded_agency_providerid 
ON conflict_dev2.excluded_agency ("ProviderID");

-- ----------------------------------------------------------------------------
-- Index 11: Excluded SSN - Trimmed SSN Lookup
-- ----------------------------------------------------------------------------
-- Optimizes NOT EXISTS subquery
-- Expected Impact: 3-5% improvement
-- Build Time: <1 minute
-- Size: <5 MB
-- ----------------------------------------------------------------------------
-- Note: Use functional index if SSN needs trimming
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excluded_ssn_ssn_trimmed 
ON conflict_dev2.excluded_ssn (TRIM("SSN"))
WHERE TRIM("SSN") IS NOT NULL AND TRIM("SSN") != '';

-- Alternative: If SSN is already trimmed in the table
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excluded_ssn_ssn 
-- ON conflict_dev2.excluded_ssn ("SSN");

-- ============================================================================
-- CHECKPOINT: Test Query Performance After Phase 1
-- ============================================================================
-- Run ANALYZE before testing
ANALYZE analytics_dev2.factvisitcallperformance_cr;
ANALYZE conflict_dev2.excluded_agency;
ANALYZE conflict_dev2.excluded_ssn;

-- Test the query with EXPLAIN ANALYZE
-- If performance is satisfactory (15-25 min), you can stop here.
-- If not, proceed to Phase 2.
-- ============================================================================

-- ============================================================================
-- PHASE 2: HIGH PRIORITY INDEXES (Run If Needed)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Index 4: dimcaregiver - SSN Lookup (Verify Existence First)
-- ----------------------------------------------------------------------------
-- This index should already exist. Verify with:
-- SELECT * FROM pg_indexes WHERE indexname = 'idx_caregiver_ssn_trimmed';
-- If missing, create it:
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_caregiver_ssn_trimmed 
ON analytics_dev2.dimcaregiver 
USING btree (TRIM("SSN"))
WHERE TRIM("SSN") IS NOT NULL AND TRIM("SSN") != '';

-- ----------------------------------------------------------------------------
-- Index 5: dimpatientaddress - Patient GPS Lookup with ROW_NUMBER
-- ----------------------------------------------------------------------------
-- Optimizes the patient_addresses CTE
-- Expected Impact: 30-40% improvement in CTE performance
-- Build Time: 2-5 minutes
-- Size: 200-500 MB
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_patientaddress_gps_lookup 
ON analytics_dev2.dimpatientaddress 
USING btree (
    "Patient Id",
    "Application Created UTC Timestamp" DESC
)
INCLUDE (
    "Patient Address Id",
    "Application Patient Address Id",
    "Address Line 1",
    "Address Line 2",
    "City",
    "Address State",
    "Zip Code",
    "County",
    "Application Patient Id",
    "Longitude",
    "Latitude"
)
WHERE "Primary Address" = TRUE 
  AND "Address Type" LIKE '%GPS%';

-- ----------------------------------------------------------------------------
-- Index 6: dimoffice - Active Office Lookup
-- ----------------------------------------------------------------------------
-- Optimizes LEFT JOIN to dimoffice
-- Expected Impact: 5-10% improvement
-- Build Time: <2 minutes
-- Size: <50 MB
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_office_active_lookup 
ON analytics_dev2.dimoffice 
USING btree ("Office Id")
INCLUDE ("Application Office Id", "Office Name", "Federal Tax Number", "NPI")
WHERE "Is Active" = TRUE;

-- ----------------------------------------------------------------------------
-- Index 7: dimprovider - Active Provider Lookup
-- ----------------------------------------------------------------------------
-- Optimizes INNER JOIN to dimprovider
-- Expected Impact: 5-10% improvement
-- Build Time: <2 minutes
-- Size: <50 MB
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_active_lookup 
ON analytics_dev2.dimprovider 
USING btree ("Provider Id")
INCLUDE (
    "Application Provider Id",
    "Provider Name",
    "Address State",
    "Federal Tax Number",
    "Phone Number 1"
)
WHERE "Is Active" = TRUE AND "Is Demo" = FALSE;

-- ----------------------------------------------------------------------------
-- Index 8: dimpayer - Active Payer Lookup
-- ----------------------------------------------------------------------------
-- Optimizes LEFT JOIN to dimpayer
-- Expected Impact: 3-5% improvement
-- Build Time: <2 minutes
-- Size: <30 MB
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payer_active_lookup 
ON analytics_dev2.dimpayer 
USING btree ("Payer Id")
INCLUDE ("Application Payer Id", "Payer Name", "Payer State")
WHERE "Is Active" = TRUE AND "Is Demo" = FALSE;

-- ----------------------------------------------------------------------------
-- Index 9: conflictvisitmaps - MERGE Target Lookup (Verify Existence First)
-- ----------------------------------------------------------------------------
-- This index should already exist. Verify with:
-- SELECT * FROM pg_indexes WHERE indexname = 'idx_cvm_visitid_appvisitid_conflictid';
-- If missing, create it:
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cvm_visitid_appvisitid_conflictid 
ON conflict_dev2.conflictvisitmaps 
USING btree ("VisitID", "AppVisitID")
WHERE "CONFLICTID" IS NOT NULL;

-- ----------------------------------------------------------------------------
-- Index 12: settings & mph - Distance Calculation Support
-- ----------------------------------------------------------------------------
-- Minor optimization for CROSS JOIN and range lookup
-- Expected Impact: 1-3% improvement
-- Build Time: <1 minute each
-- Size: <5 MB total
-- ----------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_settings_extradistanceper 
ON conflict_dev2.settings ("ExtraDistancePer");

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mph_range_lookup 
ON conflict_dev2.mph USING btree ("From", "To");

-- ============================================================================
-- POST-INDEX CREATION: CRITICAL MAINTENANCE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. ENABLE AUTOVACUUM (Currently Disabled - Line 314 in analytics tables.sql)
-- ----------------------------------------------------------------------------
-- CRITICAL: This table has autovacuum_enabled=false which causes bloat
ALTER TABLE analytics_dev2.factvisitcallperformance_cr 
SET (autovacuum_enabled = true);

-- Increase vacuum frequency for this high-traffic table
ALTER TABLE analytics_dev2.factvisitcallperformance_cr 
SET (autovacuum_vacuum_scale_factor = 0.05);  -- Default 0.2, vacuum at 5% change

-- Increase analyze frequency
ALTER TABLE analytics_dev2.factvisitcallperformance_cr 
SET (autovacuum_analyze_scale_factor = 0.05);  -- Default 0.1, analyze at 5% change

-- ----------------------------------------------------------------------------
-- 2. UPDATE STATISTICS (CRITICAL)
-- ----------------------------------------------------------------------------
-- Run ANALYZE on all affected tables to update query planner statistics
ANALYZE analytics_dev2.factvisitcallperformance_cr;
ANALYZE analytics_dev2.dimcaregiver;
ANALYZE analytics_dev2.dimpatientaddress;
ANALYZE analytics_dev2.dimoffice;
ANALYZE analytics_dev2.dimprovider;
ANALYZE analytics_dev2.dimpayer;
ANALYZE analytics_dev2.dimcontract;
ANALYZE analytics_dev2.dimservicecode;
ANALYZE analytics_dev2.dimuser;
ANALYZE conflict_dev2.conflictvisitmaps;
ANALYZE conflict_dev2.excluded_agency;
ANALYZE conflict_dev2.excluded_ssn;
ANALYZE conflict_dev2.settings;
ANALYZE conflict_dev2.mph;

-- ============================================================================
-- MONITORING & VALIDATION
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Check Index Build Progress
-- ----------------------------------------------------------------------------
SELECT 
    now()::time AS current_time,
    query_start,
    state,
    LEFT(query, 100) AS query_snippet
FROM pg_stat_activity
WHERE query LIKE '%CREATE INDEX%'
  AND state = 'active';

-- ----------------------------------------------------------------------------
-- Check Index Sizes After Creation
-- ----------------------------------------------------------------------------
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) AS index_size
FROM pg_indexes
JOIN pg_class ON pg_class.relname = indexname
WHERE schemaname IN ('analytics_dev2', 'conflict_dev2')
  AND indexname LIKE 'idx_%'
ORDER BY pg_relation_size(schemaname||'.'||indexname) DESC;

-- ----------------------------------------------------------------------------
-- Verify Index Usage After Running Query
-- ----------------------------------------------------------------------------
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan AS times_used,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched,
    pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) AS index_size
FROM pg_stat_user_indexes
WHERE schemaname IN ('analytics_dev2', 'conflict_dev2')
  AND indexname LIKE 'idx_%'
ORDER BY idx_scan DESC;

-- ----------------------------------------------------------------------------
-- Check Table/Index Bloat
-- ----------------------------------------------------------------------------
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS index_size,
    ROUND(100 * (pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename))::numeric / 
          NULLIF(pg_total_relation_size(schemaname||'.'||tablename), 0), 2) AS index_pct
FROM pg_tables
WHERE schemaname IN ('analytics_dev2', 'conflict_dev2')
  AND tablename IN ('factvisitcallperformance_cr', 'conflictvisitmaps', 'dimcaregiver', 'dimpatientaddress')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- ============================================================================
-- ROLLBACK (Use Only If Needed)
-- ============================================================================
-- If indexes cause issues or don't improve performance, drop them:
/*
-- Phase 1 indexes
DROP INDEX CONCURRENTLY IF EXISTS analytics_dev2.idx_fvcp_conflict_time_covering;
DROP INDEX CONCURRENTLY IF EXISTS analytics_dev2.idx_fvcp_conflict_billing_covering;
DROP INDEX CONCURRENTLY IF EXISTS analytics_dev2.idx_fvcp_ssn_visitdate_providerid;
DROP INDEX CONCURRENTLY IF EXISTS analytics_dev2.idx_fvcp_providerid_visitdate;
DROP INDEX CONCURRENTLY IF EXISTS conflict_dev2.idx_excluded_agency_providerid;
DROP INDEX CONCURRENTLY IF EXISTS conflict_dev2.idx_excluded_ssn_ssn_trimmed;

-- Phase 2 indexes
DROP INDEX CONCURRENTLY IF EXISTS analytics_dev2.idx_patientaddress_gps_lookup;
DROP INDEX CONCURRENTLY IF EXISTS analytics_dev2.idx_office_active_lookup;
DROP INDEX CONCURRENTLY IF EXISTS analytics_dev2.idx_provider_active_lookup;
DROP INDEX CONCURRENTLY IF EXISTS analytics_dev2.idx_payer_active_lookup;
DROP INDEX CONCURRENTLY IF EXISTS conflict_dev2.idx_settings_extradistanceper;
DROP INDEX CONCURRENTLY IF EXISTS conflict_dev2.idx_mph_range_lookup;
*/

-- ============================================================================
-- NOTES
-- ============================================================================
-- 1. CONCURRENTLY: Indexes are created with CONCURRENTLY to avoid table locks
--    This allows queries to continue running during index creation
--    Trade-off: Takes longer to build, but no downtime
--
-- 2. IF NOT EXISTS: Prevents errors if index already exists
--    Safe to re-run this script
--
-- 3. 32-Column Limit: PostgreSQL has a hard limit of 32 columns per index
--    Index 1 was split into Index 1A (time) and 1B (billing) to stay under limit
--    Both work together to provide comprehensive coverage
--
-- 4. IMMUTABLE Functions Only: Partial indexes (WHERE clause) require IMMUTABLE functions
--    CURRENT_DATE is STABLE (changes daily), not IMMUTABLE
--    Solution: Removed WHERE clauses - indexes are larger but still effective
--    The query's WHERE clause will still filter using these indexes efficiently
--
-- 5. Build Time: Total ~60-100 minutes for all Phase 1 + Phase 2 indexes
--    Increased from original estimate due to full table indexing
--    Monitor with the progress query above
--
-- 6. Disk Space: Ensure 30-40 GB free space before starting
--    Indexes will use ~15-25 GB total (larger without partial filtering)
--
-- 7. Testing: After Phase 1, test query with EXPLAIN ANALYZE
--    Only proceed to Phase 2 if performance is still below target
--
-- 8. Autovacuum: MUST be enabled or indexes will degrade over time
--    Query performance will slowly decline without it
--
-- 9. Index Maintenance: Indexes on full table will require periodic maintenance
--    Consider running REINDEX CONCURRENTLY quarterly to prevent bloat
-- ============================================================================

-- ============================================================================
-- EXPECTED RESULTS
-- ============================================================================
-- Before indexes:
--   - Runtime: 30-60 minutes
--   - Buffer hits: 10M+ 
--   - Join method: Nested Loop (slow)
--   - Index usage: None (full table scans)
--
-- After Phase 1 (without partial WHERE clauses):
--   - Runtime: 12-20 minutes (55-65% improvement)
--   - Buffer hits: 2-4M
--   - Join method: Hash Join
--   - Index usage: Both covering indexes + join indexes
--   - Note: Slightly slower than originally estimated due to full indexes
--         but still dramatic improvement over baseline
--
-- After Phase 2:
--   - Runtime: 8-15 minutes (70-75% improvement)
--   - Buffer hits: 1-2M
--   - Join method: Hash Join (optimized)
--   - Index usage: All indexes with dimension table optimization
--
-- Target Goal:
--   - Runtime: 10-15 minutes (65-75% improvement)
--   - Buffer hits: <1M
--   - Join method: Hash Join with index-only scans where possible
--
-- Trade-off Analysis:
--   - Partial indexes (with WHERE): Smaller, faster to build, but requires IMMUTABLE
--   - Full indexes (no WHERE): Larger, slower to build, but works with any query filter
--   - Result: Full indexes still provide 55-75% improvement vs no indexes
-- ============================================================================
