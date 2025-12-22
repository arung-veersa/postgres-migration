-- Fix Stuck Migration Run
-- Use this script to reset stuck chunks after applying code fixes for:
-- 1. Decimal JSON serialization error
-- 2. Timestamp constraint violation

-- ===========================================================================
-- STEP 1: Identify the current run_id
-- ===========================================================================

SELECT 
    run_id,
    status,
    started_at,
    total_tables,
    completed_tables,
    failed_tables,
    EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at)) / 3600 as hours_running
FROM migration_status.migration_runs
WHERE status IN ('running', 'partial', 'failed')
ORDER BY started_at DESC
LIMIT 5;

-- Note the run_id from above and replace 'YOUR-RUN-ID' below with actual UUID

-- ===========================================================================
-- STEP 2: Check which chunks are stuck
-- ===========================================================================

SELECT 
    source_table,
    status,
    COUNT(*) as chunk_count,
    SUM(rows_copied) as total_rows_copied,
    MAX(error_message) as sample_error
FROM migration_status.migration_chunk_status
WHERE run_id = 'YOUR-RUN-ID'
GROUP BY source_table, status
ORDER BY source_table, status;

-- ===========================================================================
-- STEP 3: View specific error details
-- ===========================================================================

-- Check for timestamp constraint violations (DIMPAYERPROVIDER)
SELECT 
    source_table,
    chunk_id,
    status,
    started_at,
    completed_at,
    retry_count,
    error_message
FROM migration_status.migration_chunk_status
WHERE run_id = 'YOUR-RUN-ID'
  AND (
    error_message LIKE '%valid_chunk_completed_at%'
    OR error_message LIKE '%Decimal is not JSON serializable%'
    OR status = 'in_progress'
  )
ORDER BY source_table, chunk_id;

-- ===========================================================================
-- STEP 4: Reset stuck chunks (OPTION A - Specific Tables)
-- ===========================================================================

-- Reset DIMPAYERPROVIDER chunks (timestamp issue)
UPDATE migration_status.migration_chunk_status
SET 
    status = 'pending',
    started_at = NULL,
    completed_at = NULL,
    error_message = NULL
WHERE run_id = 'YOUR-RUN-ID'
  AND source_table = 'DIMPAYERPROVIDER'
  AND status IN ('in_progress', 'failed');

-- Reset FACTCAREGIVERABSENCE chunks (Decimal serialization issue)
UPDATE migration_status.migration_chunk_status
SET 
    status = 'pending',
    started_at = NULL,
    completed_at = NULL,
    error_message = NULL
WHERE run_id = 'YOUR-RUN-ID'
  AND source_table = 'FACTCAREGIVERABSENCE'
  AND status IN ('in_progress', 'failed');

-- ===========================================================================
-- STEP 5: Reset stuck chunks (OPTION B - All Stuck Chunks)
-- ===========================================================================

-- Use this if you want to reset ALL stuck chunks, not just specific tables
-- CAUTION: This will reset all in_progress and failed chunks

/*
UPDATE migration_status.migration_chunk_status
SET 
    status = 'pending',
    started_at = NULL,
    completed_at = NULL,
    error_message = NULL
WHERE run_id = 'YOUR-RUN-ID'
  AND status IN ('in_progress', 'failed');
*/

-- ===========================================================================
-- STEP 6: Update table status
-- ===========================================================================

-- Reset table status for tables with reset chunks
UPDATE migration_status.migration_table_status
SET 
    status = 'in_progress',
    failed_chunks = 0
WHERE run_id = 'YOUR-RUN-ID'
  AND source_table IN ('DIMPAYERPROVIDER', 'FACTCAREGIVERABSENCE');

-- ===========================================================================
-- STEP 7: Verify the fixes
-- ===========================================================================

-- Check chunk status distribution after reset
SELECT 
    source_table,
    status,
    COUNT(*) as chunk_count
FROM migration_status.migration_chunk_status
WHERE run_id = 'YOUR-RUN-ID'
  AND source_table IN ('DIMPAYERPROVIDER', 'FACTCAREGIVERABSENCE')
GROUP BY source_table, status
ORDER BY source_table, status;

-- ===========================================================================
-- STEP 8: Resume migration via Step Functions
-- ===========================================================================

-- After running the SQL above and redeploying Lambda with code fixes:
-- 
-- Input to Step Functions:
-- {
--   "source_name": "analytics",
--   "resume_run_id": "YOUR-RUN-ID"
-- }

-- ===========================================================================
-- ALTERNATIVE: Fresh Start
-- ===========================================================================

-- If you prefer to start completely fresh instead of resuming:

/*
-- 1. Truncate all status tables
TRUNCATE TABLE migration_status.migration_chunk_status CASCADE;
TRUNCATE TABLE migration_status.migration_table_status CASCADE;
TRUNCATE TABLE migration_status.migration_runs CASCADE;

-- 2. Optionally truncate target tables with truncate_onstart: true
-- (These will auto-truncate on fresh run, so this is optional)
TRUNCATE TABLE analytics_dev.dimpayerprovider;
TRUNCATE TABLE analytics_dev.dimuseroffices;

-- 3. Run Step Functions with:
-- {
--   "source_name": "analytics",
--   "no_resume": true
-- }
*/

