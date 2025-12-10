-- Diagnostic queries for stuck migration
-- Run these in PostgreSQL to understand why migration isn't completing

-- ============================================
-- 1. Find the current incomplete run
-- ============================================
SELECT 
    run_id,
    status,
    started_at,
    completed_at,
    EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - started_at))/3600 AS duration_hours,
    total_sources,
    total_tables,
    completed_tables,
    failed_tables,
    total_rows_copied,
    config_hash,
    LEFT(error_message, 100) AS error_summary,
    created_by
FROM migration_status.migration_runs
WHERE status IN ('running', 'partial', 'failed')
ORDER BY started_at DESC
LIMIT 5;

-- ============================================
-- 2. Table-level progress for the latest run
-- ============================================
SELECT 
    mts.source_name,
    mts.source_database,
    mts.source_schema,
    mts.source_table,
    mts.target_schema,
    mts.target_table,
    mts.status,
    mts.total_chunks,
    mts.completed_chunks,
    mts.failed_chunks,
    ROUND(mts.completed_chunks::numeric / NULLIF(mts.total_chunks, 0) * 100, 1) AS percent_complete,
    mts.total_rows_copied,
    mts.indexes_disabled,
    mts.indexes_restored,
    mts.started_at,
    mts.completed_at,
    EXTRACT(EPOCH FROM (COALESCE(mts.completed_at, NOW()) - mts.started_at))/60 AS duration_minutes
FROM migration_status.migration_table_status mts
WHERE mts.run_id = (
    SELECT run_id 
    FROM migration_status.migration_runs 
    WHERE status IN ('running', 'partial', 'failed')
    ORDER BY started_at DESC 
    LIMIT 1
)
ORDER BY mts.started_at;

-- ============================================
-- 3. Stuck/Failed chunks (detailed)
-- ============================================
SELECT 
    mcs.source_database,
    mcs.source_schema,
    mcs.source_table,
    mcs.chunk_id,
    mcs.status,
    mcs.rows_copied,
    mcs.retry_count,
    mcs.started_at,
    mcs.completed_at,
    EXTRACT(EPOCH FROM (COALESCE(mcs.completed_at, NOW()) - mcs.started_at))/60 AS duration_minutes,
    LEFT(mcs.error_message, 150) AS error_summary,
    mcs.chunk_range
FROM migration_status.migration_chunk_status mcs
WHERE mcs.run_id = (
    SELECT run_id 
    FROM migration_status.migration_runs 
    WHERE status IN ('running', 'partial', 'failed')
    ORDER BY started_at DESC 
    LIMIT 1
)
AND mcs.status IN ('pending', 'in_progress', 'failed')
ORDER BY mcs.started_at, mcs.source_table, mcs.chunk_id;

-- ============================================
-- 4. Summary by status
-- ============================================
SELECT 
    mcs.status,
    COUNT(*) AS chunk_count,
    SUM(mcs.rows_copied) AS total_rows,
    MIN(mcs.started_at) AS earliest_started,
    MAX(COALESCE(mcs.completed_at, NOW())) AS latest_activity
FROM migration_status.migration_chunk_status mcs
WHERE mcs.run_id = (
    SELECT run_id 
    FROM migration_status.migration_runs 
    WHERE status IN ('running', 'partial', 'failed')
    ORDER BY started_at DESC 
    LIMIT 1
)
GROUP BY mcs.status
ORDER BY 
    CASE mcs.status
        WHEN 'failed' THEN 1
        WHEN 'in_progress' THEN 2
        WHEN 'pending' THEN 3
        WHEN 'completed' THEN 4
        ELSE 5
    END;

-- ============================================
-- 5. Tables with high failure rate
-- ============================================
SELECT 
    source_database,
    source_schema,
    source_table,
    COUNT(*) AS total_chunks,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS in_progress,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
    ROUND(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1) AS failure_rate,
    MAX(retry_count) AS max_retries
FROM migration_status.migration_chunk_status
WHERE run_id = (
    SELECT run_id 
    FROM migration_status.migration_runs 
    WHERE status IN ('running', 'partial', 'failed')
    ORDER BY started_at DESC 
    LIMIT 1
)
GROUP BY source_database, source_schema, source_table
HAVING SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) > 0
ORDER BY failure_rate DESC, failed DESC;

-- ============================================
-- 6. Recent error messages (last 10)
-- ============================================
SELECT 
    source_database,
    source_schema,
    source_table,
    chunk_id,
    status,
    retry_count,
    completed_at,
    LEFT(error_message, 200) AS error_summary
FROM migration_status.migration_chunk_status
WHERE run_id = (
    SELECT run_id 
    FROM migration_status.migration_runs 
    WHERE status IN ('running', 'partial', 'failed')
    ORDER BY started_at DESC 
    LIMIT 1
)
AND error_message IS NOT NULL
ORDER BY completed_at DESC NULLS FIRST
LIMIT 10;

-- ============================================
-- 7. Long-running chunks (> 5 minutes)
-- ============================================
SELECT 
    source_database,
    source_schema,
    source_table,
    chunk_id,
    status,
    rows_copied,
    retry_count,
    EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - started_at))/60 AS duration_minutes,
    chunk_range,
    started_at
FROM migration_status.migration_chunk_status
WHERE run_id = (
    SELECT run_id 
    FROM migration_status.migration_runs 
    WHERE status IN ('running', 'partial', 'failed')
    ORDER BY started_at DESC 
    LIMIT 1
)
AND EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - started_at))/60 > 5
ORDER BY duration_minutes DESC
LIMIT 20;

-- ============================================
-- 8. Check for orphaned in_progress chunks
-- ============================================
-- These are likely from Lambda timeouts that didn't update status
SELECT 
    source_database,
    source_schema,
    source_table,
    chunk_id,
    status,
    retry_count,
    started_at,
    EXTRACT(EPOCH FROM (NOW() - started_at))/60 AS stuck_for_minutes
FROM migration_status.migration_chunk_status
WHERE run_id = (
    SELECT run_id 
    FROM migration_status.migration_runs 
    WHERE status IN ('running', 'partial', 'failed')
    ORDER BY started_at DESC 
    LIMIT 1
)
AND status = 'in_progress'
AND EXTRACT(EPOCH FROM (NOW() - started_at))/60 > 15
ORDER BY stuck_for_minutes DESC;

-- ============================================
-- 9. Overall progress summary
-- ============================================
SELECT 
    COUNT(*) AS total_chunks,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS in_progress,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
    ROUND(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1) AS percent_complete,
    SUM(rows_copied) AS total_rows_copied
FROM migration_status.migration_chunk_status
WHERE run_id = (
    SELECT run_id 
    FROM migration_status.migration_runs 
    WHERE status IN ('running', 'partial', 'failed')
    ORDER BY started_at DESC 
    LIMIT 1
);

-- ============================================
-- 10. Data Quality Checks (for specific tables)
-- ============================================
-- Replace 'dimuseroffices' with your problem table

-- Check for duplicate primary keys in target table
SELECT 
    "User Id",
    "Office Id",
    COUNT(*) as duplicate_count
FROM conflict_management.analytics_dev.dimuseroffices
GROUP BY "User Id", "Office Id"
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- Check for NULL values in NOT NULL columns
-- (Adjust column names based on your table)
SELECT 
    COUNT(*) as total_rows,
    COUNT("User Id") as non_null_user_id,
    COUNT("Office Id") as non_null_office_id,
    COUNT(*) - COUNT("User Id") as null_user_id_count,
    COUNT(*) - COUNT("Office Id") as null_office_id_count
FROM conflict_management.analytics_dev.dimuseroffices;

-- ============================================
-- RECOMMENDED ACTIONS & FIX TEMPLATES:
-- ============================================
-- 
-- If you see:
-- 
-- 1. FAILED chunks with errors (Query #3, #6):
--    → Check error_message for details
--    → Common issues: 
--         - Memory (OOM): reduce batch_size or parallel_threads
--         - Connection timeouts: network/VPC issues
--         - Data type mismatches: check column types
--         - Constraint violations: check uniqueness_columns
--         - Duplicate keys: partial previous load or chunking issue
--         - NULL violations: source data quality or missing filter
--    → If retry_count >= 3, chunks are permanently failed
-- 
-- 2. IN_PROGRESS chunks stuck > 15 minutes (Query #8):
--    → Lambda timed out without updating status
--    → These chunks need to be reset
--    → Fix: Get the run_id from Query #1, then run:
--
--       UPDATE migration_status.migration_chunk_status 
--       SET status = 'pending', retry_count = retry_count 
--       WHERE status = 'in_progress' 
--         AND run_id = '<your_run_id>'
--         AND EXTRACT(EPOCH FROM (NOW() - started_at))/60 > 15;
-- 
-- 3. PENDING chunks never starting (Query #9):
--    → Check if tables are disabled in config.json
--    → Check CloudWatch logs for Lambda startup errors
--    → Check if Lambda has enough memory
-- 
-- 4. High failure rate for specific table (Query #5):
--    → Table-specific configuration issue
--    → Check: batch_size too large, indexes not disabled, data issues
--    → Consider: disable that table temporarily or adjust its config
-- 
-- 5. 100 resume attempts reached (Step Functions error):
--    → Migration stuck in infinite loop
--    → Same chunks failing repeatedly
--    → Actions:
--         a) Fix underlying issue (check error_message)
--         b) Reset stuck in_progress chunks (see #2 above)
--         c) Skip permanently failed chunks:
--
--            UPDATE migration_status.migration_chunk_status 
--            SET status = 'completed', rows_copied = 0
--            WHERE status = 'failed' 
--              AND retry_count >= 3
--              AND run_id = '<your_run_id>';
--
--         d) Last resort - start fresh:
--            Run Step Functions with: {"no_resume": true}
-- 
-- 6. Memory/Performance issues:
--    → Check Query #7 for long-running chunks
--    → If chunks take > 10 minutes:
--         - Reduce batch_size in config.json
--         - Reduce parallel_threads
--         - Verify disable_index: true
--         - Check PostgreSQL COPY vs UPSERT mode
--
-- 7. Duplicate key errors:
--    → Root causes:
--         - Table was partially loaded before (not truncated)
--         - Chunking on partial primary key (e.g., only User Id when PK is User Id + Office Id)
--         - Offset-based chunking with non-deterministic ORDER BY
--    → Fixes:
--         a) Set truncate_onstart: true in config.json
--         b) Set chunking_columns: null (use SingleChunkStrategy)
--         c) Include ALL primary key columns in chunking_columns
--         d) Manually truncate and restart:
--
--            TRUNCATE TABLE conflict_management.analytics_dev.{table_name} CASCADE;
--            -- Then use Option C below
--
-- 8. NULL constraint violations:
--    → Root cause: Source data has NULLs that target schema doesn't allow
--    → Fix: Add filter in config.json:
--         "source_filter": "\"Source System\" = 'hha' AND \"Column Name\" IS NOT NULL"
--    → Then truncate and reload (see Option C below)
-- 
-- ============================================
-- FIX OPTIONS (Choose based on your situation):
-- ============================================
--
-- Get your run_id from Query #1 first, then choose ONE option:
--
-- --------------------------------------------
-- OPTION A: Reset stuck in_progress chunks
-- --------------------------------------------
-- Use when: Lambda timeouts, no other errors
--
-- UPDATE migration_status.migration_chunk_status 
-- SET status = 'pending'
-- WHERE status = 'in_progress' 
--   AND run_id = '<paste_run_id_here>'
--   AND EXTRACT(EPOCH FROM (NOW() - started_at))/60 > 15;
--
-- Then: Restart Step Functions (will auto-resume)
--
-- --------------------------------------------
-- OPTION B: Skip permanently failed chunks
-- --------------------------------------------
-- Use when: Chunks failed 3+ times, can't fix, want to proceed
--
-- UPDATE migration_status.migration_chunk_status 
-- SET status = 'completed', rows_copied = 0
-- WHERE status = 'failed' 
--   AND retry_count >= 3
--   AND run_id = '<paste_run_id_here>';
--
-- Update table status:
-- UPDATE migration_status.migration_table_status
-- SET 
--     completed_chunks = (
--         SELECT COUNT(*) FROM migration_status.migration_chunk_status 
--         WHERE run_id = migration_table_status.run_id 
--           AND source_table = migration_table_status.source_table 
--           AND status = 'completed'
--     ),
--     failed_chunks = (
--         SELECT COUNT(*) FROM migration_status.migration_chunk_status 
--         WHERE run_id = migration_table_status.run_id 
--           AND source_table = migration_table_status.source_table 
--           AND status = 'failed'
--     )
-- WHERE run_id = '<paste_run_id_here>'
--   AND source_table = '<problem_table_name>';
--
-- Then: Restart Step Functions (will proceed to next table)
--
-- --------------------------------------------
-- OPTION C: Truncate and reload specific table
-- --------------------------------------------
-- Use when: Duplicate keys, data quality issues, need clean reload
--
-- Step 1: Truncate target table
-- TRUNCATE TABLE conflict_management.analytics_dev.<table_name> CASCADE;
--
-- Step 2: Reset chunks to pending
-- UPDATE migration_status.migration_chunk_status
-- SET status = 'pending', retry_count = 0, error_message = NULL
-- WHERE run_id = '<paste_run_id_here>'
--   AND source_table = '<table_name>';
--
-- Step 3: Reset table status
-- UPDATE migration_status.migration_table_status
-- SET 
--     status = 'pending',
--     completed_at = NULL,
--     completed_chunks = 0,
--     failed_chunks = 0,
--     total_rows_copied = 0
-- WHERE run_id = '<paste_run_id_here>'
--   AND source_table = '<table_name>';
--
-- Step 4: Update config.json before restarting:
--     - Set "truncate_onstart": true
--     - Add source_filter to exclude bad data
--     - Verify chunking_columns (or set to null)
--
-- Step 5: Restart Step Functions (will reload the table)
--
-- --------------------------------------------
-- OPTION D: Skip entire table
-- --------------------------------------------
-- Use when: Table not critical, fix later
--
-- UPDATE migration_status.migration_table_status
-- SET 
--     status = 'completed',
--     completed_at = NOW(),
--     completed_chunks = total_chunks,
--     failed_chunks = 0
-- WHERE run_id = '<paste_run_id_here>'
--   AND source_table = '<table_name>';
--
-- UPDATE migration_status.migration_chunk_status
-- SET status = 'completed', rows_copied = 0
-- WHERE run_id = '<paste_run_id_here>'
--   AND source_table = '<table_name>';
--
-- Then: Update config.json (set "enabled": false for this table)
-- Then: Restart Step Functions
--
-- --------------------------------------------
-- OPTION E: Complete fresh start
-- --------------------------------------------
-- Use when: Everything is broken, want to start over
--
-- \i sql/truncate_all_tables.sql  -- Clears data AND status tables
--
-- Then: Run Step Functions with: {"source_name": "analytics", "no_resume": true}
--
-- ============================================
-- VERIFICATION QUERIES (run after fixes):
-- ============================================
--
-- Check run status:
SELECT 
    run_id,
    status,
    total_tables,
    completed_tables,
    failed_tables,
    total_rows_copied
FROM migration_status.migration_runs
WHERE run_id = '<your_run_id>';

-- Check table statuses:
SELECT 
    source_table,
    status,
    total_chunks,
    completed_chunks,
    failed_chunks,
    total_rows_copied
FROM migration_status.migration_table_status
WHERE run_id = '<your_run_id>'
ORDER BY started_at;

-- Check chunk status summary:
SELECT 
    status,
    COUNT(*) as chunk_count,
    SUM(rows_copied) as total_rows
FROM migration_status.migration_chunk_status
WHERE run_id = '<your_run_id>'
GROUP BY status
ORDER BY 
    CASE status
        WHEN 'failed' THEN 1
        WHEN 'in_progress' THEN 2
        WHEN 'pending' THEN 3
        WHEN 'completed' THEN 4
    END;

-- ============================================
-- QUICK TROUBLESHOOTING WORKFLOW:
-- ============================================
--
-- 1. Run Query #1 → Get run_id and overall status
-- 2. Run Query #2 → See which tables are stuck
-- 3. Run Query #3 → See failed chunks and error messages
-- 4. Run Query #6 → Read actual error messages
-- 5. Run Query #8 → Find orphaned in_progress chunks
-- 6. Based on errors:
--    - Lambda timeout → Use Option A (reset stuck chunks)
--    - Duplicate keys → Use Option C (truncate & reload)
--    - NULL violations → Update config filter, then Option C
--    - Can't fix → Use Option B (skip) or Option D (skip table)
-- 7. Run verification queries to confirm fix
-- 8. Restart Step Functions
-- 
-- ============================================
