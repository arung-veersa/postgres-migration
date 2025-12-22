-- ============================================================================
-- QUICK MONITORING QUERIES FOR CONCURRENT MIGRATIONS
-- Use these for fast status checks of analytics_dev vs analytics_dev2
-- ============================================================================

-- 1. QUICK DASHBOARD (Run this every minute)
-- ============================================================================
SELECT 
    target_schema,
    source_table,
    ROUND(completed_chunks::NUMERIC / NULLIF(total_chunks, 0) * 100, 1) || '%' as progress,
    completed_chunks || '/' || total_chunks as chunks,
    ROUND(total_rows_copied/1000000.0, 1) || 'M' as rows_copied,
    ROUND(EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600, 1) || 'h' as elapsed,
    status
FROM migration_status.migration_table_status
WHERE target_schema IN ('analytics_dev', 'analytics_dev2')
  AND status IN ('in_progress', 'pending', 'completed')
ORDER BY target_schema, source_table;


-- 2. PERFORMANCE & ETA
-- ============================================================================
WITH stats AS (
    SELECT 
        target_schema,
        source_table,
        total_chunks,
        completed_chunks,
        total_rows_copied,
        started_at,
        EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600 as hours_elapsed
    FROM migration_status.migration_table_status
    WHERE target_schema IN ('analytics_dev', 'analytics_dev2')
      AND status IN ('in_progress', 'pending')
      AND completed_chunks > 0  -- Only calculate if some work done
)
SELECT 
    target_schema,
    source_table,
    ROUND(completed_chunks::NUMERIC / total_chunks * 100, 1) || '%' as progress,
    ROUND(total_rows_copied / NULLIF(hours_elapsed, 0), 0) as rows_per_hour,
    ROUND(completed_chunks / NULLIF(hours_elapsed, 0), 1) as chunks_per_hour,
    ROUND((total_chunks - completed_chunks) / NULLIF(completed_chunks / NULLIF(hours_elapsed, 0), 0), 1) as est_hours_remaining
FROM stats
ORDER BY target_schema, source_table;


-- 3. SIDE-BY-SIDE COMPARISON
-- ============================================================================
WITH dev1 AS (
    SELECT 
        source_table,
        ROUND(completed_chunks::NUMERIC / NULLIF(total_chunks, 0) * 100, 1) as pct_complete,
        total_rows_copied,
        EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600 as hours_elapsed
    FROM migration_status.migration_table_status
    WHERE target_schema = 'analytics_dev'
      AND status IN ('in_progress', 'pending')
),
dev2 AS (
    SELECT 
        source_table,
        ROUND(completed_chunks::NUMERIC / NULLIF(total_chunks, 0) * 100, 1) as pct_complete,
        total_rows_copied,
        EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600 as hours_elapsed
    FROM migration_status.migration_table_status
    WHERE target_schema = 'analytics_dev2'
      AND status IN ('in_progress', 'pending')
)
SELECT 
    COALESCE(dev1.source_table, dev2.source_table) as table_name,
    dev1.pct_complete || '%' as dev1_progress,
    ROUND(dev1.total_rows_copied/1000000.0, 1) || 'M' as dev1_rows,
    ROUND(dev1.hours_elapsed, 1) || 'h' as dev1_time,
    dev2.pct_complete || '%' as dev2_progress,
    ROUND(dev2.total_rows_copied/1000000.0, 1) || 'M' as dev2_rows,
    ROUND(dev2.hours_elapsed, 1) || 'h' as dev2_time
FROM dev1
FULL OUTER JOIN dev2 ON dev1.source_table = dev2.source_table
ORDER BY table_name;


-- 4. GET RUN IDs
-- ============================================================================
SELECT 
    mr.run_id,
    mts.target_schema,
    mts.source_table,
    mr.status,
    TO_CHAR(mr.started_at, 'YYYY-MM-DD HH24:MI:SS') as started,
    ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - mr.started_at))/3600, 1) as hours_running
FROM migration_status.migration_runs mr
JOIN migration_status.migration_table_status mts ON mr.run_id = mts.run_id
WHERE mts.target_schema IN ('analytics_dev', 'analytics_dev2')
  AND mr.status IN ('running', 'partial')
ORDER BY mr.started_at DESC;


-- 5. FAILED CHUNKS (If any)
-- ============================================================================
SELECT 
    mts.target_schema,
    mcs.source_table,
    mcs.chunk_id,
    mcs.error_message,
    mcs.retry_count,
    TO_CHAR(mcs.started_at, 'HH24:MI:SS') as failed_at
FROM migration_status.migration_chunk_status mcs
JOIN migration_status.migration_table_status mts 
    ON mcs.run_id = mts.run_id 
    AND mcs.source_database = mts.source_database
    AND mcs.source_schema = mts.source_schema
    AND mcs.source_table = mts.source_table
WHERE mcs.status = 'failed'
  AND mts.target_schema IN ('analytics_dev', 'analytics_dev2')
ORDER BY mcs.started_at DESC
LIMIT 20;


-- 6. ROW COUNT VERIFICATION
-- ============================================================================
-- Compare actual rows vs migration status
SELECT 
    'analytics_dev' as schema_name,
    COUNT(*) as actual_rows,
    (SELECT total_rows_copied 
     FROM migration_status.migration_table_status
     WHERE target_schema = 'analytics_dev'
       AND source_table = 'FACTVISITCALLPERFORMANCE_CR'
     LIMIT 1) as status_rows
FROM analytics_dev.factvisitcallperformance_cr

UNION ALL

SELECT 
    'analytics_dev2' as schema_name,
    COUNT(*) as actual_rows,
    (SELECT total_rows_copied 
     FROM migration_status.migration_table_status
     WHERE target_schema = 'analytics_dev2'
       AND source_table = 'FACTVISITCALLPERFORMANCE_CR'
     LIMIT 1) as status_rows
FROM analytics_dev2.factvisitcallperformance_cr;

