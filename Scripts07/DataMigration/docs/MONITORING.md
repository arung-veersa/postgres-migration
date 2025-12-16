# Monitoring Guide

Complete guide to tracking migration progress, troubleshooting issues, and analyzing performance.

---

## Quick Reference

**Most Used Query:**
```sql
-- One-line dashboard
SELECT target_schema, source_table,
       ROUND(completed_chunks::NUMERIC / NULLIF(total_chunks, 0) * 100, 1) || '%' as progress,
       total_rows_copied, status
FROM migration_status.migration_table_status
WHERE status IN ('in_progress', 'completed')
ORDER BY target_schema, source_table;
```

**Quick Monitoring File:** `sql/QUICK_MONITORING.sql`

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Status Checks](#quick-status-checks)
3. [Detailed Progress Tracking](#detailed-progress-tracking)
4. [Performance Metrics](#performance-metrics)
5. [Concurrent Migrations](#concurrent-migrations)
6. [Failed Chunks](#failed-chunks)
7. [Row Count Verification](#row-count-verification)
8. [CloudWatch Logs](#cloudwatch-logs)
9. [AWS CLI Commands](#aws-cli-commands)

---

## Overview

### Status Tables

The migration tool tracks progress in three PostgreSQL tables:

1. **`migration_status.migration_runs`** - Overall run tracking
2. **`migration_status.migration_table_status`** - Per-table progress
3. **`migration_status.migration_chunk_status`** - Per-chunk granularity

### Key Columns

- `run_id` - Unique identifier for each migration run
- `status` - running, completed, failed
- `completed_chunks` / `total_chunks` - Progress tracking
- `total_rows_copied` - Total rows migrated
- `started_at`, `completed_at` - Timestamps

---

## Quick Status Checks

### 1. Current Progress

```sql
SELECT 
    target_schema,
    source_table,
    ROUND(completed_chunks::NUMERIC / NULLIF(total_chunks, 0) * 100, 1) || '%' as progress,
    completed_chunks || '/' || total_chunks as chunks,
    ROUND(total_rows_copied/1000000.0, 1) || 'M' as rows_copied,
    ROUND(EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600, 1) || 'h' as elapsed,
    status
FROM migration_status.migration_table_status
WHERE status IN ('in_progress', 'pending', 'completed')
ORDER BY target_schema, source_table;
```

---

### 2. Get Run IDs

```sql
SELECT 
    mr.run_id,
    mts.target_schema,
    mts.source_table,
    mr.status,
    TO_CHAR(mr.started_at, 'YYYY-MM-DD HH24:MI:SS') as started,
    ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - mr.started_at))/3600, 1) as hours_running
FROM migration_status.migration_runs mr
JOIN migration_status.migration_table_status mts ON mr.run_id = mts.run_id
WHERE mr.status IN ('running', 'partial')
ORDER BY mr.started_at DESC;
```

---

### 3. All Active Runs

```sql
SELECT 
    run_id,
    config_hash,
    status,
    total_tables,
    completed_tables,
    failed_tables,
    total_rows_copied,
    started_at,
    EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600 as hours_running,
    metadata->>'source_names' as sources
FROM migration_status.migration_runs
WHERE status IN ('running', 'failed')
ORDER BY started_at DESC;
```

---

## Detailed Progress Tracking

### By Target Schema

```sql
SELECT 
    mts.run_id,
    mts.source_table,
    mts.target_database,
    mts.target_schema,
    mts.status,
    mts.total_chunks,
    mts.completed_chunks,
    mts.failed_chunks,
    mts.total_rows_copied,
    ROUND(mts.completed_chunks::NUMERIC / NULLIF(mts.total_chunks, 0) * 100, 1) as pct_complete,
    EXTRACT(EPOCH FROM (COALESCE(mts.completed_at, CURRENT_TIMESTAMP) - mts.started_at))/3600 as hours_elapsed,
    mts.started_at,
    mts.completed_at
FROM migration_status.migration_table_status mts
WHERE mts.target_schema IN ('analytics_dev', 'analytics_dev2')
  AND mts.status IN ('in_progress', 'pending', 'completed')
ORDER BY mts.target_schema, mts.source_table;
```

---

### Chunk-Level Detail

```sql
SELECT 
    chunk_id,
    chunk_range->>'start_offset' as chunk_info,
    status,
    rows_copied,
    started_at,
    completed_at,
    EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at)) as duration_seconds,
    error_message
FROM migration_status.migration_chunk_status
WHERE run_id = 'YOUR_RUN_ID_HERE'
  AND source_table = 'FACTVISITCALLPERFORMANCE_CR'
ORDER BY 
    CASE status
        WHEN 'failed' THEN 1
        WHEN 'in_progress' THEN 2
        WHEN 'pending' THEN 3
        WHEN 'completed' THEN 4
    END,
    chunk_id
LIMIT 100;
```

---

## Performance Metrics

### Throughput & ETA

```sql
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
    WHERE status IN ('in_progress', 'pending')
      AND completed_chunks > 0
)
SELECT 
    target_schema,
    source_table,
    completed_chunks,
    total_chunks,
    total_chunks - completed_chunks as remaining_chunks,
    ROUND(completed_chunks::NUMERIC / NULLIF(total_chunks, 0) * 100, 1) as pct_complete,
    total_rows_copied,
    ROUND(hours_elapsed, 2) as hours_elapsed,
    ROUND(total_rows_copied / NULLIF(hours_elapsed, 0), 0) as rows_per_hour,
    ROUND(completed_chunks / NULLIF(hours_elapsed, 0), 1) as chunks_per_hour,
    ROUND((total_chunks - completed_chunks) / NULLIF(completed_chunks / NULLIF(hours_elapsed, 0), 0), 1) as est_hours_remaining
FROM stats
ORDER BY target_schema, source_table;
```

---

### Real-Time Monitoring (Run Every 30-60 Seconds)

```sql
SELECT 
    target_schema,
    source_table,
    LPAD(completed_chunks::TEXT, 6) || '/' || LPAD(total_chunks::TEXT, 6) as chunks,
    LPAD(ROUND(completed_chunks::NUMERIC / NULLIF(total_chunks, 0) * 100, 1)::TEXT, 5) || '%' as progress,
    LPAD(ROUND(total_rows_copied/1000000.0, 1)::TEXT, 6) || 'M' as rows_m,
    LPAD(ROUND(EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600, 1)::TEXT, 5) || 'h' as elapsed,
    LPAD(ROUND((total_chunks - completed_chunks) / NULLIF(completed_chunks / NULLIF(EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600, 0), 0), 1)::TEXT, 5) || 'h' as remaining,
    CASE 
        WHEN status = 'in_progress' THEN '🟢 Active'
        WHEN status = 'pending' THEN '🟡 Starting'
        WHEN status = 'completed' THEN '✅ Done'
        ELSE '🔴 ' || status
    END as status_indicator,
    TO_CHAR(COALESCE(completed_at, CURRENT_TIMESTAMP), 'HH24:MI:SS') as last_update
FROM migration_status.migration_table_status
WHERE target_schema IN ('analytics_dev', 'analytics_dev2')
  AND status IN ('in_progress', 'pending', 'completed')
ORDER BY target_schema;
```

---

## Concurrent Migrations

### Side-by-Side Comparison

```sql
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
```

---

### Detect Conflicts (Should Be Empty)

```sql
SELECT 
    mcs1.run_id as run_id_1,
    mcs2.run_id as run_id_2,
    mcs1.source_table,
    mcs1.chunk_id,
    mcs1.status as status_1,
    mcs2.status as status_2,
    mts1.target_schema as schema_1,
    mts2.target_schema as schema_2
FROM migration_status.migration_chunk_status mcs1
JOIN migration_status.migration_chunk_status mcs2 
    ON mcs1.source_table = mcs2.source_table
    AND mcs1.chunk_id = mcs2.chunk_id
    AND mcs1.run_id != mcs2.run_id
JOIN migration_status.migration_table_status mts1 ON mcs1.run_id = mts1.run_id
JOIN migration_status.migration_table_status mts2 ON mcs2.run_id = mts2.run_id
WHERE mts1.target_schema IN ('analytics_dev', 'analytics_dev2')
  AND mts2.target_schema IN ('analytics_dev', 'analytics_dev2')
LIMIT 10;
```

---

## Failed Chunks

### All Failed Chunks

```sql
SELECT 
    mcs.run_id,
    mts.target_schema,
    mcs.source_table,
    mcs.chunk_id,
    mcs.chunk_range as chunk_info,
    mcs.error_message,
    mcs.retry_count,
    mcs.started_at,
    mcs.completed_at
FROM migration_status.migration_chunk_status mcs
JOIN migration_status.migration_table_status mts 
    ON mcs.run_id = mts.run_id 
    AND mcs.source_database = mts.source_database
    AND mcs.source_schema = mts.source_schema
    AND mcs.source_table = mts.source_table
WHERE mcs.status = 'failed'
  AND mts.target_schema IN ('analytics_dev', 'analytics_dev2')
ORDER BY mcs.started_at DESC;
```

---

### Failed Chunk Summary

```sql
SELECT 
    mts.target_schema,
    mts.source_table,
    COUNT(*) as failed_count,
    STRING_AGG(DISTINCT SUBSTRING(mcs.error_message, 1, 50), '; ') as error_sample
FROM migration_status.migration_chunk_status mcs
JOIN migration_status.migration_table_status mts 
    ON mcs.run_id = mts.run_id 
    AND mcs.source_database = mts.source_database
    AND mcs.source_schema = mts.source_schema
    AND mcs.source_table = mts.source_table
WHERE mcs.status = 'failed'
GROUP BY mts.target_schema, mts.source_table
ORDER BY failed_count DESC;
```

---

## Row Count Verification

### Compare Source vs Target

```sql
-- For analytics_dev
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

-- For analytics_dev2
SELECT 
    'analytics_dev2' as schema_name,
    COUNT(*) as actual_rows,
    (SELECT total_rows_copied 
     FROM migration_status.migration_table_status
     WHERE target_schema = 'analytics_dev2'
       AND source_table = 'FACTVISITCALLPERFORMANCE_CR'
     LIMIT 1) as status_rows
FROM analytics_dev2.factvisitcallperformance_cr;
```

---

### Row Count Match Check

```sql
WITH target_counts AS (
    SELECT 
        target_schema,
        source_table,
        total_rows_copied as status_count
    FROM migration_status.migration_table_status
    WHERE status = 'completed'
)
SELECT 
    tc.target_schema,
    tc.source_table,
    tc.status_count,
    -- Add actual count queries here per table
    CASE 
        WHEN tc.status_count = 0 THEN '⚠️ No rows in status'
        ELSE '✅ Check manually'
    END as verification
FROM target_counts tc
ORDER BY tc.target_schema, tc.source_table;
```

---

## CloudWatch Logs

### Filter Patterns

**For specific schema:**
```
[time, level, logger, message = *analytics_dev*]
```

**For errors only:**
```
[time, level=ERROR, ...]
```

**For memory usage:**
```
"Max Memory Used"
```

**For specific table:**
```
[time, level, logger, message = *FACTVISITCALLPERFORMANCE_CR*]
```

---

### Key Log Patterns to Monitor

**Successful fetch:**
```
✓ Completed: Fetch from Snowflake: FACTVISIT... in 45.2s
Fetched 25,000 rows from Snowflake
```

**Successful load:**
```
✓ Completed: Load to PostgreSQL: factvisit... in 5.7s
[FACTVISIT...] Using COPY mode
```

**Parallel threads:**
```
[FACTVISIT...] Processing 10988 chunks with 20 threads...
[FACTVISIT...] Using table-specific thread count: 20
```

**Chunking optimization:**
```
✓ Retrieved counts for 199 dates in 0.3s
Created 10988 chunks for FACTVISITCALLPERFORMANCE_CR
```

---

### CloudWatch Insights Queries

**Average fetch time:**
```
fields @timestamp, @message
| filter @message like /Completed: Fetch from Snowflake/
| parse @message "in *s" as duration
| stats avg(duration) as avg_fetch_time by bin(5m)
```

**Memory usage trend:**
```
fields @timestamp, @message
| filter @message like /Max Memory Used/
| parse @message "Max Memory Used: * MB" as memory_used
| stats max(memory_used) by bin(5m)
```

---

## AWS CLI Commands

### Get Recent Logs

```bash
# Get logs from last 2 hours
aws logs filter-log-events \
  --log-group-name /aws/lambda/snowflake-postgres-migration \
  --start-time $(($(date +%s) - 7200))000 \
  --filter-pattern "analytics_dev" \
  --region us-east-1
```

---

### Tail Logs (Real-time)

```bash
aws logs tail /aws/lambda/snowflake-postgres-migration \
  --follow \
  --region us-east-1
```

---

### Get Lambda Memory Metrics

```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name MemoryUtilization \
  --dimensions Name=FunctionName,Value=snowflake-postgres-migration \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average,Maximum \
  --region us-east-1
```

---

## Monitoring Workflow

### Every 5 Minutes
- Run "Quick Status Check" query
- Check CloudWatch for errors

### Every 30 Minutes
- Run "Performance Metrics" query
- Verify throughput is consistent
- Check for failed chunks

### If Issues Occur
- Run "Failed Chunks" query
- Check CloudWatch logs for error details
- Verify memory usage within limits
- Check Snowflake warehouse status

### Before Declaring Success
- Run "Row Count Verification"
- Compare with Snowflake source counts
- Check for any failed chunks
- Review CloudWatch for warnings

---

## Quick Monitoring Script

See `sql/QUICK_MONITORING.sql` for ready-to-use queries organized by frequency.

---

**For troubleshooting slow migrations, see [OPTIMIZATION.md](OPTIMIZATION.md)**  
**For common issues, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md)**

