# Historical Issues & Resolutions

This document tracks significant bugs that were diagnosed and fixed. Useful reference for troubleshooting similar issues.

---

## Issue #1: Duplicate CloudWatch Logs (Fixed v2.3, Dec 2025)

### Problem
Every log entry appeared twice in CloudWatch Logs with slightly different formatting:
```
2025-12-15 16:48:19 | INFO     | lambda_handler | Lambda invoked...
2025-12-15 16:48:19 | INFO | lambda_handler | Lambda invoked...
```

### Root Cause
- AWS Lambda runtime automatically adds a logging handler
- Our `lib/utils.py` code added another handler
- Both handlers output the same log message
- Even with `logger.propagate = False`, both handlers activated

### Fix Applied
**File:** `lib/utils.py` - `get_logger()` function

Added Lambda environment detection:
```python
# Check if we're in AWS Lambda
in_lambda = 'AWS_EXECUTION_ENV' in os.environ or 'AWS_LAMBDA_FUNCTION_NAME' in os.environ

if in_lambda:
    # In Lambda: Use Lambda's existing handler, just set level
    logger.setLevel(logging.INFO)
    # Don't add new handlers - Lambda already has one
else:
    # Local: Add our own handler
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
```

### Impact
- ✅ Eliminated duplicate logs (50% CloudWatch volume reduction)
- ✅ 50% cost reduction for CloudWatch Logs
- ✅ Clearer logs for debugging
- ✅ Same code works in both Lambda and local environments

---

## Issue #2: Chunking Slowness (Fixed v2.2/2.3, Dec 2025)

### Problem
Date-based chunking taking 11 minutes before migration could start.

### Root Cause
`DateRangeStrategy` was making 199 individual COUNT queries to Snowflake:
```python
# Original code (BAD):
for date in date_range:
    query = f"SELECT COUNT(*) FROM table WHERE DATE(column) = '{date}'"
    count = snowflake.execute(query)  # 3-4 seconds per query
# Total: 199 queries × 4 seconds = 11 minutes
```

### Fix Applied
**File:** `lib/chunking.py` - `DateRangeStrategy.create_chunks()`

Single aggregated query:
```python
# New code (GOOD):
aggregated_query = f"""
    SELECT 
        DATE({date_column}) as date_value,
        COUNT(*) as row_count
    FROM {schema}.{table}
    GROUP BY DATE({date_column})
    ORDER BY date_value
"""
date_counts = self.snowflake_manager.execute_query(aggregated_query)
# Total: 1 query = 0.3 seconds
```

### Impact
- ✅ Startup time: 11 minutes → 0.3 seconds (2,200x speedup)
- ✅ Reduced Snowflake compute costs
- ✅ Better user experience

---

## Issue #3: Out of Memory with 15 Threads (Fixed v2.2, Dec 2025)

### Problem
Lambda crashed with `Runtime.OutOfMemory` error when using 15 parallel threads.

### Symptoms
```
REPORT: Max Memory Used: 6144 MB
Error Type: Runtime.OutOfMemory
```

### Root Cause
- Configuration: 15 threads, 25K batch size, 6GB Lambda memory
- Each thread processing 25K rows with 250 columns
- Peak memory per thread: ~450-500MB
- Total: 15 × 500MB = 7.5GB > 6GB allocated

### Fix Applied
**File:** `config.json`

Reduced threads:
```json
{
  "parallel_threads": 10,  // Was 15
  "batch_size": 25000
}
```

Later increased Lambda memory to 10GB and set threads to 20.

### Impact
- ✅ Memory usage: ~5GB for 10 threads (safe within 6GB)
- ✅ No more OOM errors
- ✅ Later: 20 threads with 10GB for better throughput

### Lesson Learned
- Memory usage varies by chunk complexity
- Need 20-30% headroom for memory spikes
- Monitor "Max Memory Used" in CloudWatch REPORT lines

---

## Issue #4: Run Status Stuck in "running" (Fixed v2.1, Nov 2025)

### Problem
Even when all tables and chunks were marked as "completed", the migration run status remained "running" in the database.

### Root Cause
`migration_orchestrator.py` was missing code to update the run status in the database. It only returned the status in the API response but never persisted it to `migration_status.migration_runs`.

### Fix Applied
**File:** `scripts/migration_orchestrator.py` (line ~438)

Added status persistence at the end of `run_migration()`:
```python
# Update the run status in the database
if orchestrator.run_id:
    try:
        total_completed = sum(r.get('tables_completed', 0) for r in source_results.values())
        total_failed = sum(r.get('tables_failed', 0) for r in source_results.values())
        
        status_tracker.update_run_status(
            orchestrator.run_id,
            status=overall_status,
            completed_tables=total_completed,
            failed_tables=total_failed,
            total_rows_copied=total_rows_all_sources
        )
        logger.info(f"✓ Updated run status in database: {overall_status}")
    except Exception as e:
        logger.warning(f"Failed to update run status in database: {e}")
```

### Impact
- ✅ Run status now correctly updates to "completed" when done
- ✅ Resume logic no longer finds stale "running" runs
- ✅ Proper cleanup and reporting

---

## Issue #5: Missing ~50K Rows After "Completed" Migration (Fixed v2.0, 2025)

### Problem
- Snowflake source: 8,471,089 rows
- PostgreSQL target: 8,420,381 rows
- **Missing: ~50,708 rows (0.6%)**
- All chunks show status="completed"

### Root Cause: Sparse ID Distribution

The `NumericRangeStrategy` chunking creates chunks based on **ID range span**, not **actual data distribution**:

```python
# Problematic logic:
id_range = max_id - min_id + 1  # Total span of IDs
num_chunks = ceil(total_rows / batch_size)
chunk_step = ceil(id_range / num_chunks)  # Assumes uniform distribution!
```

#### Example Scenario:
```
Source Table IDs (Snowflake):
- IDs 1-1,000: 500 rows (0.05% of data) - VERY SPARSE
- IDs 1,001-1,000,000: 5,000 rows (0.59% of data) - SPARSE
- IDs 5,000,001-5,100,000: 8,465,589 rows (99.36% of data) - VERY DENSE!

Chunking Calculation:
- min_id = 1, max_id = 5,100,000
- id_range = 5,100,000
- total_rows = 8,471,089
- batch_size = 25,000
- num_chunks = ceil(8,471,089 / 25,000) = 339 chunks
- chunk_step = ceil(5,100,000 / 339) = 15,045 IDs per chunk

PROBLEM:
Most chunks process 0-100 rows (in sparse regions).
Dense regions get proper batching, BUT chunks can COMPLETELY MISS regions with data!
```

### Why Chunks Show "Completed" with 0 Rows
- A chunk querying `ID BETWEEN 1 AND 15,045` successfully runs
- Snowflake returns 0 rows (no error)
- COPY operation processes 0 rows → Success!
- Status marked as "completed" ✓
- **Nobody knows there was supposed to be data elsewhere**

### Real-World Triggers
1. **Gaps in ID sequence** (deleted records, ID jumps)
2. **Dense regions** outside the uniform distribution
3. **source_filter** that affects distribution
4. **Config changes** between runs (different filters, different batch sizes)

---

### Solutions

#### Solution 1: Use `source_filter` to Target Missing Data ⭐ **Recommended**

1. Find the current MAX(ID) in PostgreSQL:
```sql
SELECT MAX("ID") FROM conflict_dev.conflictvisitmaps;
-- Example result: 5,099,999
```

2. Update `config.json`:
```json
{
  "source": "CONFLICTVISITMAPS",
  "source_filter": "\"ID\" > 5099999",
  "truncate_onstart": false,
  "insert_only_mode": true
}
```

3. Run with `no_resume: true`

**Pros:**
- ✅ Fast - only processes missing data
- ✅ Safe - uses `insert_only_mode` to skip duplicates
- ✅ Targeted - explicitly handles the gap

#### Solution 2: Switch Chunking Strategy 🔀 **Long-term**

If you have a date column (e.g., `CREATED_DATE`, `UPDATED_DATE`):

```json
{
  "source": "CONFLICTVISITMAPS",
  "chunking_columns": ["CREATED_DATE"],
  "chunking_column_types": ["timestamp"],
  "source_watermark": "UPDATED_DATE",
  "target_watermark": "updated_date"
}
```

**Pros:**
- ✅ Better for sparse IDs
- ✅ Natural incremental load support
- ✅ More predictable row distribution

---

### Key Learnings

1. **Chunking strategies matter**: Uniform range chunking assumes uniform data distribution
2. **Always validate row counts**: "Completed" status doesn't guarantee all data was processed
3. **Sparse IDs are dangerous**: Large ID gaps can cause chunks to miss dense regions
4. **Date-based chunking is safer**: Natural grouping, more predictable
5. **source_filter is powerful**: Use it to explicitly target data ranges for catch-up loads

---

## Troubleshooting Patterns

### Pattern: Migration Completes but Row Counts Don't Match

**Symptoms:**
- All chunks show "completed"
- No errors in logs
- Row count mismatch between source and target

**Check:**
1. Run row count comparison:
```sql
-- Source (Snowflake)
SELECT COUNT(*) FROM source_table;

-- Target (PostgreSQL)
SELECT COUNT(*) FROM target_table;
```

2. Check ID distribution:
```sql
-- Find gaps
WITH id_sequence AS (
    SELECT "ID", 
           "ID" - LAG("ID") OVER (ORDER BY "ID") as gap_size
    FROM source_table
)
SELECT COUNT(*) as gaps_found,
       SUM(gap_size) as total_gap_size
FROM id_sequence
WHERE gap_size > 1000;
```

3. Check chunking strategy in config.json

**Solutions:**
- Use date-based chunking for sparse IDs
- Use source_filter to target missing data
- Switch to OFFSET-based strategy (slow but complete)

---

### Pattern: Lambda Out of Memory

**Symptoms:**
```
Error Type: Runtime.OutOfMemory
Max Memory Used: [close to allocated memory]
```

**Check:**
1. CloudWatch "Max Memory Used" across multiple invocations
2. Current configuration:
   - `parallel_threads`
   - `batch_size`
   - Lambda memory allocation

**Solutions:**
1. Reduce `parallel_threads`
2. Reduce `batch_size`
3. Increase Lambda memory allocation
4. Memory calculation: threads × ~500MB per thread

**Rule of Thumb:**
Keep memory usage at 70-80% of allocated memory for safety.

---

### Pattern: Duplicate Logs

**Symptoms:**
Every log entry appears multiple times in CloudWatch.

**Cause:**
Multiple logging handlers active (Lambda + application).

**Solution:**
Implement Lambda environment detection in logging setup (see Issue #1).

---

### Pattern: Slow Chunking

**Symptoms:**
- Migration stuck in "Creating chunks..." for minutes
- Many individual COUNT queries in Snowflake query history

**Cause:**
Individual queries for each date/range instead of aggregated query.

**Solution:**
Use aggregated query approach (see Issue #2).

---

## Verification Checklist

After any fix, verify:

```sql
-- 1. Check run status is correct
SELECT run_id, status, completed_at, total_rows_copied
FROM migration_status.migration_runs
ORDER BY started_at DESC LIMIT 5;

-- 2. Verify row counts match
SELECT 
    'source' as location,
    COUNT(*) FROM source_schema.table_name
UNION ALL
SELECT 
    'target' as location,
    COUNT(*) FROM target_schema.table_name;

-- 3. Check for any failed chunks
SELECT * FROM migration_status.migration_chunk_status
WHERE status = 'failed'
ORDER BY started_at DESC LIMIT 10;

-- 4. Check memory usage in CloudWatch
-- Look for "Max Memory Used" in REPORT lines
```

---

**Last Updated:** 2025-12-16  
**Note:** This document tracks resolved issues. For current issues, check TODO.md or PROJECT_CONTEXT.md

