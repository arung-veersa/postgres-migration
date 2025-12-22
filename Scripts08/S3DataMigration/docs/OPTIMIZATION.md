# Performance Optimization Guide

Complete guide to optimizing migration performance for large-scale data transfers.

---

## Table of Contents

1. [Overview](#overview)
2. [Key Optimizations (v2.3)](#key-optimizations-v23)
3. [Configuration Tuning](#configuration-tuning)
4. [Memory Management](#memory-management)
5. [Snowflake Optimization](#snowflake-optimization)
6. [PostgreSQL Optimization](#postgresql-optimization)
7. [Monitoring Performance](#monitoring-performance)
8. [Cost Optimization](#cost-optimization)
9. [Troubleshooting Slow Migrations](#troubleshooting-slow-migrations)

---

## Overview

### Performance Targets

| Table Size | Target Time | Configuration |
|------------|-------------|---------------|
| < 1M rows | < 5 minutes | Default settings |
| 1-10M rows | 10-30 minutes | Tuned threads/batch |
| 10-100M rows | 1-4 hours | Optimized, date chunking |
| 100M+ rows | 8-24 hours | Full optimization |

### Bottlenecks (Most Common)

1. **Snowflake query time** (70-80% of duration)
2. **PostgreSQL COPY time** (15-20% of duration)
3. **Network latency** (5-10% of duration)
4. **Chunking overhead** (<1% if optimized)

---

## Key Optimizations (v2.3)

### 1. Chunking Performance (2,200x Speedup)

**Problem:** Date-based chunking taking 11 minutes before migration starts

**Solution:** Single aggregated query instead of individual COUNT queries

**Impact:**
- Before: 11 minutes (199 queries × 3-4 seconds each)
- After: 0.3 seconds (1 aggregated query)
- **2,200x faster startup**

**Implementation:** Automatic in v2.3+

```python
# Single query gets all dates at once
SELECT DATE(column) as date_value, COUNT(*) as row_count
FROM table
GROUP BY DATE(column)
ORDER BY date_value
```

---

### 2. Duplicate Logging Elimination (50% Cost Reduction)

**Problem:** Every log entry appeared twice in CloudWatch

**Solution:** Lambda environment detection prevents duplicate handlers

**Impact:**
- 50% reduction in CloudWatch log volume
- 50% reduction in CloudWatch costs
- Clearer logs for debugging

**Implementation:** Automatic in v2.3+

---

### 3. PostgreSQL Connection Optimization

**Problem:** Default connection settings not optimized for bulk loads

**Solution:** Session-level performance settings

**Impact:**
- 20-30% faster COPY operations
- No server-level access required

**Implementation:** Automatic in v2.3+

```sql
SET synchronous_commit = off;         -- Don't wait for disk
SET work_mem = '256MB';                -- Better sorting
SET maintenance_work_mem = '512MB';    -- Faster indexes
```

---

### 4. Memory-Optimized Parallelism

**Problem:** 15 threads caused Out of Memory errors

**Solution:** Balanced thread count based on memory analysis

**Configuration:**
```json
{
  "parallel_threads": 20,
  "batch_size": 25000,
  "lambda_memory": "10GB"
}
```

**Memory Calculation:**
```
20 threads × 300MB per thread = 6GB
10GB allocated - 6GB used = 4GB headroom (40%)
✅ Safe configuration
```

---

## Configuration Tuning

### Parallel Threads

**Guidelines:**
```
Memory per thread ≈ 300-500 MB
Total memory = parallel_threads × 500 MB
Keep usage < 80% of Lambda allocation
```

**Recommendations:**

| Lambda Memory | Safe Threads | Aggressive Threads |
|---------------|--------------|-------------------|
| 3GB | 5 | 8 |
| 6GB | 10 | 12 |
| 8GB | 13 | 15 |
| 10GB | 16 | 20 |

**Trade-offs:**
- More threads = Faster migration, higher memory risk
- Fewer threads = Slower migration, safer memory usage

---

### Batch Size

**Impact on Performance:**

| Batch Size | Snowflake Query Time | Chunks Created | Best For |
|------------|---------------------|----------------|----------|
| 10,000 | 20-30 seconds | More chunks | Small tables |
| 25,000 | 45-60 seconds | Balanced | Large tables |
| 50,000 | 5-9 minutes | Fewer chunks | Small data |

**Recommendations:**
- **Large tables (100M+ rows):** 25,000
- **Medium tables (1-10M rows):** 10,000-25,000
- **Small tables (<1M rows):** 5,000-10,000

**Why 25K is optimal for large tables:**
- Snowflake queries stay under 1 minute
- Good parallel distribution
- Memory usage manageable
- Network transfer efficient

---

### Per-Table Optimization

Override global settings for specific tables:

```json
{
  "source": "HUGE_FACT_TABLE",
  "parallel_threads": 20,    // More than global
  "batch_size": 25000         // Different from global
}
```

**When to override:**
- Table much larger than others
- Table has different characteristics
- Table needs special handling

---

## Memory Management

### Monitoring Memory Usage

**CloudWatch:**
Look for "Max Memory Used" in REPORT lines:
```
REPORT: Memory Size: 10240 MB Max Memory Used: 5500 MB
```

**Analysis:**
- < 50% usage: Can increase threads
- 50-70% usage: Optimal range
- 70-85% usage: Safe but monitor
- > 85% usage: Risk of OOM, reduce threads

---

### Memory Troubleshooting

**Symptom:** Out of Memory errors
```
Error Type: Runtime.OutOfMemory
Max Memory Used: 6144 MB (at limit)
```

**Solutions (in order):**
1. Reduce `parallel_threads` by 20-30%
2. Reduce `batch_size` by 20-30%
3. Increase Lambda memory allocation
4. Check for memory leaks (rare)

**Example fix:**
```json
// Before (OOM at 6GB)
{
  "parallel_threads": 15,
  "batch_size": 25000
}

// After (safe at 6GB)
{
  "parallel_threads": 10,
  "batch_size": 25000
}
```

---

## Snowflake Optimization

### Warehouse Sizing

**Impact:** Snowflake warehouse size is the BIGGEST performance factor

| Warehouse Size | Query Time (25K rows) | Cost per Hour | Use For |
|----------------|----------------------|---------------|---------|
| X-Small | 3-5 minutes | $1 | Testing only |
| Small | 1-3 minutes | $2 | Small migrations |
| Medium | 30-60 seconds | $4 | **Recommended** |
| Large | 15-30 seconds | $8 | Very large/urgent |

**Cost Analysis (272M records example):**
```
Medium warehouse:
  10,988 chunks × 45 sec = 8.2 hours compute
  Migration time: ~10 hours total
  Snowflake cost: ~$40
  Lambda savings: $50 (faster completion)
  Net benefit: +$10 + 13 days time saved

Small warehouse:
  10,988 chunks × 2 min = 18.3 hours compute
  Migration time: ~21 days total
  Snowflake cost: ~$25
  Lambda cost: ~$100 (long running)
  Net cost: -$75 + 21 days wasted
```

**Recommendation:** Use MEDIUM warehouse for large migrations

---

### Warehouse Settings

**Optimal configuration:**
```sql
ALTER WAREHOUSE migration_warehouse SET
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 600              -- 10 minutes
    AUTO_RESUME = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 3600;
```

**During migration:**
```sql
-- Keep warehouse running
ALTER WAREHOUSE migration_warehouse RESUME IF SUSPENDED;

-- After migration
ALTER WAREHOUSE migration_warehouse SET AUTO_SUSPEND = 60;
```

---

### Query Optimization

**Indexes in Snowflake:**
- Ensure clustering keys on chunking columns
- Check query profiles for full table scans

**Example:**
```sql
-- Check if clustered
SHOW TABLES LIKE 'FACTVISITCALLPERFORMANCE_CR';

-- Add clustering if needed
ALTER TABLE FACTVISITCALLPERFORMANCE_CR 
CLUSTER BY (DATE("Visit Updated Timestamp"));
```

---

## PostgreSQL Optimization

### Connection Settings (v2.3 Automatic)

These are now applied automatically:
```sql
SET synchronous_commit = off;        -- Don't wait for disk writes
SET work_mem = '256MB';               -- Better sorting/hashing
SET maintenance_work_mem = '512MB';   -- Faster index operations
SET temp_buffers = '128MB';           -- Temp table performance
SET effective_cache_size = '4GB';     -- Query planner hint
```

**Impact:** 20-30% faster COPY operations

---

### Table Optimization

**Before large migrations:**
```sql
-- Disable autovacuum during bulk load
ALTER TABLE target_schema.target_table 
SET (autovacuum_enabled = false);

-- Check no active locks
SELECT * FROM pg_locks 
WHERE relation = 'target_schema.target_table'::regclass;
```

**After migration:**
```sql
-- Re-enable autovacuum
ALTER TABLE target_schema.target_table 
SET (autovacuum_enabled = true);

-- Run vacuum analyze
VACUUM ANALYZE target_schema.target_table;
```

---

### Index Management

**Automatic (in config):**
```json
{
  "disable_index": true    // Auto disable/restore indexes
}
```

**Manual (if needed):**
```sql
-- Before migration
SELECT index_name FROM information_schema.statistics
WHERE table_name = 'target_table';

-- Save index definitions, then drop
DROP INDEX index_name_1;
DROP INDEX index_name_2;

-- After migration
CREATE INDEX ... -- Recreate with saved definitions
```

---

## Monitoring Performance

### Real-Time Metrics

**Query:**
```sql
-- Performance dashboard
WITH stats AS (
    SELECT 
        source_table,
        completed_chunks,
        total_chunks,
        total_rows_copied,
        started_at,
        EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/3600 as hours_elapsed
    FROM migration_status.migration_table_status
    WHERE status = 'in_progress'
      AND completed_chunks > 0
)
SELECT 
    source_table,
    ROUND(completed_chunks::NUMERIC / total_chunks * 100, 1) || '%' as progress,
    ROUND(total_rows_copied / 1000000.0, 1) || 'M rows' as copied,
    ROUND(total_rows_copied / NULLIF(hours_elapsed, 0), 0) as rows_per_hour,
    ROUND(completed_chunks / NULLIF(hours_elapsed, 0), 1) as chunks_per_hour,
    ROUND((total_chunks - completed_chunks) / NULLIF(completed_chunks / NULLIF(hours_elapsed, 0), 0), 1) as est_hours_remaining
FROM stats;
```

---

### CloudWatch Analysis

**Key metrics to monitor:**

1. **Memory usage:**
   ```
   "Max Memory Used: XXXX MB"
   ```
   Keep < 85% of allocation

2. **Fetch times:**
   ```
   "✓ Completed: Fetch from Snowflake: FACTVISIT... in 45.2s"
   ```
   Should be < 2 minutes for 25K rows

3. **Load times:**
   ```
   "✓ Completed: Load to PostgreSQL: factvisit... in 5.7s"
   ```
   Should be 5-10 seconds for 25K rows

4. **Parallel activity:**
   ```
   Count of "Starting: Fetch from Snowflake" at same time
   ```
   Should match parallel_threads setting

---

### Bottleneck Identification

**If Snowflake fetch slow (> 2 min):**
- ✅ Increase Snowflake warehouse size
- ✅ Reduce batch_size
- ✅ Check Snowflake query history for issues

**If PostgreSQL load slow (> 15 sec):**
- ✅ Disable indexes during migration
- ✅ Disable autovacuum
- ✅ Check PostgreSQL server load

**If memory high (> 85%):**
- ✅ Reduce parallel_threads
- ✅ Reduce batch_size
- ✅ Increase Lambda memory

---

## Cost Optimization

### Cost Breakdown (272M row example)

**Optimized (10 hours, v2.3):**
```
Lambda (10GB × 10 hours):    $6.50
Step Functions:               $0.01
CloudWatch Logs (optimized):  $0.50
Snowflake (MEDIUM, 10 hrs):  $40.00
Total:                       $47.01
```

**Unoptimized (21 days, v2.1):**
```
Lambda (6GB × 504 hours):    $100.80
Step Functions:               $0.05
CloudWatch Logs (duplicate): $12.00
Snowflake (Small, 504 hrs):  $25.00
Total:                       $137.85
```

**Savings:** $90.84 (66% reduction) + 20.6 days time saved

---

### Cost Optimization Tips

1. **Use MEDIUM Snowflake warehouse**
   - Costs more per hour BUT completes much faster
   - Total cost lower due to shorter duration

2. **Optimize parallel_threads**
   - Find sweet spot: Speed vs memory vs cost
   - 10-15 threads usually optimal

3. **Monitor CloudWatch costs**
   - v2.3 eliminates duplicate logs (50% savings)
   - Set log retention to 7-14 days

4. **Use insert_only_mode for full loads**
   - COPY faster than UPSERT
   - Reduces Lambda runtime

5. **Right-size Lambda memory**
   - Don't over-allocate
   - Monitor actual usage, adjust down if possible

---

## Troubleshooting Slow Migrations

### Checklist

- [ ] Check Snowflake warehouse size (should be MEDIUM for large tables)
- [ ] Verify batch_size appropriate (25K for large tables)
- [ ] Monitor parallel_threads (10-20 for large tables)
- [ ] Check CloudWatch for slow fetch times (should be < 2 min)
- [ ] Verify no duplicate logs (v2.3 should fix this)
- [ ] Check PostgreSQL indexes (should be disabled during bulk load)
- [ ] Monitor Lambda memory usage (should be 50-80%)
- [ ] Verify chunking strategy (date-based for large tables)

---

### Performance Regression

**Symptoms:** Migration slower than expected after code changes

**Check:**
1. Config changes (batch_size, threads)
2. Lambda code deployment (verify latest version)
3. Snowflake warehouse size changed
4. Network issues (check CloudWatch for timeouts)

**Verify deployed version:**
```bash
# Check Lambda environment variable
aws lambda get-function-configuration \
  --function-name snowflake-postgres-migration \
  --query 'Environment.Variables.MIGRATION_VERSION'
```

---

### Expected Performance

**Realistic benchmarks (v2.3, MEDIUM warehouse):**

```
10M rows:   ~1 hour
50M rows:   ~4 hours
100M rows:  ~8 hours
272M rows:  ~10-12 hours

Throughput: ~25-30K rows/minute
Chunk rate: ~1-1.5 chunks/minute per thread
```

**If significantly slower:**
- Check Snowflake warehouse size
- Review CloudWatch logs for errors
- Verify configuration deployed correctly

---

## Best Practices Summary

### ✅ Do

1. Use date-based chunking for tables > 10M rows
2. Set Snowflake warehouse to MEDIUM for large migrations
3. Monitor memory usage and adjust threads accordingly
4. Disable indexes during bulk loads (enable after)
5. Use insert_only_mode for full loads
6. Test with small batch first
7. Monitor CloudWatch for bottlenecks

### ❌ Don't

1. Use X-Small/Small warehouse for large migrations
2. Set parallel_threads > 20 (diminishing returns)
3. Use batch_size > 50K (slow Snowflake queries)
4. Leave indexes enabled during bulk loads
5. Over-allocate Lambda memory (unnecessary cost)
6. Use UUID chunking if alternative exists
7. Ignore memory warnings (leads to OOM)

---

**For configuration details, see [CONFIGURATION.md](CONFIGURATION.md)**  
**For monitoring queries, see [MONITORING.md](MONITORING.md)**  
**For current optimizations, see [.context/PROJECT_CONTEXT.md](../.context/PROJECT_CONTEXT.md)**

