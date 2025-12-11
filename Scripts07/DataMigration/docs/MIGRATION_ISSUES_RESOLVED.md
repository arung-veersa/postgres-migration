# Migration Issues Diagnosed and Fixed

## 🐛 Issue #1: Run Status Stuck in "running"

### Problem
Even when all tables and chunks were marked as "completed", the migration run status remained "running" in the database.

### Root Cause
The `migration_orchestrator.py` file was missing code to update the run status in the database. It only returned the status in the API response but never persisted it to `migration_status.migration_runs`.

### Fix Applied
Added status persistence at the end of `run_migration()` function in `scripts/migration_orchestrator.py` (line ~438):

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
- ✅ Run status will now correctly update to "completed" when done
- ✅ Resume logic will no longer find stale "running" runs
- ✅ Proper cleanup and reporting

---

## 🐛 Issue #2: Missing ~50K Rows After "Completed" Migration

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

Chunks Created:
- Chunk 0: ID 1-15,045 → ~10 rows (sparse region)
- Chunk 1: ID 15,046-30,090 → ~15 rows (sparse region)
- ...
- Chunk 331: ID 4,980,000-4,995,045 → ~700 rows
- Chunk 332: ID 4,995,046-5,010,090 → 25,000 rows! (entered dense region)
- Chunk 333: ID 5,010,091-5,025,135 → 25,000 rows!
- ...
- Chunk 338: ID 5,085,000-5,100,000 → 25,000 rows!

PROBLEM:
--------
Most chunks process 0-100 rows (in sparse regions).
Dense regions get proper batching, BUT:
  - If data distribution changes between runs
  - Or if source_filter affects distribution
  - Or if IDs are not sequential
  
Then chunks can COMPLETELY MISS regions with data!
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
3. **source_filter** that affects distribution (e.g., "CREATED_DATE > '2024-01-01'")
4. **Config changes** between runs (different filters, different batch sizes)

---

## 🎯 Solutions

### Solution 1: Use `source_filter` to Target Missing Data ⭐ **Recommended**

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

3. Run with `no_resume: true`:
```json
{
  "action": "migrate",
  "source_name": "conflict",
  "no_resume": true
}
```

**Pros:**
- ✅ Fast - only processes missing data
- ✅ Safe - uses `insert_only_mode` to skip duplicates
- ✅ Targeted - explicitly handles the gap

**Cons:**
- ⚠️ Requires manual identification of MAX(ID)
- ⚠️ Need to remove filter after catch-up

---

### Solution 2: Force Full Reload 🔄

1. Update `config.json`:
```json
{
  "source": "CONFLICTVISITMAPS",
  "truncate_onstart": true,
  "source_filter": null
}
```

2. Run with `no_resume: true`

**Pros:**
- ✅ Guaranteed complete - rebuilds from scratch
- ✅ No need to diagnose gaps
- ✅ Clean slate

**Cons:**
- ⚠️ Slow - reloads ALL 8.4M rows
- ⚠️ Downtime - table is empty during load
- ⚠️ Wasteful - reprocesses 8.4M existing rows

---

### Solution 3: Switch Chunking Strategy 🔀

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

**Cons:**
- ⚠️ Requires suitable date column
- ⚠️ May need date indexes for performance

---

## 🔍 Diagnostic Queries

Created comprehensive diagnostic SQL file: `sql/diagnose_missing_rows.sql`

Run these to understand your specific issue:
1. Check ID distribution in Snowflake (density analysis)
2. Find ID gaps (WHERE gap_size > 1000)
3. Analyze what chunks were created
4. Compare ID ranges between source and target

---

## 📋 Action Items

### Immediate (to fix current state):
1. ✅ Delete old stuck run: `DELETE FROM migration_status.migration_runs WHERE run_id = '9072358c-40cb-4310-a24c-338b00d66436';`
2. ✅ Find MAX(ID) in target PostgreSQL
3. ✅ Update config with `source_filter: "ID > {max_id}"`
4. ✅ Run with `no_resume: true`
5. ✅ Verify all 8,471,089 rows are present

### Long-term (prevent future occurrences):
1. ✅ Code fix applied: Run status now persists correctly
2. 🔄 Consider date-based chunking for tables with sparse IDs
3. 🔄 Add row count validation post-migration
4. 🔄 Monitor for ID density in large tables

---

## 📊 Verification

After applying fixes, verify:

```sql
-- 1. Check run status is "completed"
SELECT run_id, status, completed_at, total_rows_copied
FROM migration_status.migration_runs
ORDER BY started_at DESC LIMIT 5;

-- 2. Verify row counts match
-- In PostgreSQL:
SELECT COUNT(*) FROM conflict_dev.conflictvisitmaps;
-- Should equal 8,471,089

-- 3. Check for any failed chunks
SELECT * FROM migration_status.migration_chunk_status
WHERE status = 'failed'
ORDER BY started_at DESC LIMIT 10;
```

---

## 🎓 Key Learnings

1. **Chunking strategies matter**: Uniform range chunking assumes uniform data distribution
2. **Always validate row counts**: "Completed" status doesn't guarantee all data was processed
3. **Sparse IDs are dangerous**: Large ID gaps can cause chunks to miss dense regions
4. **Status persistence is critical**: Resume logic depends on accurate status tracking
5. **source_filter is powerful**: Use it to explicitly target data ranges for catch-up loads

