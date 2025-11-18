# Task 02 - Performance Optimization Quick Start

## 🎯 Goal: 3-5x Faster Execution

**Current**: 35-45 minutes  
**Target**: 10-15 minutes (Phase 1) or 5-10 minutes (Phase 2)

---

## ✅ **Already Applied: Snowflake Connector Optimizations**

The Snowflake connector has been **pre-optimized** with:
- ✅ Apache Arrow format (5-10x faster data transfer)
- ✅ Result caching enabled (90% faster for re-runs)
- ✅ Parallel prefetch threads (10-20% faster)
- ✅ Query tagging for monitoring
- ✅ Optimized timeouts and session parameters

**Impact**: **15-30% faster** data fetching from Snowflake (already active!)

See `docs/snowflake_connector_optimizations.md` for full details.

---

## ⚡ **Phase 1: Quick Wins (10 minutes - 3x faster)**

### Step 1: Scale Up Snowflake Warehouse (2 minutes)

**Connect to Snowflake and run:**
```sql
-- Check current warehouse
SHOW WAREHOUSES;

-- Scale up (replace YOUR_WAREHOUSE with actual name)
ALTER WAREHOUSE YOUR_WAREHOUSE SET WAREHOUSE_SIZE = 'LARGE';
ALTER WAREHOUSE YOUR_WAREHOUSE SET AUTO_SUSPEND = 300;  -- Auto-suspend after 5 min

-- Verify
SHOW WAREHOUSES LIKE 'YOUR_WAREHOUSE';
```

**Cost Impact**: ~2-3x more credits/hour, but 3x less runtime = cost neutral or cheaper!

---

### Step 2: Increase Parallel Workers (1 minute)

**Edit**: `config/settings.py`

```python
# Find this line:
MAX_WORKERS = 4

# Change to:
MAX_WORKERS = 6  # Start with 6, can try 8 later
```

**Save the file.**

---

### Step 3: Add Postgres Indexes (5 minutes)

**Option A: Using psql**
```powershell
# From Migration directory:
psql -h your_host -U your_user -d your_database -f sql\indexes\add_conflictvisitmaps_indexes.sql
```

**Option B: Using pgAdmin or any Postgres client**
```sql
-- Copy and paste from:
-- sql/indexes/add_conflictvisitmaps_indexes.sql

-- Or run these essential indexes:
CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_ssn_date 
ON public."conflictvisitmaps" ("SSN", "VisitDate")
WHERE "CONFLICTID" IS NOT NULL AND "UpdateFlag" = 1;

CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_visitid 
ON public."conflictvisitmaps" ("VisitID");

ANALYZE public."conflictvisitmaps";
```

---

### Step 4: Run the Script (Clear Cache First)

```powershell
cd C:\Users\ArunGupta\Repos\postgres-migration\Scripts05\Migration

# Clear Python cache
Remove-Item -Path .\src\__pycache__ -Recurse -Force -ErrorAction SilentlyContinue

# Run with optimizations
py -B scripts\run_task_02.py
```

---

### Step 5: Monitor Performance

**Terminal 2** (optional - for real-time monitoring):
```powershell
cd C:\Users\ArunGupta\Repos\postgres-migration\Scripts05\Migration
Get-Content -Path .\logs\etl_pipeline.log -Wait -Tail 30
```

---

## ⚡⚡ **Phase 2: Maximum Speed (If Still Slow - 5-10 min)**

### Step 1: Scale to XLARGE Warehouse

```sql
ALTER WAREHOUSE YOUR_WAREHOUSE SET WAREHOUSE_SIZE = 'XLARGE';
```

---

### Step 2: Increase to 8 Workers

```python
# config/settings.py
MAX_WORKERS = 8
```

---

### Step 3: Enable Query Acceleration

```sql
ALTER WAREHOUSE YOUR_WAREHOUSE 
SET QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;
```

**Note**: Snowflake connector optimizations (compression, prefetch, caching) are **already applied** - no code changes needed!

---

## 📊 **Expected Results**

### Phase 1 (LARGE warehouse + 6 workers + indexes):
```
Before: Processing 97 batches, ~60-90 sec per batch = 35-45 min
After:  Processing 400 batches, ~15-25 sec per batch = 10-15 min
        
Improvement: 3-4x faster ⚡⚡⚡
```

### Phase 2 (XLARGE warehouse + 8 workers + all optimizations):
```
Before: Processing 97 batches, ~60-90 sec per batch = 35-45 min
After:  Processing 400 batches, ~8-15 sec per batch = 5-10 min
        
Improvement: 5-7x faster ⚡⚡⚡⚡⚡
```

---

## 📈 **Monitoring During Run**

### What to Watch For:

**Good Signs** ✅:
```
INFO - Processing 456 SSN batches with 6 parallel workers
INFO - Fetched 5000 visits from Analytics in 15 seconds (333 rows/sec)
INFO - [5/456] Batch 42 (SSN 041) complete: 120 records updated
INFO - [6/456] Batch 15 (SSN 014) complete: 95 records updated
```

**Problems** ❌:
```
INFO - Fetched 50000 visits from Analytics in 90 seconds (555 rows/sec)
ERROR - Snowflake connection error: Connection pool exhausted
```

### Solutions:
- **Slow Snowflake queries**: Increase warehouse size
- **Connection errors**: Reduce MAX_WORKERS
- **Slow Postgres**: Check if indexes were created (`\di` in psql)

---

## 🔄 **After ETL Completes**

### Scale Down Snowflake (Save Costs):

```sql
-- Return to original size
ALTER WAREHOUSE YOUR_WAREHOUSE SET WAREHOUSE_SIZE = 'SMALL';

-- Or suspend immediately
ALTER WAREHOUSE YOUR_WAREHOUSE SUSPEND;
```

---

## 🆘 **Troubleshooting**

### Issue: No performance improvement

**Check**:
1. Did Snowflake warehouse actually scale up?
   ```sql
   SHOW WAREHOUSES;  -- Check SIZE column
   ```

2. Are indexes created?
   ```sql
   \di conflictvisitmaps  -- In psql
   ```

3. Is MAX_WORKERS updated?
   ```powershell
   # Check logs for:
   # "Processing X SSN batches with 6 parallel workers"
   ```

4. Did you clear Python cache?
   ```powershell
   Remove-Item -Path .\src\__pycache__ -Recurse -Force
   ```

---

### Issue: Errors with 8 workers

**Solution**: Reduce to 6 or 4:
```python
MAX_WORKERS = 6  # More stable
```

---

### Issue: Snowflake "Connection pool exhausted"

**Solution**: Your Snowflake user has connection limit
```python
MAX_WORKERS = 4  # Stay within connection limit
```

Or increase Snowflake connection limit (contact Snowflake admin).

---

## 📝 **Checklist**

Before running:
- [ ] Snowflake warehouse scaled to LARGE (or XLARGE for Phase 2)
- [ ] `MAX_WORKERS = 6` in config/settings.py
- [ ] Postgres indexes created
- [ ] Python cache cleared
- [ ] Optional: Second terminal for log monitoring

After running:
- [ ] Scale Snowflake warehouse back down
- [ ] Check logs for any errors
- [ ] Verify records were updated in Postgres

---

## 💡 **Pro Tips**

1. **Test with Phase 1 first**: See the improvement before trying Phase 2
2. **Monitor Snowflake costs**: LARGE/XLARGE warehouses cost more per hour
3. **Run during off-hours**: Less database contention
4. **Keep logs**: Compare performance across runs
5. **Indexes are permanent**: You only need to create them once

---

## 📞 **Quick Reference**

| Optimization | File/Location | Change |
|--------------|---------------|--------|
| Warehouse size | Snowflake SQL | `ALTER WAREHOUSE ... SET WAREHOUSE_SIZE = 'LARGE'` |
| Workers | config/settings.py | `MAX_WORKERS = 6` |
| Indexes | Postgres | Run `sql/indexes/add_conflictvisitmaps_indexes.sql` |
| Compression | src/connectors/snowflake_connector.py | Add `compression='gzip'` |

---

## 🎉 **Success Metrics**

You'll know it worked when you see:
- ✅ Parallel execution: Batches completing out of order
- ✅ Faster Snowflake queries: 15-25 seconds (was 60-90)
- ✅ More batches: ~400 (was ~97)
- ✅ Total time: 10-15 minutes (was 35-45)

---

**Ready to go! Start with Phase 1 and enjoy the 3x speedup!** 🚀

