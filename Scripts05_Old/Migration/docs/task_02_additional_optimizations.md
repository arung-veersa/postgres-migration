# Task 02 - Additional Performance Optimizations

## Constraints
- ❌ Cannot reduce 2-year + 45-day time period (business requirement)
- ❌ Cannot create views in Snowflake (permission restriction)
- ✅ Must fetch all 98 columns (needed for final UPDATE)

---

## 🚀 **Recommended Optimizations (Ranked by Impact)**

### 1. **Scale Up Snowflake Warehouse** ⭐⭐⭐⭐⭐
**Impact**: 2-5x faster queries  
**Effort**: 5 minutes  
**Cost**: Temporary increase during ETL run

#### Current State
Your Snowflake queries take 60-90 seconds per batch. This is the main bottleneck.

#### Action
```sql
-- In Snowflake, before running the script:
ALTER WAREHOUSE <YOUR_WAREHOUSE> SET WAREHOUSE_SIZE = 'XLARGE';

-- After the script completes:
ALTER WAREHOUSE <YOUR_WAREHOUSE> SET WAREHOUSE_SIZE = 'SMALL';
```

#### Expected Impact
| Warehouse Size | Query Time | Total Runtime |
|----------------|------------|---------------|
| SMALL (current?) | 60-90 sec | 35-45 min |
| MEDIUM | 30-45 sec | 18-25 min |
| LARGE | 15-25 sec | 10-15 min |
| XLARGE | 8-15 sec | 5-10 min |

**Recommendation**: Use LARGE or XLARGE during the ETL window.

---

### 2. **Increase Parallel Workers** ⭐⭐⭐⭐
**Impact**: 1.5-2x faster overall  
**Effort**: 1 minute  
**Cost**: Free (just CPU/Memory)

#### Current State
```python
# config/settings.py
MAX_WORKERS = 4
```

#### Action
```python
# config/settings.py
MAX_WORKERS = 8  # or 6 if system resources are limited
```

#### Guidelines
- **4 workers**: Good baseline (current)
- **6 workers**: Optimal for most systems
- **8 workers**: Best performance if you have good Snowflake concurrency
- **10+ workers**: May hit Snowflake connection limits

#### Expected Impact
| Workers | Batches in Parallel | Est. Time (LARGE warehouse) |
|---------|---------------------|------------------------------|
| 4 | 4 at once | 10-15 min |
| 6 | 6 at once | 7-10 min |
| 8 | 8 at once | 5-8 min |

**Recommendation**: Try 6 first, then 8 if no connection issues.

---

### 3. **Add Postgres Indexes** ⭐⭐⭐⭐
**Impact**: 30-50% faster Postgres queries  
**Effort**: 5 minutes (one-time setup)  
**Cost**: Free (small storage overhead)

#### Current State
The view `vw_conflictvisitmaps_base` queries the `conflictvisitmaps` table, which may not have optimal indexes.

#### Action
```sql
-- Connect to Postgres and run:

-- Index on SSN for batch filtering
CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_ssn 
ON public."conflictvisitmaps" ("SSN");

-- Composite index for the view's WHERE clause
CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_update_flag 
ON public."conflictvisitmaps" ("CONFLICTID", "UpdateFlag", "VisitDate")
WHERE "CONFLICTID" IS NOT NULL;

-- Index on VisitID for the final UPDATE
CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_visitid 
ON public."conflictvisitmaps" ("VisitID");

-- Analyze table to update statistics
ANALYZE public."conflictvisitmaps";
```

#### Expected Impact
- Postgres queries: 5-10 sec → 2-5 sec
- Bulk updates: Slightly faster

---

### 4. **Enable Snowflake Result Caching** ⭐⭐⭐
**Impact**: Massive for re-runs (90% faster)  
**Effort**: Already enabled by default  
**Cost**: Free

#### How It Works
- Snowflake caches query results for 24 hours
- If you run the same query twice, 2nd run is instant
- Works across SSN batches with similar patterns

#### Benefit
If you need to re-run Task 02 (e.g., after fixing a bug), many queries will hit cache and complete instantly.

**Already active** - no action needed!

---

### 5. **Optimize Postgres Connection Pooling** ⭐⭐⭐
**Impact**: 10-20% faster  
**Effort**: 5 minutes  
**Cost**: Free

#### Current State
Check if the Postgres connector uses connection pooling efficiently.

#### Action
Update `src/connectors/postgres_connector.py` to use connection pooling:

```python
# In PostgresConnector.__init__:
self.connection = psycopg2.connect(
    ...,
    # Add these options:
    options='-c statement_timeout=600000',  # 10 min timeout
    connect_timeout=10
)

# Ensure cursor reuse
self.cursor = self.connection.cursor()
```

**Alternative**: Use `psycopg2.pool.SimpleConnectionPool` for better multi-threading support.

---

### 6. **Batch Size Tuning** ⭐⭐
**Impact**: 5-15% faster  
**Effort**: 10 minutes (testing)  
**Cost**: Free

#### Experiment
You're currently using 3-character SSN prefixes (~20-40 SSNs per batch).

Try different batch sizes:
```python
# In _get_ssn_batches(), change:
LEFT("SSN", 3)  # Current: ~400 batches
LEFT("SSN", 4)  # Experiment: ~4000 batches (very small batches)
LEFT("SSN", 2)  # Fallback: ~100 batches (large batches)
```

**Trade-off**:
- **Smaller batches (4-char)**: More parallelism, but more overhead
- **Larger batches (2-char)**: Less overhead, but less parallelism

**Recommendation**: Stick with 3-character (current) unless you see bottlenecks.

---

### 7. **Use Snowflake Query Acceleration** ⭐⭐
**Impact**: 1.5-2x faster for specific slow queries  
**Effort**: 5 minutes  
**Cost**: Extra Snowflake credits

#### Action
```sql
-- Enable query acceleration service:
ALTER WAREHOUSE <YOUR_WAREHOUSE> 
SET QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;
```

This automatically scales up compute for slow queries.

---

### 8. **Monitor and Tune During Execution** ⭐⭐
**Impact**: Varies  
**Effort**: Active monitoring  
**Cost**: Free

#### What to Monitor

**Terminal 1**: Run the script
```powershell
py -B scripts\run_task_02.py
```

**Terminal 2**: Monitor performance
```powershell
# Watch logs
Get-Content -Path .\logs\etl_pipeline.log -Wait -Tail 30
```

**Snowflake UI**: Check query history
- Go to Snowflake → History tab
- Watch for slow queries (>30 sec)
- Check "Execution Time" and "Queued" time

**Postgres**: Check active queries
```sql
-- In Postgres:
SELECT pid, query_start, state, query 
FROM pg_stat_activity 
WHERE datname = 'your_database';
```

#### What to Look For
- **Snowflake queuing**: Warehouse too small
- **Long Postgres queries**: Missing indexes
- **Few parallel batches**: Increase MAX_WORKERS

---

### 9. **Enable Network Compression** ⭐
**Impact**: 5-10% faster (reduces data transfer time)  
**Effort**: 2 minutes  
**Cost**: Free (slight CPU overhead)

#### Action
Update `src/connectors/snowflake_connector.py`:

```python
def __init__(self, ...):
    self.connection = snowflake.connector.connect(
        ...,
        # Add compression:
        client_session_keep_alive=True,
        network_timeout=300,
        compression='gzip'  # NEW!
    )
```

---

### 10. **Process-Based Parallelism (Advanced)** ⭐
**Impact**: Potentially 20-30% faster  
**Effort**: 2-3 hours (code refactoring)  
**Cost**: Free

#### Current State
Using `ThreadPoolExecutor` (thread-based parallelism).
Python's GIL (Global Interpreter Lock) limits true parallelism for CPU-bound tasks.

#### Action
Switch to `ProcessPoolExecutor` for true multi-core parallelism:

```python
# In task_02_update_conflicts.py:
from concurrent.futures import ProcessPoolExecutor  # Instead of ThreadPoolExecutor

# In _process_batches():
with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
    ...
```

**Caveat**: Requires pickling all data (more complex), may not be worth it for I/O-bound tasks.

---

## 📊 **Combined Impact Estimates**

### Scenario 1: Quick Wins (10 minutes effort)
| Optimization | Action |
|--------------|--------|
| Snowflake LARGE warehouse | Scale up |
| MAX_WORKERS = 6 | config/settings.py |
| Add Postgres indexes | Run SQL |

**Expected**: 35-45 min → **10-15 min** (3x faster)

---

### Scenario 2: Maximum Performance (30 minutes effort)
| Optimization | Action |
|--------------|--------|
| Snowflake XLARGE warehouse | Scale up |
| MAX_WORKERS = 8 | config/settings.py |
| Add Postgres indexes | Run SQL |
| Enable Snowflake query acceleration | ALTER WAREHOUSE |
| Enable network compression | Update connector |

**Expected**: 35-45 min → **5-10 min** (4-7x faster)

---

## 🎯 **Recommended Action Plan**

### Phase 1: Immediate (Do Now - 10 minutes)
1. **Scale Snowflake warehouse to LARGE**
   ```sql
   ALTER WAREHOUSE <NAME> SET WAREHOUSE_SIZE = 'LARGE';
   ```

2. **Increase workers to 6**
   ```python
   # config/settings.py
   MAX_WORKERS = 6
   ```

3. **Add Postgres indexes**
   ```sql
   -- Run the CREATE INDEX commands above
   ```

### Phase 2: If Still Slow (Next Run - 20 minutes)
4. Scale to XLARGE warehouse
5. Increase workers to 8
6. Enable query acceleration
7. Add network compression

### Phase 3: Long-Term (Future)
8. Implement process-based parallelism
9. Consider incremental processing (only changed data)
10. Explore database partitioning strategies

---

## 📈 **Performance Tracking**

After implementing optimizations, track:

```bash
# Before optimization:
Total Time: 35-45 minutes
Snowflake Query: 60-90 seconds per batch
Postgres Query: 5-10 seconds per batch
Total Batches: ~400

# After Phase 1:
Total Time: 10-15 minutes (target)
Snowflake Query: 15-25 seconds per batch
Postgres Query: 2-5 seconds per batch

# After Phase 2:
Total Time: 5-10 minutes (target)
Snowflake Query: 8-15 seconds per batch
Postgres Query: 1-3 seconds per batch
```

---

## ⚠️ **Important Notes**

1. **Snowflake Costs**: Larger warehouses cost more credits/hour, but shorter runtime may offset this
2. **MAX_WORKERS**: Don't exceed your Snowflake connection limit
3. **Postgres Load**: Monitor CPU/Memory during parallel updates
4. **Testing**: Try Phase 1 first, measure results before Phase 2

---

## 🔧 **Quick Commands Reference**

```powershell
# Update config
code config\settings.py  # Change MAX_WORKERS

# Add Postgres indexes (one-time)
psql -U your_user -d your_db -f scripts\add_indexes.sql

# Clear Python cache before rerun
Remove-Item -Path .\src\__pycache__ -Recurse -Force

# Run with optimizations
py -B scripts\run_task_02.py

# Monitor performance
Get-Content -Path .\logs\etl_pipeline.log -Wait -Tail 30
```

---

## 🎉 **Expected Final Result**

With Phase 1 optimizations:
- **From**: 35-45 minutes (current)
- **To**: 10-15 minutes
- **Improvement**: **3-4x faster**

With Phase 2 optimizations:
- **From**: 35-45 minutes (current)
- **To**: 5-10 minutes
- **Improvement**: **5-7x faster**

This is achievable within your constraints! 🚀

