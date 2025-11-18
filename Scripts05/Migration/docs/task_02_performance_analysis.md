# Task 02 - Performance Analysis & Optimization Options

## Current Performance Profile

### Batch 6 Analysis (Typical Large Batch)
- **Conflict visits in Postgres**: 15,348
- **SSNs to fetch**: 1,284
- **Analytics rows fetched**: 638,216 rows (!)
- **Conflicts detected**: 14,236
- **Records updated**: 11,617

### Time Breakdown
| Step | Time | % of Total | Throughput |
|------|------|------------|------------|
| Snowflake fetch | ~114s | 82% | 5,613 rows/sec |
| Conflict calculation | ~6s | 4% | - |
| Bulk update | ~19s | 14% | 611 records/sec |
| **Total per batch** | **~139s** | **100%** | - |

### Projected Total Runtime
- **97 batches × ~90s avg** = ~2.4 hours (if all batches similar size)
- **Current rate**: 6 batches in ~15 minutes = ~2.5 hours total

## Identified Issues

### 1. FutureWarning - dtype Incompatibility ⚠️
**Location**: `src/utils/conflict_calculator.py:238`

**Issue**: 
```python
result = pd.Series(0, index=conflicts.index)  # Creates int64 dtype
result[mask] = float_values  # Assigning float to int64 → warning
```

**Impact**: 
- Not critical now, but will break in future pandas versions
- Slight performance overhead from implicit conversions

**Fix**: Initialize with float dtype:
```python
result = pd.Series(0.0, index=conflicts.index, dtype='float64')
```

### 2. Performance Bottleneck - Snowflake Fetch 🐌
**Root Cause**: Large cartesian product when fetching analytics data
- Batch 6: 1,284 SSNs → 638K rows returned
- Average ratio: ~497 rows per SSN
- Many SSNs have historical data that creates large result sets

## Optimization Options

### Option 1: Parallel Batch Processing ⚡ (HIGH IMPACT)
**Approach**: Process multiple SSN batches concurrently

**Implementation**:
```python
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def run(self):
    # ... setup ...
    
    # Parallel processing with thread pool
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(self._process_single_batch, prefix, exclusions): prefix 
            for prefix in ssn_batches
        }
        
        for future in as_completed(futures):
            prefix = futures[future]
            try:
                updated = future.result()
                total_updated += updated
            except Exception as e:
                self.logger.error(f"Batch {prefix} failed: {e}")
```

**Pros**:
- **4x speedup with 4 workers** (~30-40 minutes instead of 2.5 hours)
- No code changes to core logic
- Database connections are thread-safe

**Cons**:
- More database connections needed (4 Snowflake + 4 Postgres)
- Higher memory usage (4 batches in memory)
- Need to manage connection pool size

**Effort**: Low (1-2 hours)

---

### Option 2: Optimize Snowflake Query 🔍 (MEDIUM IMPACT)
**Approach**: Add filters to reduce data fetched from Snowflake

**Current issue**: Fetching all visits for SSNs without date constraints on ConVisit

**Optimization A - Add date filter for ConVisits**:
```sql
-- Current: Gets ALL visits for these SSNs (historical + future)
WHERE CR1."SSN" IN (list_of_ssns)

-- Optimized: Limit ConVisits to same date range
WHERE CR1."SSN" IN (list_of_ssns)
  AND CR1."Visit Date" >= @date_from  -- Add this!
  AND CR1."Visit Date" <= @date_to    -- Add this!
```

**Expected reduction**: 50-70% fewer rows (from 638K → ~200K)

**Optimization B - Add visit status filter**:
```sql
-- Only get non-cancelled, relevant visits
AND CR1."Visit Status" NOT IN ('Cancelled', 'Deleted')
```

**Expected reduction**: Additional 10-20%

**Optimization C - Limit columns**:
```python
# Only fetch columns actually needed for conflict detection
# Currently fetching 98 columns, but may only need ~50
```

**Expected speedup**: 
- **2-3x reduction in fetch time** (114s → 40-60s per batch)
- **Total runtime**: ~1-1.5 hours

**Effort**: Low-Medium (2-4 hours)

---

### Option 3: Increase Batch Granularity 📊 (LOW-MEDIUM IMPACT)
**Approach**: Split large SSN prefixes into smaller chunks

**Current**: 2-digit prefixes (00, 01, 02, ... 99, 0A, 0B, ...)
**Optimized**: 3-digit prefixes for numeric (000-999 = 1000 batches)

**Implementation**:
```python
def _get_ssn_batches(self) -> list:
    # 3-digit batches for numeric SSNs
    batches = [f"{i:03d}*" for i in range(1000)]  # 000*, 001*, ..., 999*
    # Keep 2-digit for alpha
    batches.extend([f"{n}{a}*" for n in range(10) for a in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'])
    return batches
```

**Pros**:
- Smaller, more consistent batch sizes
- Better progress tracking
- Easier to recover from failures

**Cons**:
- More batches (1000+ instead of 97)
- More overhead from connection setup
- Longer total runtime if not parallelized

**Note**: Only beneficial if combined with Option 1 (parallel processing)

**Effort**: Low (1 hour)

---

### Option 4: Async/Pipeline Processing 🔄 (MEDIUM IMPACT)
**Approach**: Fetch next batch while processing current batch

**Implementation**:
```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

async def run_with_pipeline(self):
    executor = ThreadPoolExecutor(max_workers=2)
    
    # Start first fetch
    future_fetch = executor.submit(self._fetch_data, batches[0])
    
    for i in range(len(batches)):
        # Wait for current fetch
        current_data = future_fetch.result()
        
        # Start next fetch while processing current
        if i + 1 < len(batches):
            future_fetch = executor.submit(self._fetch_data, batches[i+1])
        
        # Process current batch
        self._process_batch(current_data)
```

**Expected speedup**: 20-30% (overlapping I/O with compute)

**Effort**: Medium (4-6 hours)

---

### Option 5: Caching & Incremental Processing 💾 (HIGH IMPACT, LONG TERM)
**Approach**: Don't reprocess unchanged data

**Strategy A - Cache Analytics Data**:
```python
# Cache Snowflake results for 1 hour
@lru_cache(maxsize=100)
def _fetch_analytics_visits_cached(self, ssns_tuple, date_from, date_to):
    return self._fetch_analytics_visits(list(ssns_tuple), ...)
```

**Strategy B - Track Processed Batches**:
```python
# Store last successful run timestamp
# Only process batches with changes since last run
SELECT "SSN" FROM conflictvisitmaps
WHERE "UpdatedDate" > @last_run_time
```

**Expected speedup**: 
- First run: Same as current
- Subsequent runs: **80-90% reduction** (only process changed data)

**Effort**: Medium-High (8-12 hours)

---

### Option 6: Database-Side Conflict Detection 🗄️ (VERY HIGH IMPACT)
**Approach**: Move conflict detection logic to Postgres

**Implementation**:
- Create stored procedure in Postgres
- Use Postgres foreign data wrapper (FDW) to query Snowflake
- Run conflict detection in SQL (parallel query execution)

**Pros**:
- Massive speedup (database-native operations)
- No data transfer to Python
- Leverages database query optimizer

**Cons**:
- Requires Postgres FDW setup for Snowflake
- More complex SQL logic
- Defeats purpose of migration to Python

**Effort**: High (20+ hours)

**Note**: Contradicts migration goal, not recommended

---

### Option 7: Filter Batches by Change Detection 🎯 (MEDIUM IMPACT)
**Approach**: Skip batches with no recent updates

**Implementation**:
```python
def _get_active_ssn_prefixes(self) -> set:
    """Only get SSN prefixes that have UpdateFlag=1"""
    query = f"""
        SELECT DISTINCT SUBSTRING("SSN", 1, 2) as prefix
        FROM "{self.pg.schema}"."conflictvisitmaps"
        WHERE "UpdateFlag" = 1
    """
    result = self.pg.fetch_dataframe(query)
    return set(result['prefix'])

def run(self):
    all_batches = self._get_ssn_batches()
    active_prefixes = self._get_active_ssn_prefixes()
    
    # Only process batches with data
    active_batches = [b for b in all_batches if b[:2] in active_prefixes]
    
    self.logger.info(f"Processing {len(active_batches)}/{len(all_batches)} batches with data")
```

**Expected reduction**: Skip 30-50% of empty batches

**Effort**: Low (1 hour)

---

## Recommended Approach 🎯

### Phase 1: Quick Wins (Immediate)
1. ✅ **Fix dtype warning** (15 minutes)
2. ✅ **Filter empty batches** (Option 7) - 1 hour
3. ✅ **Optimize Snowflake query** (Option 2A & 2B) - 2 hours

**Expected result**: ~50% speedup (2.5h → 1.25h)

### Phase 2: Parallel Processing (Next)
4. ✅ **Implement parallel batch processing** (Option 1) - 2 hours

**Expected result**: Additional 3-4x speedup (1.25h → 20-25 minutes) ✨

### Phase 3: Long-term Optimization (Future)
5. **Caching for incremental runs** (Option 5) - 8 hours
6. **Pipeline processing** (Option 4) - 4 hours

**Total expected speedup**: **6-8x faster** (2.5h → 15-25 minutes)

---

## Implementation Priority

| Option | Impact | Effort | Priority | Speedup |
|--------|--------|--------|----------|---------|
| **Fix dtype warning** | Low | 15 min | 🔴 Critical | - |
| **Filter empty batches** | Medium | 1 hour | 🟠 High | 1.3x |
| **Optimize Snowflake query** | High | 2 hours | 🟠 High | 2x |
| **Parallel processing** | Very High | 2 hours | 🟠 High | 4x |
| Pipeline processing | Medium | 4 hours | 🟡 Medium | 1.3x |
| Increase granularity | Low | 1 hour | 🟢 Low | 1.1x |
| Caching (incremental) | High | 8 hours | 🟢 Low | 5x |
| Database-side | Very High | 20+ hours | ⚫ No | 10x+ |

---

## Next Steps

**Immediate**:
1. Fix the dtype warning (required)
2. Decide on optimization strategy

**Recommended Quick Path** (4-5 hours work):
- Fix warning ✅
- Add date filters to Snowflake query ✅
- Filter empty batches ✅
- Add parallel processing ✅
- **Result**: Runtime drops from 2.5h to ~20-30 minutes

Let me know which optimizations you'd like to implement!

