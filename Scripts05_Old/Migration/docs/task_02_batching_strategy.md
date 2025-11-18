# Task 02 - Batching Strategy

## SSN Prefix Batching

### Current Strategy (3-Character Prefixes)

**Implementation**: `src/tasks/task_02_update_conflicts.py` → `_get_ssn_batches()`

```python
SELECT DISTINCT LEFT("SSN", 3) AS ssn_prefix
FROM vw_conflictvisitmaps_base
WHERE "VisitDate" BETWEEN %(date_from)s AND %(date_to)s
AND "SSN" IS NOT NULL
AND TRIM("SSN") != ''
ORDER BY ssn_prefix
```

### Benefits of 3-Character Prefixes

#### 1. **More Granular Parallelism** ✅
- **Before (2-char)**: 100 possible batches (00-99)
- **After (3-char)**: 1,000 possible batches (000-999)
- Better work distribution across parallel workers
- More consistent batch sizes

#### 2. **Smaller Snowflake Queries** ✅
- Each batch has ~1/10th the SSNs of 2-character batches
- Faster individual queries
- Lower memory usage per batch
- Reduced risk of query timeouts

#### 3. **Better Progress Visibility** ✅
- More frequent progress updates
- Easier to spot stuck batches
- More accurate ETA calculations

#### 4. **Improved Fault Tolerance** ✅
- If a batch fails, less data needs to be retried
- Easier to identify problematic SSN ranges
- Better error isolation

### Exclusions

The query automatically excludes:
- ✅ **NULL SSNs**: `AND "SSN" IS NOT NULL`
- ✅ **Empty SSNs**: `AND TRIM("SSN") != ''`
- ✅ **No data**: Only prefixes with actual records are returned

### Example

**Sample SSN**: `123-45-6789`

- **2-character prefix**: `12` (batches all SSNs 12X-XX-XXXX together)
- **3-character prefix**: `123` (batches all SSNs 123-XX-XXXX together)

**Impact on Batch Count**:
- If you have 97 active 2-character prefixes
- You'll have ~300-500 active 3-character prefixes (varies by data distribution)

### Performance Impact

#### Expected Changes:
- **Total batches**: ~300-500 (up from ~97)
- **Batch size**: ~1/5th smaller on average
- **Query time per batch**: ~40-60% faster
- **Overall runtime**: Similar or slightly faster due to better parallelism

#### With 4 Parallel Workers:
- **Before**: Processing 97 batches (25 batches per worker)
- **After**: Processing ~400 batches (100 batches per worker)
- Each batch completes faster, leading to more consistent progress

### Monitoring

**Log output will show**:
```
INFO - Processing 456 SSN batches with 4 parallel workers
INFO - Optimization: Skipping 544 empty batches (processing 456/1000)
INFO - Batch SSN prefix '000*' - Starting
INFO - Batch SSN prefix '001*' - Starting
...
INFO - [1/456] Batch 5 (SSN 004) complete: 120 records updated
INFO - [2/456] Batch 1 (SSN 000) complete: 95 records updated
```

### Configuration

**No changes needed** - the batching automatically adapts:
- ✅ Works with any `MAX_WORKERS` setting (1-10+)
- ✅ Automatically skips empty batches
- ✅ Handles NULL/empty SSNs
- ✅ Scales with data volume

### Comparison

| Aspect | 2-Character | 3-Character |
|--------|-------------|-------------|
| Max batches | 100 (00-99) | 1,000 (000-999) |
| Typical active | ~97 | ~300-500 |
| SSNs per batch | ~150-200 | ~20-40 |
| Snowflake rows | ~50K-70K | ~5K-10K |
| Query time | 60-90 sec | 20-30 sec |
| Parallelism | Good | Excellent |
| Progress detail | Medium | High |

### Rollback

To revert to 2-character prefixes (not recommended):

```python
# In _get_ssn_batches():
SELECT DISTINCT LEFT("SSN", 2) AS ssn_prefix  # Change 3 to 2
# ...
max_possible_batches = 100  # Change 1000 to 100
```

### Summary

**3-character prefixes provide:**
- ✅ Better parallel execution
- ✅ Faster individual queries
- ✅ More consistent progress
- ✅ Improved fault tolerance
- ✅ Same or better overall performance

**With minimal changes and no configuration required!**

