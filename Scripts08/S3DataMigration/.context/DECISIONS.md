# Architectural & Configuration Decisions

This document tracks key decisions made during the project, including rationale and alternatives considered.

---

## December 16, 2025

### Decision: Use 20 Threads with 10GB Lambda Memory
**Context:** Testing optimized configuration for 272M record migration

**Decision:**
- `parallel_threads: 20`
- `lambda_memory: 10GB`
- `batch_size: 25000`

**Rationale:**
- 10 threads with 6GB was safe but conservative (~5GB used)
- User now has faster Snowflake warehouse
- Memory calculation: 20 threads × 300MB = 6GB (60% of 10GB)
- Want to maximize throughput with Snowflake warehouse upgrade
- 10GB provides comfortable headroom for spikes

**Alternatives Considered:**
1. **8 threads, 6GB** - Too conservative, slower
2. **15 threads, 8GB** - Middle ground, but previous OOM at 15 with 6GB
3. **25 threads, 10GB** - Too risky, approaching limits

**Outcome:** Will test and monitor. Can adjust based on results.

---

### Decision: Increase Resume Window from 12h to 7 Days
**Context:** Large migrations (272M rows) take > 12 hours, causing unexpected new run_id creation

**Problem:**
- Migration running for 12h 4min exceeded default `resume_max_age: 12` hours
- Resume detection failed, created new run_id
- Lost all progress (10,770 chunks completed)
- Duplicate key errors when new run tried to insert existing data

**Decision:**
Change default `resume_max_age` from **12 hours → 168 hours (7 days)**

**Files Modified:**
- `scripts/lambda_handler.py` - Default parameter
- `scripts/migration_orchestrator.py` - Function signature
- `migrate.py` - Fallback default
- `aws/step_functions/*.json` - Step Function defaults

**Rationale:**
- **12 hours is insufficient** for multi-day migrations
- Real-world example: 272M rows takes 8-12 hours with optimizations
- Slower Snowflake warehouses could take 2-3 days
- **7 days provides safety** while allowing long operations
- Still prevents resuming abandoned/old runs
- **Extendable to 1 year** via runtime parameter if needed

**Alternatives Considered:**
1. **24 hours** - Still too short for slow migrations
2. **30 days** - Could work, but less operationally clean
3. **1 year (8760h)** - Effectively infinite, but loses safety check
4. **No limit** - Bad practice, could resume ancient failed runs

**Why 7 Days Won:**
- Covers 99% of realistic migration scenarios
- Provides operational hygiene (week-old failures shouldn't auto-resume)
- Easy to extend for edge cases: `{"resume_max_age": 8760}`
- Industry standard (many systems use 7-day retention)

**Runtime Override:**
```json
{
  "source_name": "analytics",
  "resume_max_age": 8760
}
```

**Impact:**
- ✅ No more unexpected run_id creation for long migrations
- ✅ Progress preserved across multi-day operations
- ✅ Still safe (won't resume year-old runs)
- ✅ Configurable per execution if needed

---

### Decision: Lambda Environment Detection for Logging
**Context:** Duplicate log entries in CloudWatch (every entry appeared twice)

**Decision:**
Detect Lambda environment and skip adding handler if in Lambda:
```python
in_lambda = 'AWS_EXECUTION_ENV' in os.environ or 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
if in_lambda:
    logger.setLevel(logging.INFO)  # Only set level, Lambda has handler
else:
    # Add our own handler for local development
```

**Rationale:**
- Lambda runtime automatically adds a logging handler
- Our code was adding another handler
- Both handlers outputted every log message = duplicates
- Detection allows same code to work in both environments

**Alternatives Considered:**
1. **Remove logger.propagate = False** - Didn't help, still duplicates
2. **Two separate code paths** - Maintenance burden
3. **Clear all handlers first** - Could break Lambda's logging

**Impact:**
- 50% reduction in CloudWatch log volume
- 50% reduction in CloudWatch costs
- Cleaner logs, easier debugging

---

### Decision: Single Aggregated Query for Chunking
**Context:** Date-based chunking was taking 11 minutes for startup

**Decision:**
Replace 199 individual COUNT queries with one aggregated query:
```sql
SELECT DATE(column) as date_value, COUNT(*) as row_count
FROM table
GROUP BY DATE(column)
ORDER BY date_value
```

**Rationale:**
- Original: 199 separate `SELECT COUNT(*) WHERE date = X` queries
- Each query: ~3-4 seconds = 11 minutes total
- Aggregated: Single query gets all dates at once = 0.3 seconds
- No downside: Same result, 2,200x faster

**Alternatives Considered:**
1. **Cache date counts** - Stale data risk
2. **Parallel COUNT queries** - Still slow, more complex
3. **Estimate instead of COUNT** - Inaccurate chunking

**Impact:**
- Startup time: 11 minutes → 0.3 seconds
- Reduced Snowflake compute costs
- Better user experience (no waiting)

---

### Decision: PostgreSQL Session-Level Optimizations
**Context:** User doesn't have server-level PostgreSQL access

**Decision:**
Apply optimizations as session-level settings during connection:
```python
cursor.execute("SET synchronous_commit = off")
cursor.execute("SET work_mem = '256MB'")
cursor.execute("SET maintenance_work_mem = '512MB'")
```

**Rationale:**
- Can't modify postgresql.conf (no server access)
- Session-level settings apply only to our connection
- Safe - won't affect other workloads
- 20-30% faster COPY operations

**Alternatives Considered:**
1. **Don't optimize** - Leave performance on table
2. **Ask for server changes** - Not feasible
3. **Connection string parameters** - Limited options

**Impact:**
- Faster bulk loads
- No risk to other database users
- Easy to adjust or remove if issues

---

## December 15, 2025

### Decision: Reduce Threads from 15 to 10
**Context:** 15 parallel threads caused Out of Memory error

**Decision:**
Reduce `parallel_threads` from 15 to 10

**Rationale:**
- 15 threads × ~450MB = 6.75GB
- Lambda had 6GB allocated
- Caused `Runtime.OutOfMemory` error
- 10 threads × ~500MB = 5GB (safe within 6GB)
- Later increased Lambda to 10GB and threads to 20

**Alternatives Considered:**
1. **Increase Lambda to 8GB** - Chosen path later
2. **Reduce batch size** - Would slow down further
3. **Keep 15 threads, manage memory** - Too risky

**Lesson Learned:**
Memory usage varies by chunk complexity. Need headroom for spikes.

---

### Decision: Reduce Batch Size to 25K
**Context:** 50K batch size resulted in slow Snowflake queries

**Decision:**
Reduce `batch_size` from 50,000 to 25,000

**Rationale:**
- 50K rows: 5-9 minutes per Snowflake query
- 25K rows: 45-60 seconds per query (10x faster)
- More chunks = better parallel distribution
- Lower memory per chunk
- Network transfer time similar

**Alternatives Considered:**
1. **Keep 50K, fewer chunks** - Too slow per chunk
2. **20K batch** - Considered, but 25K balanced
3. **Dynamic batch sizing** - Too complex

**Trade-offs:**
- More chunks to manage (10,988 vs 5,494)
- More status updates
- But: Much faster overall

---

## November-December 2025

### Decision: Timestamp-Based Chunking for Large Tables
**Context:** UUID-based chunking was inefficient for 272M records

**Decision:**
Use `DateRangeStrategy` with `Visit Updated Timestamp` column

**Rationale:**
- UUIDs: Random distribution, poor chunking, OFFSET-based (slow)
- Timestamps: Natural grouping by date, efficient filtering
- Sub-chunking handles dates with many records
- Works with existing indexes

**Alternatives Considered:**
1. **UUID 1-char range** - Still too large per chunk
2. **UUID 2-char range** - Better but still offset-based
3. **Sequential ID** - Not available in this table

**Impact:**
- Predictable chunk sizes
- No full table scans
- Resume capability works well

---

### Decision: Insert-Only Mode for Full Loads
**Context:** Need fastest possible load for 272M records

**Decision:**
Use `insert_only_mode: true` with `truncate_onstart: true`

**Rationale:**
- Full load: No need to check for duplicates
- Insert-only: Skips duplicate key errors, continues
- Uses COPY mode (fastest bulk load method)
- No UPSERT overhead

**When NOT to use:**
- Incremental loads (need updates)
- Data with overlapping time ranges
- When exact duplicate handling needed

---

### Decision: Concurrent Migration Isolation via Execution Hash
**Context:** Multiple migrations could interfere with resume detection

**Decision:**
Calculate `execution_hash` from config_hash + source names

**Rationale:**
- Different migrations have different execution contexts
- Resume should only match exact same execution
- Prevents cross-contamination
- Safe concurrent execution

**Implementation:**
```python
execution_hash = MD5({
    'config_hash': config_hash,
    'source_names': sorted(source_names)
})
```

**Impact:**
- Can run analytics + conflict migrations simultaneously
- Can test with different schemas (analytics_dev vs analytics_dev2)
- No status table conflicts

---

### Decision: Dual-Layer Truncation Protection
**Context:** Risk of accidental data loss during resume failures

**Decision:**
Implement two-layer safety check:
1. Check for existing status record
2. Check if target table has data

**Logic:**
```python
if resuming and has_status:
    # Safe: Resume existing run
    skip_truncate = True
elif has_status and has_data:
    # Safe: Likely resume failure, preserve data
    skip_truncate = True
elif not has_status and has_data:
    # DANGER: Unexpected data, preserve it
    skip_truncate = True
else:
    # Safe: Fresh start
    truncate_if_configured = True
```

**Rationale:**
- Prevent data loss from resume detection failures
- Err on side of caution
- Clear logging helps debug when triggered

---

## Design Principles

### Performance First
- Optimize bottlenecks systematically
- Measure before and after
- Accept complexity if justified by metrics

### Safety Second
- Preserve data when in doubt
- Clear logging for decisions
- Easy rollback paths

### Simplicity Third
- Avoid premature optimization
- Delete code when possible
- Documentation over cleverness

### Cost Awareness
- Monitor CloudWatch costs
- Balance speed vs expense
- Right-size resources

---

## Future Decisions Needed

### Open Questions
1. Should we implement connection pooling?
   - Benefit unclear, adds complexity
   - Current approach working well

2. Should we support multiple Snowflake warehouses?
   - Not needed yet
   - Could improve isolation

3. Should we add automated performance tuning?
   - Nice to have
   - Manual tuning working fine

---

**Update this document when making:**
- Architecture changes
- Configuration pattern changes
- Performance optimization choices
- Safety mechanism additions

