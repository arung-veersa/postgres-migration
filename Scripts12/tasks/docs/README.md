# Task 02 Conflict Updater - Complete Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Implementation Details](#implementation-details)
4. [Configuration](#configuration)
5. [Deployment](#deployment)
6. [Testing](#testing)
7. [Troubleshooting](#troubleshooting)

---

## Overview

### Purpose
Migrate Task 02 (Conflict Detection and Update) from Snowflake stored procedures to AWS Lambda, maintaining bi-directional updates between Snowflake (analytics) and PostgreSQL (operational database).

### Key Features
- **Streaming Processing**: Handles 150K+ conflict records in batches of 5,000
- **7 Conflict Rules**: Time overlaps, distance-based impossible travel detection
- **Smart Change Detection**: Only updates rows with actual changes (~98% reduction)
- **Dual Modes**: Symmetric (fast) vs Asymmetric (comprehensive, production-ready)
- **Pair-Precise Stale Cleanup**: Accurately resolves stale conflicts using exact (VisitDate, SSN) pairs from Snowflake
- **Preserve Overrides**: Maintains manual status flags (W=Whitelisted, I=Ignored) and conflict IDs

### Performance (Asymmetric Mode with Stale Cleanup)
- **Execution Time**: ~6 minutes (36-hour lookback)
- **Memory Usage**: ~140 MB
- **Throughput**: ~150K conflict records processed per run
- **Updates**: 200-300 rows per execution (with change detection)
- **Stale Cleanup**: Pair-precise scoping, batched updates in 100K chunks

---

## Architecture

### Component Structure

```
+-------------------+
|  AWS Lambda       |
|  (Python 3.11)    |
+--------+----------+
         |
    +----+----+
    |         |
+---v---+ +--v--------+
|Snowfl | |PostgreSQL  |
|ake    | |            |
|(Read) | |(Read/      |
|       | |Write)      |
+-------+ +------------+
```

### File Organization
```
Scripts12/tasks/
├── config/
│   └── config.json                          # All configuration settings
├── lib/
│   ├── connections.py                       # SnowflakeManager, PostgresManager
│   ├── query_builder.py                     # SQL generation (v3 only)
│   ├── conflict_processor.py                # Core business logic (v3 only)
│   └── utils.py                             # Logging, formatting utilities
├── scripts/
│   └── lambda_handler.py                    # Entry point, orchestration
├── sql/                                     # Production SQL templates (loaded by Lambda)
│   ├── sf_task02_v3_step1_delta_keys.sql    # Step 1: Delta keys extraction
│   ├── sf_task02_v3_step2_base_visits.sql   # Step 2: Base visits materialization
│   ├── sf_task02_v3_step3_final_query.sql   # Step 3: Conflict detection self-join
│   └── pg_fetch_*.sql                       # Reference data queries
├── docs/
│   └── README.md                            # This documentation
└── tests/
    ├── sql/                                 # Generated test queries (manual Snowflake testing)
    │   ├── sf_task02_v3-sym-defaults.sql    # Symmetric mode standalone query
    │   └── sf_task02_v3-asym-defaults.sql   # Asymmetric mode standalone query
    ├── generate_v3_test_queries.py          # Regenerates test queries from templates
    ├── test_comprehensive.py                # Unit tests
    └── test_conditional_logic.py            # Feature flag tests
```

### Data Flow

1. **Fetch Reference Data** (PostgreSQL)
   - Excluded agencies (3 items)
   - Excluded SSNs (~7K items, loaded via batched INSERT to Snowflake temp table)
   - MPH lookup (4 ranges for distance calculations)
   - Settings (ExtraDistancePer multiplier)

2. **Build & Execute Queries** (Snowflake - v3 Temp Table Approach)
   - Step 1: Create `delta_keys` temp table (distinct VisitDate, SSN pairs from recent updates)
   - Step 2: Create `base_visits` temp table (two-part: delta rows + related non-delta rows)
   - Step 2d: Stream (VisitDate, SSN) pairs from `delta_keys` to PostgreSQL `_tmp_delta_pairs`
   - Step 3: Execute conflict detection self-join on `base_visits`, stream results

3. **Process Conflicts** (Python)
   - For each conflict pair:
     - Check if exists in PostgreSQL
     - Determine if data changed (if `skip_unchanged_records=true`)
     - Build UPDATE statement with conditional flag logic
   - Commit every 5,000 rows
   - Track all seen (VisitID, ConVisitID) pairs for stale cleanup

4. **Resolve Stale Conflicts** (PostgreSQL - Pair-Precise Seen-Based Anti-Join)
   - Phase 1: JOIN `conflictvisitmaps` with `_tmp_delta_pairs` to scope, anti-join with `_tmp_seen_conflicts` to identify stale records
   - Phase 2: UPDATE stale records in batches of 100K (StatusFlag='R', UpdatedDate=CURRENT_TIMESTAMP)
   - Preserves W (Whitelisted) and I (Ignored) statuses

---

## Implementation Details

### v3 Temp Table Approach

**Problem Solved**: The v2 CTE approach timed out (>15 minutes) in asymmetric mode because CTEs are re-evaluated during query execution, causing the full 2-year dataset to be processed multiple times.

**Solution**: v3 uses Snowflake temporary tables to materialize intermediate results, with a two-step `base_visits` creation and an `is_delta` flag for efficient self-join filtering.

#### Execution Flow

```
V3 EXECUTION FLOW:

STEP 1: Create delta_keys temp table
+-------------------------------------------+
| SELECT DISTINCT VisitDate, SSN            |
| FROM FACTVISITCALLPERFORMANCE_CR          |
| WHERE Updated in last N hours             |
+-------------------------------------------+
        | Result: ~5K-12M unique (date, SSN) pairs
        v
STEP 2: Create base_visits temp table (two-part)
+-------------------------------------------+
| Part A: Delta rows only (fast, ~70K rows) |
|   CREATE TABLE base_visits AS             |
|   SELECT *, 1 as is_delta FROM ...        |
|   WHERE timestamp in last N hours         |
+-------------------------------------------+
| Part B (asymmetric only):                 |
|   INSERT INTO base_visits                 |
|   SELECT *, 0 as is_delta FROM ...        |
|   INNER JOIN delta_keys ON (date, SSN)    |
|   WHERE timestamp NOT in last N hours     |
+-------------------------------------------+
        | Symmetric: ~70K rows
        | Asymmetric: up to ~9.6M rows (materialized)
        v
STEP 2d: Stream delta pairs to Postgres
+-------------------------------------------+
| SELECT visit_date, ssn FROM delta_keys    |
| -> Chunked COPY to _tmp_delta_pairs (PG)  |
| -> Composite index (ssn, visit_date)      |
+-------------------------------------------+
        v
STEP 3: Final conflict detection
+-------------------------------------------+
| SELECT V1.*, V2.*                         |
| FROM base_visits V1                       |
| JOIN base_visits V2                       |
|   ON V1.VisitDate = V2.VisitDate          |
|  AND V1.SSN = V2.SSN                      |
|  AND V1.VisitID != V2.VisitID             |
|  AND V1.is_delta = 1  (asymmetric only)   |
| WHERE <7 conflict rules>                  |
+-------------------------------------------+
        v
STEP 4: Pair-precise stale cleanup
+-------------------------------------------+
| Phase 1: Identify stale records           |
|   JOIN conflictvisitmaps WITH              |
|   _tmp_delta_pairs ON (SSN, VisitDate)    |
|   ANTI-JOIN _tmp_seen_conflicts            |
| Phase 2: Batched UPDATE (100K chunks)     |
|   SET StatusFlag='R', UpdatedDate=NOW()   |
+-------------------------------------------+
```

#### Key Design Decisions

1. **Two-Part base_visits (CREATE + INSERT, not UNION ALL)**: Part A creates the table with delta rows (fast, uses partition pruning). Part B inserts related non-delta rows via `INNER JOIN delta_keys`. Both use the same SQL template (`sf_task02_v3_step2_base_visits.sql`) formatted with different placeholders (`TABLE_CLAUSE`, `is_delta_value`, `DELTA_KEYS_JOIN`, `TIMESTAMP_CONDITION`). The two-statement approach was chosen over UNION ALL because:
   - Each statement gets separate timing in logs, making it easy to identify which leg is slow
   - If Part B times out, Part A's data is already materialized in the temp table
   - The original single-query approach (`OR + IN subquery`) prevented Snowflake partition pruning and caused timeouts
   - Both parts share 120+ columns and 11 dimension JOINs -- this duplication is unavoidable since Step 3 needs all columns resolved before the self-join

2. **`is_delta` Flag**: In asymmetric mode, constrains the Step 3 self-join to `V1.is_delta = 1`, avoiding the all-vs-all join on ~9.6M rows while still detecting Delta-vs-All and All-vs-Delta conflicts.

3. **Streaming Cursor with Batch Processing**: Step 3 results are streamed from Snowflake via a server-side cursor (not fetched into memory all at once). Rows are accumulated into batches of `batch_size` (default 5,000). Each batch is processed as follows:
   - Fetch existing `conflictvisitmaps` records from PostgreSQL for the batch's VisitIDs
   - Match Snowflake rows to existing PostgreSQL records by `(VisitID, ConVisitID)`
   - Apply change detection (7 conditional flags + 40 business columns) to skip unchanged records
   - Build and execute UPDATE statements for dirty records only
   - Commit after each batch (ensures progress is saved even if Lambda times out mid-run)
   - Track all seen `(VisitID, ConVisitID)` pairs in memory for stale cleanup
   - Single-threaded by design: the bottleneck is Snowflake query time (Steps 1-2), not Python batch processing

4. **Pair-Precise Stale Cleanup**: Streams actual `(VisitDate, SSN)` pairs from `delta_keys` to PostgreSQL via chunked COPY (100K rows/chunk). This eliminates the "cross-product problem" where separate DISTINCT SSN and DISTINCT date lists would create millions of false stale candidates (e.g., 507K SSNs x 597 dates = 302M combos vs ~12M actual pairs).

5. **UUID Type Matching**: `_tmp_seen_conflicts` uses UUID columns to match `conflictvisitmaps` index types, enabling proper index usage in the anti-join.

6. **Conflict Flag Simplification**: The 7 conflict rules in `conflicts_with_flags` CTE do not repeat the `ProviderID != ConProviderID` check (already enforced by the `conflict_pairs` JOIN condition). Equality checks use direct column comparison instead of CONCAT-based string comparison.

#### Configuration Toggle

- `enable_asymmetric_join: true` (default): Processes delta + all related visits for comprehensive detection
- `enable_asymmetric_join: false`: Only processes delta visits (~70K rows, faster but may miss some conflicts)
- `enable_stale_cleanup: true` (default): Resolves stale conflicts via pair-precise seen-based anti-join
- `enable_stale_cleanup: false`: Skips stale cleanup (useful for testing)

#### Code References

- **Templates**: `sql/sf_task02_v3_step1_delta_keys.sql`, `sf_task02_v3_step2_base_visits.sql`, `sf_task02_v3_step3_final_query.sql`
- **Builder**: `lib/query_builder.py::build_conflict_detection_query_v3()`
- **Processor**: `lib/conflict_processor.py::stream_and_process_conflicts_v3()`
- **Test Queries**: `tests/sql/sf_task02_v3-sym-defaults.sql`, `tests/sql/sf_task02_v3-asym-defaults.sql`
- **Test Generator**: `tests/generate_v3_test_queries.py`

---

### Conflict Detection Rules

1. **Same Scheduled Time** - Both unstarted, identical scheduled times, different providers
2. **Same Visit Time** - Both completed, identical visit times, different providers
3. **Scheduled = Visit Time** - One unstarted, one completed, scheduled matches visit time
4. **Scheduled Overlap** - Both unstarted, scheduled times overlap
5. **Visit Overlap** - Both completed, visit times overlap
6. **Scheduled Overlaps Visit** - One unstarted, one completed, times overlap
7. **Distance Flag** - Impossible travel (ETA > time difference between visits)

### Change Detection Logic

When `skip_unchanged_records: true`, checks if any of these changed:
- **7 Conflict Flags** (Y/N values, conditional: only update if existing='N')
- **Business Columns** (40+ columns): Provider, Visit, Caregiver, Office, Patient, Payer, ServiceCode IDs and names, Times, Dates, Rates, Status fields

If no changes detected, the UPDATE statement is skipped entirely.

### Update Logic

**Preserves**:
- Existing `CONFLICTID` (never overwrite)
- `StatusFlag` if 'W' (Whitelisted) or 'I' (Ignored)

**Updates**:
- All conflict flag columns (7 rules, conditional: only N->Y)
- All business data columns
- Sets `StatusFlag='U'` (Updated) unless current value is 'W' (Whitelisted) or 'I' (Ignored)

### Stale Cleanup Logic

**What is a stale conflict?** A conflict record in PostgreSQL that was within the scope of Snowflake's detection run (matching VisitDate + SSN pair) but was NOT re-detected in the current run. This means the conflict no longer exists in the source data.

**How it works (Pair-Precise Seen-Based Anti-Join):**
1. During Step 2d, actual `(VisitDate, SSN)` pairs from `delta_keys` are streamed to PostgreSQL `_tmp_delta_pairs`
2. During Step 3 streaming, all detected `(VisitID, ConVisitID)` pairs are collected into `_tmp_seen_conflicts`
3. Phase 1 joins `conflictvisitmaps` with `_tmp_delta_pairs` (scope) and anti-joins with `_tmp_seen_conflicts` (seen) to identify stale records
4. Phase 2 updates stale records: `StatusFlag='R'`, `UpdatedDate=CURRENT_TIMESTAMP`
5. Records with `StatusFlag` in ('W', 'I', 'R') are excluded from cleanup

---

## Configuration

### config.json Structure

```json
{
  "snowflake": {
    "account": "${SNOWFLAKE_ACCOUNT}",
    "user": "${SNOWFLAKE_USER}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "rsa_key": "${SNOWFLAKE_PRIVATE_KEY}",
    "analytics_database": "ANALYTICS",
    "analytics_schema": "BI"
  },
  "postgres": {
    "host": "${POSTGRES_HOST}",
    "port": 5432,
    "user": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}",
    "conflict_database": "conflict_management",
    "conflict_schema": "conflict_dev"
  },
  "task02_parameters": {
    "lookback_hours": 36,
    "lookback_years": 2,
    "lookforward_days": 45,
    "batch_size": 5000,
    "timeout_buffer_seconds": 90,
    "max_retry_attempts": 3,
    "skip_unchanged_records": true,
    "enable_asymmetric_join": true,
    "enable_stale_cleanup": true
  }
}
```

### Feature Flags

| Flag | Default | Purpose | Status |
|------|---------|---------|--------|
| `skip_unchanged_records` | `true` | Only update rows with actual changes | Production |
| `enable_asymmetric_join` | `true` | Comprehensive conflict detection (Delta vs All) | Production |
| `enable_stale_cleanup` | `true` | Pair-precise stale conflict resolution | Production |

### Parameter Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lookback_hours` | 36 | Hours to look back for recently updated visits |
| `lookback_years` | 2 | Years of visit history for conflict detection window |
| `lookforward_days` | 45 | Days ahead for future visit conflict detection |
| `batch_size` | 5000 | Number of records per processing batch |
| `timeout_buffer_seconds` | 90 | Seconds before Lambda timeout to stop processing |
| `max_retry_attempts` | 3 | Maximum retry attempts for transient errors |

---

## Deployment

### Prerequisites

**Lambda Configuration**:
- Runtime: Python 3.11
- Memory: 10240 MB (10 GB)
- Timeout: 900 seconds (15 minutes)
- Architecture: x86_64

**Lambda Layers**:
1. psycopg2 layer (PostgreSQL driver)
2. Snowflake connector layer

**Environment Variables**:
```bash
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_PRIVATE_KEY=your-base64-encoded-key
POSTGRES_HOST=your-pg-host
POSTGRES_USER=your-pg-user
POSTGRES_PASSWORD=your-pg-password
```

### Deployment Steps

1. **Package Code**:
```bash
cd Scripts12/tasks
zip -r task02-code.zip config/ lib/ scripts/ sql/ -x "*.pyc" -x "__pycache__/*"
```

2. **Upload to Lambda**:
```bash
aws lambda update-function-code \
  --function-name task02-conflict-updater \
  --zip-file fileb://task02-code.zip
```

3. **Test Invocation**:
```bash
aws lambda invoke \
  --function-name task02-conflict-updater \
  --payload '{"action":"task02_00_run_conflict_update"}' \
  response.json
```

### PostgreSQL Indexes (Required)

```sql
-- Critical for batch lookup performance
CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_visitid
  ON conflict_dev.conflictvisitmaps ("VisitID");

-- For stale cleanup: pair-precise scoping via (VisitDate, SSN) JOIN
CREATE INDEX IF NOT EXISTS idx_cvm_visitdate_ssn
  ON conflict_dev.conflictvisitmaps ("VisitDate", "SSN")
  WHERE "CONFLICTID" IS NOT NULL
    AND "InserviceStartDate" IS NULL AND "InserviceEndDate" IS NULL
    AND "PTOStartDate" IS NULL AND "PTOEndDate" IS NULL
    AND "ConInserviceStartDate" IS NULL AND "ConInserviceEndDate" IS NULL
    AND "ConPTOStartDate" IS NULL AND "ConPTOEndDate" IS NULL;
```

---

## Testing

### Unit Tests

```bash
cd Scripts12/tasks
python -m pytest tests/ -v
```

**Test Coverage**:
- Connection management
- Query building (symmetric/asymmetric modes)
- Change detection logic
- Update statement generation
- Column name mapping

### Integration Testing

**Via AWS Console**:
1. Navigate to Lambda function
2. Test tab -> Configure test event
3. Use payload: `{"action": "task02_00_run_conflict_update"}`
4. Execute and review CloudWatch logs

**Via CLI**:
```bash
aws lambda invoke \
  --function-name task02-conflict-updater \
  --payload '{"action":"task02_00_run_conflict_update"}' \
  --log-type Tail \
  response.json

# View response
cat response.json | jq .
```

**Override Parameters**:
```bash
# Test with custom lookback hours
aws lambda invoke \
  --function-name task02-conflict-updater \
  --payload '{"action":"task02_00_run_conflict_update","lookback_hours":40}' \
  response.json
```

### Expected Output

```json
{
  "statusCode": 200,
  "body": {
    "status": "completed",
    "action": "task02_00_run_conflict_update",
    "statistics": {
      "rows_fetched": 156949,
      "rows_processed": 156949,
      "matched_in_postgres": 57483,
      "rows_updated": 32,
      "rows_skipped_no_changes": 41365,
      "new_conflicts": 99466,
      "skip_unchanged_records": true,
      "asymmetric_join_enabled": true,
      "stale_cleanup_enabled": true,
      "stale_conflicts_resolved": 0,
      "delta_pairs_count": 12054514
    },
    "duration_seconds": 349.5,
    "parameters": {
      "lookback_hours": 36,
      "lookback_years": 2,
      "lookforward_days": 45,
      "batch_size": 5000,
      "skip_unchanged_records": true,
      "enable_asymmetric_join": true,
      "enable_stale_cleanup": true
    }
  }
}
```

---

## Troubleshooting

### Common Issues

#### 1. Lambda Timeout (900 seconds)

**Symptoms**: `Task timed out after 900.00 seconds`

**Causes**:
- `lookback_hours` set too high (>40 hours can cause large datasets)
- Snowflake warehouse too small for the data volume
- Network issues with Snowflake/PostgreSQL

**Solutions**:
- Reduce `lookback_hours` to 36 (default)
- Use a larger Snowflake warehouse for higher lookback windows
- Check Snowflake query execution time in Step 2 logs

#### 2. Zero Stale Conflicts

**Symptoms**: `stale_conflicts_resolved: 0` when expecting stale records

**Explanation**: This is expected behavior when all conflicts detected in previous runs are still valid. The pair-precise approach only marks records as stale if they are within the exact `(VisitDate, SSN)` scope of the current run but were not re-detected.

#### 3. SQL Compilation Errors

**Symptom**: `SQL compilation error: syntax error line X at position Y`

**Causes**:
- Placeholder replacement issues in SQL templates
- Column name mismatches
- Missing/invalid parameters

**Solutions**:
- Verify SQL template files have correct placeholder format
- Check CloudWatch logs for query structure debug messages
- Validate all placeholders are replaced (no `{...}` remaining)

#### 4. Column Name Mismatches

**Symptom**: `invalid identifier '"ColumnName"'` or `column "columnname" does not exist`

**Causes**:
- Snowflake uses case-sensitive quoted identifiers
- PostgreSQL columns are case-sensitive when quoted
- Schema differences between environments

**Solutions**:
- Use column name mapping in `conflict_processor.py`
- Verify schema matches between Snowflake and PostgreSQL
- Check `cursor.description` output in logs

#### 5. Zero Records Updated

**Symptom**: `rows_updated: 0` when expecting updates

**Possible Causes**:
- All records unchanged (change detection working correctly)
- No matching records in PostgreSQL (new conflicts not yet inserted)
- Filter criteria too restrictive

**Verification**:
```sql
-- Check PostgreSQL for expected conflicts
SELECT COUNT(*) 
FROM conflict_dev.conflictvisitmaps
WHERE "VisitDate" >= CURRENT_DATE - INTERVAL '7 days';

-- Check recent Snowflake updates
SELECT COUNT(*) 
FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR
WHERE "Visit Updated Timestamp" >= DATEADD(HOUR, -36, GETDATE());
```

### Debug Mode

Enable detailed logging by checking CloudWatch logs for:
- Step timing (Step 1, Step 2 Part A/B, Step 2d, Step 3, Step 4)
- Row counts at each stage (fetched, matched, updated, stale)
- Delta pair counts (distinct SSNs, distinct dates, total pairs)
- Phase 1/Phase 2 timing for stale cleanup
- Batch processing progress

### Performance Monitoring

**Key Metrics**:
- Step 2 duration (base_visits creation, should be <3 minutes)
- Step 2d duration (delta pairs streaming, depends on pair count)
- Step 3 streaming duration
- Step 4 Phase 1/Phase 2 timing
- Overall Lambda duration (should be <10 minutes for 36h lookback)

---

## Future Enhancements

### 1. INSERT Logic for New Conflicts
Currently, the ~99K new conflicts detected but not present in PostgreSQL are counted but not inserted. Implementing INSERT logic would complete the full sync cycle.

### 2. UpdateFlag Cleanup
Deferred for now. Records with `UpdateFlag` set could be cleaned up in a separate maintenance task.

### 3. Conflicts Table Updates
The parent `conflicts` table updates (aggregating from `conflictvisitmaps`) are planned as a subsequent step.

---

**Version**: 3.0
**Last Updated**: 2026-02-08
**Status**: Production Ready (Asymmetric Mode + Stale Cleanup)
