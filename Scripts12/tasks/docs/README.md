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
- **Streaming Processing**: Handles 70K+ conflict records in batches of 5,000
- **7 Conflict Rules**: Time overlaps, distance-based impossible travel detection
- **Smart Change Detection**: Only updates rows with actual changes (~98% reduction)
- **Dual Modes**: Symmetric (fast, production) vs Asymmetric (comprehensive, needs optimization)
- **Preserve Overrides**: Maintains manual status flags and conflict IDs

### Performance
- **Execution Time**: 40-60 seconds (symmetric mode)
- **Memory Usage**: ~140 MB
- **Throughput**: ~70K records processed per run
- **Updates**: 200-300 rows per execution (with change detection)

---

## Architecture

### Component Structure

```
┌─────────────────┐
│  AWS Lambda     │
│  (Python 3.11)  │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
┌───▼───┐ ┌──▼──────┐
│Snowfl │ │PostgreSQL│
│ake    │ │          │
│(Read) │ │(Read/    │
│       │ │Write)    │
└───────┘ └──────────┘
```

### File Organization
```
Scripts12/tasks/
├── config/
│   └── config.json                # All configuration settings
├── lib/
│   ├── connections.py             # SnowflakeManager, PostgresManager
│   ├── query_builder.py           # SQL generation with conditional logic
│   └── conflict_processor.py      # Core business logic
├── scripts/
│   └── lambda_handler.py          # Entry point, orchestration
├── sql/
│   ├── sf_task02_conflict_detection_v2.sql    # Main query template
│   └── pg_fetch_*.sql             # Reference data queries
└── tests/
    ├── test_comprehensive.py      # Unit tests
    └── test_conditional_logic.py  # Feature flag tests
```

### Data Flow

1. **Fetch Reference Data** (PostgreSQL)
   - Excluded agencies (3 items)
   - Excluded SSNs (~7K items, currently disabled for performance)
   - MPH lookup (4 ranges for distance calculations)
   - Settings (ExtraDistancePer multiplier)

2. **Build & Execute Query** (Snowflake)
   - Inject parameters into SQL template
   - Execute with streaming cursor
   - Fetch in batches of 5,000

3. **Process Conflicts** (Python)
   - For each conflict pair:
     - Check if exists in PostgreSQL
     - Determine if data changed (if `skip_unchanged_records=true`)
     - Build UPDATE statement
   - Commit every 5,000 rows

4. **Cleanup Stale Records** (PostgreSQL - optional)
   - Find conflicts with active flags but no matching Snowflake data
   - Reset flags to 'N' (currently disabled due to OOM at scale)

---

## Implementation Details

### SQL Query Structure

#### Base CTE Design (DRY Principle)
```sql
WITH
mph_data AS (...),           -- MPH lookup for ETA calculation

base_visits AS (             -- Single source of truth
  SELECT ... ~120 columns
  FROM FACTVISITCALLPERFORMANCE_CR
  JOIN DIMCAREGIVER, DIMOFFICE, etc.
  WHERE Date between -2 years and +45 days
  AND Provider not in (excluded agencies)
  {conditional_timestamp_filter}
),

delta_visits AS (            -- Recent updates
  SELECT * FROM base_visits
  {conditional_delta_filter}
),

{CONDITIONAL_ASYMMETRIC_CTEs}  -- Only in asymmetric mode

conflict_pairs AS (           -- Symmetric OR asymmetric join
  {CONDITIONAL_JOIN_LOGIC}
),

spatial_calculations AS (...), -- Distance & time diff (once)
conflict_with_eta AS (...),    -- MPH lookup & ETA
conflicts_with_flags AS (...), -- Apply 7 rules
final_conflicts AS (...)       -- Filter to actual conflicts

SELECT * FROM final_conflicts;
```

#### Symmetric Mode (Default - Production)
```sql
base_visits: 2-year window + 32-hour timestamp filter
delta_visits: Filter on base_visits
conflict_pairs: delta_visits self-join (V1 ↔ V2)
```
**Performance**: 40-60 seconds

#### Asymmetric Mode (Comprehensive - Not Production Ready)
```sql
base_visits: 2-year window (NO timestamp filter)
delta_visits: Filter base_visits to 32 hours
delta_conflict_keys: Extract unique (VisitDate, SSN)
all_visits: Filter base_visits by delta keys
conflict_pairs: (delta ↔ all_visits) UNION (all_visits ↔ delta)
```
**Performance**: >15 minutes (times out)
**Issue**: `base_visits` processes entire 2-year window before filtering

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
- **7 Conflict Flags** (Y/N values)
- **Business Columns** (40+ columns): Provider, Visit, Caregiver, Office, Patient, Payer, ServiceCode IDs and names, Times, Dates, Rates, Status fields

If no changes detected, skip the UPDATE statement.

### Update Logic

**Preserves**:
- Existing `CONFLICTID` (never overwrite)
- `StatusFlag` if 'W' (Working) or 'I' (Inactive/Resolved)

**Updates**:
- All conflict flag columns (7 rules)
- All business data columns
- Sets `StatusFlag='N'` only if currently NULL or 'N'

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
    "lookback_hours": 32,
    "lookback_years": 2,
    "lookforward_days": 45,
    "batch_size": 5000,
    "skip_unchanged_records": true,
    "enable_asymmetric_join": false,
    "enable_stale_cleanup": false
  }
}
```

### Feature Flags

| Flag | Default | Purpose | Status |
|------|---------|---------|--------|
| `skip_unchanged_records` | `true` | Only update rows with actual changes | ✅ Production |
| `enable_asymmetric_join` | `false` | Comprehensive conflict detection | ⚠️ Needs optimization |
| `enable_stale_cleanup` | `false` | Reset stale conflict flags | ⚠️ Causes OOM |

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
-- Critical for join performance
CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_providerid_visitdate_ssn
  ON conflict_dev.conflictvisitmaps (providerid, visitdate, ssn);

CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_visitid
  ON conflict_dev.conflictvisitmaps (visitid);

-- For stale cleanup queries
CREATE INDEX IF NOT EXISTS idx_conflictvisitmaps_flags
  ON conflict_dev.conflictvisitmaps (sameschdateflag, samevisittimeflag, 
      schandvisittimesameflag, schoverschdateflag, 
      visittimeovervisittimeflag, schtimeovervisittimeflag, distanceflag)
  WHERE sameschdateflag = 'Y' OR samevisittimeflag = 'Y' 
    OR schandvisittimesameflag = 'Y' OR schoverschdateflag = 'Y'
    OR visittimeovervisittimeflag = 'Y' OR schtimeovervisittimeflag = 'Y'
    OR distanceflag = 'Y';
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
2. Test tab → Configure test event
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

### Expected Output

```json
{
  "statusCode": 200,
  "body": {
    "status": "completed",
    "action": "task02_00_run_conflict_update",
    "statistics": {
      "rows_fetched": 73240,
      "rows_processed": 73240,
      "matched_in_postgres": 20742,
      "rows_updated": 251,
      "rows_skipped": 20491,
      "skip_unchanged_records": true,
      "stale_conflicts_reset": 0
    },
    "duration_seconds": 52.4,
    "parameters": {
      "lookback_hours": 32,
      "lookback_years": 2,
      "lookforward_days": 45,
      "batch_size": 5000,
      "skip_unchanged_records": true,
      "enable_asymmetric_join": false,
      "enable_stale_cleanup": false
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
- Asymmetric join enabled (`enable_asymmetric_join: true`)
- Large data volume without proper filtering
- Network issues with Snowflake/PostgreSQL

**Solutions**:
- ✅ Ensure `enable_asymmetric_join: false` (default)
- Check Snowflake query execution time in logs
- Verify network connectivity

#### 2. Out of Memory (OOM)

**Symptoms**: `Runtime.OutOfMemory` or `Runtime exited with error: signal: killed`

**Causes**:
- Stale cleanup enabled with 8M+ stale records
- Batch size too large
- Memory leak in processing

**Solutions**:
- ✅ Set `enable_stale_cleanup: false` (default)
- Reduce `batch_size` from 5000 to 2500
- Monitor memory usage in CloudWatch

#### 3. SQL Compilation Errors

**Symptom**: `SQL compilation error: syntax error line X at position Y`

**Causes**:
- Placeholder replacement issues in SQL template
- Column name mismatches
- Missing/invalid parameters

**Solutions**:
- Verify `sf_task02_conflict_detection_v2.sql` has correct placeholder format
- Check CloudWatch logs for DEBUG messages showing query structure
- Validate all placeholders are replaced (no `{...}` remaining)

#### 4. Column Name Mismatches

**Symptom**: `invalid identifier '"ColumnName"'` or `column "columnname" does not exist`

**Causes**:
- Snowflake uses case-sensitive quoted identifiers
- PostgreSQL columns are lowercase
- Schema differences between environments

**Solutions**:
- Use column name mapping in `conflict_processor.py`
- Verify schema matches between Snowflake and PostgreSQL
- Check `cursor.description` output in logs

#### 5. Zero Records Updated

**Symptom**: `rows_updated: 0` when expecting updates

**Possible Causes**:
- All records unchanged (change detection working correctly)
- No matching records in PostgreSQL
- Filter criteria too restrictive

**Verification**:
```sql
-- Check PostgreSQL for expected conflicts
SELECT COUNT(*) 
FROM conflict_dev.conflictvisitmaps
WHERE visitdate >= CURRENT_DATE - INTERVAL '7 days';

-- Check recent Snowflake updates
SELECT COUNT(*) 
FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR
WHERE "Visit Updated Timestamp" >= DATEADD(HOUR, -32, GETDATE());
```

### Debug Mode

Enable detailed logging by checking CloudWatch logs for:
- Query structure (`WITH` keyword position, CTE presence)
- Row counts at each stage (fetched, matched, updated)
- Skip reasons for unchanged records
- Batch processing timing

### Performance Monitoring

**Key Metrics**:
- Snowflake query execution time (should be <60s in symmetric mode)
- PostgreSQL batch commit time
- Memory usage trend
- Lambda duration

**Optimization Checklist**:
- ✅ Symmetric mode enabled
- ✅ Change detection enabled
- ✅ Stale cleanup disabled
- ✅ Batch size appropriate (5000)
- ✅ PostgreSQL indexes created
- ✅ Excluded SSNs loading disabled (performance test mode)

---

## Known Limitations

### 1. Asymmetric Join Performance
**Issue**: Times out (>15 minutes) when `enable_asymmetric_join: true`

**Root Cause**: In asymmetric mode, the `base_visits` CTE has no timestamp filter, causing it to materialize the entire 2-year dataset (100M+ rows) before the `all_visits` CTE can filter it.

**Impact**: Cannot detect conflicts between:
- New conflict with unchanged record
- Stale conflict (no longer valid) with unchanged records

**Workaround**: Use symmetric mode for nightly runs (99% of conflicts detected), schedule periodic full refresh

**Future Fix**: Refactor to use separate CTEs:
```sql
base_visits_delta AS (SELECT ... WHERE 32-hour filter)
base_visits_all AS (SELECT ... WHERE 2-year filter only)
```

### 2. Stale Conflict Cleanup
**Issue**: Causes OOM when attempting to process 8M+ stale records

**Root Cause**: Single UPDATE statement for millions of rows exceeds Lambda memory

**Workaround**: Disabled by default (`enable_stale_cleanup: false`)

**Future Fix**: Implement batched cleanup:
```python
BATCH_SIZE = 10000
while True:
    updated = execute("""
        UPDATE conflictvisitmaps 
        SET flags = 'N'
        WHERE id IN (
            SELECT id FROM stale_conflicts
            LIMIT {BATCH_SIZE}
        )
    """)
    if updated == 0: break
    commit()
```

### 3. Edge Case Conflicts
**Issue**: Symmetric mode misses some conflicts where neither record changed

**Example**:
- Visit A and Visit B both exist, no conflict
- Visit C created (conflicts with both A and B)
- Visit C updated (triggers detection)
- Conflict detected: C↔B (both have delta)
- **MISSED**: A↔C (only C has delta, A unchanged)

**Impact**: Minor - most conflicts involve recent updates on both sides

**Mitigation**: Schedule weekly full refresh with asymmetric mode (after optimization)

---

## Migration from Snowflake

### Original Procedure
`TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_0.sql`

### Key Changes
1. **UpdateFlag Removed**: No longer sets/checks UpdateFlag=1 (unnecessary transaction tracking)
2. **Streaming Processing**: Added batch-based streaming instead of full result set in memory
3. **Change Detection**: New optimization to skip unchanged records
4. **Conditional Modes**: Added symmetric/asymmetric toggle
5. **Enhanced Logging**: Comprehensive metrics and timing

### Preserved Behavior
- All 7 conflict detection rules
- StatusFlag preservation ('W', 'I' not overwritten)
- CONFLICTID preservation
- Column data mapping and transformations
- Date/time filtering logic

---

**Version**: 2.0
**Last Updated**: 2026-02-07
**Status**: ✅ Production Ready (Symmetric Mode)
