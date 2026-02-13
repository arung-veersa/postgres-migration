# Task 02 Conflict Updater - ECS Container

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
Task 02 detects and updates visit conflicts between Snowflake (analytics source) and PostgreSQL (operational database). The application runs as a Docker container on AWS ECS/Fargate, providing unlimited execution time compared to the original Lambda architecture.

### Key Features
- **Streaming Processing**: Handles 150K+ conflict records in batches of 5,000
- **7 Conflict Rules**: Time overlaps, distance-based impossible travel detection
- **Smart Change Detection**: Only updates rows with actual changes (~98% reduction)
- **Comprehensive Mode**: Asymmetric join detects Delta-vs-All conflicts (production default)
- **Pair-Precise Stale Cleanup**: Accurately resolves stale conflicts using exact (VisitDate, SSN) pairs from Snowflake
- **Preserve Overrides**: Maintains manual status flags (W=Whitelisted, I=Ignored) and conflict IDs
- **Flexible Execution**: Comma-separated ACTION env var for sequential task execution

### Performance (Asymmetric Mode with Stale Cleanup)
- **Execution Time**: ~8 minutes (36-hour lookback)
- **Memory Usage**: ~455 MB peak
- **Throughput**: ~154K conflict records processed per run
- **Updates**: 200-300 rows per execution (with change detection)
- **Stale Cleanup**: Pair-precise scoping, batched updates in 100K chunks

---

## Architecture

### Component Structure

```
+----------------------------+
|  AWS ECS/Fargate           |
|  (Docker / Python 3.11)    |
+--------+-------------------+
         |
    +----+----+
    |         |
+---v---+ +---v---------+
|Snowfl | |PostgreSQL    |
|ake    | |              |
|(Read) | |(Read/Write)  |
+-------+ +--------------+
```

### File Organization
```
Scripts13/tasks/
├── config/
│   ├── config.json                          # Runtime configuration (env var substitution)
│   └── settings.py                          # Configuration loader with JSON-safe env escaping
├── lib/
│   ├── connections.py                       # SnowflakeManager, PostgresManager
│   ├── query_builder.py                     # SQL generation (v3 temp table approach)
│   ├── conflict_processor.py                # Core business logic (streaming, batching, stale cleanup)
│   └── utils.py                             # Logging, formatting utilities
├── scripts/
│   ├── __init__.py                          # Package metadata (version 2.0.0)
│   ├── main.py                              # ECS container entry point
│   └── actions/                             # Pipeline action modules
│       ├── __init__.py                      # Naming convention docs
│       ├── task00_preflight.py              # Pre-run validation, pg_cron, InProgressFlag
│       ├── task01_copy_to_staging.py        # PPR sync + staging populate
│       ├── task02_00_conflict_update.py     # v3 conflict detection UPDATE pipeline
│       ├── task99_postflight.py             # Cleanup, VACUUM, email, summary
│       ├── validate_config.py              # Standalone config validation
│       └── test_connections.py             # Standalone connectivity test
├── sql/                                     # Snowflake SQL templates
│   ├── sf_task01_dim_payer_provider.sql     # Task 01: Payer-provider dimension query
│   ├── sf_task02_00_step1_delta_keys.sql    # Step 1: Delta keys extraction
│   ├── sf_task02_00_step2_base_visits.sql   # Step 2: Base visits materialization
│   ├── sf_task02_00_step3_final_query.sql   # Step 3: Conflict detection self-join
│   └── pg_fetch_*.sql                       # PostgreSQL reference data queries
├── deploy/
│   ├── build-and-push-ecr.ps1               # Interactive build/push/run PowerShell script
│   └── ecs-task-definition.json             # ECS Fargate task definition template
├── docs/
│   ├── README.md                            # This documentation
│   ├── ECS_DEPLOYMENT_GUIDE.md              # Step-by-step deployment via AWS Console
│   ├── TESTING_GUIDE.md                     # Unit test documentation
│   └── TROUBLESHOOTING.md                   # Bug fix history and lessons learned
├── tests/
│   ├── sql/                                 # Generated standalone test queries
│   ├── generate_v3_test_queries.py          # Regenerates test queries from templates
│   └── test_comprehensive.py                # Unit tests (81 pytest tests)
├── Dockerfile                               # Container image definition
├── .dockerignore                            # Build context exclusions
├── .env.example                             # Environment variable template
├── .gitignore                               # Git exclusions
└── requirements.txt                         # Python dependencies
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

**Problem Solved**: The v2 CTE approach timed out in asymmetric mode because CTEs are re-evaluated during query execution, causing the full 2-year dataset to be processed multiple times.

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

1. **Two-Part base_visits (CREATE + INSERT, not UNION ALL)**: Same SQL template formatted twice with different placeholders. Two separate statements give per-leg timing in logs and avoid `OR + IN` subquery that prevented partition pruning.

2. **`is_delta` Flag**: In asymmetric mode, constrains the Step 3 self-join to `V1.is_delta = 1`, avoiding the all-vs-all join on ~9.6M rows.

3. **Streaming Cursor with Batch Processing**: Step 3 results streamed from Snowflake via server-side cursor. Rows accumulated into batches of 5,000. Each batch: fetch existing PG records, detect changes, UPDATE dirty rows, commit. Single-threaded by design.

4. **Pair-Precise Stale Cleanup**: Streams actual `(VisitDate, SSN)` pairs to PostgreSQL via chunked COPY (100K rows/chunk), eliminating the cross-product problem.

5. **UUID Type Matching**: `_tmp_seen_conflicts` uses UUID columns to match `conflictvisitmaps` index types.

6. **Graceful Shutdown**: SIGTERM handler enables clean connection teardown when ECS stops the task.

#### Configuration Toggles

- `enable_asymmetric_join: true` (default): Comprehensive Delta-vs-All detection
- `enable_asymmetric_join: false`: Delta-only (~70K rows, faster but less comprehensive)
- `enable_stale_cleanup: true` (default): Pair-precise stale resolution
- `enable_stale_cleanup: false`: Skips stale cleanup

#### Code References

- **Templates**: `sql/sf_task02_00_step1_delta_keys.sql`, `sf_task02_00_step2_base_visits.sql`, `sf_task02_00_step3_final_query.sql`
- **Builder**: `lib/query_builder.py::build_conflict_detection_query_v3()`
- **Processor**: `lib/conflict_processor.py::stream_and_process_conflicts_v3()`
- **Test Queries**: `tests/sql/sf_task02_00-sym-defaults.sql`, `tests/sql/sf_task02_00-asym-defaults.sql`

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
- Sets `StatusFlag='U'` (Updated) unless current value is 'W' or 'I'

### Stale Cleanup Logic

**What is a stale conflict?** A conflict record in PostgreSQL that was within the scope of Snowflake's detection run (matching VisitDate + SSN pair) but was NOT re-detected in the current run.

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
    "skip_unchanged_records": true,
    "enable_asymmetric_join": true,
    "enable_stale_cleanup": true
  }
}
```

Environment variables are substituted at load time via `settings.py`. Values are JSON-escaped to handle multi-line strings (e.g., RSA private keys with embedded newlines).

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

Parameters can be overridden via environment variables: `LOOKBACK_HOURS`, `BATCH_SIZE`, etc.

### ACTION Environment Variable

| Value | Behavior |
|-------|----------|
| (empty/unset) | Runs full pipeline: `task00_preflight` -> `task01_copy_to_staging` -> `task02_00_conflict_update` -> `task99_postflight` |
| `task00_preflight` | Pre-run validation, disable pg_cron, set InProgressFlag |
| `task01_copy_to_staging` | Sync PPR from Snowflake dims, populate staging table |
| `task02_00_conflict_update` | Run the conflict detection and update pipeline |
| `task99_postflight` | Post-run cleanup, VACUUM/ANALYZE, email summary |
| `validate_config` | Print and validate config only (standalone) |
| `test_connections` | Test Snowflake and PostgreSQL connectivity (standalone) |
| `validate_config,test_connections` | Comma-separated: run actions sequentially |

---

## Deployment

See **[ECS_DEPLOYMENT_GUIDE.md](ECS_DEPLOYMENT_GUIDE.md)** for full step-by-step instructions via the AWS Console.

### Quick Reference (PowerShell)

```powershell
cd Scripts13\tasks\deploy
.\build-and-push-ecr.ps1
```

The interactive script handles: SSO login, Docker build, ECR push, and optional ECS task execution.

### Prerequisites

- **Docker Desktop** installed and running
- **AWS CLI v2** configured with SSO profile
- **AWS Resources**: ECS cluster, ECR repository, CloudWatch log group, IAM execution role
- **Python 3.11** (runtime inside container)

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
cd Scripts13/tasks
python -m pytest tests/ -v
```

**Test Coverage**: 81 pytest tests covering connection management, query building, change detection, update logic, column mapping, and utility functions.

### Integration Testing

Run individual actions via the deploy script or CLI:

```powershell
# Validate config only
.\build-and-push-ecr.ps1   # Select option 2

# Test connections only
.\build-and-push-ecr.ps1   # Select option 3

# Full pipeline
.\build-and-push-ecr.ps1   # Select option 1
```

Or via AWS Console: ECS > Clusters > conflict-batch-1 > Run new task.

### Expected Output (CloudWatch Logs)

```
CONFLICT MANAGEMENT - ECS CONTAINER
Actions: task00_preflight -> task01_copy_to_staging -> task02_00_conflict_update -> task99_postflight
...
EXECUTION SUMMARY
  task00_preflight: success (4.82s)
  task01_copy_to_staging: success (287.43s)
  task02_00_conflict_update: completed (509.23s)
  task99_postflight: success (18.61s)
Overall: completed
Total duration: 13m 39s
```

---

## Troubleshooting

See **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** for the complete bug fix history and lessons learned.

### Common Issues

#### 1. Container Startup: JSON Parse Error
**Symptom**: `json.decoder.JSONDecodeError: Invalid control character`
**Cause**: Multi-line RSA private key in environment variable not JSON-escaped.
**Fix**: `settings.py` now JSON-escapes all env var values before parsing config.json.

#### 2. CloudWatch Logs Not Appearing
**Symptom**: `ResourceNotFoundException: The specified log group does not exist`
**Fix**: Create the log group `/ecs/task02-conflict-updater` in CloudWatch before running.

#### 3. AWS CLI Authentication
**Symptom**: `UnrecognizedClientException` or `InvalidClientTokenId`
**Fix**: Always include `--profile <YOUR_PROFILE>` with every `aws` command.

---

## Future Enhancements

### 1. INSERT Logic for New Conflicts
Currently, ~99K new conflicts detected but not present in PostgreSQL are counted but not inserted. Implementing INSERT logic would complete the full sync cycle.

### 2. UpdateFlag Cleanup
Records with `UpdateFlag` set could be cleaned up in a separate maintenance task.

### 3. Conflicts Table Aggregation
The parent `conflicts` table updates (aggregating from `conflictvisitmaps`) are planned as a subsequent step.

### 4. AWS Secrets Manager
Migrate sensitive environment variables to Secrets Manager for production security.

---

**Version**: 2.1 (ECS Container)
**Last Updated**: 2026-02-12
**Status**: Production Ready (Asymmetric Mode + Stale Cleanup + Staging Pipeline)
**History**: Migrated from Scripts12 (AWS Lambda) to Scripts13 (AWS ECS/Fargate)
