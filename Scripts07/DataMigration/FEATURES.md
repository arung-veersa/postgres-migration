# PostgreSQL Migration Tool - Features

Complete guide to the Snowflake-to-PostgreSQL migration tool with Lambda and Step Functions support.

---

## Table of Contents

1. [Core Features](#core-features)
2. [AWS Lambda & Step Functions](#aws-lambda--step-functions)
3. [Performance Optimizations](#performance-optimizations)
4. [Configuration](#configuration)
5. [Deployment](#deployment)

---

## Core Features

### 1. Resume Capability

**Purpose:** Automatically resume interrupted migrations from the point of failure.

**How it works:**
- Tracks migration progress in PostgreSQL status tables
- Detects incomplete runs with matching `config_hash` within time window (default: 12 hours)
- Reprocesses only failed/pending/in_progress chunks
- Skips already-completed chunks

**Resume Conditions:**
- Status: `running`, `partial`, or `failed` (not `completed`)
- Config hash matches current configuration
- Run age within time limit (default: 12 hours)

**Status Tables:**
- `migration_status.migration_runs` - Overall run tracking
- `migration_status.migration_table_status` - Per-table progress
- `migration_status.migration_chunk_status` - Granular chunk tracking

**CLI Usage:**
```bash
# Auto-resume (default)
python migrate.py

# Force fresh start
python migrate.py --no-resume

# Extend resume window
python migrate.py --resume-max-age 24

# Resume specific run
python migrate.py --resume-run-id <UUID>
```

**Step Functions:**
- Automatic resume on Lambda timeout
- Max 100 resume attempts per execution
- 5-second wait between resume attempts
- Graceful shutdown 120 seconds before timeout

---

### 2. Smart Chunking Strategies

**Purpose:** Efficiently process large datasets by splitting into manageable chunks.

**Strategies:**

1. **SingleChunkStrategy** - Small tables (< batch_size rows)
2. **NumericRangeStrategy** - Integer/numeric ID columns
3. **DateRangeStrategy** - Date/timestamp columns with watermarks
4. **OffsetBasedStrategy** - UUID, varchar, or no chunking columns
5. **GroupedValuesStrategy** - High-cardinality columns (IN clause)

**Auto-Selection:**
- Timestamp columns → DateRangeStrategy
- Numeric columns → NumericRangeStrategy
- UUID/varchar → OffsetBasedStrategy
- No chunking columns → SingleChunkStrategy

**Sub-Chunking:**
- Large single-date datasets automatically split using LIMIT/OFFSET
- Prevents memory errors on dates with millions of rows
- Each sub-chunk ≤ batch_size

---

### 3. Incremental Load Optimization

**Purpose:** Skip unchanged data, process only new/modified records.

**How it works:**
- Queries `MAX(target_watermark)` before chunking
- Only creates chunks for data AFTER max watermark
- Dramatically reduces processing time for up-to-date tables

**Performance:**
```
Before: 695 chunks created
After:  1 chunk (99% reduction)
```

**Requirements:**
- `source_watermark` and `target_watermark` configured
- Watermark column exists and populated

---

### 4. COPY vs UPSERT Modes

**COPY Mode (Fast):**
- Used when: `truncate_onstart: true` or empty table with no watermark
- 3-5x faster than UPSERT
- No conflict handling
- Truncates table before loading

**UPSERT Mode (Safe):**
- Used when: Incremental loads, date-based chunking, existing data
- Handles conflicts with `ON CONFLICT DO UPDATE`
- Updates rows where watermark is newer
- Prevents duplicates

**Auto-Detection:**
- System automatically chooses optimal mode
- Date-based chunking forces UPSERT (prevents race conditions)
- Empty table detection happens once before threading

---

### 5. Per-Table Memory Optimization

**Purpose:** Optimize Lambda memory usage for tables of different sizes.

**Global Defaults:**
```json
{
  "parallel_threads": 4,
  "batch_size": 50000,
  "batch_size_copy_mode": 100000
}
```

**Per-Table Override:**
```json
{
  "source": "LARGE_TABLE",
  "parallel_threads": 2,
  "batch_size": 30000,
  "batch_size_copy_mode": 35000
}
```

**When to Override:**
- Tables with high bytes-per-row (> 20 KB/row)
- Memory usage approaching Lambda limits
- OOM errors during migration

**Memory Formula:**
```
Memory (GB) = parallel_threads × batch_size × bytes_per_row × 2.0 / 1024³
```

---

## AWS Lambda & Step Functions

### 6. Lambda Deployment

**Architecture:**
- Lambda Function: `cm-datacopy-test01`
- Memory: 8 GB (configurable)
- Timeout: 15 minutes (AWS maximum)
- Runtime: Python 3.11

**Layer Strategy:**
```
Layer 1: psycopg2-layer (10 MB)
  └─ PostgreSQL client library

Layer 2: dependencies-layer (50 MB)
  └─ pandas, numpy, snowflake-connector, etc.

Main Package: lambda_deployment.zip (250 KB)
  └─ Application code only
```

**Benefits:**
- Quick deployments (250 KB vs 50+ MB)
- Separation of code and dependencies
- Easy updates without full rebuild

---

### 7. Step Functions Orchestration

**State Machine:** `migration-workflow`

**Features:**
- **Automatic resume** on Lambda timeout
- **Optional parameters** (all inputs optional)
- **Multi-source support** (defaults to all enabled sources)
- **Error handling** (routes timeouts to resume, not failure)
- **Retry logic** (up to 100 resume attempts)

**Input Parameters:**
```json
{
  "source_name": "analytics",     // Optional (defaults to all)
  "no_resume": false,             // Optional
  "resume_max_age": 12,           // Optional (hours)
  "resume_run_id": null           // Optional (specific run)
}
```

**Timeout Handling:**
1. Lambda detects approaching timeout (120s buffer)
2. Saves progress and exits gracefully
3. Step Functions catches timeout error
4. Waits 5 seconds
5. Resumes migration automatically
6. Repeats up to 100 times

---

### 8. Multi-Source Support

**Purpose:** Migrate data from multiple Snowflake databases.

**Configuration:**
```json
{
  "sources": [
    {
      "source_name": "analytics",
      "source_sf_database": "ANALYTICS",
      "source_sf_schema": "BI",
      "target_pg_database": "conflict_management",
      "target_pg_schema": "analytics_dev"
    },
    {
      "source_name": "conflict",
      "source_sf_database": "CONFLICTREPORT",
      "source_sf_schema": "PUBLIC",
      "target_pg_database": "conflict_management",
      "target_pg_schema": "conflict_dev"
    }
  ]
}
```

**Usage:**
```bash
# Migrate specific source
python migrate.py --source analytics

# Migrate all enabled sources
python migrate.py
```

---

## Performance Optimizations

### 9. Initial Full Load Detection

**Purpose:** Use fast COPY mode for initial loads on empty tables.

**Problem Solved:**
- Race condition: Multiple threads checking if table is empty
- First thread loads data, others switch to slow UPSERT
- Inconsistent performance across chunks

**Solution:**
- Check table emptiness ONCE before threading
- Decision passed to all worker threads
- All threads use same method (COPY or UPSERT)

**Performance Impact:**
```
Before Fix:  Variable (COPY/UPSERT mix)
After Fix:   Consistent (all COPY or all UPSERT)
Result:      40-60% faster for initial loads
```

---

### 10. Deterministic ORDER BY

**Purpose:** Prevent duplicate keys and data gaps in pagination.

**Problem:**
- `ORDER BY "Updated Timestamp"` alone is non-deterministic
- Multiple records with same timestamp cause unstable pagination
- LIMIT/OFFSET can skip or duplicate rows

**Solution:**
- Append `uniqueness_columns` (primary key) to ORDER BY
- Ensures stable, repeatable pagination

**SQL Generated:**
```sql
ORDER BY "Updated Timestamp", "Patient Address Id"
         ↑ timestamp           ↑ primary key
```

---

### 11. Parallel Processing

**Configuration:**
- Default: 4 parallel threads
- Configurable: Global or per-table
- Processing: Concurrent chunk execution

**Thread Safety:**
- Each thread has own database connection
- Status updates are atomic
- No shared state between threads

**Lambda Constraints:**
- CPU scales with memory allocation
- 8 GB memory = ~4 vCPUs
- Optimal: 4-6 threads for 8 GB

---

## Configuration

### Global Settings

```json
{
  "parallel_threads": 4,
  "batch_size": 50000,
  "batch_size_copy_mode": 100000,
  "max_retry_attempts": 3,
  "lambda_timeout_buffer_seconds": 120
}
```

### Table Configuration

```json
{
  "enabled": true,
  "source": "DIMPATIENT",
  "target": "dimpatient",
  "source_filter": "\"Source System\" = 'hha' AND \"Status\" = 'Active'",
  "chunking_columns": ["Patient Id"],
  "chunking_column_types": ["varchar"],
  "uniqueness_columns": ["Patient Id"],
  "source_watermark": "Updated Datatimestamp",
  "target_watermark": "Updated Datatimestamp",
  "truncate_onstart": false,
  "disable_index": true
}
```

### Per-Table Overrides

```json
{
  "source": "LARGE_TABLE",
  "parallel_threads": 2,        // Override global
  "batch_size": 30000,          // Override global
  "batch_size_copy_mode": 35000 // Override global
}
```

---

## Deployment

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp env.example .env
# Edit .env with your credentials

# Run migration
python migrate.py
```

### Lambda Deployment

**Prerequisites:**
- Docker Desktop running
- PowerShell
- AWS Console access

**Build Scripts:**
```powershell
# First time: Build dependency layer (8-10 min)
cd deploy
.\rebuild_layer.ps1

# Upload dependencies_layer.zip via AWS Console
# Attach layer to Lambda function

# Daily: Quick app-only build (10-20 sec)
.\rebuild_app_only.ps1

# Upload lambda_deployment.zip via AWS Console
```

**Layer Management:**
- Build layers once
- Update only when dependencies change
- App updates are fast (250 KB vs 50 MB)

---

## Architecture

### Chunking Flow

```
1. Determine strategy (based on column types)
2. Query Snowflake for row counts/ranges
3. Create chunks (each ≤ batch_size)
4. Save chunk metadata to status tables
5. Process chunks in parallel
6. Mark completed chunks
```

### Load Flow

```
1. Fetch chunk data from Snowflake (pandas DataFrame)
2. Apply filters and transformations
3. Check if empty (skip if no rows)
4. Choose load method (COPY vs UPSERT)
5. Load to PostgreSQL
6. Update chunk status
7. Handle errors with retry logic
```

### Resume Flow

```
1. Calculate config_hash (MD5 of config)
2. Query for resumable run
   - Same config_hash
   - Status != 'completed'
   - Age < resume_max_age
3. If found: Resume from pending chunks
4. If not found: Start fresh run
```

---

## Status Tracking

### Migration Runs

```sql
CREATE TABLE migration_status.migration_runs (
    run_id UUID PRIMARY KEY,
    config_hash VARCHAR(32),
    status VARCHAR(20),  -- running, partial, completed, failed
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    total_tables INT,
    completed_tables INT,
    failed_tables INT,
    total_rows_copied BIGINT
);
```

### Chunk Status

```sql
CREATE TABLE migration_status.migration_chunk_status (
    id SERIAL PRIMARY KEY,
    run_id UUID,
    chunk_id VARCHAR(100),
    status VARCHAR(20),  -- pending, in_progress, completed, failed
    rows_copied INT,
    chunk_metadata JSONB,
    error_message TEXT,
    completed_at TIMESTAMP
);
```

---

## Error Handling

### Retry Logic

- **Max Attempts:** 3 (configurable)
- **Backoff:** Exponential with jitter
- **Errors Caught:** Connection errors, timeouts, transient failures
- **Not Retried:** Data validation errors, schema mismatches

### Lambda Timeout

- **Buffer:** 120 seconds before hard timeout
- **Detection:** Check remaining time before each chunk
- **Action:** Save progress, exit gracefully, resume later
- **Recovery:** Step Functions automatically retries

### Step Functions Errors

- **Lambda.Unknown:** Timeout error → Resume
- **Lambda.TooManyRequestsException:** Throttling → Retry
- **States.ALL:** Other errors → Fail state

---

## Best Practices

### For Performance

1. **Use truncate_onstart for full reloads** (3-5x faster)
2. **Enable disable_index for large tables** (faster loads)
3. **Set appropriate batch_size** (50K-100K for most tables)
4. **Configure watermarks for incremental loads** (99% fewer chunks)

### For Reliability

1. **Always configure uniqueness_columns** (enables UPSERT, prevents duplicates)
2. **Use deterministic chunking columns** (numeric ID, timestamp)
3. **Test with small batch first** (validate config before full run)
4. **Monitor Lambda memory usage** (adjust threads if OOM)

### For Maintenance

1. **Review migration_runs table regularly** (check for failures)
2. **Clean old status records** (keep last 30 days)
3. **Update layers when upgrading dependencies** (rebuild_layer.ps1)
4. **Version control config.json** (track changes over time)

---

## Concurrent Execution

### Running Multiple Migrations Simultaneously

**Purpose:** Run analytics and conflict table migrations **concurrently** for faster overall migration time.

**Architecture:**
- ✅ **Single Lambda function** (shared code)
- ✅ **Multiple Step Functions** (separate orchestration)
- ✅ **Isolated execution contexts** (separate run_ids, execution_hash)
- ✅ **Parallel execution** (independent Lambda instances)

**How it works:**
- Each Step Functions execution launches its own Lambda instance
- Instances don't share state except for PostgreSQL tracking tables
- Each execution gets a unique `run_id` and `execution_hash`
- Resume logic matches by `execution_hash` to prevent cross-execution conflicts

**Setup:**

1. **Deploy Lambda once** (already supports concurrent execution):
```powershell
cd deploy
.\rebuild_app_only.ps1
# Upload to AWS Lambda
```

2. **Create separate Step Functions**:
   - Copy `migration_workflow.json` for each source
   - Modify `InitializeDefaults` to set `source_name`:
   
```json
{
  "InitializeDefaults": {
    "Type": "Pass",
    "Parameters": {
      "action": "migrate",
      "input": {
        "source_name": "analytics"  ← Set per Step Function
      },
      "defaults": {
        "resume_attempt_count": 0,
        "resume_max_age": 12
      }
    },
    "Next": "ValidateConfig"
  }
}
```

3. **Execute concurrently**:
   - Start analytics Step Function
   - Start conflict Step Function immediately
   - Both run in parallel without interference

**Execution Hash:**
- Combines `config_hash` + sorted `source_names`
- Ensures analytics migration can only resume analytics runs
- Ensures conflict migration can only resume conflict runs
- Stored in `migration_runs.metadata` as `execution_hash`

**Example:**
```python
# Analytics execution
execution_hash = MD5({config: "abc...", sources: ["analytics"]})
→ "111aaa..."

# Conflict execution  
execution_hash = MD5({config: "abc...", sources: ["conflict"]})
→ "222bbb..."

# Different hashes = No cross-execution resume conflicts!
```

**Benefits:**
- ✅ Faster total migration time (parallel processing)
- ✅ Independent progress tracking
- ✅ Separate retry logic per source
- ✅ Safe concurrent execution

**Limitations:**
- PostgreSQL connection pool must support concurrent connections
- Snowflake warehouse must support concurrent queries
- Lambda concurrency limits apply (default: 1000 concurrent executions)

---

## Production Metrics

### Typical Performance

| Table Size | Records | Chunks | Time | Throughput |
|------------|---------|--------|------|------------|
| Small | < 100K | 1-5 | 1-2 min | 1,000 rows/sec |
| Medium | 100K-1M | 5-50 | 5-15 min | 2,000 rows/sec |
| Large | 1M-10M | 50-200 | 30-60 min | 3,000 rows/sec |
| Very Large | 10M+ | 200+ | 2-4 hours | 2,500 rows/sec |

### Memory Usage

| Threads | Batch | B/Row | Memory | Status |
|---------|-------|-------|--------|--------|
| 4 | 50K | 5 KB | 1.9 GB | ✅ Safe |
| 4 | 50K | 10 KB | 3.8 GB | ✅ Safe |
| 4 | 50K | 20 KB | 7.6 GB | ⚠️ Limit |
| 3 | 40K | 20 KB | 4.6 GB | ✅ Safe |

**Recommendation:** Keep memory usage < 80% of Lambda allocation (6.4 GB for 8 GB Lambda)

---

## Files Reference

### Core Application
- `migrate.py` - Main orchestrator
- `config.json` - Migration configuration
- `sql/migration_status_schema.sql` - Status tables schema
- `requirements.txt` - Python dependencies
- `env.example` - Environment template

### Library Code
- `lib/chunking.py` - Chunking strategies
- `lib/migration_worker.py` - Data processing and loading
- `lib/status_tracker.py` - Resume tracking and status management
- `lib/connections.py` - Database connection pooling
- `lib/config_loader.py` - Configuration management
- `lib/config_validator.py` - Configuration validation
- `lib/index_manager.py` - Index disable/enable
- `lib/utils.py` - Utilities and helpers

### Lambda Integration
- `scripts/lambda_handler.py` - AWS Lambda entry point
- `scripts/migration_orchestrator.py` - Lambda orchestration wrapper

### AWS Resources
- `aws/step_functions/migration_workflow.json` - Step Functions definition
- `aws/README.md` - AWS deployment guide

### Deployment
- `deploy/rebuild_app.ps1` - Full package build
- `deploy/rebuild_app_only.ps1` - Quick app rebuild
- `deploy/rebuild_layer.ps1` - Dependency layer build
- `deploy/requirements_layer.txt` - Lambda dependencies

---

## Status: Production Ready ✅

**Tested Features:**
- ✅ Full loads (millions of rows)
- ✅ Incremental loads with watermarks
- ✅ Resume capability after failures
- ✅ Lambda timeout handling
- ✅ Parallel processing (4-6 threads)
- ✅ Multi-source migrations
- ✅ Per-table memory optimization

**Deployment Options:**
- ✅ Local execution (development)
- ✅ AWS Lambda (production)
- ✅ AWS Step Functions (orchestration)

**Data Integrity:**
- ✅ No duplicate keys
- ✅ Watermark-based updates
- ✅ Deterministic pagination
- ✅ Transaction safety

---

**Version:** 2.0  
**Last Updated:** December 2025  
**License:** Internal Use
