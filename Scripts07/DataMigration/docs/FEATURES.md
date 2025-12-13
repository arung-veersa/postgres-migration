# PostgreSQL Migration Tool - Features & Configuration Guide

**Version:** 2.1  
**Last Updated:** December 13, 2025

---

## 📋 **Table of Contents**

1. [Core Features](#core-features)
2. [New in Version 2.1](#new-in-version-21)
3. [Smart COPY/UPSERT Mode](#smart-copyupsert-mode)
4. [Tiered Error Handling](#tiered-error-handling)
5. [Insert-Only Mode](#insert-only-mode)
6. [Chunking Strategies](#chunking-strategies)
7. [VARCHAR as Numeric IDs](#varchar-as-numeric-ids)
8. [Incremental Loads](#incremental-loads)
9. [Memory Management](#memory-management)
10. [Configuration Reference](#configuration-reference)
11. [Performance Tuning](#performance-tuning)

---

## Core Features

### ✅ **Production-Ready Capabilities**

| Feature | Description | Benefit |
|---------|-------------|---------|
| **Configuration-Driven** | All logic in `config.json` | No code changes needed |
| **Smart Mode** | Auto-detects COPY vs UPSERT | 8-10x faster for new data |
| **Tiered Error Handling** | Auto-retry, auto-fallback, skip | Resilient to transient errors |
| **Insert-Only Mode** | Skip duplicates, don't update | Fast catch-up loads |
| **Adaptive Chunking** | Auto-selects optimal strategy | Works with any data distribution |
| **Parallel Processing** | Multi-threaded chunk execution | Maximum throughput |
| **Resume Capability** | Granular status tracking | Recovers from failures |
| **Watermark Incremental** | Only copy changed data | Efficient updates |
| **Index Management** | Auto disable/restore | Faster bulk loads |
| **VARCHAR Numeric Handling** | Auto-CAST for numeric VARCHAR | Correct sorting/chunking |
| **Lambda Timeout Handling** | Graceful shutdown, auto-resume | Works with 15min limit |
| **Concurrent Execution Isolation** | Hash-based run isolation | Multiple migrations run safely |
| **Bulletproof Truncation Protection** | Dual-layer data safety | Prevents accidental data loss |
| **Enhanced Diagnostics** | Comprehensive logging | Fast troubleshooting |

---

## New in Version 2.1

### 🛡️ **Concurrent Execution Isolation**

**Problem Solved:** Multiple Step Functions executions (e.g., analytics + conflict migrations) can now run simultaneously without interfering with each other.

**How It Works:**
- Each execution gets a unique `execution_hash` based on configuration and source names
- Resume detection only finds runs matching the same `execution_hash`
- Prevents cross-contamination between different migration jobs

**Configuration:**
```json
{
  "sources": [
    {"source_name": "analytics", ...},
    {"source_name": "conflict", ...}
  ]
}
```

Each source runs independently with its own tracking.

---

### 🔒 **Bulletproof Truncation Protection**

**Problem Solved:** Prevents accidental table truncation during resume after Lambda timeout, even if resume detection fails.

**Dual-Layer Protection:**

**Layer 1:** Check migration status table
```sql
-- Does this table have a status record for current run?
SELECT status FROM migration_table_status 
WHERE run_id = ? AND table_name = ?
```

**Layer 2:** Direct table existence check
```sql
-- Does the target table actually contain data?
SELECT EXISTS (SELECT 1 FROM schema.table LIMIT 1)
```

**Decision Logic:**
```
IF no status record found for current run:
    IF target table contains data:
        ⚠️  SKIP TRUNCATION (data protection)
        ✅ Continue with existing data
    ELSE:
        ✅ Safe to truncate (truly empty)
ELSE:
    Use status record to decide
```

**Why This Matters:**
- If resume detection fails and creates a new `run_id`
- The new run won't find the old table status
- BUT Layer 2 detects existing data and prevents truncation
- **Your data is safe even if resume logic has issues**

---

### 📊 **Enhanced Diagnostic Logging**

**New Logging Features:**

**1. Resume Detection Phase**
```
================================================================================
RESUME DETECTION PHASE
================================================================================
Config hash: ea0e2edd02766225db1c479d13a9350c
Execution hash: f6e396e03b0070b567679a848362d239
Resume settings: no_resume=False, resume_max_age=2h, resume_run_id=None
Requested sources: analytics
```

**2. Truncation Safety Check**
```
================================================================================
TRUNCATION SAFETY CHECK: factvisitcallperformance_cr
================================================================================
Phase 1: Check migration_table_status for run_id=abc123
  → Status record: NOT FOUND for this run_id

Phase 2: Direct table data check
  → Query: SELECT EXISTS(SELECT 1 FROM analytics_dev.factvisitcallperformance_cr LIMIT 1)
  → Result: Table contains data (533580 rows)

Phase 3: Final Decision
  ⚠️  SKIPPING TRUNCATION - Data protection activated
  Reason: No status for current run, but table has data
```

**3. Resume Run Matching**
```
[STATUS] Looking for resumable run...
[STATUS] Found 2 candidate runs to evaluate
[STATUS] Evaluating run abc123: execution_hash=f6e39... (age: 0.5h)
[STATUS] ✅ Execution hash matches! Found resumable run: abc123
```

**Benefits:**
- Quickly diagnose resume issues
- Verify truncation decisions
- Understand execution isolation
- Faster troubleshooting

---

### ⚙️ **Per-Table Memory Optimization**

**Problem Solved:** Different tables have different memory requirements. Global settings cause OOM for large tables or waste resources for small tables.

**New Configuration:**
```json
{
  "parallel_threads": 6,        // Global default
  "batch_size": 100000,         // Global default
  
  "tables": [
    {
      "source": "LARGE_TABLE",
      "parallel_threads": 3,    // Override for this table
      "batch_size": 30000       // Override for this table
    },
    {
      "source": "SMALL_TABLE"
      // Uses global settings (6 threads, 100K batch)
    }
  ]
}
```

**Memory Calculation:**
```
Total rows in memory = parallel_threads × batch_size

Example (analytics table with wide columns):
- 6 threads × 100K batch = 600K rows → 7 GB RAM (OOM!)
- 3 threads × 30K batch = 90K rows → 5 GB RAM (safe)
```

**Best Practices:**
- Start with recommended settings (see Performance Tuning section)
- Monitor Lambda memory usage in CloudWatch
- Adjust per-table if OOM occurs
- Tables with wide VARCHAR/TEXT columns need smaller batches

---

### 🔧 **Improved Resume Logic**

**Problem Solved:** Resume detection was failing due to incorrect `no_resume` parameter handling in Step Functions.

**What Was Fixed:**
- Step Functions now properly defaults `no_resume` to `false`
- Value persists across Lambda invocations
- Resume detection works correctly after timeout

**Step Functions Changes:**
```json
{
  "InitializeDefaults": {
    "Type": "Pass",
    "Result": {
      "no_resume": false,        // Explicit default
      "resume_max_age": 2
    }
  },
  "IncrementResumeCount": {
    "Parameters": {
      "no_resume.$": "$.defaults.no_resume"  // Preserve value
    }
  }
}
```

**Before Fix:**
```
Invocation 1: Creates run_id=abc123
Timeout...
Invocation 2: no_resume=True (implicit) → Creates NEW run_id=def456
Result: Duplicate runs, reprocessing chunks
```

**After Fix:**
```
Invocation 1: Creates run_id=abc123
Timeout...
Invocation 2: no_resume=False (explicit) → Resumes run_id=abc123
Result: Proper resumption, no duplicate work
```

---

## Smart COPY/UPSERT Mode

### 🎯 **What It Does**

Automatically decides per chunk whether to use:
- **COPY mode** (fast bulk insert) for **new data**
- **UPSERT mode** (INSERT...ON CONFLICT DO UPDATE) for **existing data**

**Performance Impact:**
- COPY: ~3 seconds per 100K rows
- UPSERT: ~40 seconds per 100K rows
- **8-10x faster** for chunks with no existing data!

---

### 🔧 **How It Works**

**1. During Chunking:**
```python
# For each chunk (e.g., ID range 1000-2000):
1. Query PostgreSQL: "Does ID range 1000-2000 exist?"
2. If NO → Mark chunk: use_copy_mode = True
3. If YES → Mark chunk: use_copy_mode = False
```

**2. During Migration:**
```python
if chunk.use_copy_mode:
    use COPY (fast bulk insert)
else:
    use UPSERT (handles existing rows)
```

**3. Error Fallback:**
```python
try:
    COPY  # Try fast path
except DuplicateKeyError:
    UPSERT  # Fallback to safe path
```

---

### 📊 **Example Scenario**

**Table:** `CONFLICTS` (8.4M rows)

**Situation:** PostgreSQL already has IDs 1-5M, Snowflake has IDs 1-8.4M

**Smart Mode Result:**
```
📊 SMART MODE RESULTS:
  ✅ COPY mode chunks: 145 (68%) - NEW data (IDs 5M-8.4M)
  🔄 UPSERT mode chunks: 68 (32%) - EXISTING data (IDs 1-5M)
  ⚡ Estimated time: 2.3h (saves 4.8h vs all UPSERT)
```

---

### ⚙️ **Configuration**

**Enabled by default!** No config needed.

**Requirements:**
- Table must have `uniqueness_columns` defined
- Works with `NumericRangeStrategy` (int/bigint columns)
- Compatible with `insert_only_mode`

**Interaction with `truncate_onstart`:**
```json
{
  "truncate_onstart": true  // → All chunks use COPY (table is empty)
}
```

---

## Tiered Error Handling

### 🛡️ **4-Tier Resilience Strategy**

Ensures migrations continue despite errors:

---

### **Tier 1: Transient Error Retry**

**Handles:** Network errors, connection timeouts, deadlocks

**Action:** Automatic retry with exponential backoff
```
Attempt 1: Immediate
Attempt 2: 2 seconds later
Attempt 3: 4 seconds later
```

**Config:**
```json
{
  "max_retry_attempts": 3  // Global default
}
```

---

### **Tier 2: Auto-Fallback (COPY → UPSERT)**

**Handles:** Duplicate key errors during COPY

**Action:**
```python
try:
    COPY (fast)
except UniqueViolation:
    if insert_only_mode:
        log_warning("Skipping duplicates")
        continue
    else:
        UPSERT (safe)  # Fallback
```

**When it happens:**
- Smart mode prediction was wrong
- Data changed between check and load
- Concurrent modifications

---

### **Tier 3: Adaptive Batch Sizing**

**Handles:** Out-of-Memory (OOM) errors

**Action:** Progressively smaller batches
```
Try 40,000 rows → OOM
Try 20,000 rows → OOM
Try 10,000 rows → OOM
Try 5,000 rows → Success!
```

**Logged as:**
```
⚠️ OOM detected, retrying with batch_size 20000 (was 40000)
✓ Chunk processed with reduced batch size: 5000 rows
```

---

### **Tier 4: Orchestrator-Level Resilience**

**Handles:** Systemic chunk failures

**Action:** Continue with other chunks, log failures
```python
for chunk in chunks:
    try:
        process_chunk(chunk)
    except Exception as e:
        log_error(f"Chunk {chunk.id} failed: {e}")
        failed_chunks.append(chunk)
        continue  # Don't stop entire migration
```

**Result:**
- Other chunks complete
- Failed chunks logged for retry
- Table marked as `partial` (not `failed`)

---

### 📊 **Error Handling Examples**

**Example 1: Network Blip**
```
[ERROR] Connection timeout
[INFO] Retrying chunk 42 (attempt 2/3)...
[INFO] ✓ Chunk 42 completed on retry
```

**Example 2: Unexpected Duplicate**
```
[ERROR] COPY failed: duplicate key "ID=5203519"
[INFO] Falling back to UPSERT mode for chunk 42
[INFO] ✓ Chunk 42 completed via UPSERT
```

**Example 3: Memory Pressure**
```
[ERROR] MemoryError: Unable to allocate array
[INFO] Retrying with batch_size 20000 (was 40000)
[INFO] ✓ Chunk processed with reduced batch size
```

**Example 4: Isolated Chunk Failure**
```
[ERROR] Chunk 42 failed after 3 retries: [...]
[INFO] Continuing with remaining 200 chunks
[INFO] ✓ Table completed: 200/201 chunks (99.5%)
[WARNING] Table status: partial (1 chunk failed)
```

---

## Insert-Only Mode

### 🎯 **Use Case**

**Skip existing records, insert only new ones** - No updates!

Perfect for:
- Fast catch-up loads after interruption
- Resume scenarios where updates aren't needed
- Tables where updates are rare

---

### ⚙️ **Configuration**

**Global (all tables):**
```json
{
  "insert_only_mode": false  // Default
}
```

**Per-table override:**
```json
{
  "source": "CONFLICTS",
  "insert_only_mode": true  // Override for this table only
}
```

---

### 🔧 **How It Works**

**Normal Mode (insert_only_mode: false):**
```python
try:
    COPY (bulk insert)
except UniqueViolation:
    UPSERT (insert new, update existing)  # Fallback
```

**Insert-Only Mode (insert_only_mode: true):**
```python
try:
    COPY (bulk insert)
except UniqueViolation:
    log_warning("Duplicates found, skipping (insert_only_mode)")
    # NO UPSERT! Just skip and continue
```

---

### ⚠️ **Important Behavior**

**PostgreSQL COPY is atomic:**
- If **any** row in the batch is a duplicate, **all rows** are rejected
- Returns `0 rows inserted` (not partial)

**Example:**
```
Batch: 50,000 rows
First duplicate: Row 1 (ID=5203519)
Result: 0 rows inserted (entire batch rejected)
```

**Solution:** Use `source_filter` to skip processed ranges:
```json
{
  "source_filter": "\"ID\" > 8420381"  // Start after max existing ID
}
```

---

### 📊 **Example: Resume Scenario**

**Situation:**
- Target has: 8,420,381 rows (IDs 1 - 8,420,381)
- Source has: 8,471,089 rows (IDs 1 - 8,471,089)
- **Goal:** Load the missing 50,708 rows

**Wrong Config (would fail):**
```json
{
  "source_filter": "1=1",  // Tries to load IDs 1-8.4M
  "insert_only_mode": true  // Skips duplicates
}
// Result: 0 rows loaded (all chunks hit duplicates)
```

**Correct Config:**
```json
{
  "source_filter": "\"ID\" > 8420381",  // Only load NEW rows
  "insert_only_mode": true
}
// Result: 50,708 rows loaded successfully
```

---

### 🔄 **Interaction with Other Features**

**With `truncate_onstart`:**
```json
{
  "truncate_onstart": true,   // Empties table first
  "insert_only_mode": true    // Redundant (no duplicates possible)
}
// insert_only_mode has no effect (table is empty)
```

**With Smart Mode:**
```json
{
  "insert_only_mode": true
}
// Smart mode still checks for existing data
// If exists: Skips chunk with warning (no UPSERT)
// If new: Uses COPY mode (fast)
```

---

## Chunking Strategies

### 🎯 **Automatic Strategy Selection**

The tool analyzes `chunking_column_types` and selects the optimal strategy:

| Column Type | Strategy | Method | Best For |
|-------------|----------|--------|----------|
| `int`, `bigint` | **Numeric Range** | Divide ID range into equal chunks | Sequential IDs |
| `uuid`, `varchar` | **Grouped Values** | Group distinct values | High-cardinality keys |
| `date`, `timestamp` | **Date Range** | Group by date/time periods | Time-series data |
| `null` or small table | **Single Chunk** | Process entire table | <10K rows |

---

### 📊 **Numeric Range Strategy**

**Best for:** Auto-incrementing IDs, sequential numbers

**Config:**
```json
{
  "chunking_columns": ["ID"],
  "chunking_column_types": ["int"],
  "batch_size": 50000
}
```

**How it works:**
```sql
SELECT MIN(ID), MAX(ID), COUNT(*) FROM source;
-- Result: min=1, max=8471089, count=8471089

-- Creates chunks:
Chunk 0: ID >= 1 AND ID <= 168000
Chunk 1: ID >= 168001 AND ID <= 336000
...
Chunk 50: ID >= 8403001 AND ID <= 8471089
```

---

### 📊 **Grouped Values Strategy**

**Best for:** UUIDs, sparse IDs, varchar keys

**Config:**
```json
{
  "chunking_columns": ["Payer Id"],
  "chunking_column_types": ["uuid"],
  "batch_size": 10000
}
```

**How it works:**
```sql
SELECT DISTINCT "Payer Id" FROM source ORDER BY "Payer Id";
-- Groups distinct values into batches of 10,000

-- Creates chunks with IN (...) filters:
Chunk 0: "Payer Id" IN ('uuid1', 'uuid2', ..., 'uuid10000')
Chunk 1: "Payer Id" IN ('uuid10001', ..., 'uuid20000')
```

---

### 📊 **Date Range Strategy**

**Best for:** Time-series data, incremental loads

**Config:**
```json
{
  "chunking_columns": ["Updated Timestamp"],
  "chunking_column_types": ["timestamp"],
  "source_watermark": "Updated Timestamp",
  "target_watermark": "Updated Timestamp"
}
```

**How it works:**
```sql
-- Only process data newer than target's max timestamp
SELECT MAX("Updated Timestamp") FROM target;  -- 2025-05-15

-- Chunk by date:
Chunk 0: "Updated Timestamp"::DATE = '2025-05-16'
Chunk 1: "Updated Timestamp"::DATE = '2025-05-17'
...

-- If a single date has >batch_size rows, sub-chunk with LIMIT/OFFSET
```

---

### 📊 **Single Chunk Strategy**

**Best for:** Small tables, no chunking column

**Config:**
```json
{
  "chunking_columns": null,  // No chunking
  "batch_size": 10000
}
```

**How it works:**
```sql
-- Processes entire table in one operation
SELECT * FROM source WHERE source_filter ORDER BY primary_key;
```

---

## VARCHAR as Numeric IDs

### 🐛 **Problem**

Some tables store numeric IDs in **VARCHAR** columns:
```sql
-- EXAMPLE (not actual DDL):
CREATE TABLE example_table (
    "Numeric ID Column" VARCHAR(50)  -- Stores '1000000', '1000001', etc.
);
```

**Issue:** Snowflake's `MIN()` and `MAX()` on VARCHAR columns perform **lexicographic (alphabetical)** comparison:
```sql
MIN('1000000000', '999999999') = '1000000000'  -- ❌ Wrong!
MAX('1000000000', '999999999') = '999999999'   -- ❌ Wrong!
```

**Expected numeric behavior:**
```sql
MIN(1000000000, 999999999) = 999999999  -- ✅ Correct
MAX(1000000000, 999999999) = 1000000000 -- ✅ Correct
```

---

### ✅ **Solution**

**Declare the column type in config:**
```json
{
  "chunking_columns": ["Application Visit Id"],
  "chunking_column_types": ["int"]  // ← Triggers auto-CAST!
}
```

**Code automatically adds CAST:**
```sql
-- Without CAST (VARCHAR, alphabetic):
SELECT MIN("Application Visit Id"), MAX("Application Visit Id") ...

-- With CAST (numeric):
SELECT 
    MIN(CAST("Application Visit Id" AS BIGINT)),
    MAX(CAST("Application Visit Id" AS BIGINT)) ...
```

**Filters also use CAST:**
```sql
WHERE ... 
  AND CAST("Application Visit Id" AS BIGINT) >= 1000000
  AND CAST("Application Visit Id" AS BIGINT) <= 2000000
```

---

### 📋 **When to Use**

✅ **Use `chunking_column_types: ["int"]` when:**
- Column is VARCHAR/TEXT storing numeric strings
- IDs like: `'1000000'`, `'1000001'`, `'1000002'`
- You want numeric range chunking

❌ **Don't use for:**
- True VARCHAR data (UUIDs, codes, names) → Use `"uuid"` or `"varchar"`
- Columns already INT/BIGINT in Snowflake → CAST is harmless but unnecessary

---

### 🔍 **How to Check Column Type**

```sql
-- In Snowflake:
DESCRIBE TABLE HHAX_NYPROD.PUBLIC.FACTVISITCALLPERFORMANCE_CR;

-- Or:
SELECT TYPEOF("Application Visit Id"), "Application Visit Id"
FROM HHAX_NYPROD.PUBLIC.FACTVISITCALLPERFORMANCE_CR
LIMIT 1;
```

**If result is `VARCHAR` and values are numeric → Use `chunking_column_types: ["int"]`**

---

## Incremental Loads

### 🎯 **Watermark-Based Updates**

Only copy changed/new records since last migration.

**Config:**
```json
{
  "source_watermark": "Updated Timestamp",
  "target_watermark": "Updated Timestamp",
  "uniqueness_columns": ["Payer Id"],
  "truncate_onstart": false
}
```

---

### 🔧 **How It Works**

**1. Query target's max watermark:**
```sql
SELECT MAX("Updated Timestamp") FROM target_table;
-- Result: 2025-05-15 10:00:00
```

**2. Filter source data:**
```sql
SELECT * FROM source_table
WHERE "Updated Timestamp" > '2025-05-15 10:00:00'  -- Only newer data
```

**3. Use UPSERT for existing records:**
```sql
INSERT INTO target_table (...)
VALUES (...)
ON CONFLICT ("Payer Id") DO UPDATE SET
    "Column1" = EXCLUDED."Column1",
    "Updated Timestamp" = EXCLUDED."Updated Timestamp";
```

---

### ⚠️ **Important Notes**

**Watermarks are ignored when:**
```json
{
  "truncate_onstart": true  // Full reload, watermarks not needed
}
```

**Performance:**
- Best for tables with frequent updates
- Reduces data transfer (only changed rows)
- Requires indexed watermark column

---

## Memory Management

### 🎯 **Per-Table Configuration**

Override global settings for specific tables:

```json
{
  "parallel_threads": 4,    // Global default
  "batch_size": 50000,      // Global default
  
  "tables": [
    {
      "source": "SMALL_TABLE",
      // Uses global settings (4 threads, 50K batch)
    },
    {
      "source": "HUGE_TABLE",
      "parallel_threads": 2,   // Override: Less parallelism
      "batch_size": 25000      // Override: Smaller batches
    }
  ]
}
```

---

### 📊 **Lambda Memory Recommendations**

| Lambda Memory | Recommended Settings |
|---------------|----------------------|
| 2GB | `parallel_threads: 1`, `batch_size: 10000` |
| 4GB | `parallel_threads: 2`, `batch_size: 25000` |
| 8GB | `parallel_threads: 3-4`, `batch_size: 35000` |
| 10GB | `parallel_threads: 4-6`, `batch_size: 50000` |

---

### 🛡️ **OOM Protection**

**Tier 3 Error Handling** automatically reduces batch size on OOM:
```
40,000 → 20,000 → 10,000 → 5,000
```

**Manual tuning for problematic tables:**
```json
{
  "source": "PROBLEMATIC_TABLE",
  "parallel_threads": 2,  // Reduce concurrency
  "batch_size": 20000     // Smaller batches
}
```

---

## Configuration Reference

### 📋 **Complete Example**

```json
{
  "snowflake": {
    "account": "${SNOWFLAKE_ACCOUNT}",
    "user": "${SNOWFLAKE_USER}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "rsa_key": "${SNOWFLAKE_RSA_KEY}"
  },
  "postgres": {
    "host": "${POSTGRES_HOST}",
    "user": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}"
  },
  
  "parallel_threads": 4,
  "batch_size": 50000,
  "max_retry_attempts": 3,
  "insert_only_mode": false,
  "lambda_timeout_buffer_seconds": 90,
  
  "sources": [
    {
      "enabled": true,
      "source_name": "analytics",
      "source_sf_database": "HHAX_NYPROD",
      "source_sf_schema": "PUBLIC",
      "target_pg_database": "conflict_management",
      "target_pg_schema": "analytics_dev",
      
      "tables": [
        {
          "enabled": true,
          "source": "FACTVISITCALLPERFORMANCE_CR",
          "target": "factvisitcallperformance_cr",
          "source_filter": "\"External Source\" = 'HHAX' AND \"Permanent Deleted\" = FALSE",
          "chunking_columns": ["Application Visit Id"],
          "chunking_column_types": ["int"],
          "uniqueness_columns": ["Visit Id"],
          "sort_columns": ["Application Visit Id"],
          "source_watermark": "Visit Updated Timestamp",
          "target_watermark": "Visit Updated Timestamp",
          "truncate_onstart": false,
          "disable_index": true,
          "insert_only_mode": false,
          "parallel_threads": 4,
          "batch_size": 35000
        }
      ]
    }
  ]
}
```

---

### 📖 **Field Descriptions**

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `enabled` | boolean | Process this table/source | true |
| `source` | string | Snowflake table name | Required |
| `target` | string | PostgreSQL table name | Required |
| `source_filter` | string | SQL WHERE clause | `"1=1"` |
| `chunking_columns` | array | Columns for chunking | `null` |
| `chunking_column_types` | array | Data types (`int`, `uuid`, `date`, `varchar`) | `null` |
| `uniqueness_columns` | array | Primary/unique key columns | `null` |
| `sort_columns` | array | ORDER BY columns | `chunking_columns` |
| `source_watermark` | string | Timestamp column for incremental | `null` |
| `target_watermark` | string | Timestamp column in target | `null` |
| `truncate_onstart` | boolean | Empty table before loading | `false` |
| `disable_index` | boolean | Disable indexes during load | `false` |
| `insert_only_mode` | boolean | Skip duplicates, don't update | `false` |
| `parallel_threads` | integer | Number of parallel threads | 4 |
| `batch_size` | integer | Rows per chunk | 10000 |

---

## Performance Tuning

### 🚀 **Maximum Throughput**

```json
{
  "parallel_threads": 8,
  "batch_size": 100000,
  "truncate_onstart": true,
  "disable_index": true,
  "insert_only_mode": true
}
```

**Best for:** Full reloads of large tables (>1M rows)

---

### ⚡ **Incremental Updates**

```json
{
  "parallel_threads": 4,
  "batch_size": 25000,
  "source_watermark": "Updated Timestamp",
  "target_watermark": "Updated Timestamp",
  "truncate_onstart": false,
  "insert_only_mode": false
}
```

**Best for:** Regular updates, small deltas

---

### 🐢 **Memory-Constrained (Lambda 2GB)**

```json
{
  "parallel_threads": 1,
  "batch_size": 10000,
  "disable_index": false
}
```

**Best for:** Smaller Lambda instances, wide tables

---

### 📊 **Typical Performance**

| Scenario | Config | Throughput |
|----------|--------|------------|
| **Small tables** (<10K rows) | Default | ~1,000 rows/sec |
| **Medium tables** (100K rows) | 4 threads, 50K batch | ~5,000 rows/sec |
| **Large tables** (1M+ rows) | 8 threads, 100K batch, COPY mode | ~20,000 rows/sec |
| **Very large** (10M+ rows) | Same + disable_index | ~30,000 rows/sec |

---

**For more information:**
- **[README.md](README.md)** - Main documentation
- **[QUICKSTART.md](QUICKSTART.md)** - Getting started
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common issues and solutions
- **[sql/README.md](sql/README.md)** - SQL helper scripts

**Version:** 2.0  
**Last Updated:** December 11, 2025  
**Status:** ✅ Production Ready
