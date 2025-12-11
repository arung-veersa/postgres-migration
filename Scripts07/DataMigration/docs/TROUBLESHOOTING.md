# Troubleshooting Guide

**Quick diagnostics for common migration issues**

---

## 🚨 **Quick Diagnosis**

Run this SQL script first for comprehensive diagnostics:

```sql
-- In PostgreSQL:
\i sql/diagnose_stuck_migration.sql

-- Provides:
-- ✅ 10 diagnostic queries
-- ✅ Data quality checks
-- ✅ 5 fix options with SQL templates
-- ✅ Verification queries
```

---

## 📋 **Common Issues**

### 1. **Type Conversion Error**

**Symptoms:**
```
TypeError: unsupported operand type(s) for -: 'str' and 'str'
```

**Root Cause:** Snowflake returns numeric values as strings

**Fix:** ✅ Already fixed in code (automatic int() conversion)

**Deploy:**
```bash
cd Scripts07/DataMigration
zip -r lambda.zip lib/ scripts/ config.json
aws lambda update-function-code --function-name cm-datacopy-test01 --zip-file fileb://lambda.zip
```

---

### 2. **VARCHAR ID Min > Max**

**Symptoms:**
```
ID range [1000000000, 999999999]  ← Backwards!
```

**Root Cause:** Column is VARCHAR storing numeric strings, Snowflake does alphabetical comparison

**Fix:** Declare column type in config
```json
{
  "chunking_columns": ["Application Visit Id"],
  "chunking_column_types": ["int"]  // ← Auto-CAST to BIGINT
}
```

**See:** `VARCHAR_NUMERIC_CHUNKING.md` for details

---

### 3. **Missing Rows After Migration**

**Symptoms:**
- Chunks report "X rows copied"
- PostgreSQL has fewer rows than expected
- `insert_only_mode: true` is enabled

**Root Cause:** PostgreSQL COPY is atomic - rejects entire batch on first duplicate

**Diagnosis:**
```sql
-- Check target max ID
SELECT MAX("ID") FROM conflict_dev.conflictvisitmaps;
-- Result: 8,420,381

-- Check source max ID
SELECT MAX("ID") FROM HHAX_NYPROD.PUBLIC.CONFLICTVISITMAPS;
-- Result: 8,471,089

-- Missing: 50,708 rows
```

**Fix:** Use `source_filter` to skip existing IDs
```json
{
  "source_filter": "\"ID\" > 8420381",  // Only load new rows
  "insert_only_mode": true
}
```

**See:** `BUG_FIX_ROW_TRACKING.md` for technical details

---

### 4. **No Data Found (Despite Large Source)**

**Symptoms:**
```
⚠️ No data found for FACTVISITCALLPERFORMANCE_CR
```

**Root Cause:** Conflicting `source_filter` and `target_watermark`

**Example:**
```json
{
  "source_filter": "\"Visit Date\" = '2024-12-01'",  // Only Dec 1, 2024
  "target_watermark": "2025-05-15"  // Filters to data AFTER May 15, 2025
}
// Result: No data matches BOTH conditions!
```

**Fix Option 1:** Remove date filter
```json
{
  "source_filter": "\"External Source\" = 'HHAX' AND \"Permanent Deleted\" = FALSE"
  // Let watermark handle date filtering
}
```

**Fix Option 2:** Disable watermarks for full load
```json
{
  "source_watermark": null,
  "target_watermark": null,
  "truncate_onstart": true
}
```

---

### 5. **Migration Status Stuck in "running"**

**Symptoms:**
- All chunks show `status = 'completed'`
- Table status shows `status = 'completed'`
- Run status still shows `status = 'running'`

**Root Cause:** ✅ Fixed - Missing database update call

**Verify Fix:**
```sql
SELECT status, completed_at 
FROM migration_status.migration_runs 
ORDER BY started_at DESC 
LIMIT 1;
```

**If still stuck:**
```sql
-- Manual fix:
UPDATE migration_status.migration_runs
SET status = 'completed',
    completed_at = NOW()
WHERE run_id = 'your-run-id';
```

---

### 6. **Duplicate Key Errors**

**Symptoms:**
```
ERROR: duplicate key value violates unique constraint
DETAIL: Key ("ID")=(5203519) already exists
```

**Root Cause:** Chunking on partial primary key or previous partial load

**Fix Option A:** Use `truncate_onstart`
```json
{
  "truncate_onstart": true  // Start fresh
}
```

**Fix Option B:** Use `insert_only_mode` with `source_filter`
```json
{
  "source_filter": "\"ID\" > {max_existing_id}",
  "insert_only_mode": true
}
```

**Fix Option C:** Change chunking strategy
```json
{
  "chunking_columns": null  // Use SingleChunkStrategy
}
```

---

### 7. **Out of Memory (OOM) Errors**

**Symptoms:**
```
MemoryError: Unable to allocate array
[Lambda exceeded memory limit]
```

**Immediate Fix:** Reduce batch size and threads
```json
{
  "source": "PROBLEMATIC_TABLE",
  "parallel_threads": 2,  // Was 4
  "batch_size": 20000     // Was 50000
}
```

**Long-term:** Tier 3 error handling auto-reduces batch size

**Tune based on Lambda memory:**
| Memory | Threads | Batch Size |
|--------|---------|------------|
| 2GB | 1 | 10,000 |
| 4GB | 2 | 25,000 |
| 8GB | 3-4 | 35,000 |
| 10GB | 4-6 | 50,000 |

---

### 8. **Lambda Timeout (15 minutes)**

**Symptoms:**
```
Task timed out after 900.00 seconds
```

**This is NORMAL for large tables!**

**Solution:** Step Functions auto-resumes

**Verify resume configuration:**
```json
// In Step Functions:
{
  "resume_max_age": "12h",
  "CheckResumeAttempts": "< 100"
}
```

**Check resume status:**
```sql
SELECT 
    run_id, 
    status, 
    attempt_count,
    EXTRACT(EPOCH FROM (NOW() - started_at))/3600 as hours_running
FROM migration_status.migration_runs
WHERE status = 'running'
ORDER BY started_at DESC;
```

---

### 9. **Column Not Found**

**Symptoms:**
```
ERROR: column "payer_id" does not exist
HINT: Perhaps you meant to reference column "Payer Id"
```

**Root Cause:** Incorrect casing in config

**Fix:** Use exact casing from PostgreSQL
```json
{
  "chunking_columns": ["Payer Id"],      // ✅ Correct
  "uniqueness_columns": ["Payer Id"],    // ✅ Correct
  
  "chunking_columns": ["payer_id"]       // ❌ Wrong
}
```

**Check PostgreSQL casing:**
```sql
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'dimpayer';
```

---

### 10. **Connection Errors**

**Snowflake:**
```
ERROR: Failed to connect to Snowflake
```

**Check:**
- RSA key path/content in `.env`
- Snowflake account format: `account.region` (e.g., `IKB38126.us-east-1`)
- User has warehouse access
- Firewall/VPN not blocking

**PostgreSQL:**
```
ERROR: FATAL: database "xxx" does not exist
```

**Check:**
- Database exists: `\l` in psql
- User has access: `GRANT ALL ON DATABASE xxx TO user;`
- Correct credentials in `.env`

---

## 🔍 **Diagnostic Queries**

### Check Migration Progress

```sql
-- Overall status
SELECT * FROM migration_status.v_active_migrations;

-- Table-level progress
SELECT * FROM migration_status.v_table_progress;

-- Chunk-level details
SELECT 
    chunk_id,
    status,
    rows_copied,
    attempt_count,
    error_message
FROM migration_status.migration_chunk_status
WHERE run_id = 'your-run-id'
ORDER BY chunk_id;
```

### Find Stuck Chunks

```sql
SELECT 
    chunk_id,
    status,
    started_at,
    NOW() - started_at as duration
FROM migration_status.migration_chunk_status
WHERE status = 'in_progress'
  AND NOW() - started_at > interval '30 minutes';
```

### Verify Row Counts

```sql
-- Source (Snowflake):
SELECT COUNT(*) FROM HHAX_NYPROD.PUBLIC.CONFLICTS;

-- Target (PostgreSQL):
SELECT COUNT(*) FROM conflict_dev.conflicts;

-- Difference:
SELECT 
    (SELECT COUNT(*) FROM HHAX_NYPROD.PUBLIC.CONFLICTS) as source_count,
    (SELECT COUNT(*) FROM conflict_dev.conflicts) as target_count,
    (SELECT COUNT(*) FROM HHAX_NYPROD.PUBLIC.CONFLICTS) - 
    (SELECT COUNT(*) FROM conflict_dev.conflicts) as difference;
```

---

## 🛠️ **Quick Fixes**

### Reset Stuck Migration

```sql
-- Option 1: Reset stuck chunks only
UPDATE migration_status.migration_chunk_status
SET status = 'pending',
    started_at = NULL,
    completed_at = NULL
WHERE run_id = 'your-run-id'
  AND status = 'in_progress';

-- Option 2: Reset entire run
DELETE FROM migration_status.migration_chunk_status WHERE run_id = 'your-run-id';
DELETE FROM migration_status.migration_table_status WHERE run_id = 'your-run-id';
DELETE FROM migration_status.migration_runs WHERE run_id = 'your-run-id';
```

### Force Fresh Start

```sql
-- Clear all status (keeps data):
TRUNCATE TABLE migration_status.migration_chunk_status;
TRUNCATE TABLE migration_status.migration_table_status;
TRUNCATE TABLE migration_status.migration_runs;

-- Or via Step Functions:
{
  "source_name": "analytics",
  "no_resume": true
}
```

### Clear Data and Status

```sql
-- ⚠️ DESTRUCTIVE: Clears all data and status
\i sql/truncate_all_tables.sql
-- Follow instructions in file (uncomment sections)
```

---

## 📚 **Additional Resources**

- **`sql/diagnose_stuck_migration.sql`** - Comprehensive diagnostic tool
- **`BUG_FIX_ROW_TRACKING.md`** - Row tracking accuracy fix
- **`BUG_FIX_TYPE_CONVERSION.md`** - Type conversion error fix
- **`VARCHAR_NUMERIC_CHUNKING.md`** - VARCHAR as numeric IDs
- **`MIGRATION_ISSUES_RESOLVED.md`** - Known issues and resolutions
- **`FEATURES.md`** - Complete feature documentation

---

## 🆘 **Still Stuck?**

1. ✅ Run `sql/diagnose_stuck_migration.sql`
2. ✅ Check CloudWatch logs (if using Lambda)
3. ✅ Review error messages (designed to be helpful)
4. ✅ Check this guide for similar issues
5. ✅ Review config.json for typos/casing

**Common mistakes:**
- Column name casing (`"Payer Id"` vs `"payer_id"`)
- Conflicting filters (`source_filter` + `target_watermark`)
- Missing `source_filter` with `insert_only_mode`
- Wrong `chunking_column_types` for VARCHAR IDs

---

**Version:** 2.0  
**Last Updated:** December 11, 2025  
**Status:** ✅ Covers all known issues

