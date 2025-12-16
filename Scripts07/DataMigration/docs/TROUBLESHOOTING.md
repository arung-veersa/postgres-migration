# Troubleshooting Guide

Complete guide to diagnosing and resolving common migration issues.

---

## Table of Contents

1. [Quick Diagnosis](#quick-diagnosis)
2. [Connection Issues](#connection-issues)
3. [Performance Problems](#performance-problems)
4. [Memory Issues](#memory-issues)
5. [Data Issues](#data-issues)
6. [Resume Problems](#resume-problems)
7. [Configuration Errors](#configuration-errors)
8. [AWS/Lambda Issues](#awslambda-issues)
9. [When to Check Historical Issues](#when-to-check-historical-issues)

---

## Quick Diagnosis

### Symptom Checklist

| Symptom | Likely Cause | Quick Fix |
|---------|--------------|-----------|
| Migration won't start | Config error | Run validate_config |
| Can't connect to Snowflake | Auth/network | Check RSA key, firewall |
| Can't connect to PostgreSQL | VPC/security group | Check VPC config |
| Migration very slow | Warehouse too small | Upgrade to MEDIUM |
| Out of memory | Too many threads | Reduce parallel_threads |
| Missing rows | Wrong chunking strategy | Use date-based chunking |
| Won't resume | Config changed | Use explicit resume_run_id |
| Duplicate logs | Old code version | Deploy v2.3+ |

---

## Connection Issues

### Can't Connect to Snowflake

**Symptoms:**
```
Error: Snowflake authentication failed
Error: Could not connect to Snowflake account
```

**Diagnosis:**
1. **Check RSA key format:**
   ```bash
   # Key must be PEM format
   -----BEGIN PRIVATE KEY-----
   MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC...
   -----END PRIVATE KEY-----
   ```

2. **Verify environment variable:**
   ```bash
   # In Lambda, newlines must be \n
   SNOWFLAKE_RSA_KEY="-----BEGIN PRIVATE KEY-----\nMIIEv...\n-----END PRIVATE KEY-----"
   ```

3. **Test Snowflake user:**
   ```sql
   -- Run in Snowflake console
   SHOW GRANTS TO USER migration_user;
   -- Should have: USAGE on warehouse, database, schema
   --               SELECT on tables
   ```

**Solutions:**
- ✅ Re-generate RSA key pair if corrupted
- ✅ Ensure Snowflake user has correct permissions
- ✅ Check account identifier format (should be `account.region`)
- ✅ Verify warehouse exists and is running

---

### Can't Connect to PostgreSQL

**Symptoms:**
```
Error: could not connect to server: Connection timed out
Error: FATAL: password authentication failed
```

**Diagnosis:**
1. **VPC Configuration (Lambda):**
   - Lambda must be in VPC with route to PostgreSQL
   - Security groups must allow outbound to PostgreSQL port
   - PostgreSQL security group must allow inbound from Lambda

2. **Test from Lambda:**
   ```python
   # Use test_connections action
   {
     "action": "test_connections"
   }
   ```

3. **Check PostgreSQL logs:**
   ```sql
   -- Enable logging
   ALTER SYSTEM SET log_connections = on;
   ALTER SYSTEM SET log_disconnections = on;
   SELECT pg_reload_conf();
   
   -- Check logs
   SELECT * FROM pg_stat_activity;
   ```

**Solutions:**
- ✅ Verify security group rules (inbound 5432 from Lambda SG)
- ✅ Check VPC route tables (NAT gateway if private subnets)
- ✅ Test credentials locally first
- ✅ Ensure PostgreSQL allows remote connections

---

### Intermittent Connection Drops

**Symptoms:**
```
Error: SSL connection has been closed unexpectedly
Error: server closed the connection unexpectedly
```

**Solutions:**
- ✅ Increase PostgreSQL `max_connections`
- ✅ Reduce `parallel_threads` (fewer concurrent connections)
- ✅ Check network stability
- ✅ Verify no idle connection timeouts

---

## Performance Problems

### Migration Very Slow

**Symptoms:**
- 10+ minutes per 25K row chunk
- CloudWatch shows long "Fetch from Snowflake" times
- ETA showing days instead of hours

**Diagnosis:**
```sql
-- Check Snowflake warehouse size
SHOW WAREHOUSES LIKE 'your_warehouse';

-- Check query execution time
SELECT query_text, execution_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text LIKE '%FACTVISIT%'
ORDER BY start_time DESC
LIMIT 10;
```

**Solutions (in priority order):**
1. **Upgrade Snowflake warehouse:**
   ```sql
   ALTER WAREHOUSE migration_wh SET WAREHOUSE_SIZE = 'MEDIUM';
   ```
   **Impact:** 3-5 min → 45-60 sec per chunk (80% faster)

2. **Reduce batch_size:**
   ```json
   {
     "batch_size": 25000  // From 50000
   }
   ```
   **Impact:** Faster Snowflake queries, more chunks

3. **Increase parallel_threads:**
   ```json
   {
     "parallel_threads": 15  // From 10
   }
   ```
   **Impact:** More concurrent chunks (watch memory)

4. **Check for slow queries in PostgreSQL:**
   ```sql
   SELECT query, mean_exec_time, calls
   FROM pg_stat_statements
   WHERE query LIKE '%INSERT%'
   ORDER BY mean_exec_time DESC;
   ```

See [OPTIMIZATION.md](OPTIMIZATION.md) for detailed tuning guide.

---

### Chunks Taking Too Long

**Symptoms:**
- Single chunk > 5 minutes
- "Fetch from Snowflake" > 2 minutes

**Diagnosis:**
- Check Snowflake query profile for full table scans
- Verify chunking column has clustering/indexing

**Solutions:**
- ✅ Add clustering key in Snowflake:
  ```sql
  ALTER TABLE fact_table CLUSTER BY (date_column);
  ```
- ✅ Use date-based chunking instead of ID-based
- ✅ Reduce batch_size if queries still slow

---

## Memory Issues

### Out of Memory (OOM)

**Symptoms:**
```
Error Type: Runtime.OutOfMemory
Max Memory Used: 6144 MB (at limit)
```

**Diagnosis:**
```
# Check CloudWatch REPORT lines
REPORT: Memory Size: 6144 MB Max Memory Used: 6144 MB

# Calculate memory usage
parallel_threads × batch_size × columns × avg_row_size
```

**Solutions (in order of preference):**

1. **Reduce parallel_threads:**
   ```json
   {
     "parallel_threads": 8  // From 15
   }
   ```
   **Expected memory reduction:** 7.5GB → 4GB

2. **Reduce batch_size:**
   ```json
   {
     "batch_size": 10000  // From 25000
   }
   ```
   **Expected memory reduction:** 40% per chunk

3. **Increase Lambda memory:**
   ```bash
   aws lambda update-function-configuration \
     --function-name snowflake-postgres-migration \
     --memory-size 10240 \
     --region us-east-1
   ```

**Memory Calculation:**
```
Safe configuration:
  Lambda Memory: 10GB
  Parallel Threads: 20
  Expected Usage: 20 × 300MB = 6GB (60% utilization) ✅

Unsafe configuration:
  Lambda Memory: 6GB
  Parallel Threads: 15
  Expected Usage: 15 × 500MB = 7.5GB (125% utilization) ❌
```

See [OPTIMIZATION.md](OPTIMIZATION.md) for memory tuning details.

---

### Memory Creeping Up

**Symptoms:**
- Memory usage increases over time
- Eventually hits OOM after hours

**Diagnosis:**
- Memory leak (rare)
- Large result sets being held in memory

**Solutions:**
- ✅ Deploy latest code (memory leaks fixed in v2.3)
- ✅ Reduce batch_size
- ✅ Force Lambda container refresh:
  ```bash
  aws lambda update-function-configuration \
    --function-name snowflake-postgres-migration \
    --environment "Variables={MIGRATION_VERSION=refresh,...}" \
    --region us-east-1
  ```

---

## Data Issues

### Missing Rows After Completion

**Symptoms:**
- All chunks show "completed"
- Row count doesn't match source
- No errors in logs

**Diagnosis:**
```sql
-- Compare counts
-- Source (Snowflake)
SELECT COUNT(*) FROM snowflake_table;

-- Target (PostgreSQL)
SELECT COUNT(*) FROM postgres_table;

-- Check for failed chunks
SELECT * FROM migration_status.migration_chunk_status
WHERE status = 'failed';

-- Check ID distribution (for ID-based chunking)
SELECT 
    MIN(id) as min_id,
    MAX(id) as max_id,
    COUNT(*) as row_count,
    MAX(id) - MIN(id) + 1 as id_span,
    ROUND((MAX(id) - MIN(id) + 1.0) / COUNT(*), 2) as sparsity
FROM source_table;
-- If sparsity > 10, IDs are sparse
```

**Root Cause:**
- Numeric ID-based chunking with sparse IDs
- Chunks created based on ID range, not actual data distribution
- Chunks with no data show "completed" with 0 rows

**Solutions:**

**Option 1: Switch to Date-Based Chunking (Recommended)**
```json
{
  "chunking_columns": ["created_date"],
  "chunking_column_types": ["timestamp"]
}
```

**Option 2: Use source_filter for Missing Data**
```sql
-- Find max ID in target
SELECT MAX(id) FROM target_table;
-- Returns: 5,099,999
```

```json
{
  "source_filter": "id > 5099999",
  "truncate_onstart": false,
  "insert_only_mode": true
}
```

**Option 3: Full Reload**
```json
{
  "truncate_onstart": true,
  "source_filter": null
}
```

See [.context/HISTORICAL_ISSUES.md](../.context/HISTORICAL_ISSUES.md) Issue #5 for detailed analysis.

---

### Duplicate Rows

**Symptoms:**
- More rows in target than source
- Duplicate primary key errors

**Root Cause:**
- Resume without proper truncation protection
- Multiple migrations to same target
- Failed resume with new run_id

**Solutions:**
- ✅ Check for multiple run_ids:
  ```sql
  SELECT run_id, COUNT(*) 
  FROM migration_status.migration_table_status
  WHERE source_table = 'YOUR_TABLE'
  GROUP BY run_id;
  ```

- ✅ Use explicit resume_run_id:
  ```json
  {
    "resume_run_id": "correct-run-id-here"
  }
  ```

- ✅ Clean and restart:
  ```sql
  TRUNCATE TABLE target_schema.target_table;
  DELETE FROM migration_status.migration_table_status WHERE run_id = 'old-run-id';
  ```

---

## Resume Problems

### Resume Window Expired (New Run Created)

**Symptoms:**
- Migration was running for > 7 days
- New `run_id` created unexpectedly
- Logs show "NO RESUMABLE RUN FOUND"
- Migration starts from scratch despite partial data

**Cause:**
Default `resume_max_age` is 168 hours (7 days). Runs older than this are not auto-resumed.

**Solution:**

**Option 1: Start with extended window (before migration):**
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT_ID:stateMachine:migration-analytics \
  --input '{"source_name":"analytics","resume_max_age":8760}' \
  --region us-east-1
```

**Option 2: Resume specific run (if already expired):**
```bash
# Find your run_id
psql -d conflict_management -c "
  SELECT run_id, started_at, status 
  FROM migration_status.migration_runs 
  WHERE status = 'running' 
  ORDER BY started_at DESC LIMIT 5;
"

# Resume it explicitly
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT_ID:stateMachine:migration-analytics \
  --input '{"source_name":"analytics","resume_run_id":"your-run-id-here"}' \
  --region us-east-1
```

**Option 3: Extend expiration for running migration (emergency fix):**
```sql
-- Reset the clock (only if > 7 days old)
UPDATE migration_status.migration_runs
SET started_at = NOW()
WHERE status = 'running'
  AND NOW() - started_at > INTERVAL '7 days'
  AND metadata->>'source_names' LIKE '%analytics%'
RETURNING run_id, 'Clock reset' as message;
```

**Prevention:**
- For large migrations (> 7 days), start with `resume_max_age: 8760` (1 year)
- Monitor run age using: `SELECT NOW() - started_at FROM migration_status.migration_runs WHERE status = 'running'`

---

### Won't Resume After Config Change

**Symptoms:**
- Changed config.json
- Step Function creates new run instead of resuming
- Logs show: "NO RESUMABLE RUN FOUND"

**Root Cause:**
Config change alters `config_hash`, breaking resume detection

**Solution:**
Use explicit `resume_run_id`:
```json
{
  "action": "migrate",
  "source_name": "analytics",
  "resume_run_id": "ad27ffdc-d104-4ccf-a2bc-9bc31e93d43c"
}
```

**Get run_id:**
```sql
SELECT run_id, started_at, status
FROM migration_status.migration_runs
ORDER BY started_at DESC
LIMIT 5;
```

---

### Resume Starts Fresh (Unwanted)

**Symptoms:**
- Resume expected but table gets truncated
- Data loss
- Logs show: "TRUNCATION SAFETY CHECK TRIGGERED"

**Root Cause:**
- Truncation protection activated
- Target table has data but no status record
- Resume detection failed

**What to do:**
1. **Check if data exists:**
   ```sql
   SELECT COUNT(*) FROM target_schema.target_table;
   ```

2. **If data is from failed run:**
   ```sql
   -- Clear partial data
   TRUNCATE TABLE target_schema.target_table;
   
   -- Clear status
   DELETE FROM migration_status.migration_table_status 
   WHERE run_id = 'failed-run-id';
   ```

3. **Start fresh:**
   ```json
   {
     "action": "migrate",
     "source_name": "analytics",
     "no_resume": true
   }
   ```

---

## Configuration Errors

### Validation Fails

**Symptoms:**
```
Error: source_name is required
Error: chunking_columns must be an array
Error: parallel_threads must be between 1 and 20
```

**Solution:**
Run validation first:
```bash
python scripts/lambda_handler.py validate_config
```

Fix errors shown in output.

---

### Table Not Found

**Symptoms:**
```
Error: Table SOURCE_TABLE does not exist
```

**Diagnosis:**
- Check case sensitivity (Snowflake uses uppercase)
- Verify table exists in specified schema
- Check permissions

**Solution:**
```sql
-- Verify table exists
SHOW TABLES LIKE 'SOURCE_TABLE' IN SCHEMA schema_name;

-- Check permissions
SHOW GRANTS ON TABLE schema_name.SOURCE_TABLE;
```

---

### Column Type Mismatch

**Symptoms:**
```
Error: column "date_column" is of type timestamp without time zone but expression is of type text
```

**Solution:**
Check column types match between source and target:
```sql
-- Snowflake
DESC TABLE schema_name.table_name;

-- PostgreSQL
\d schema_name.table_name
```

Fix mismatches in PostgreSQL schema.

---

## AWS/Lambda Issues

### Lambda Timeout

**Symptoms:**
```
Task timed out after 900.00 seconds
```

**Expected Behavior:**
- This is NORMAL for large migrations
- Lambda times out every 15 minutes
- Step Functions automatically retries
- Migration resumes from checkpoint

**Only a Problem If:**
- No progress after multiple timeouts
- Same chunks fail repeatedly
- No status updates in database

**Solutions:**
- ✅ Check CloudWatch for actual errors
- ✅ Verify chunks completing before timeout:
  ```sql
  SELECT 
      completed_chunks,
      EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))/60 as minutes
  FROM migration_status.migration_table_status
  WHERE status = 'in_progress';
  ```
- ✅ If no progress, check for stuck chunks

---

### Step Functions Quota Exceeded

**Symptoms:**
```
Error: ExecutionLimitExceeded
```

**Cause:**
Too many concurrent Step Function executions

**Solution:**
- Request quota increase from AWS
- Or wait for other executions to complete

---

### VPC Issues

**Symptoms:**
```
Error: Task timed out after 30.00 seconds
Error: Unable to connect to database
```

**Diagnosis:**
1. Lambda in VPC but no NAT gateway
2. Security group doesn't allow outbound
3. Route table missing routes

**Solution:**
```bash
# Check Lambda VPC config
aws lambda get-function-configuration \
  --function-name snowflake-postgres-migration \
  --query 'VpcConfig'

# Verify:
# - SubnetIds are private subnets with NAT
# - SecurityGroupIds allow outbound 5432, 443
```

---

## When to Check Historical Issues

### Check `.context/HISTORICAL_ISSUES.md` if:

1. **Missing rows after completed migration**
   → See Issue #5 (Sparse ID distribution)

2. **Migration "completed" but row counts don't match**
   → See Issue #5 (Numeric chunking problems)

3. **Run status stuck in "running"**
   → See Issue #4 (Status persistence bug) - Fixed in v2.1

4. **Every log entry appears twice**
   → See Issue #1 (Duplicate logging) - Fixed in v2.3

5. **Chunking taking 10+ minutes**
   → See Issue #2 (Chunking optimization) - Fixed in v2.2/2.3

6. **Out of memory with 15 threads**
   → See Issue #3 (Memory tuning) - Fixed in v2.2

---

## Getting More Help

### Diagnostic Checklist

Before asking for help, gather:
- [ ] CloudWatch logs (last 1-2 hours)
- [ ] Lambda memory usage (REPORT lines)
- [ ] Current configuration (config.json snippet)
- [ ] Migration status query results
- [ ] Row count comparison (source vs target)
- [ ] Failed chunks (if any)

### Useful Debug Queries

**See `sql/diagnose_stuck_migration.sql` for:**
- Run status and progress
- Failed chunks with errors
- Performance metrics
- Chunk distribution analysis

**See [MONITORING.md](MONITORING.md) for:**
- Real-time progress tracking
- Performance dashboards
- CloudWatch analysis

---

**For performance optimization, see [OPTIMIZATION.md](OPTIMIZATION.md)**  
**For monitoring progress, see [MONITORING.md](MONITORING.md)**  
**For deployment issues, see [DEPLOYMENT.md](DEPLOYMENT.md)**
