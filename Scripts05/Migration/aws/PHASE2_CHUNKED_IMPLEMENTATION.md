# Phase 2: Comprehensive Implementation Guide

This document provides detailed implementation guidance for Phase 2 chunked parallel execution. For quick start and deployment, see [README.md](README.md).

---

## Table of Contents
1. [Implementation Checklist](#implementation-checklist)
2. [Detailed Code Walkthrough](#detailed-code-walkthrough)
3. [Testing Guide](#testing-guide)
4. [Production Deployment](#production-deployment)
5. [Advanced Scenarios](#advanced-scenarios)

---

## Implementation Checklist

### ✅ Phase 2 Components

**Python Code:**
- [x] `src/tasks/task_02_get_chunks.py` - Chunk generation
- [x] `src/tasks/task_02_process_chunk.py` - Chunk processing
- [x] `config/chunking_config.py` - Configuration
- [x] `sql/task_02_update_conflicts_chunked.sql` - Parameterized SQL

**AWS Infrastructure:**
- [x] `step_functions/etl_pipeline_phase2_chunked.json` - Template
- [x] `step_functions/etl_pipeline_phase2_ready.json` - Production
- [x] `deploy_step_functions.ps1` - Deployment script
- [x] `deploy_step_functions.sh` - Bash version

**Testing:**
- [x] `scripts/test_phase2_chunking.py` - Local testing
- [x] `scripts/lambda_handler.py` - Lambda actions added

**Documentation:**
- [x] `README.md` - Main AWS documentation
- [x] `../IDEMPOTENCY_AND_RESUME_DESIGN.md` - Idempotency details
- [x] `../PHASE2_LAMBDA_PAYLOAD_FIX.md` - Payload optimization
- [x] This file - Implementation guide

---

## Detailed Code Walkthrough

### 1. Chunk Generation (`task_02_get_chunks.py`)

**Flow:**
```python
execute()
  ↓
_get_distribution()  # Query (VisitDate, SSN, COUNT(*))
  ↓
_create_balanced_chunks()  # Group into ~10K row chunks
  ↓
_get_chunks_directory()  # /tmp (Lambda) or temp/ (local)
  ↓
Write to task02_chunks.json
  ↓
Return {num_chunks, chunk_ids, chunks_file}
```

**Key Methods:**

#### `_get_distribution()`
```python
query = f"""
    SELECT 
        "VisitDate"::date as visit_date,
        "SSN" as ssn,
        COUNT(*) as row_count
    FROM {CONFLICT_SCHEMA}.conflictvisitmaps
    WHERE "CONFLICTID" IS NOT NULL
      AND "VisitDate"::date BETWEEN 
          (NOW() - INTERVAL '2 years')::date 
          AND (NOW() + INTERVAL '30 days')::date
    GROUP BY "VisitDate"::date, "SSN"
    HAVING COUNT(*) > 0
    ORDER BY "VisitDate"::date, "SSN"
"""
```
- Returns list of {visit_date, ssn, row_count}
- Filters by date range to reduce chunk count
- Ordered by date, SSN for balanced distribution

#### `_create_balanced_chunks()`
```python
for row in distribution:
    date = row['visit_date']
    ssn = row['ssn']
    count = row['row_count']
    
    # Check if adding this key would exceed limits
    if (current_chunk_rows + count > max_size) and current_chunk_keys:
        # Finalize current chunk, start new one
        chunks.append(finalize_chunk(...))
        current_chunk_keys = []
        current_chunk_rows = 0
    
    # Add key to current chunk
    current_chunk_keys.append({'date': date, 'ssn': ssn, 'rows': count})
    current_chunk_rows += count
```
- Greedy algorithm: Fill each chunk until it would exceed max_size
- Balances: Target 10K rows, allow up to 50K rows per chunk
- Tracks: Keys per chunk (max 1000 to avoid SQL parameter limits)

---

### 2. Chunk Processing (`task_02_process_chunk.py`)

**Flow:**
```python
execute(chunk_id, keys=None)
  ↓
_load_chunk_keys(chunk_id)  # If keys not provided
  ↓
_mark_chunk_rows(keys)  # Phase 1: Lock rows
  ↓
if marked_rows == 0:
    return "already processed"  # Idempotent skip
  ↓
_update_chunk(keys)  # Phase 2: Process & release lock
  ↓
return {rows_marked, rows_updated, duration}
```

**Key Methods:**

#### `_mark_chunk_rows()`
```sql
UPDATE conflictvisitmaps
SET "UpdateFlag" = 1
WHERE "CONFLICTID" IS NOT NULL
  AND {keys_filter}
  AND ("UpdateFlag" IS NULL OR "UpdateFlag" != 1)
```
- Returns count of newly marked rows
- If 0: Chunk already processed → Skip Phase 2
- If >0: Proceed to Phase 2

#### `_update_chunk()`
```sql
-- Loads task_02_update_conflicts_chunked.sql
-- Injects: {conflict_schema}, {analytics_schema}, {chunk_filter}
-- Executes complex UPDATE with CTEs
-- Sets "UpdateFlag" = NULL on completion
```

#### `_build_keys_filter()`
```python
# Option 1: Small number of keys (<100)
# Use tuple IN clause
filter = '("VisitDate", "SSN") IN ((date1, ssn1), (date2, ssn2), ...)'

# Option 2: Large number of keys (>100)
# Use date range + SSN list
filter = '("VisitDate" BETWEEN start AND end) AND ("SSN" IN (ssn1, ssn2, ...))'
```

#### `_load_chunk_keys()`
```python
# Determine file location
if AWS_LAMBDA:
    chunks_file = '/tmp/task02_chunks.json'
else:
    chunks_file = 'temp/task02_chunks.json'

# Load and find chunk by ID
with open(chunks_file) as f:
    all_chunks = json.load(f)
    
for chunk in all_chunks:
    if chunk['chunk_id'] == chunk_id:
        return chunk['keys']
```

---

### 3. Lambda Handler Integration

**New Actions:**

```python
# GET_TASK02_CHUNKS
elif action == 'get_task02_chunks':
    connector = PostgresConnector(**POSTGRES_CONFIG)
    task = Task02GetChunks(connector)
    result = task.run()
    
    if result['status'] == 'success':
        chunk_data = result['result']
        return chunk_data  # Direct return for Step Functions
    else:
        raise Exception(f"Chunk generation failed: {result.get('error')}")

# PROCESS_TASK02_CHUNK
elif action == 'process_task02_chunk':
    chunk_id = event.get('chunk_id')
    if chunk_id is None:
        raise Exception("Missing required parameter: chunk_id")
    
    connector = PostgresConnector(**POSTGRES_CONFIG)
    task = Task02ProcessChunk(connector)
    result = task.execute(chunk_id=chunk_id)  # Keys loaded from /tmp
    
    if result['status'] == 'success':
        return result  # Direct return for Step Functions
    else:
        raise Exception(f"Chunk {chunk_id} failed: {result.get('error')}")
```

**Key Changes:**
- Returns data directly (not wrapped in `{statusCode, body}`)
- Raises exceptions for failures (Step Functions handles retries)
- `process_task02_chunk` only needs `chunk_id`, loads keys automatically

---

## Testing Guide

### Local Testing

**1. Test Chunk Generation:**
```powershell
cd Scripts05/Migration
python scripts/test_phase2_chunking.py
```

Select: `y` for chunk generation test

**Expected Output:**
```
✅ Chunk generation successful!

Summary:
  Total rows: 50,234
  Total (date, ssn) keys: 1,542
  Number of chunks: 30
  Chunks file: temp/task02_chunks.json
  Duration: 12.34s

Chunk IDs: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  ... and 20 more

First 5 chunks:
  Chunk 0: 2,145 rows, 45 keys, dates 2023-01-01 to 2023-01-15
  Chunk 1: 1,823 rows, 38 keys, dates 2023-01-16 to 2023-02-03
  ...
```

**2. Test Single Chunk Processing:**

Select: `y` for single chunk processing test

**Expected Output:**
```
✅ Chunk processing successful!

Results:
  Rows marked: 500
  Rows updated: 500
  Duration: 45.23s
  Throughput: 11.05 rows/sec
```

**3. Test Idempotency:**

Select: `y` for idempotency test

**Expected Output:**
```
Run 1:
  Rows updated: 500
  Duration: 45.23s

Run 2 (same chunk):
  Rows updated: 0
  Duration: 2.15s

✅ IDEMPOTENCY VERIFIED!
   Second run updated 0 rows (already processed)
   Safe to re-run failed chunks!
```

---

### AWS Testing

**1. Deploy Lambda:**
```powershell
cd deploy
.\build_lambda.ps1
# Upload lambda_deployment.zip to AWS
```

**2. Test Individual Actions:**

**Test chunk generation:**
```json
{
  "action": "get_task02_chunks"
}
```

**Expected Response:**
```json
{
  "num_chunks": 30,
  "chunk_ids": [0, 1, 2, ..., 29],
  "total_rows": 50234,
  "chunks_file": "/tmp/task02_chunks.json"
}
```

**Test chunk processing:**
```json
{
  "action": "process_task02_chunk",
  "chunk_id": 0
}
```

**Expected Response:**
```json
{
  "status": "success",
  "chunk_id": 0,
  "rows_marked": 2145,
  "rows_updated": 2145,
  "duration_seconds": 45.23
}
```

**3. Deploy Step Functions:**
```powershell
cd ..\aws
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:..." `
  -RoleArn "arn:aws:iam::..." `
  -StateMachineName "cm-etl-pipeline-phase2" `
  -DefinitionFile "step_functions/etl_pipeline_phase2_ready.json"
```

**4. Start Execution:**
```powershell
aws stepfunctions start-execution `
  --state-machine-arn "arn:aws:states:..." `
  --input "{}"
```

**5. Monitor:**
- AWS Console → Step Functions → Executions
- View visual graph
- Check CloudWatch logs per chunk

---

## Production Deployment

### Pre-Deployment Checklist

- [ ] Local testing passed (all 3 tests)
- [ ] Lambda package built with Docker (Linux binaries)
- [ ] Lambda timeout set to 15 minutes
- [ ] Lambda memory set to 2048 MB (recommended)
- [ ] Environment variables configured
- [ ] psycopg2 Lambda Layer attached
- [ ] IAM role has correct permissions
- [ ] Step Functions definition updated with correct ARNs
- [ ] Chunking config reviewed (`config/chunking_config.py`)

### Deployment Steps

**Step 1: Build Lambda Package**
```powershell
cd Scripts05/Migration/deploy
.\build_lambda.ps1

# Verify ZIP created
ls lambda_deployment.zip
```

**Step 2: Upload to AWS Lambda**
```powershell
# Via CLI
aws lambda update-function-code `
  --function-name cm-task-ag-test01 `
  --zip-file fileb://lambda_deployment.zip

# Or via Console: Upload ZIP manually
```

**Step 3: Update Lambda Configuration**
```powershell
aws lambda update-function-configuration `
  --function-name cm-task-ag-test01 `
  --timeout 900 `
  --memory-size 2048
```

**Step 4: Deploy Step Functions**
```powershell
cd ..\aws
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:us-east-1:354073143602:function:cm-task-ag-test01" `
  -RoleArn "arn:aws:iam::354073143602:role:cm-step-function-lambda-role" `
  -StateMachineName "cm-etl-pipeline-phase2" `
  -DefinitionFile "step_functions/etl_pipeline_phase2_ready.json"
```

**Step 5: Test Execution**
```powershell
aws stepfunctions start-execution `
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:cm-etl-pipeline-phase2" `
  --input "{}"
```

**Step 6: Verify Results**
```sql
-- Check processing status
SELECT 
    CASE WHEN "UpdateFlag" = 1 THEN 'locked'
         WHEN "UpdatedDate" > NOW() - INTERVAL '1 hour' THEN 'completed'
         ELSE 'pending'
    END as status,
    COUNT(*)
FROM conflictvisitmaps
WHERE "CONFLICTID" IS NOT NULL
GROUP BY status;
```

---

## Advanced Scenarios

### Scenario 1: Partial Execution Failure

**Situation:** Execution fails after processing 15/30 chunks

**Recovery:**
```powershell
# Option A: Re-run entire execution (recommended)
aws stepfunctions start-execution \
  --state-machine-arn "..." \
  --input "{}"
  
# Chunks 0-14: Will skip (UpdateFlag = NULL)
# Chunks 15-29: Will process normally
```

```sql
-- Option B: Manual inspection
SELECT "UpdateFlag", COUNT(*) 
FROM conflictvisitmaps 
WHERE "CONFLICTID" IS NOT NULL
GROUP BY "UpdateFlag";

-- If needed, reset stuck locks
UPDATE conflictvisitmaps 
SET "UpdateFlag" = NULL 
WHERE "UpdateFlag" = 1;
```

### Scenario 2: Database Connection Limits

**Situation:** Too many parallel chunks causing connection exhaustion

**Solution:**
```json
// Reduce MaxConcurrency in state machine
{
  "ProcessTask02Chunks": {
    "MaxConcurrency": 3,  // Down from 5
    ...
  }
}
```

### Scenario 3: Uneven Chunk Distribution

**Situation:** Some chunks take 2 min, others take 12 min

**Analysis:**
```python
# Check chunk distribution
with open('temp/task02_chunks.json') as f:
    chunks = json.load(f)
    
for chunk in chunks:
    print(f"Chunk {chunk['chunk_id']}: {chunk['estimated_rows']} rows")
```

**Solution:**
```python
# Adjust target_chunk_size
CHUNKING_CONFIG = {
    'target_chunk_size': 5000,  # Smaller chunks
    'max_chunk_size': 10000,    # Tighter limit
    ...
}
```

### Scenario 4: Very Large SSN Groups

**Situation:** Single SSN has 100K rows, exceeds max_chunk_size

**Current Behavior:** Entire SSN goes into one chunk (may timeout)

**Future Enhancement:** Split large SSN groups by date ranges
```python
# To be implemented
if single_ssn_rows > max_chunk_size:
    split_by_date_ranges(ssn, dates)
```

**Workaround:** Manually process large SSNs separately

---

## Performance Metrics

### Typical Execution (50K rows)

**Phase 1 (Sequential):**
- ValidateConfig: 5s
- Task01: 180s
- Task02: 1080s (18 min)
- **Total: ~20 minutes**

**Phase 2 (Chunked, 5 parallel):**
- ValidateConfig: 5s
- Task01: 180s
- GetTask02Chunks: 15s
- ProcessChunks (30 chunks, 5 at a time):
  - Batch 1 (chunks 0-4): 360s (parallel)
  - Batch 2 (chunks 5-9): 360s
  - ... (6 batches total)
  - Average: 360s per batch × 6 batches = ~360s
- **Total: ~6 minutes**

**Speedup: 3.3x faster**

---

## Related Documentation

- **[README.md](README.md)** - Main AWS documentation and quick start
- **[../IDEMPOTENCY_AND_RESUME_DESIGN.md](../IDEMPOTENCY_AND_RESUME_DESIGN.md)** - Idempotency design with detailed scenarios
- **[../PHASE2_LAMBDA_PAYLOAD_FIX.md](../PHASE2_LAMBDA_PAYLOAD_FIX.md)** - How we solved the 6MB payload limit
- **[../README.md](../README.md)** - Project overview

---

## License
Internal use only.
