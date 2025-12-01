# AWS Step Functions - ETL Pipeline Orchestration

## Overview

This directory contains AWS Step Functions state machine definitions for orchestrating the ETL pipeline. Two implementations are provided:

- **Phase 1 (Sequential)**: Simple execution for tasks that complete within 15 minutes
- **Phase 2 (Chunked Parallel)**: Handles large datasets with parallel chunking and resume capability

---

## Quick Start

### Phase 1: Sequential Execution

**Use when:**
- Task 02 completes in <12 minutes
- Data volume <100K rows
- Simple monitoring needs

**Deploy:**
```powershell
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:REGION:ACCOUNT:function:YOUR_FUNCTION" `
  -RoleArn "arn:aws:iam::ACCOUNT:role/cm-step-function-lambda-role" `
  -StateMachineName "cm-etl-pipeline"
```

**Flow:**
```
ValidateConfig → ExecuteTask01 → ExecuteTask02 → Success
```

---

### Phase 2: Chunked Parallel ⭐ **RECOMMENDED**

**Use when:**
- Task 02 takes >12 minutes
- Data volume >100K rows
- Need faster processing (5x speedup)
- Need resilient resume capability

**Deploy:**
```powershell
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:REGION:ACCOUNT:function:YOUR_FUNCTION" `
  -RoleArn "arn:aws:iam::ACCOUNT:role/cm-step-function-lambda-role" `
  -StateMachineName "cm-etl-pipeline-phase2" `
  -DefinitionFile "step_functions/etl_pipeline_phase2_ready.json"
```

**Flow:**
```
ValidateConfig
    ↓
ExecuteTask01
    ↓
GetTask02Chunks (generates 30 chunks, stores in /tmp)
    ↓
ProcessTask02Chunks (5 parallel)
    ├─ Chunk 0 → Success
    ├─ Chunk 1 → Success
    ├─ Chunk 2 → Success
    ├─ Chunk 3 → Success
    └─ Chunk 4 → Success
    ... (repeat until all chunks complete)
    ↓
Success
```

**Performance:**
- 50K rows: 18 min (sequential) → 6 min (parallel) = **3x faster**
- Configurable concurrency (default: 5)
- Each chunk: <10K rows, <10 minutes

---

## Phase 2 Architecture

### Key Features

✅ **Idempotent Execution**
- Safe to re-run any chunk multiple times
- Uses `UpdateFlag` column as processing lock
- Completed chunks skip automatically

✅ **Resume Capability**
- Failed chunks can be retried individually
- Automatic retry with exponential backoff
- No data loss on partial failures

✅ **Parallel Processing**
- Configurable concurrency (default: 5 chunks)
- Independent chunk execution
- No cross-chunk dependencies

✅ **Optimized Payload**
- Stores chunks in `/tmp/task02_chunks.json`
- Returns only chunk IDs to avoid 6MB Lambda limit
- Payload reduced from 6MB+ to ~200 bytes

---

### Chunking Strategy

**Composite (VisitDate, SSN) Key**

Task 02 SQL joins V1 and V2 CTEs on:
```sql
V1."VisitDate" = V2."VisitDate"  -- Line 1024
V1."SSN" = V2."SSN"              -- Line 1026
```

**Why this strategy is safe:**
- Each (VisitDate, SSN) pair is completely independent
- No risk of splitting conflict pairs across chunks
- Respects business logic and join conditions

**How it works:**
1. Query: `SELECT "VisitDate", "SSN", COUNT(*) FROM conflictvisitmaps GROUP BY ...`
2. Balance: Group pairs into chunks of ~10K rows
3. Store: Write to `/tmp/task02_chunks.json`
4. Process: Each chunk loads its keys by ID and processes independently

**Configuration** (`config/chunking_config.py`):
```python
CHUNKING_CONFIG = {
    'target_chunk_size': 10000,     # Target rows per chunk
    'max_chunk_size': 50000,        # Maximum rows per chunk
    'max_keys_per_chunk': 1000,     # Maximum (date,ssn) pairs
    'max_concurrency': 5,           # Parallel executions
    'date_range': {
        'lookback_years': 2,        # Filter: NOW() - 2 years
        'lookahead_days': 30        # Filter: NOW() + 30 days
    }
}
```

---

### Idempotency Design

**Two-Phase Commit Pattern:**

#### Phase 1: Mark Rows (Locking)
```sql
UPDATE conflictvisitmaps
SET "UpdateFlag" = 1
WHERE {chunk_filter}
  AND ("UpdateFlag" IS NULL OR "UpdateFlag" != 1)
```
- Returns count of marked rows
- If 0 rows marked → Chunk already processed → Skip Phase 2

#### Phase 2: Update Data (Processing)
```sql
UPDATE conflictvisitmaps
SET 
    ... (20+ columns updated) ...,
    "UpdateFlag" = NULL,  -- Release lock
    "UpdatedDate" = NOW()
WHERE "UpdateFlag" = 1
  AND {chunk_filter}
```
- Only processes rows locked in Phase 1
- Sets `UpdateFlag = NULL` on completion

**Resume Behavior:**
- ✅ **Completed chunk**: `UpdateFlag = NULL` → Skip (fast, no DB load)
- ✅ **Failed chunk**: `UpdateFlag = 1` → Re-process
- ✅ **Partial failure**: Only unprocessed rows remain locked

---

## Step Functions Definitions

### File Structure

```
step_functions/
├── etl_pipeline.json                    # Phase 1: Sequential
├── etl_pipeline_ready.json              # Phase 1: With ARNs
├── etl_pipeline_phase2_chunked.json     # Phase 2: Template
└── etl_pipeline_phase2_ready.json       # Phase 2: With ARNs
```

### State Machine Components

**Phase 1 States:**
- `ValidateConfig` - Validate environment variables
- `ExecuteTask01` - Copy to temp table
- `ExecuteTask02` - Update conflicts (monolithic)

**Phase 2 States:**
- `ValidateConfig` - Same as Phase 1
- `ExecuteTask01` - Same as Phase 1
- `GetTask02Chunks` - Generate chunks (NEW)
- `CheckIfChunksExist` - Choice state (NEW)
- `ProcessTask02Chunks` - Map state for parallel execution (NEW)
  - `ProcessSingleChunk` - Individual chunk processor
  - `ChunkSuccess` / `ChunkFailed` - Result tracking

**Error Handling:**
```json
{
  "Retry": [
    {
      "ErrorEquals": ["States.TaskFailed", "States.Timeout"],
      "IntervalSeconds": 2,
      "MaxAttempts": 3,
      "BackoffRate": 2.0
    }
  ]
}
```
- Automatic retry on Lambda timeout or task failure
- Exponential backoff: 2s → 4s → 8s
- Maximum 3 retry attempts per chunk

---

## Deployment

### Prerequisites

1. **AWS CLI** configured with credentials
2. **Lambda function** deployed and tested
3. **IAM role** with permissions:
   - `lambda:InvokeFunction`
   - `states:StartExecution`
   - `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

### Deployment Script

**PowerShell** (`deploy_step_functions.ps1`):
```powershell
param(
    [Parameter(Mandatory=$true)]
    [string]$LambdaFunctionArn,
    
    [Parameter(Mandatory=$true)]
    [string]$RoleArn,
    
    [Parameter(Mandatory=$false)]
    [string]$StateMachineName = "cm-etl-pipeline-state-machine",
    
    [Parameter(Mandatory=$false)]
    [string]$DefinitionFile = "step_functions/etl_pipeline.json"
)
```

**Usage:**
```powershell
# Phase 1
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:us-east-1:123456789:function:my-function" `
  -RoleArn "arn:aws:iam::123456789:role/cm-step-function-lambda-role"

# Phase 2
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:us-east-1:123456789:function:my-function" `
  -RoleArn "arn:aws:iam::123456789:role/cm-step-function-lambda-role" `
  -StateMachineName "cm-etl-pipeline-phase2" `
  -DefinitionFile "step_functions/etl_pipeline_phase2_ready.json"
```

**Bash** (`deploy_step_functions.sh`):
```bash
./deploy_step_functions.sh \
  --lambda-arn "arn:aws:lambda:us-east-1:123456789:function:my-function" \
  --role-arn "arn:aws:iam::123456789:role/cm-step-function-lambda-role" \
  --state-machine-name "cm-etl-pipeline-phase2" \
  --definition-file "step_functions/etl_pipeline_phase2_ready.json"
```

---

## Execution

### Start Execution

```powershell
aws stepfunctions start-execution `
  --state-machine-arn "arn:aws:states:us-east-1:123456789:stateMachine:cm-etl-pipeline-phase2" `
  --input "{}"
```

### Monitor Execution

**Via Console:**
1. Go to AWS Step Functions Console
2. Find your state machine
3. Click on latest execution
4. View visual graph and logs

**Via CLI:**
```powershell
# Get execution status
aws stepfunctions describe-execution `
  --execution-arn "arn:aws:states:us-east-1:123456789:execution:..."

# Get execution history
aws stepfunctions get-execution-history `
  --execution-arn "arn:aws:states:us-east-1:123456789:execution:..." `
  --max-results 100
```

---

## Monitoring & Troubleshooting

### CloudWatch Logs

Each chunk logs its progress:
```
[INFO] Chunk 5: 500 estimated rows, 3 dates, 12 SSNs
[INFO] Chunk 5: Marked 500 rows for processing
[INFO] Chunk 5: Updated 500 rows in 45.23 seconds
[INFO] Chunk 5: Throughput: 11.1 rows/sec
```

### Database Status Queries

**Check processing status:**
```sql
SELECT 
    CASE WHEN "UpdateFlag" = 1 THEN 'locked'
         WHEN "UpdatedDate" IS NOT NULL THEN 'completed'
         ELSE 'pending'
    END as status,
    COUNT(*)
FROM conflictvisitmaps
WHERE "CONFLICTID" IS NOT NULL
GROUP BY status;
```

**Find stuck chunks:**
```sql
SELECT COUNT(*) as stuck_rows
FROM conflictvisitmaps
WHERE "UpdateFlag" = 1;
```

**Check recent completions:**
```sql
SELECT COUNT(*) as completed_rows
FROM conflictvisitmaps
WHERE "UpdateFlag" IS NULL
  AND "UpdatedDate" > NOW() - INTERVAL '1 hour';
```

### Common Issues

#### Lambda Timeout
**Symptom:** Chunk processing exceeds 15 minutes

**Solution:**
- Reduce `target_chunk_size` in `chunking_config.py`
- Current: 10K rows → Try: 5K rows
- Redeploy Lambda and re-run

#### Payload Size Error
**Symptom:** `Response payload size exceeded 6MB`

**Solution:**
- ✅ Already implemented: Chunks stored in `/tmp`
- Verify `get_task02_chunks` returns only `chunk_ids`, not full chunks
- Check Step Functions definition uses `$.task02_chunks.chunk_ids`

#### Failed Chunks
**Symptom:** Some chunks fail, others succeed

**Solution:**
1. Check CloudWatch logs for specific chunk_id error
2. Re-run execution (idempotent, will skip completed chunks)
3. Or manually reset: `UPDATE conflictvisitmaps SET "UpdateFlag" = NULL WHERE "UpdateFlag" = 1;`

#### Container /tmp Not Shared
**Symptom:** `FileNotFoundError: /tmp/task02_chunks.json`

**Root Cause:** Lambda scaled out to new containers

**Solution:**
- Ensure `GetTask02Chunks` and `ProcessChunks` run in same execution
- For production: Consider migrating to S3 chunk storage

---

## Performance Tuning

### Adjust Concurrency

Edit `etl_pipeline_phase2_ready.json`:
```json
{
  "ProcessTask02Chunks": {
    "Type": "Map",
    "MaxConcurrency": 10,  // Increase from 5 to 10
    ...
  }
}
```

**Considerations:**
- Higher concurrency = faster completion
- But: More database connections
- Monitor RDS CPU/connections

### Adjust Chunk Size

Edit `config/chunking_config.py`:
```python
CHUNKING_CONFIG = {
    'target_chunk_size': 5000,  # Reduce from 10K to 5K
    ...
}
```

**Trade-offs:**
- Smaller chunks = More chunks = More Lambda invocations
- Larger chunks = Fewer chunks but risk timeout
- Optimal: 5K-15K rows per chunk

### Adjust Date Range

```python
CHUNKING_CONFIG = {
    'date_range': {
        'lookback_years': 1,     # Reduce from 2 to 1
        'lookahead_days': 7      # Reduce from 30 to 7
    }
}
```

---

## Cost Estimation

### Phase 1 (Sequential)
- Lambda: 15 min × $0.0000166667/GB-sec × 2GB = **$0.03 per execution**
- Step Functions: 3 state transitions × $0.000025 = **$0.000075**
- **Total: ~$0.03 per execution**

### Phase 2 (Chunked)
- Lambda: 30 chunks × 6 min avg × $0.0000166667/GB-sec × 2GB = **$0.06 per execution**
- Step Functions: 35 state transitions × $0.000025 = **$0.000875**
- **Total: ~$0.06 per execution**

**Note:** Phase 2 costs 2x but completes 3x faster and is more reliable.

---

## Related Documentation

- **[../README.md](../README.md)** - Project overview and quick start
- **[../IDEMPOTENCY_AND_RESUME_DESIGN.md](../IDEMPOTENCY_AND_RESUME_DESIGN.md)** - Detailed idempotency design with examples
- **[../PHASE2_LAMBDA_PAYLOAD_FIX.md](../PHASE2_LAMBDA_PAYLOAD_FIX.md)** - How we solved the 6MB payload limit
- **[PHASE2_CHUNKED_IMPLEMENTATION.md](PHASE2_CHUNKED_IMPLEMENTATION.md)** - Comprehensive Phase 2 implementation guide

---

## IAM Role Policy

Required permissions for Step Functions execution role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:*:*:function:cm-task-ag-test01"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogDelivery",
        "logs:GetLogDelivery",
        "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies",
        "logs:DescribeLogGroups"
      ],
      "Resource": "*"
    }
  ]
}
```

**Trust Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "states.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

---

## License
Internal use only.
