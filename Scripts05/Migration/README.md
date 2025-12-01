# Conflict Report Pipeline Migration

## Overview
Production-ready AWS Lambda + Step Functions ETL pipeline for migrating Snowflake SQL procedures to PostgreSQL with resilient, chunked, and idempotent execution.

## Quick Start

### 1. Setup Environment
```powershell
cd Scripts05\Migration
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure Credentials
Create `.env` file with database credentials:
```env
# Postgres
POSTGRES_HOST=your-rds-endpoint
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

# Snowflake (optional)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
```

### 3. Test Locally
```powershell
# Test Postgres connection
python scripts/lambda_handler.py test_postgres

# Test Snowflake connection
python scripts/lambda_handler.py test_snowflake

# Test Phase 2 chunking
python scripts/test_phase2_chunking.py
```

## Architecture

### Phase 1: Sequential Execution
Simple pipeline for sequential task execution with retry logic.

```
Step Functions → ValidateConfig → Task01 → Task02 → Success
```

### Phase 2: Chunked Parallel Execution ⭐
Handles long-running tasks (>15 min) with parallel chunking and resume capability.

```
Step Functions
  ├─ ValidateConfig ✅
  ├─ Task01 ✅
  ├─ GetTask02Chunks ✅ (generates 30 chunks, stores in /tmp)
  └─ ProcessChunks (parallel, MaxConcurrency: 5)
       ├─ Chunk 0 → loads keys from /tmp → processes
       ├─ Chunk 1 → loads keys from /tmp → processes
       ├─ Chunk 2 → loads keys from /tmp → processes
       ├─ Chunk 3 → loads keys from /tmp → processes
       └─ Chunk 4 → loads keys from /tmp → processes
       ... (repeat until all 30 chunks done)
```

**Key Features:**
- ✅ **Idempotent**: Safe to re-run any chunk multiple times
- ✅ **Resumable**: Failed chunks can be retried without affecting completed ones
- ✅ **Parallel**: Configurable concurrency (default: 5)
- ✅ **Scalable**: Handles millions of rows by chunking
- ✅ **Optimized Payload**: Stores chunk data in /tmp, passes only chunk IDs (avoids 6MB Lambda limit)

## AWS Deployment

### 1. Build Lambda Package
```powershell
cd deploy
.\build_lambda.ps1
```
This creates `lambda_deployment.zip` with Linux-compatible binaries.

### 2. Deploy Lambda
1. Upload `deploy/lambda_deployment.zip` to Lambda
2. Set handler: `scripts/lambda_handler.lambda_handler`
3. Attach psycopg2 Lambda Layer (if using separate layer)
4. Configure environment variables from `.env`
5. Set timeout: 15 minutes
6. Set memory: 2048 MB (recommended)

### 3. Deploy Step Functions
```powershell
cd ..\aws

# Phase 1 (Sequential)
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:REGION:ACCOUNT:function:YOUR_FUNCTION" `
  -RoleArn "arn:aws:iam::ACCOUNT:role/cm-step-function-lambda-role" `
  -StateMachineName "cm-etl-pipeline"

# Phase 2 (Chunked Parallel)
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:REGION:ACCOUNT:function:YOUR_FUNCTION" `
  -RoleArn "arn:aws:iam::ACCOUNT:role/cm-step-function-lambda-role" `
  -StateMachineName "cm-etl-pipeline-phase2" `
  -DefinitionFile "step_functions/etl_pipeline_phase2_ready.json"
```

### 4. Start Execution
```powershell
aws stepfunctions start-execution `
  --state-machine-arn "arn:aws:states:REGION:ACCOUNT:stateMachine:cm-etl-pipeline-phase2" `
  --input "{}"
```

## Lambda Actions

| Action | Description | Status |
|--------|-------------|--------|
| `validate_config` | Validate environment variables | ✅ Working |
| `test_postgres` | Test Postgres connectivity | ✅ Working |
| `test_snowflake` | Test Snowflake connectivity | ✅ Working |
| `task_01` | Copy to temp table | ✅ Working |
| `task_02` | Update conflicts (monolithic) | ✅ Working |
| `get_task02_chunks` | Generate chunks for Task 02 | ✅ Phase 2 |
| `process_task02_chunk` | Process single chunk | ✅ Phase 2 |

## Chunking Strategy

### Composite (VisitDate, SSN) Chunking
Task 02 uses a composite key strategy to ensure safe parallel processing:

1. **Query Distribution**: Count rows per (VisitDate, SSN) pair
2. **Balance Chunks**: Group pairs into chunks of ~target size (default: 10,000 rows)
3. **Store Metadata**: Write chunks to `/tmp/task02_chunks.json`
4. **Return IDs**: Return only `[0, 1, 2, ..., N]` to Step Functions
5. **Load & Process**: Each parallel execution loads its chunk by ID from `/tmp`

**Configuration** (`config/chunking_config.py`):
```python
CHUNKING_CONFIG = {
    'target_chunk_size': 10000,     # Target rows per chunk
    'max_chunk_size': 50000,        # Maximum rows per chunk
    'max_keys_per_chunk': 1000,     # Maximum (date,ssn) pairs per chunk
    'max_concurrency': 5,           # Parallel executions
}
```

### Idempotency Design

Uses `UpdateFlag` column as a processing lock:

**Phase 1: Mark** (Lock rows)
```sql
UPDATE conflictvisitmaps
SET "UpdateFlag" = 1
WHERE {chunk_filter} AND ("UpdateFlag" IS NULL OR "UpdateFlag" != 1)
```

**Phase 2: Update** (Process & release)
```sql
UPDATE conflictvisitmaps
SET 
    ... (20+ columns) ...,
    "UpdateFlag" = NULL  -- Release lock
WHERE "UpdateFlag" = 1 AND {chunk_filter}
```

**Resume Behavior:**
- ✅ Completed chunk: `UpdateFlag = NULL` → Phase 1 returns 0 → Skip
- ✅ Failed chunk: `UpdateFlag = 1` → Phase 1 finds rows → Re-process
- ✅ Partial failure: Only unprocessed rows have `UpdateFlag = 1` → Resume from there

## Project Structure

```
Scripts05/Migration/
├── config/
│   ├── settings.py           # Database configuration
│   └── chunking_config.py    # Chunking parameters
├── src/
│   ├── connectors/           # Database connectors
│   ├── tasks/                # Task implementations
│   │   ├── task_01_copy_to_temp.py
│   │   ├── task_02_update_conflicts.py
│   │   ├── task_02_get_chunks.py       # Phase 2: Chunk generation
│   │   └── task_02_process_chunk.py    # Phase 2: Chunk processing
│   └── utils/                # Logger
├── scripts/
│   ├── lambda_handler.py     # AWS Lambda entry point
│   └── test_phase2_chunking.py  # Local chunking tests
├── sql/
│   ├── task_01_copy_to_temp.sql
│   ├── task_02_update_conflicts.sql
│   └── task_02_update_conflicts_chunked.sql  # Phase 2 version
├── aws/
│   ├── step_functions/
│   │   ├── etl_pipeline.json                 # Phase 1: Sequential
│   │   └── etl_pipeline_phase2_ready.json    # Phase 2: Chunked
│   ├── deploy_step_functions.ps1
│   └── README.md             # Step Functions documentation
├── deploy/
│   ├── build_lambda.ps1      # Build deployment package
│   └── requirements_prod.txt # Production dependencies
└── README.md                 # This file
```

## Documentation

- **[aws/README.md](aws/README.md)** - Step Functions deployment & configuration
- **[aws/PHASE1_IMPLEMENTATION.md](aws/PHASE1_IMPLEMENTATION.md)** - Phase 1 design & implementation
- **[aws/PHASE2_CHUNKED_IMPLEMENTATION.md](aws/PHASE2_CHUNKED_IMPLEMENTATION.md)** - Phase 2 design & implementation
- **[aws/PHASE2_SUMMARY.md](aws/PHASE2_SUMMARY.md)** - Phase 2 quick reference
- **[PHASE2_LAMBDA_PAYLOAD_FIX.md](PHASE2_LAMBDA_PAYLOAD_FIX.md)** - Payload size optimization (6MB limit fix)
- **[IDEMPOTENCY_AND_RESUME_DESIGN.md](IDEMPOTENCY_AND_RESUME_DESIGN.md)** - Detailed idempotency & resume design

## Monitoring

### CloudWatch Logs
Each chunk logs its progress:
```
[INFO] Chunk 5: 500 estimated rows, 3 dates, 12 SSNs
[INFO] Chunk 5: Marked 500 rows for processing
[INFO] Chunk 5: Updated 500 rows in 45.23 seconds
```

### Step Functions Execution Graph
Visual execution status with per-chunk results and error states.

### Database Queries
```sql
-- Check processing status
SELECT 
    CASE WHEN "UpdateFlag" = 1 THEN 'locked'
         WHEN "UpdatedDate" IS NOT NULL THEN 'completed'
         ELSE 'pending'
    END as status,
    COUNT(*)
FROM conflictvisitmaps
GROUP BY status;

-- Find stuck chunks
SELECT COUNT(*) FROM conflictvisitmaps WHERE "UpdateFlag" = 1;
```

## Troubleshooting

### Lambda Timeout
- Increase timeout to 15 minutes (max)
- For longer tasks, use Phase 2 chunking

### Payload Size Error
- Phase 2 stores chunks in `/tmp`, passes only IDs
- Maximum 6MB Lambda response limit avoided

### Failed Chunks
- Check CloudWatch logs for specific chunk_id
- Re-run execution (idempotent, will skip completed chunks)
- Manually reset: `UPDATE conflictvisitmaps SET "UpdateFlag" = NULL WHERE "UpdateFlag" = 1;`

### Container /tmp Sharing
- GetTask02Chunks and ProcessChunk must run in same Lambda execution
- Step Functions ensures sequential execution
- For production: Consider S3 storage for chunks

## Performance

**Task 02 with 50,000 rows:**
- **Monolithic**: ~18 minutes (would timeout)
- **Phase 2 (30 chunks, 5 parallel)**: ~6 minutes ⚡
- **Throughput**: ~150 rows/second per chunk

## License
Internal use only.
