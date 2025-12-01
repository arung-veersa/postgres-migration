# Phase 2: Lambda Payload Size Fix

## Problem

When implementing Phase 2 chunking, the `get_task02_chunks` Lambda function returned all chunk data (including thousands of (VisitDate, SSN) keys) in the response. With 30+ chunks, each containing 100-1000 keys, the response exceeded AWS Lambda's 6MB payload limit:

```
Response payload size exceeded maximum allowed payload size (6291556 bytes)
```

## Root Cause

The Step Functions Map state was configured to iterate over `$.task02_chunks.chunks`, where each chunk contained:
- `chunk_id`
- `keys`: Array of {date, ssn, rows} objects (could be 1000+ items)
- `estimated_rows`
- `date_range`
- Other metadata

Returning 30 chunks × 500 keys × ~50 bytes each = ~750KB minimum, plus JSON overhead = easily over 6MB.

## Solution

**Store chunks in filesystem, return only chunk IDs**

### Changes Made

#### 1. **task_02_get_chunks.py**
- Store full chunk data in `/tmp/task02_chunks.json` (Lambda) or `temp/task02_chunks.json` (local)
- Return only lightweight metadata:
  ```python
  {
    "num_chunks": 30,
    "chunk_ids": [0, 1, 2, ...],  # Just IDs, not full data
    "chunks_file": "/tmp/task02_chunks.json",
    "total_rows": 50000,
    ...
  }
  ```

#### 2. **task_02_process_chunk.py**
- Added `_load_chunk_keys(chunk_id)` method
- Modified `execute()` to accept optional `keys` parameter
- If `keys` not provided, loads from `/tmp/task02_chunks.json` automatically
- Lambda execution: Gets chunk_id → loads keys from file → processes

#### 3. **lambda_handler.py**
- `get_task02_chunks`: Returns metadata directly (not wrapped in statusCode/body)
- `process_task02_chunk`: Only requires `chunk_id` parameter (not `keys`)

#### 4. **etl_pipeline_phase2_*.json**
- `ItemsPath`: Changed from `$.task02_chunks.chunks` to `$.task02_chunks.chunk_ids`
- `Parameters`: Changed from `{"chunk_id.$": "$.chunk_id", "keys.$": "$.keys"}` to `{"chunk_id.$": "$"}`
- Each parallel Lambda invocation gets just an integer (0, 1, 2, ...) and loads its own keys

#### 5. **test_phase2_chunking.py**
- Updated to load chunks from file instead of expecting them in result
- Modified `test_single_chunk_processing()` and `test_idempotency()` to call `execute(chunk_id)` without keys

### Architecture

**Before:**
```
GetTask02Chunks Lambda
    ↓ (returns 6MB+ of chunk data)
Step Functions (stores in execution state)
    ↓ (passes each chunk with all keys)
ProcessChunk Lambda × 30
```

**After:**
```
GetTask02Chunks Lambda
    ↓ (writes chunks to /tmp/task02_chunks.json)
    ↓ (returns only [0,1,2,...,29])
Step Functions
    ↓ (passes just chunk_id: 0, 1, 2, ...)
ProcessChunk Lambda × 30
    ↓ (each reads /tmp/task02_chunks.json)
    ↓ (finds its chunk by ID)
    ↓ (processes)
```

### Benefits

1. **Payload size**: ~200 bytes (just metadata) instead of 6MB+
2. **Scalability**: Can handle 1000+ chunks without hitting payload limits
3. **Lambda filesystem reuse**: Multiple chunk processing invocations in the same Lambda container can reuse the cached chunks file
4. **Simplicity**: Step Functions definition is simpler (just passes integers)

### Trade-offs

- **Dependency**: ProcessChunk tasks depend on GetChunks having run first in the same execution
- **Container affinity**: If Lambda creates new containers, each reads the file (minor overhead)
- **Not suitable for long-running workflows**: If the Step Functions execution spans days, Lambda containers may be recycled

### Alternative Approaches Considered

1. **S3 Storage**: Store chunks in S3
   - Pros: Works across executions, persistent
   - Cons: Adds S3 dependency, latency, permissions, cost
   - Verdict: Overkill for this use case

2. **DynamoDB Storage**: Store chunks in DynamoDB
   - Pros: Fast, scalable, persistent
   - Cons: Adds DynamoDB dependency, cost, complexity
   - Verdict: Unnecessary for ephemeral data

3. **Distributed Map with S3**: Use Step Functions Distributed Map
   - Pros: Built for large-scale parallel processing
   - Cons: Requires S3, more complex setup
   - Verdict: Future enhancement if needed

## Testing

Run local tests:
```powershell
cd Scripts05/Migration
python scripts/test_phase2_chunking.py
```

Verify:
- Chunks are stored in `temp/task02_chunks.json`
- `get_task02_chunks` returns only chunk IDs
- `process_task02_chunk` can load keys from file and process successfully

## Deployment

1. **Build Lambda package**:
   ```powershell
   cd deploy
   .\build_lambda.ps1
   ```

2. **Upload to AWS Lambda** (via Console or CLI)

3. **Update Step Functions state machine**:
   ```powershell
   cd ..\aws
   # Via Console: Copy/paste etl_pipeline_phase2_ready.json
   # Or via script:
   .\deploy_step_functions.ps1 -StateMachineName "cm-etl-pipeline-phase2" ...
   ```

4. **Start execution**:
   ```powershell
   aws stepfunctions start-execution --state-machine-arn "..." --input "{}"
   ```

## Status

✅ **Fixed and tested locally**
⏳ **Pending AWS deployment and validation**

