# Codebase Map - Where to Find What

Quick reference guide to the codebase structure and where specific functionality lives.

---

## Core Migration Logic

### `migrate.py` (1,169 lines)
**Main orchestrator and migration execution**

**Key Classes:**
- `MigrationOrchestrator` - Main orchestration class

**Key Methods:**
- `_process_table()` (lines 471-761) - Table processing with truncation safety checks
- `run_single_source()` (lines 1122-1169) - Processes all tables for a source
- `truncate_table()` (lines 991-1008) - Executes TRUNCATE CASCADE

**What to look for here:**
- Truncation safety logic
- Table-level orchestration
- Index management integration
- Error handling and retry logic

---

### `lib/migration_worker.py` (1,080 lines)
**Core chunk processing and data loading**

**Key Classes:**
- `MigrationWorker` - Handles individual chunk migration

**Key Methods:**
- `__init__()` (lines 22-80) - Handles truncate_onstart logic, watermark nulling
- `process_chunk()` - Main chunk processing entry point
- `_build_fetch_query()` (lines 457-566) - Constructs Snowflake SELECT with filtering
- `_load_to_postgres()` (lines 721-766) - COPY vs UPSERT decision logic
- `_fetch_from_snowflake()` - Fetches data from Snowflake
- `_filter_columns_for_target()` (lines 867-874) - Excludes columns not in target

**What to look for here:**
- COPY vs UPSERT mode selection
- Watermark filtering logic
- Chunk query construction
- Data transformation

**Recent Changes:**
- Column exclusion logging (reduced to once per table)

---

### `lib/chunking.py` (685 lines)
**Chunking strategy implementations**

**Key Classes:**
- `ChunkingStrategyFactory` - Creates appropriate strategy
- `OffsetBasedStrategy` - Simple LIMIT/OFFSET chunking
- `DateRangeStrategy` - Date-based chunking (OPTIMIZED in v2.3)
- `UUIDRangeStrategy` - UUID-based chunking

**Key Methods:**
- `DateRangeStrategy.create_chunks()` - Creates date-based chunks
  - **v2.3 optimization here:** Single aggregated query (lines ~300-350)

**What to look for here:**
- Chunking algorithm selection
- Date aggregation query (performance critical)
- Sub-chunking logic for large dates

**Recent Changes:**
- v2.3: Replaced multiple COUNT queries with single aggregated query
- Added timing logs for chunk creation

---

### `lib/connections.py` (441 lines)
**Database connection management**

**Key Classes:**
- `SnowflakeConnectionManager` - Manages Snowflake connections
- `PostgresConnectionManager` - Manages PostgreSQL connections

**Key Methods:**
- `SnowflakeConnectionManager.connect()` (lines 67-91) - Snowflake connection setup
- `PostgresConnectionManager.get_connection()` (lines 188-227) - PostgreSQL connection
  - **v2.3 optimization here:** Session-level performance settings

**What to look for here:**
- Connection pooling (intentionally simple, no pooling)
- Snowflake session parameters
- PostgreSQL session optimizations

**Recent Changes:**
- v2.3: Added `synchronous_commit=off` and session memory settings

---

### `lib/status_tracker.py` (579 lines)
**Migration status tracking in PostgreSQL**

**Key Classes:**
- `StatusTracker` - Manages status tables

**Key Methods:**
- `create_run()` - Creates new migration run
- `create_table_status()` - Creates table status record
- `create_chunk_status()` - Creates chunk status record
- `find_resumable_run()` (lines 331-449) - Resume detection logic
- `get_pending_chunks()` - Gets chunks to process

**What to look for here:**
- Resume detection algorithm
- Config hash and execution hash logic
- Status update mechanisms

**Tables Managed:**
- `migration_status.migration_runs`
- `migration_status.migration_table_status`
- `migration_status.migration_chunk_status`

---

### `lib/utils.py` (326 lines)
**Utility functions and logging setup**

**Key Functions:**
- `setup_logging()` (lines 14-51) - Full logging setup
- `get_logger()` (lines 54-88) - Get/create logger instance
  - **v2.3 fix here:** Lambda environment detection to prevent duplicates
- `format_number()` - Number formatting
- `format_duration()` - Duration formatting
- `Timer` class - Context manager for timing operations

**What to look for here:**
- Logger configuration
- Lambda vs local environment detection
- Utility formatters

**Recent Changes:**
- v2.3: Added `os` import
- v2.3: Lambda environment detection (`AWS_EXECUTION_ENV`)
- v2.3: Prevents adding handler in Lambda (stops duplicates)

---

### `lib/config_loader.py`
**Configuration loading and parsing**

**What to look for here:**
- Config.json parsing
- Default value handling
- Source/table filtering

---

### `lib/index_manager.py`
**PostgreSQL index management**

**Key Methods:**
- `disable_indexes()` - Disables indexes for faster bulk loads
- `restore_indexes()` - Re-enables indexes after load

---

## AWS Integration

### `scripts/lambda_handler.py` (315 lines)
**AWS Lambda entry point**

**Key Functions:**
- `lambda_handler()` (lines 37-182) - Main Lambda handler
  - Handles: validate_config, test_connections, migrate actions
- `local_main()` - Local testing entry point

**What to look for here:**
- Lambda timeout detection
- Action routing (migrate vs validate vs test)
- Context extraction from Step Functions

**Recent Changes:**
- v2.2: Removed duplicate "Lambda invoked" log

---

### `scripts/migration_orchestrator.py` (273 lines)
**Orchestrates migration runs**

**Key Functions:**
- `run_migration()` (lines 13-63) - Main orchestration function
- Resume detection logic (lines 171-273)

**What to look for here:**
- Run ID creation
- Resume vs new run detection
- Config hash calculation
- Execution hash calculation

---

## Configuration

### `config.json` (746 lines)
**Main configuration file**

**Structure:**
```json
{
  "parallel_threads": 20,
  "batch_size": 25000,
  "sources": [
    {
      "source_name": "analytics",
      "tables": [...]
    }
  ]
}
```

**Key Sections:**
- Global defaults (lines 2-4)
- Source configurations (lines 5-740)
- Table-specific overrides (per table)

**What to look for here:**
- Thread and batch size settings
- Watermark columns
- Chunking columns
- truncate_onstart and insert_only_mode flags

**Recent Changes:**
- v2.3: parallel_threads: 15 → 20
- v2.3: batch_size remains 25000
- v2.2: parallel_threads: 3 → 15
- v2.2: batch_size: 50000 → 25000

---

## AWS Deployment

### `aws/step_functions/`
**Step Function definitions**

**Files:**
- `migration_workflow_analytics.json` - Analytics source workflow
- `migration_workflow_conflict.json` - Conflict source workflow

**What to look for here:**
- Lambda invocation parameters
- Retry policies
- Timeout handling
- Task sequencing

---

### `deploy/`
**Deployment scripts**

**Files:**
- `rebuild_app_only.ps1` - Rebuilds Lambda application code
- `rebuild_layer.ps1` - Rebuilds dependencies layer
- `requirements_layer.txt` - Python dependencies

**What to look for here:**
- Package building process
- Dependency management
- Layer creation

---

## SQL Scripts

### `sql/`
**Database scripts and queries**

**Files:**
- `migration_status_schema.sql` - Creates status tracking tables
- `diagnose_stuck_migration.sql` - Diagnostic queries
- `fix_stuck_run.sql` - Fix utilities
- `QUICK_MONITORING.sql` - Quick status queries

---

## Documentation

### `.context/` (Developer Context)
- `PROJECT_CONTEXT.md` - Current state, recent changes
- `SESSION_HANDOFF_TEMPLATE.md` - Session transition template
- `CODEBASE_MAP.md` - This file
- `TODO.md` - Task tracking
- `DECISIONS.md` - Architectural decisions
- `HISTORICAL_ISSUES.md` - Resolved bugs

### `docs/` (User Documentation)
- `QUICKSTART.md` - 5-minute setup guide
- `CONFIGURATION.md` - Config reference
- `OPTIMIZATION.md` - Performance tuning
- `MONITORING.md` - Monitoring queries
- `TROUBLESHOOTING.md` - Common issues
- `DEPLOYMENT.md` - AWS deployment guide

---

## Data Flow

```
1. Lambda Handler (scripts/lambda_handler.py)
   ↓
2. Migration Orchestrator (scripts/migration_orchestrator.py)
   ↓ [Resume detection, run creation]
3. Orchestrator (migrate.py)
   ↓ [Per-table processing, truncation safety]
4. Chunking Strategy (lib/chunking.py)
   ↓ [Chunk creation]
5. Migration Worker (lib/migration_worker.py)
   ↓ [Fetch from Snowflake, load to PostgreSQL]
6. Status Tracker (lib/status_tracker.py)
   ↓ [Update progress]
7. Back to Step 5 for next chunk
```

---

## Key Optimization Points

### Performance Critical
1. **lib/chunking.py** - Date aggregation query
   - v2.3: 11 minutes → 0.3 seconds
2. **lib/connections.py** - PostgreSQL session settings
   - v2.3: Added synchronous_commit=off
3. **config.json** - Parallel threads and batch size
   - Balance: memory vs throughput

### Memory Critical
1. **config.json** - parallel_threads setting
   - Each thread: ~300-500 MB
   - Monitor: CloudWatch "Max Memory Used"
2. **lib/migration_worker.py** - Batch processing
   - Larger batches = more memory
   - Currently: 25K rows per batch

### Cost Critical
1. **lib/utils.py** - Logging configuration
   - v2.3: Fixed duplicates (50% CloudWatch reduction)
2. **config.json** - Thread count
   - More threads = faster but more Lambda cost
3. **Snowflake warehouse** - Query execution time
   - Larger warehouse = faster but higher Snowflake cost

---

## Common Tasks & Where to Start

### "I need to change chunking strategy"
→ Start in `lib/chunking.py`

### "I need to add a new configuration option"
→ Start in `config.json`, then `lib/config_loader.py`

### "I need to modify the COPY/UPSERT logic"
→ Start in `lib/migration_worker.py` → `_load_to_postgres()`

### "I need to change resume detection"
→ Start in `lib/status_tracker.py` → `find_resumable_run()`

### "I need to add monitoring queries"
→ Start in `sql/QUICK_MONITORING.sql` or `docs/MONITORING.md`

### "I need to optimize performance"
→ Check:
1. `lib/chunking.py` - Chunk creation time
2. `lib/connections.py` - Connection settings
3. `config.json` - Thread/batch configuration
4. CloudWatch logs - Identify bottlenecks

### "I need to troubleshoot a failed migration"
→ Check:
1. CloudWatch logs - Error messages
2. `sql/diagnose_stuck_migration.sql` - Status query
3. `migration_status.migration_chunk_status` - Failed chunks
4. `docs/TROUBLESHOOTING.md` - Known issues

---

## Testing Locally

**Run locally:**
```bash
python scripts/lambda_handler.py migrate analytics
```

**Environment variables needed:**
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_RSA_KEY`
- `SNOWFLAKE_WAREHOUSE`
- `POSTGRES_HOST`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

---

**Last Updated:** 2025-12-16

