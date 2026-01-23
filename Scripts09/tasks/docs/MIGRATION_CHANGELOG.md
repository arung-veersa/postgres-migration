# Postgres Migration - Project Log

## Critical Changes (Session: 2026-01-23)

### 1. Schema & Data Type Fixes
- **BigInt Casting**: Initially added `::bigint` casts to all "AppID" columns in `task01` scripts to handle Snowflake `VARCHAR` vs Postgres `INT8` mismatches.
- **Reversion**: Reverted these casts after the source database schema was corrected to `bigint` natively.
- **Result**: Scripts now run cleanly without explicit casting, relying on correct source types.

### 2. Task 01 Performance Optimization
- **Baseline**: 192 seconds for ~8.3M rows (~43k rows/sec).
- **Optimization Attempt 1 (ANALYZE)**: Added `ANALYZE` command. **Regression**: Slowed down to 286 seconds. Reverted.
- **Optimization Attempt 2 (Indexes)**: Created specific indexes on the `conflictvisitmaps` table:
    ```sql
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cvm_visitdate_conflictid 
    ON {schema}.conflictvisitmaps ("VisitDate", "CONFLICTID");
    ```
- **Final Result**: Execution time dropped to **160 seconds** (~53k rows/sec). A **17% improvement** over baseline.

### 3. Observability Improvements
- **Throughput Metrics**: Added "Rows/Sec" logging to `task01.py`.
- **Date Range Context**: Added explicit logging of the processing window (e.g., `Range: 2024-01-23 to 2026-03-09`) to Step 4 logs.

### 4. SQL Compatibility
- **DBeaver Compatibility**: Modified `Alter Script.sql` to remove `psql` specific `\set` variables and use hardcoded schema names (`conflict_dev2`, `analytics_dev2`) for direct execution in DBeaver/pgAdmin.

## Architecture Decisions
- **SQL Files**: Decided to keep SQL queries in separate `.sql` files (vs Python strings) for maintainability and tooling support.
- **Indexing**: Avoided `CLUSTER` indexing due to maintenance overhead; stuck with standard B-Tree indexes which are auto-maintained.
