# Session Handoff: Scripts13 Conflict Management Pipeline

**Last updated**: 2026-02-15

## How Context Works in Cursor

The cursor rules files provide comprehensive architectural context and are **automatically loaded** by Cursor:

- **`.cursor/rules/project-overview.mdc`** (`alwaysApply: true`): Repo structure, all ScriptsNN folders, what's done vs not done in Scripts13. Always injected into every chat.
- **`.cursor/rules/postgres-tasks.mdc`** (glob-triggered on `Scripts13/**`): Deep technical reference -- v3 execution flow, critical design decisions, performance benchmarks, ECS architecture, deployment, all pipeline actions. Injected when working with Scripts13 files.

**This handoff document captures what the cursor rules don't**: current session state, pending work, recent changes, and immediate next steps.

## Project Summary

`Scripts13/tasks/` is a conflict management pipeline running as a Docker container on AWS ECS/Fargate. It detects scheduling conflicts by comparing Snowflake visit data against PostgreSQL conflict tables (`conflict_management.conflict_dev` schema), with streaming batch processing, conditional flag updates, and pair-precise stale cleanup.

### Default Pipeline (~30 min full)

```
task00_preflight → task01_copy_to_staging → task02_00_conflict_update → task02_01_inservice_conflict → task03_status_management → task99_postflight
```

## Key Files (full reference)

| File | Lines | Role |
|---|---|---|
| `scripts/main.py` | 260 | Lean orchestrator, ACTION_REGISTRY, DEFAULT_ACTIONS, SIGTERM |
| `scripts/actions/task00_preflight.py` | 612 | Validate, disable pg_cron, InProgressFlag, VACUUM/ANALYZE |
| `scripts/actions/task01_copy_to_staging.py` | 414 | PPR sync + staging populate |
| `scripts/actions/task02_00_conflict_update.py` | 147 | v3 conflict detection entry point |
| `scripts/actions/task02_01_inservice_conflict.py` | 587 | InService conflict detection |
| `scripts/actions/task03_status_management.py` | 956 | Status cascade, deleted visits, computed columns (15 steps) |
| `scripts/actions/task99_postflight.py` | 535 | Cleanup, MV refresh, pg_cron, email |
| `lib/conflict_processor.py` | 1,285 | Core engine: v3 streaming, batch UPDATE/INSERT, stale cleanup |
| `lib/query_builder.py` | 760 | SQL builders, INSERT_COLUMN_MAP (206 cols), InService templates |
| `lib/connections.py` | 268 | Snowflake + PostgreSQL connection managers |
| `lib/email_sender.py` | 264 | AWS SES HTML email sender |
| `lib/utils.py` | 147 | Logging, formatting, chunking |
| `config/config.json` | 62 | Runtime config (lookback, batch_size, pipeline, email) |
| `config/settings.py` | 120 | Config loader with `${ENV_VAR}` substitution, parameter merging |
| `tests/test_comprehensive.py` | ~1,950 | 210 pytest tests (all passing) |
| `sql/` | 12 files | Snowflake + PostgreSQL SQL templates |
| `deploy/build-and-push-ecr.ps1` | - | Interactive deploy (SSO, build, push, register, run) |

## Current State (2026-02-15)

### Everything is working and deployed:
- **task00 through task99**: All 6 pipeline actions operational on ECS Fargate
- **task02_00** (conflict detection): v3 architecture with asymmetric delta joins, streaming cursor + 5K batch, 7 conditional flags, change detection, cross-state filter, pair-precise stale cleanup. ~7-8 min at 36h lookback.
- **task02_01** (InService): Visits vs InService events at different providers. Temporal overlap join, synthetic MD5 VisitIDs, `_norm_key` UUID normalisation (strips dashes, lowercase for PG/Snowflake matching), caregiver semi-join optimisation (28% row reduction). ~14 min, ~166K rows.
- **task03** (status management): 15-step sequential executor in 3 phases. Phase B (deleted visits via Snowflake delta + UNION CTE) **validated on ECS 2026-02-15**: 84K IDs, 3.3K CVM + 216 CF rows, 99.6s. Phase A (9 steps, ~7 min) and Phase C (3 steps, ~2 min) stable.
- **210 pytest tests** all passing
- **Deploy script** fully operational

### Config state (`config/config.json`):
```json
"task03_parameters": {
    "enable_phase_b": true,   // Phase B enabled
    "only_steps": ""          // No filter -- all 15 steps will run
}
```

## Immediate Next Steps

1. **Full pipeline run with Phase B enabled** -- Phase B was validated individually (ONLY_STEPS filter). The config is now set for a full run (`enable_phase_b: true`, `only_steps: ""`). Deploy and run the complete pipeline to validate all 15 task03 steps together with the rest of the pipeline. Expected task03 time: ~12 min.

2. **AWS Secrets Manager integration** -- Currently using plain environment variables in ECS task definition for all secrets (Snowflake RSA key, PG password, etc.). Should migrate to Secrets Manager for production readiness.

## Recent Changes (this session, 2026-02-15)

1. **Phase B validated on ECS**: Ran task03 with `only_steps: "deleted_visits_cvm,deleted_visits_cf"` and `enable_phase_b: true`. Confirmed the UNION CTE fix works (previously OR-join caused nested-loop hang). 84K IDs fetched from Snowflake in 2.6s, 3,320 CVM + 216 CF rows updated in 99.6s.

2. **Config updated**: Cleared `only_steps` from test filter back to `""` for full pipeline run.

3. **Code cleanup**:
   - Fixed `scripts/actions/__init__.py` docstring (was missing task02_01 and task03 from pipeline list)
   - Fixed test file docstring (said "Task 02" but covers entire pipeline)

4. **Tests added** (204 → 210):
   - `_load_deleted_ids_to_pg`: Now verifies critical ANALYZE call on temp table
   - `_execute_step`: Verifies statement_timeout SET LOCAL
   - `_get_env_int`: 3 tests for env var parsing helper
   - `deleted_visits_cvm` guards: Verifies both UNION legs have StatusFlag != 'D' guard, UPDATE targets by primary key

5. **Documentation updated**: This file, cursor rules (project-overview.mdc + postgres-tasks.mdc) with Phase B validation, 210 test count, performance data.

## Key Design Decisions (not in cursor rules)

These are hard-won lessons from debugging that are worth preserving:

- **UNION CTE vs OR join**: The original `deleted_visits_cvm` used `WHERE CVM."VisitID" = DEL."VisitID"::uuid OR CVM."ConVisitID" = DEL."VisitID"::uuid`. PostgreSQL cannot use hash joins with OR -- falls back to nested-loop cross-join (84K × 9M = catastrophic). Fix: UNION CTE splits into two equi-joins, each hash-joinable. Cast on DEL side (`::uuid`) preserves CVM index usage.
- **ANALYZE on temp tables**: Without `ANALYZE _tmp_deleted_visits`, PG uses default stats (estimates ~200 rows). With 84K actual rows, this causes wrong plan selection (nested loop instead of hash join). Always ANALYZE temp tables after bulk insert.
- **Forced seq scan for partial-index traps**: `noresponse_flag_cvm` and `computed_billed_rate` use `SET LOCAL enable_indexscan = OFF` because PostgreSQL's partial indexes on StatusFlag cause catastrophic random I/O when the query doesn't filter on the indexed column.
- **Epsilon for float comparison**: `BilledRateMinute` is stored as `real` (float4) but CASE expressions compute in `float8`. `IS DISTINCT FROM` produces false positives from precision drift. Use `ABS(col - computed) > 0.0001` with `COALESCE(..., 1)` for NULL handling.

## Performance Summary

| Action | Runtime | Key metric |
|---|---|---|
| task00_preflight | ~2 min | VACUUM + ANALYZE on all tables |
| task01_copy_to_staging | ~5 min | 8M rows into conflictlog_staging |
| task02_00 (36h) | ~7 min | ~100K pairs, delta-scoped |
| task02_01 | ~14 min | ~166K pairs, full scan |
| task03 (Phase B) | ~1.5 min | 84K IDs, 3.5K rows updated |
| task03 (Phase A+C) | ~10 min | 12 steps, ~8.5M row table |
| task99_postflight | ~3 min | VACUUM + ANALYZE + MV refresh |
| **Full pipeline** | **~30 min** | |

## Deployment

```
cd Scripts13/tasks/deploy/
.\build-and-push-ecr.ps1
```
Steps: SSO login → Docker build → ECR push → register task def (if env vars changed) → run-task with action selection menu (option 6 = task03 only, option 8 = full pipeline).
