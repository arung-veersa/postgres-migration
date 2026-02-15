# Conflict Management Pipeline -- Actions Reference

## Overview

The conflict management pipeline runs as a Docker container on AWS ECS/Fargate. It reads from
Snowflake (read-only, RSA key auth) and writes to PostgreSQL (`conflict_management.conflict_dev`
schema). The entry point is `scripts/main.py`, which dispatches to modular actions in
`scripts/actions/`.

## Default Pipeline Sequence

```
task00_preflight
  → task01_copy_to_staging
    → task02_00_conflict_update
      → task02_01_inservice_conflict
        → task03_status_management
          → task99_postflight
```

Typical full-pipeline runtime: ~30 min (varies with data volume and lookback window).

---

## Actions

### task00_preflight -- Pre-Run Validation & Setup

**Purpose:** Ensure the environment is healthy and lock the pipeline before heavy processing.

- Validate config parameters (`lookback_hours > 0`, `batch_size > 0`, etc.)
- Verify Snowflake and PostgreSQL connectivity
- Check all required PostgreSQL tables exist (`conflicts`, `conflictvisitmaps`,
  `conflictlog_staging`, `settings`, `excluded_agency`, `excluded_ssn`, `mph`,
  `payer_provider_reminders`)
- Disable the `pg_cron` job (prevents materialized view refresh during pipeline)
- Set `InProgressFlag = 1` in the `settings` table
- Sync identity sequences (advance to `MAX(ID)` if behind -- prevents PK collisions on INSERT)
- VACUUM all tables (reclaim dead-tuple space so subsequent tasks start clean)
- ANALYZE all tables (refresh planner statistics for optimal query plans)
- Capture pre-run row counts for key tables (used by postflight for delta reporting)

**Snowflake SPs covered:** None (pure infrastructure/housekeeping)

---

### task01_copy_to_staging -- Staging Data Sync

**Purpose:** Synchronize dimension data from Snowflake and prepare the staging table for
downstream consumers.

- Sync `payer_provider_reminders` from Snowflake dimension tables
  (INSERT new + UPDATE existing, ~20K records, ~3s)
- Truncate `conflictlog_staging`
- Populate `conflictlog_staging` from `conflictvisitmaps` + `conflicts`
  (date-filtered, ~8M rows, ~280s)

**Snowflake SPs covered:**

| Snowflake SP | Coverage |
|---|---|
| `TASK_01_COPY_DATA_FROM_CONFLICTVISITMAPS_TO_TEMP.sql` | Fully covered |

---

### task02_00_conflict_update -- Conflict Detection (UPDATE + INSERT)

**Purpose:** Detect scheduling conflicts by comparing Snowflake visit data against PostgreSQL,
update existing conflict records, insert newly detected conflicts, and clean up stale pairs.

- Load excluded SSNs from PostgreSQL into Snowflake temp table
- Create `delta_keys` temp table (recently-changed visits within `lookback_hours`)
- Build `base_visits` temp table (delta rows + related non-delta rows via asymmetric join)
- Self-join `base_visits` to produce conflict pairs (with cross-state filter)
- Stream conflict pairs from Snowflake in 5K-row batches
- For each batch: fetch existing PG records, detect changes (7 flags + 40 business columns),
  UPDATE dirty rows, INSERT new conflicts (`StatusFlag='N'`)
- Pair-precise stale cleanup: identify stale conflicts via anti-join, mark `StatusFlag='R'`

**Key features:**
- Delta-scoped via `lookback_hours` (default 36h) -- only processes recently-changed visits
- Asymmetric join (`is_delta` flag) -- avoids All-vs-All self-join on 9.6M rows
- Cross-state conflict filter -- excludes conflicts where no address state matches across sides
- Streaming cursor + batch processing -- 5K rows at a time, commit per batch
- Change detection -- 7 conflict flags + 40 business columns, only updates rows with actual changes
- Self-healing PK retry for INSERT -- handles concurrent identity sequence issues

**Snowflake SPs covered:**

| Snowflake SP | Coverage |
|---|---|
| `TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_0.sql` | Fully covered (conflict detection + UPDATE) |
| `TASK_03_INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_0.sql` | Fully covered (INSERT new conflicts) |
| `TASK_04_UPDATE_CONFLICT_VISIT_MAPS.sql` | Partially covered (stale conflict cleanup) |
| `TASK_05_INSERT_CONFLICTS.sql` | Fully covered (parent conflict record creation) |

---

### task02_01_inservice_conflict -- InService Conflict Detection (UPDATE + INSERT)

**Purpose:** Detect conflicts between regular visits and InService events for the same caregiver
at different providers.

- Fetch excluded agencies/SSNs from PostgreSQL
- Build Snowflake SQL in 3 steps:
  - Step 1: Create visits temp table (caregiver semi-join pre-filter, ~28% row reduction)
  - Step 2: Create InService events temp table (synthetic MD5 VisitIDs)
  - Step 3: UNION ALL both directions (visit-vs-event + event-vs-visit)
- Stream and process conflict pairs (temporal overlap join)
- UPDATE existing InService conflict records
  (7 flags hardcoded 'N', `InServiceFlag='Y'`)
- INSERT new InService conflicts (row-by-row fallback for UniqueViolation safety)

**Key features:**
- Caregiver semi-join in Step 1 pre-filters to only caregivers with InService events (~28% reduction)
- Synthetic MD5 VisitIDs for InService events (no native VisitID in source)
- UUID key normalisation (`_norm_key`) -- Snowflake MD5 (32-char hex) vs PostgreSQL UUID (with dashes)
- Both UPDATE and INSERT in a single action (replaces two separate SPs)

**Snowflake SPs covered:**

| Snowflake SP | Coverage |
|---|---|
| `TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_1.sql` | InService-specific portions (UPDATE) |
| `TASK_03_INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_2.sql` | InService-specific portions (INSERT) |

---

### task03_status_management -- Status Cascade & Computed Columns

**Purpose:** Consolidate all post-conflict-creation processing: handle deleted visits, cascade
status changes through the conflict hierarchy, and compute derived columns.

Runs 14 SQL steps + 1 Snowflake fetch across 3 phases. Each step commits independently with
`shutdown_check` between steps. Execution order matters: B must run before A (A's cascades
depend on B's 'D' markings); C is independent of both.

#### Phase B -- Deleted Visit Handling (Snowflake + PostgreSQL, 3 steps)

Disabled by default. Enable via `enable_phase_b: true` in config or `ENABLE_PHASE_B=1` env var.

- **Step 0**: Query Snowflake `FACTVISITCALLPERFORMANCE_DELETED_CR` with delta filter
  (`"Visit Updated Timestamp"` >= lookback_hours). Reduces 25.5M rows to ~97K.
- **Step 1**: Load deleted Visit IDs into indexed PostgreSQL temp table.
  Mark CVM `StatusFlag='D'` when VisitID or ConVisitID is a deleted visit
  (UNION CTE for hash-join-friendly equi-joins on both columns).
- **Step 2**: Cascade `StatusFlag='D'` to parent conflicts when primary VisitID is deleted.

#### Phase A -- Status Cascade (pure PostgreSQL, 9 steps)

- **Step 3** (`ismissed_cascade_cf`): Set CF `StatusFlag='R'` when any CVM has
  `IsMissed=TRUE` or `ConIsMissed=TRUE`
- **Step 4** (`ismissed_cascade_cvm`): Set CVM `StatusFlag='R'` when `IsMissed=TRUE`
  or `ConIsMissed=TRUE`
- **Step 5** (`updateflag_orphan_cleanup`): Resolve CVM with `UpdateFlag=1`
  (orphaned re-detection markers)
- **Step 6** (`aggregation_mark_updatedrflag`): Set `UpdatedRFlag='1'` on conflicts
  with CVM in the date window
- **Step 7** (`aggregation_status_u_propagation`): Propagate `StatusFlag='U'` from CVM
  to parent conflicts
- **Step 8** (`cascade_resolve_cvm`): CTE-driven cascade-resolve CVM under R/D conflicts
  (combined: singleton + near-all-resolved + all-resolved in a single scan)
- **Step 9** (`cascade_resolve_cf`): CTE-driven cascade-resolve conflicts where all CVM
  are R/D (combined: singleton_cf + all_cvm_resolved_cf in a single scan)
- **Step 10** (`noresponse_flag_cf`): Reset CF `StatusFlag='N'` when `NoResponseFlag='Yes'`
- **Step 11** (`noresponse_flag_cvm`): Reset CVM `StatusFlag='N'` when
  `ConNoResponseFlag='Yes'` (forced seq scan to avoid partial-index trap)

#### Phase C -- Computed Columns (pure PostgreSQL, 3 steps)

- **Step 12** (`computed_time_columns`): Compute `ShVTSTTime`/`ShVTENTime`/`CShVTSTTime`/
  `CShVTENTime` via COALESCE of visit/scheduled/inservice times
- **Step 13** (`computed_billed_rate`): Compute `BilledRateMinute`/`ConBilledRateMinute`
  (rate-per-minute with `::real` cast + epsilon comparison to avoid float precision false
  positives; forced seq scan)
- **Step 14** (`computed_reverse_uuid`): Compute `ReverseUUID` for new rows only
  (canonical pair key, `WHERE IS NULL` optimisation)

**Key features:**
- All cascade steps preserve `StatusFlag='D'` (never overwrite D)
- No-op update filtering on every step (PostgreSQL MVCC: avoids unnecessary row versions)
- SARGable date windows (`col >= lower AND col < upper + 1 day`, no `::date` casts)
- `only_steps` config/env var for targeted step execution during testing
- Forced sequential scan on two steps to avoid partial-index random-I/O traps

**Snowflake SPs covered:**

| Snowflake SP | Coverage |
|---|---|
| `TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_3.sql` | Fully covered (deleted visits, IsMissed cascade, UpdateFlag cleanup, conflicts aggregation, NoResponseFlag reset, StatusFlag cascade) |
| `TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_1.sql` | Non-InService portions (computed columns: BilledRateMinute, ShVTSTTime, ReverseUUID) |
| `TASK_03_INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_2.sql` | Non-InService portions (computed columns: BilledRateMinute, ShVTSTTime) |

---

### task99_postflight -- Post-Run Cleanup & Reporting

**Purpose:** Restore the database to normal operating state and report pipeline results.

- VACUUM all tables in the `conflict_dev` schema
- ANALYZE all tables in the `conflict_dev` schema
- Set `InProgressFlag = 0` in the `settings` table
- Refresh materialized view `mv_payer_conflicts_common` (CONCURRENTLY)
- Re-enable the `pg_cron` job with the schedule saved during preflight
- Capture post-run row counts and compute deltas from preflight counts
- Log pipeline summary to CloudWatch (plain-text, always runs)
- Send HTML status email via AWS SES (if configured)

**Snowflake SPs covered:** None (pure infrastructure/reporting)

---

## Standalone Actions

These are not part of the default pipeline but can be run individually via `ACTION=<name>`:

| Action | Purpose |
|---|---|
| `validate_config` | Print and validate configuration summary (also runs as part of preflight) |
| `test_connections` | Test Snowflake and PostgreSQL connectivity (also runs as part of preflight) |

---

## Snowflake SPs NOT Migrated

These downstream reporting/dashboard SPs run after the core conflict detection pipeline and are
outside the scope of Scripts13:

| Snowflake SP | Description |
|---|---|
| `TASK_06_ASSIGN_GROUP_IDS.sql` | Assign group IDs to conflicts |
| `TASK_07_UPDATE_PHONE_CONTACT.sql` | Update phone/contact info |
| `TASK_08_CREATE_NEW_LOG_HISTORY.sql` | Create log history entries |
| `TASK_09_0_UPDATE_CREATE_LOG_HISTORY.sql` | Update/create log history |
| `TASK_09_1_SP_GET_FINAL_BILLABLE_UNITS_OPTIMIZED.sql` | Billable units computation |
| `TASK_10_LOAD_PROVIDER_DASHBOARD_DATA.sql` | Provider dashboard ETL |
| `TASK_11_LOAD_PAYER_DASHBOARD_DATA.sql` | Payer dashboard ETL |
| `TASK_12_LOAD_PAYER_DASHBOARD_CHART_DATA.sql` | Dashboard chart data ETL |
| `TASK_13_LOAD_PAYER_CONFLICT_SUMMARY.sql` | Conflict summary ETL |

---

## Configuration

### Common Parameters (shared across tasks)

| Parameter | Default | Description |
|---|---|---|
| `lookback_years` | 2 | Date window: how far back to scan for visits |
| `lookforward_days` | 45 | Date window: how far forward to scan for visits |
| `lookback_hours` | 36 | Delta window: only process visits changed within this window |
| `batch_size` | 5000 | Streaming batch size (task02_00) |

### Task 02 Parameters

| Parameter | Default | Description |
|---|---|---|
| `skip_unchanged_records` | true | Skip UPDATE when no data changes detected |
| `enable_asymmetric_join` | true | Use delta-vs-all join (vs all-vs-all) |
| `enable_stale_cleanup` | true | Run pair-precise stale conflict cleanup |
| `enable_insert` | true | INSERT newly detected conflicts |
| `enable_inservice` | true | Run InService conflict detection (task02_01) |

### Task 03 Parameters

| Parameter | Default | Description |
|---|---|---|
| `enable_phase_b` | false | Enable deleted visit handling (Snowflake query) |
| `only_steps` | "" | Comma-separated step names for targeted execution |

### Environment Variable Overrides

All parameters can be overridden via environment variables (uppercase):
`LOOKBACK_YEARS`, `LOOKFORWARD_DAYS`, `LOOKBACK_HOURS`, `ENABLE_PHASE_B`, `ONLY_STEPS`, etc.
Environment variables take precedence over `config.json` values.
