# SQL Scripts Reference Guide

## 📁 Location
All helper SQL scripts are in: `Scripts07/DataMigration/sql/`

---

## 🔍 Diagnostic Scripts

### `diagnose_stuck_migration.sql` - PostgreSQL
**Purpose:** Comprehensive troubleshooting guide for stuck migrations and missing rows

**When to use:**
- Migration appears stuck
- Tables not completing
- **All chunks completed but row counts don't match source**
- Need to check migration status
- Errors in CloudWatch logs
- Step Functions timeout after 100 retries

**Features:**
- ✅ **11 diagnostic queries** to identify issues:
  - Queries #1-9: Standard status/progress/errors
  - Query #10: Data quality checks
  - **Query #11: Missing rows analysis** (when status=completed but counts differ)
- ✅ **Data quality checks** (duplicates, NULLs)
- ✅ **5 fix options** with copy-paste SQL templates:
  - Option A: Reset stuck in_progress chunks
  - Option B: Skip permanently failed chunks
  - Option C: Truncate and reload specific table
  - Option D: Skip entire table
  - Option E: Complete fresh start
- ✅ **Verification queries** to confirm fixes
- ✅ **Quick troubleshooting workflow**

**This is your ONE-STOP troubleshooting script!** 🔧

---

### `fix_stuck_run.sql` - PostgreSQL
**Purpose:** Fix specific errors after code updates (Decimal serialization, timestamp constraints)

**When to use:**
- After deploying code fixes for known bugs
- Need to reset specific stuck chunks without restarting entire migration
- Errors like "Decimal is not JSON serializable" or "valid_chunk_completed_at"

**Features:**
- ✅ Step-by-step guided fix process
- ✅ Identify specific run_id
- ✅ Reset stuck chunks for specific tables
- ✅ Verification queries
- ✅ Option to resume or fresh start

**Use this after deploying fixes, then resume the same run!** 🔄

---

## 📊 Count Comparison Scripts

### `count_source_tables.sql` - Snowflake
**Purpose:** Count records in all Snowflake source tables (with filters)

**Features:**
- ✅ Applies all `source_filter` from config.json
- ✅ Uses absolute database references
- ✅ 48 tables across 3 sources (Analytics, Aggregator, Conflict)

**Output:**
```
source_name | table_name | record_count
ANALYTICS   | DIMPAYER   | 5,234
CONFLICT    | CONFLICTS  | 7,874,916
```

---

### `count_target_tables.sql` - PostgreSQL
**Purpose:** Count records in all PostgreSQL target tables

**Features:**
- ✅ Uses absolute database.schema.table references
- ✅ Same structure as `count_source_tables.sql` for easy comparison
- ✅ Counts all 48 target tables

**Output:**
```
source_name | table_name | record_count
ANALYTICS   | DIMPAYER   | 5,234
CONFLICT    | CONFLICTS  | 7,874,916
```

**Compare with Snowflake:** If counts match, migration is successful! ✅

---

## 🗑️ Truncation Scripts

### `truncate_all_tables.sql` - PostgreSQL
**Purpose:** Delete ALL data from all migration target tables

**⚠️ WARNING: DESTRUCTIVE OPERATION**

**When to use:**
- Starting a complete fresh migration
- Clearing out bad data after failed migrations
- Testing from scratch

**What it does:**
- Truncates all 15 analytics tables
- Truncates 1 aggregator table
- Truncates all 33 conflict tables
- Optionally truncates migration status tables (commented out)

**Usage:**
```sql
-- Run entire script to clear all tables:
\i sql/truncate_all_tables.sql

-- Or run sections individually (safer)
```

---

### `truncate_status_tables.sql` - PostgreSQL
**Purpose:** Reset migration state WITHOUT deleting data tables

**When to use:**
- Force a fresh migration run (bypass resume logic)
- Clear stuck migration state
- Reset after config.json changes that break resume

**What it does:**
- Truncates `migration_runs`
- Truncates `migration_table_status`
- Truncates `migration_chunk_status`
- **Does NOT touch data tables**

**This is SAFER than truncate_all_tables.sql**

**Usage:**
```sql
\i sql/truncate_status_tables.sql
```

Then run Step Functions with `{"source_name": "analytics", "no_resume": true}`

---

### `truncate_single_table.sql` - PostgreSQL
**Purpose:** Template for truncating a specific table

**When to use:**
- Only one table has issues
- Testing a specific table migration
- Reloading a single table with `truncate_onstart: true`

**Examples provided:**
```sql
-- Analytics:
TRUNCATE TABLE conflict_management.analytics_dev.dimuseroffices CASCADE;

-- Conflict:
TRUNCATE TABLE conflict_management.conflict_dev.conflictvisitmaps CASCADE;
```

---

## 📋 Schema Definition

### `migration_status_schema.sql` - PostgreSQL
**Purpose:** DDL for migration tracking tables

**Creates:**
- Schema: `migration_status`
- `migration_runs` - Track each migration execution
- `migration_table_status` - Track progress per table
- `migration_chunk_status` - Track progress per chunk
- Views: `v_active_migrations`, `v_table_progress`

**When to use:**
- First-time setup
- Recreating status tables after dropping them
- **Referenced automatically by migration code** (deployed with Lambda)

---

## 🚀 Typical Workflow

### 1️⃣ Before Migration
```sql
-- Get baseline counts from Snowflake:
-- Run in Snowflake:
\i sql/count_source_tables.sql
```

### 2️⃣ Fresh Start (if needed)
```sql
-- In PostgreSQL:
\i sql/truncate_all_tables.sql     -- Clear everything (⚠️ destructive)
-- This now includes both data tables AND status tables
```

### 3️⃣ After Migration
```sql
-- Verify counts match:
-- Run in PostgreSQL:
\i sql/count_target_tables.sql

-- Compare with Snowflake counts from step 1
```

### 4️⃣ If Migration Stuck
```sql
-- In PostgreSQL:
\i sql/diagnose_stuck_migration.sql  -- Find issues AND get fix templates
-- This script now includes 5 fix options with copy-paste SQL
-- Choose the option that matches your situation and run it
```

---

## 📝 Quick Reference

| Script | Database | Purpose | Destructive? |
|--------|----------|---------|--------------|
| `count_source_tables.sql` | Snowflake | Count source records | ❌ No |
| `count_target_tables.sql` | PostgreSQL | Count target records | ❌ No |
| `diagnose_stuck_migration.sql` | PostgreSQL | **Diagnose & Fix** (All-in-one) | ⚠️ Yes (if you run fix templates) |
| `fix_stuck_run.sql` | PostgreSQL | Reset stuck chunks after code fixes | ⚠️ Yes (updates status) |
| `truncate_all_tables.sql` | PostgreSQL | Clear ALL tables + status | ❌ **VERY DESTRUCTIVE** |
| `migration_status_schema.sql` | PostgreSQL | Create status tables | ❌ No (DDL only) |

---

## ✅ Best Practices

1. **Always run count_source_tables.sql BEFORE migration** to establish baseline
2. **Use truncate_status_tables.sql instead of truncate_all_tables.sql** when possible
3. **Run diagnose_stuck_migration.sql first** before attempting fixes
4. **Compare source and target counts** after each migration run
5. **Keep Snowflake counts in a spreadsheet** for historical comparison

---

🎯 **All scripts use absolute references - no connection switching required!**

