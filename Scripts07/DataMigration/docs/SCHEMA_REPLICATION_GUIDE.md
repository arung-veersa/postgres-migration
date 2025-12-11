# PostgreSQL Schema Replication Guide

## 📋 **Overview**

Instead of running the Snowflake migration multiple times for different schemas, you can:
1. **Migrate ONCE** from Snowflake → PostgreSQL (to a "source" schema)
2. **Copy/replicate** from that PostgreSQL schema → other PostgreSQL schemas

**Time Savings:**
- Snowflake → PostgreSQL migration: ~50-60 minutes (per schema)
- PostgreSQL → PostgreSQL copy: ~1-2 minutes (per schema)
- **Speed improvement: 30-50x faster!** 🚀

---

## 🎯 **Use Cases**

### ✅ **When to Use Schema Replication**
- Creating dev/test/prod environments with identical data
- Promoting changes from dev → test → prod
- Creating backups before risky operations
- Testing different configurations on same data
- Disaster recovery scenarios

### ❌ **When NOT to Use**
- Schemas need different data (different source filters)
- Schemas have different table structures
- You need ongoing real-time sync (use triggers/logical replication instead)

---

## 🛠️ **Method 1: Simple Table Copy (Fastest)**

### **Basic Syntax**

```sql
-- Create target schema
CREATE SCHEMA IF NOT EXISTS target_schema;

-- Copy table with structure and data
CREATE TABLE target_schema.table_name 
  (LIKE source_schema.table_name INCLUDING ALL)
  AS SELECT * FROM source_schema.table_name;
```

The `INCLUDING ALL` clause copies:
- ✅ Column definitions
- ✅ Default values
- ✅ Constraints (NOT NULL, CHECK)
- ✅ Indexes
- ⚠️ But NOT: Foreign keys, triggers, sequences ownership

### **Complete Example**

```sql
-- Example: Copy conflict_dev → conflict_prod

-- Step 1: Create target schema
CREATE SCHEMA IF NOT EXISTS conflict_prod;

-- Step 2: Copy all tables
CREATE TABLE conflict_prod.conflictvisitmaps 
  (LIKE conflict_dev.conflictvisitmaps INCLUDING ALL)
  AS SELECT * FROM conflict_dev.conflictvisitmaps;

CREATE TABLE conflict_prod.conflicts 
  (LIKE conflict_dev.conflicts INCLUDING ALL)
  AS SELECT * FROM conflict_dev.conflicts;

CREATE TABLE conflict_prod.log_history 
  (LIKE conflict_dev.log_history INCLUDING ALL)
  AS SELECT * FROM conflict_dev.log_history;

-- Step 3: Verify counts
SELECT 'conflict_dev' as schema, COUNT(*) FROM conflict_dev.conflictvisitmaps
UNION ALL
SELECT 'conflict_prod' as schema, COUNT(*) FROM conflict_prod.conflictvisitmaps;

-- Step 4: Grant permissions (if needed)
GRANT USAGE ON SCHEMA conflict_prod TO your_app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA conflict_prod TO your_app_user;
```

**Performance:** ~1-2 minutes for 8M rows

---

## 🤖 **Method 2: Automated Schema Copy Script**

### **Full Schema Replication (All Tables at Once)**

```sql
-- ========================================
-- SCHEMA REPLICATION SCRIPT
-- Copy all tables from source_schema to target_schema
-- ========================================

DO $$
DECLARE
    source_schema_name TEXT := 'conflict_dev';     -- ← Change this
    target_schema_name TEXT := 'conflict_prod';    -- ← Change this
    table_rec RECORD;
    start_time TIMESTAMP;
    table_count INT := 0;
    total_rows BIGINT := 0;
BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE 'Starting schema replication: % → %', source_schema_name, target_schema_name;
    
    -- 1. Create target schema if it doesn't exist
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', target_schema_name);
    RAISE NOTICE '✓ Target schema created/verified';
    
    -- 2. Loop through all tables in source schema
    FOR table_rec IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = source_schema_name
        ORDER BY tablename
    LOOP
        -- Copy table with structure and data
        EXECUTE format(
            'CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL) AS SELECT * FROM %I.%I',
            target_schema_name, table_rec.tablename,
            source_schema_name, table_rec.tablename,
            source_schema_name, table_rec.tablename
        );
        
        -- Get row count
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', target_schema_name, table_rec.tablename)
        INTO STRICT table_rec.row_count;
        
        table_count := table_count + 1;
        total_rows := total_rows + table_rec.row_count;
        
        RAISE NOTICE '  ✓ Copied table: % (% rows)', 
            table_rec.tablename, 
            to_char(table_rec.row_count, 'FM999,999,999');
    END LOOP;
    
    -- 3. Summary
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ Schema replication completed!';
    RAISE NOTICE '   Tables copied: %', table_count;
    RAISE NOTICE '   Total rows: %', to_char(total_rows, 'FM999,999,999');
    RAISE NOTICE '   Duration: %', clock_timestamp() - start_time;
    RAISE NOTICE '========================================';
    
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Schema replication failed: %', SQLERRM;
END $$;
```

**Usage:**
1. Edit `source_schema_name` and `target_schema_name` at the top
2. Run the script in PostgreSQL
3. Watch progress in NOTICE messages

**Performance:** ~2-5 minutes for multiple tables

---

## 🔧 **Method 3: pg_dump / pg_restore (Most Complete)**

### **Dump and Restore with Schema Rename**

```bash
# Method A: Dump to file, restore with rename
# ========================================

# 1. Dump the source schema to a file
pg_dump -h your-postgres-host \
  -U your-user \
  -d conflict_management \
  -n conflict_dev \
  --format=custom \
  -f conflict_dev_backup.dump

# 2. Restore to target schema
# Note: You need to manually edit the dump or use search/replace
pg_dump -h your-postgres-host \
  -U your-user \
  -d conflict_management \
  -n conflict_dev \
  --format=plain \
  | sed 's/conflict_dev/conflict_prod/g' \
  | psql -h your-postgres-host -U your-user -d conflict_management


# Method B: Direct pipe (no intermediate file)
# ========================================
pg_dump -h your-postgres-host \
  -U your-user \
  -d conflict_management \
  -n conflict_dev \
  | sed 's/conflict_dev/conflict_prod/g' \
  | psql -h your-postgres-host -U your-user -d conflict_management


# Method C: Using pg_restore with transformations
# ========================================
pg_dump -h your-postgres-host \
  -U your-user \
  -d conflict_management \
  -n conflict_dev \
  --format=directory \
  -f conflict_dev_dump_dir

# Then restore with schema transformation
pg_restore -h your-postgres-host \
  -U your-user \
  -d conflict_management \
  --schema-only \
  --schema=conflict_prod \
  conflict_dev_dump_dir
```

**Pros:**
- ✅ Copies everything: tables, indexes, constraints, sequences, triggers
- ✅ Complete schema clone
- ✅ Industry standard backup/restore method

**Cons:**
- ⚠️ Requires command-line access
- ⚠️ Schema renaming requires sed/awk manipulation

**Performance:** ~5-10 minutes for full schema with 8M rows

---

## 📊 **Method 4: Incremental Updates (For Ongoing Sync)**

If you need to periodically refresh target schemas from source:

### **Option A: Truncate and Reload**

```sql
-- Fast refresh: Delete all data, reload from source
DO $$
DECLARE
    table_rec RECORD;
BEGIN
    FOR table_rec IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'conflict_prod'
    LOOP
        -- Truncate target table
        EXECUTE format('TRUNCATE TABLE conflict_prod.%I CASCADE', table_rec.tablename);
        
        -- Reload from source
        EXECUTE format(
            'INSERT INTO conflict_prod.%I SELECT * FROM conflict_dev.%I',
            table_rec.tablename, table_rec.tablename
        );
        
        RAISE NOTICE '✓ Refreshed table: %', table_rec.tablename;
    END LOOP;
END $$;
```

### **Option B: Drop and Recreate**

```sql
-- Complete refresh: Drop schema, recreate from scratch
DROP SCHEMA IF EXISTS conflict_prod CASCADE;

-- Then run Method 1 or Method 2 script
```

---

## 🎨 **Practical Scenarios**

### **Scenario 1: Dev → Test → Prod Promotion**

```sql
-- Step 1: Migrate from Snowflake to dev (once)
-- Run your migration tool with target: conflict_dev

-- Step 2: Promote dev → test (when ready for testing)
CREATE SCHEMA conflict_test;
CREATE TABLE conflict_test.conflictvisitmaps 
  (LIKE conflict_dev.conflictvisitmaps INCLUDING ALL)
  AS SELECT * FROM conflict_dev.conflictvisitmaps;

-- Step 3: Promote test → prod (when ready for production)
CREATE SCHEMA conflict_prod;
CREATE TABLE conflict_prod.conflictvisitmaps 
  (LIKE conflict_test.conflictvisitmaps INCLUDING ALL)
  AS SELECT * FROM conflict_test.conflictvisitmaps;
```

**Time:** 60 min (migration) + 2 min (test) + 2 min (prod) = **64 minutes total**

vs.

**Without replication:** 60 min + 60 min + 60 min = **180 minutes total**

**Savings: 116 minutes!** ⏱️

---

### **Scenario 2: Create Testing Sandbox**

```sql
-- Create a sandbox for testing without affecting dev
CREATE SCHEMA conflict_sandbox;

CREATE TABLE conflict_sandbox.conflictvisitmaps 
  (LIKE conflict_dev.conflictvisitmaps INCLUDING ALL)
  AS SELECT * FROM conflict_dev.conflictvisitmaps;

-- Now you can test destructive operations:
-- DELETE, UPDATE, experiment with queries, etc.

-- When done testing, simply drop the sandbox:
DROP SCHEMA conflict_sandbox CASCADE;
```

---

### **Scenario 3: Backup Before Major Changes**

```sql
-- Before a risky operation (e.g., data migration, bulk update):

-- Create backup schema
CREATE SCHEMA conflict_dev_backup_20251211;

-- Copy all tables
CREATE TABLE conflict_dev_backup_20251211.conflictvisitmaps 
  (LIKE conflict_dev.conflictvisitmaps INCLUDING ALL)
  AS SELECT * FROM conflict_dev.conflictvisitmaps;

-- Perform risky operation on conflict_dev...

-- If something goes wrong:
DROP SCHEMA conflict_dev CASCADE;
ALTER SCHEMA conflict_dev_backup_20251211 RENAME TO conflict_dev;

-- If successful:
DROP SCHEMA conflict_dev_backup_20251211 CASCADE;
```

---

## ⚡ **Performance Tips**

### **1. Parallel Execution (For Multiple Tables)**

```sql
-- Copy multiple tables in parallel (requires separate connections)
-- Connection 1:
CREATE TABLE conflict_prod.conflictvisitmaps 
  (LIKE conflict_dev.conflictvisitmaps INCLUDING ALL)
  AS SELECT * FROM conflict_dev.conflictvisitmaps;

-- Connection 2 (simultaneously):
CREATE TABLE conflict_prod.conflicts 
  (LIKE conflict_dev.conflicts INCLUDING ALL)
  AS SELECT * FROM conflict_dev.conflicts;

-- Connection 3 (simultaneously):
CREATE TABLE conflict_prod.log_history 
  (LIKE conflict_dev.log_history INCLUDING ALL)
  AS SELECT * FROM conflict_dev.log_history;
```

### **2. Disable Indexes During Copy (For Large Tables)**

```sql
-- For very large tables, create without indexes first:
CREATE TABLE conflict_prod.conflictvisitmaps 
  AS SELECT * FROM conflict_dev.conflictvisitmaps;

-- Then add indexes separately:
CREATE UNIQUE INDEX conflictvisitmaps_pkey 
  ON conflict_prod.conflictvisitmaps ("ID");

ALTER TABLE conflict_prod.conflictvisitmaps 
  ADD PRIMARY KEY USING INDEX conflictvisitmaps_pkey;
```

### **3. Use UNLOGGED Tables for Temporary Copies**

```sql
-- If you don't need durability (e.g., for testing):
CREATE UNLOGGED TABLE conflict_test.conflictvisitmaps 
  (LIKE conflict_dev.conflictvisitmaps INCLUDING ALL)
  AS SELECT * FROM conflict_dev.conflictvisitmaps;

-- Faster writes, but data lost on crash
```

---

## 🔍 **Verification Queries**

### **Compare Row Counts Between Schemas**

```sql
-- Check if all tables have matching counts
SELECT 
    'conflict_dev' as schema,
    tablename,
    (SELECT COUNT(*) FROM conflict_dev.conflictvisitmaps) as row_count
FROM pg_tables 
WHERE schemaname = 'conflict_dev'
UNION ALL
SELECT 
    'conflict_prod' as schema,
    tablename,
    (SELECT COUNT(*) FROM conflict_prod.conflictvisitmaps) as row_count
FROM pg_tables 
WHERE schemaname = 'conflict_prod'
ORDER BY tablename, schema;
```

### **Check Schema Disk Usage**

```sql
-- See how much space each schema uses
SELECT 
    schemaname,
    SUM(pg_total_relation_size(schemaname||'.'||tablename))::BIGINT as total_bytes,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))::BIGINT) as total_size
FROM pg_tables
WHERE schemaname IN ('conflict_dev', 'conflict_prod', 'conflict_test')
GROUP BY schemaname
ORDER BY schemaname;
```

---

## ⚠️ **Important Notes**

### **What Gets Copied**
✅ Table structure (columns, data types)
✅ Data (all rows)
✅ Indexes (with `INCLUDING ALL`)
✅ Constraints (PRIMARY KEY, UNIQUE, CHECK, NOT NULL)
✅ Default values
✅ Comments (if using pg_dump)

### **What Does NOT Get Copied Automatically**
❌ Foreign keys (need manual recreation)
❌ Sequences ownership (need to reassign)
❌ Triggers (need manual recreation)
❌ Views (need manual recreation)
❌ Functions/Procedures (need manual recreation)
❌ Permissions/Grants (need manual recreation)

### **To Copy Foreign Keys Manually**

```sql
-- Get foreign key definitions from source
SELECT 
    'ALTER TABLE ' || quote_ident(target_schema) || '.' || quote_ident(c.conrelid::regclass::text) ||
    ' ADD CONSTRAINT ' || quote_ident(c.conname) || ' ' ||
    pg_get_constraintdef(c.oid) || ';' as fk_creation_sql
FROM pg_constraint c
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname = 'conflict_dev'
  AND c.contype = 'f';

-- Execute the resulting SQL after replacing schema name
```

---

## 🎯 **Recommendation Summary**

| Scenario | Recommended Method | Est. Time |
|----------|-------------------|-----------|
| Quick single table copy | Method 1 (Simple Copy) | 10-30 sec |
| Copy entire schema (few tables) | Method 1 (Simple Copy) | 1-2 min |
| Copy entire schema (many tables) | Method 2 (Automated Script) | 2-5 min |
| Need exact clone with all objects | Method 3 (pg_dump/restore) | 5-10 min |
| Periodic refresh | Method 4 (Truncate/Reload) | 1-3 min |
| Create test sandbox | Method 1 (Simple Copy) | 1-2 min |
| Backup before changes | Method 1 (Simple Copy) | 1-2 min |

---

## 📚 **Quick Reference Commands**

```sql
-- One-liner to copy a table:
CREATE TABLE target_schema.table_name (LIKE source_schema.table_name INCLUDING ALL) 
AS SELECT * FROM source_schema.table_name;

-- One-liner to verify copy:
SELECT 'source' as schema, COUNT(*) FROM source_schema.table_name
UNION ALL 
SELECT 'target' as schema, COUNT(*) FROM target_schema.table_name;

-- One-liner to drop target schema:
DROP SCHEMA IF EXISTS target_schema CASCADE;

-- One-liner to grant permissions:
GRANT ALL ON SCHEMA target_schema TO your_user;
GRANT ALL ON ALL TABLES IN SCHEMA target_schema TO your_user;
```

---

## 🎉 **Benefits**

1. **Time Savings**: 30-50x faster than re-running Snowflake migration
2. **Cost Savings**: Less Snowflake compute usage
3. **Flexibility**: Easy to create test/dev/prod environments
4. **Safety**: Quick backups before risky operations
5. **Simplicity**: Just SQL, no complex tools needed

---

## 📞 **Support**

For questions or issues:
1. Check PostgreSQL documentation: https://www.postgresql.org/docs/
2. Review this guide for examples
3. Test in dev environment first
4. Always verify row counts after copying

---

**Last Updated:** December 11, 2025
**Version:** 1.0

