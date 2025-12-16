# Quick Start Guide

Get the PostgreSQL Migration Tool running locally in 5 minutes.

---

## Prerequisites

- **Python 3.11+**
- **Snowflake account** with RSA key authentication
- **PostgreSQL database** (local or RDS)
- **Access credentials** for both

---

## Step 1: Install (2 minutes)

```bash
# Navigate to project directory
cd Scripts07/DataMigration

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Step 2: Configure Environment (1 minute)

### Create `.env` file

```bash
# Copy template
cp env.example .env

# Edit with your credentials
nano .env  # or use any text editor
```

**Required variables:**
```bash
SNOWFLAKE_ACCOUNT=myaccount.us-east-1
SNOWFLAKE_USER=MIGRATION_USER
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_RSA_KEY=-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASC...
-----END PRIVATE KEY-----

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=mypassword
```

**Note:** RSA key should be the full PEM format with newlines preserved.

---

## Step 3: Configure Migration (1 minute)

Edit `config.json` with your source and target settings.

**Minimal working example:**

```json
{
  "parallel_threads": 5,
  "batch_size": 10000,
  "sources": [
    {
      "source_name": "my_source",
      "enabled": true,
      "source_sf_database": "MY_DATABASE",
      "source_sf_schema": "PUBLIC",
      "target_pg_database": "my_target_db",
      "target_pg_schema": "public",
      "tables": [
        {
          "enabled": true,
          "source": "MY_TABLE",
          "target": "my_table",
          "chunking_columns": ["id"],
          "chunking_column_types": ["numeric"],
          "uniqueness_columns": ["id"],
          "source_watermark": null,
          "target_watermark": null,
          "source_filter": null,
          "truncate_onstart": true,
          "insert_only_mode": true,
          "disable_index": false
        }
      ]
    }
  ]
}
```

**Key fields:**
- `chunking_columns` - Columns to split data (e.g., ["id"] or ["created_date"])
- `chunking_column_types` - Data types: "numeric", "timestamp", "uuid", "varchar_numeric"
- `uniqueness_columns` - Primary key columns
- `truncate_onstart` - True for full load, false for incremental
- `insert_only_mode` - True to skip duplicates, false to update

---

## Step 4: Setup PostgreSQL Schema (<1 minute)

Create the status tracking schema:

```bash
psql -h localhost -U postgres -d my_target_db -f sql/migration_status_schema.sql
```

**Or manually:**
```sql
CREATE SCHEMA IF NOT EXISTS migration_status;
-- See sql/migration_status_schema.sql for full DDL
```

---

## Step 5: Validate Configuration (<1 minute)

```bash
python scripts/lambda_handler.py validate_config
```

**Expected output:**
```
✓ Connected to Snowflake account: myaccount.us-east-1
✓ PostgreSQL connection manager initialized
✓ Configuration validated: 1 enabled sources
✓ Found 1 enabled tables
```

---

## Step 6: Test Connections

```bash
python scripts/lambda_handler.py test_connections
```

**Expected output:**
```
✓ Snowflake: myaccount.us-east-1
✓ PostgreSQL: Connected
```

---

## Step 7: Run Migration

```bash
python scripts/lambda_handler.py migrate my_source
```

**Output:**
```
[MY_TABLE] Starting migration...
[MY_TABLE] Using table-specific batch size: 10,000
[MY_TABLE] Created 50 chunks for MY_TABLE
[MY_TABLE] Processing 50 chunks with 5 threads...
✓ Completed: Fetch from Snowflake: MY_TABLE in 12.3s
✓ Completed: Load to PostgreSQL: my_table in 2.1s
[MY_TABLE] Progress: 50/50 chunks (100%)
✓ [MY_TABLE] Completed: 500,000 rows migrated
```

---

## Configuration Patterns

### Pattern 1: Full Table Load (Fastest)

**Use case:** Initial load, complete refresh

```json
{
  "enabled": true,
  "source": "USERS",
  "target": "users",
  "chunking_columns": ["user_id"],
  "chunking_column_types": ["numeric"],
  "uniqueness_columns": ["user_id"],
  "truncate_onstart": true,
  "insert_only_mode": true,
  "disable_index": false
}
```

**Behavior:**
- ✅ Truncates target table
- ✅ Uses COPY mode (fastest)
- ✅ No watermark logic

---

### Pattern 2: Incremental Updates with Watermark

**Use case:** Ongoing sync, delta loads

```json
{
  "enabled": true,
  "source": "TRANSACTIONS",
  "target": "transactions",
  "chunking_columns": ["updated_at"],
  "chunking_column_types": ["timestamp"],
  "uniqueness_columns": ["transaction_id"],
  "source_watermark": "updated_at",
  "target_watermark": "updated_at",
  "truncate_onstart": false,
  "insert_only_mode": false,
  "disable_index": false
}
```

**Behavior:**
- ✅ Queries max watermark from target
- ✅ Only fetches changed rows
- ✅ Uses UPSERT mode (inserts new, updates existing)

---

### Pattern 3: Large Table with Date Chunking

**Use case:** Tables > 10M rows with timestamp column

```json
{
  "enabled": true,
  "source": "FACT_TABLE",
  "target": "fact_table",
  "chunking_columns": ["created_date"],
  "chunking_column_types": ["timestamp"],
  "uniqueness_columns": ["composite_key_1", "composite_key_2"],
  "truncate_onstart": true,
  "insert_only_mode": true,
  "disable_index": true,
  "parallel_threads": 10,
  "batch_size": 25000
}
```

**Behavior:**
- ✅ Date-based chunking (efficient)
- ✅ Disables indexes during load
- ✅ Higher parallelism for speed

---

### Pattern 4: Catch-up Load with Filter

**Use case:** Loading specific date range, missing data

```json
{
  "enabled": true,
  "source": "ORDERS",
  "target": "orders",
  "source_filter": "order_date >= '2025-01-01'",
  "chunking_columns": ["order_date"],
  "chunking_column_types": ["timestamp"],
  "uniqueness_columns": ["order_id"],
  "truncate_onstart": false,
  "insert_only_mode": true
}
```

**Behavior:**
- ✅ Filters source data
- ✅ Appends to existing data
- ✅ Skips duplicates

---

## Important Configuration Rules

### Column Names
**MUST match exact casing in both databases:**

```json
// ✅ CORRECT - Matches actual column names
{
  "chunking_columns": ["Visit Updated Timestamp"],  // If column is "Visit Updated Timestamp"
  "uniqueness_columns": ["Visit Id"]                // If column is "Visit Id"
}

// ❌ WRONG - Case mismatch
{
  "chunking_columns": ["visit_updated_timestamp"],  // Will fail if actual is "Visit Updated Timestamp"
  "uniqueness_columns": ["visit_id"]                // Will fail
}
```

**Verify column names:**
```sql
-- PostgreSQL
SELECT column_name FROM information_schema.columns 
WHERE table_name = 'my_table';

-- Snowflake
DESC TABLE MY_SCHEMA.MY_TABLE;
```

---

### Chunking Column Types

| Type | Use For | Example |
|------|---------|---------|
| `numeric` | Integer IDs | `["id"]` |
| `timestamp` | Date/time columns | `["created_date"]` |
| `uuid` | UUID columns | `["uuid_column"]` |
| `varchar_numeric` | Numeric stored as VARCHAR | `["string_id"]` |

**Recommendation:** Use `timestamp` for large tables (> 10M rows)

---

### Required vs Optional Fields

**Required:**
- `enabled` - Must be `true` to process
- `source` - Snowflake table name
- `target` - PostgreSQL table name

**Optional but Recommended:**
- `chunking_columns` - For tables > 100K rows
- `chunking_column_types` - Required if chunking_columns specified
- `uniqueness_columns` - Required for UPSERT mode
- `source_watermark` / `target_watermark` - For incremental loads

---

## Monitoring Progress

### Check Migration Status

```sql
-- Current progress
SELECT 
    source_table,
    ROUND(completed_chunks::NUMERIC / NULLIF(total_chunks, 0) * 100, 1) || '%' as progress,
    total_rows_copied,
    status
FROM migration_status.migration_table_status
WHERE status IN ('in_progress', 'completed')
ORDER BY source_table;
```

### View Recent Runs

```sql
SELECT 
    run_id,
    status,
    total_tables,
    completed_tables,
    total_rows_copied,
    started_at,
    completed_at
FROM migration_status.migration_runs
ORDER BY started_at DESC
LIMIT 5;
```

### Check for Failed Chunks

```sql
SELECT 
    source_table,
    chunk_id,
    error_message,
    retry_count
FROM migration_status.migration_chunk_status
WHERE status = 'failed'
ORDER BY started_at DESC;
```

**See `sql/QUICK_MONITORING.sql` for more queries.**

---

## Troubleshooting Quick Fixes

### Issue: "RSA key authentication failed"

**Check RSA key format:**
```bash
# Key must be PEM format with header/footer
cat your_key.pem
# Should show:
# -----BEGIN PRIVATE KEY-----
# MIIEvQIBADANBg...
# -----END PRIVATE KEY-----
```

**In .env file:**
```bash
# Option 1: Path to file
SNOWFLAKE_RSA_KEY=/path/to/key.pem

# Option 2: Inline (preserve actual newlines)
SNOWFLAKE_RSA_KEY="-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBg...
-----END PRIVATE KEY-----"
```

---

### Issue: "Column 'xxx' does not exist"

**Verify column names in PostgreSQL:**
```sql
-- Check exact column names
SELECT column_name 
FROM information_schema.columns 
WHERE table_schema = 'public' 
  AND table_name = 'my_table'
ORDER BY ordinal_position;
```

**Update config.json with exact casing.**

---

### Issue: "Connection refused" (PostgreSQL)

**Check PostgreSQL is running:**
```bash
# Check status
pg_isready -h localhost

# Test connection
psql -h localhost -U postgres -d postgres -c "SELECT version();"
```

**Verify `postgresql.conf`:**
```
listen_addresses = '*'  # or 'localhost'
```

**Verify `pg_hba.conf`:**
```
host    all    all    0.0.0.0/0    md5
```

---

### Issue: "Table already has data, truncation skipped"

**This is SAFETY PROTECTION working correctly.**

**If you want fresh start:**
```sql
-- Clear data manually
TRUNCATE TABLE target_schema.target_table;

-- Clear status
DELETE FROM migration_status.migration_table_status 
WHERE source_table = 'YOUR_TABLE';

-- Then re-run migration
```

---

### Issue: Migration slow

**Quick fixes:**
1. **Increase threads:** `"parallel_threads": 10` (watch memory)
2. **Reduce batch size:** `"batch_size": 10000` (faster Snowflake queries)
3. **Use date chunking:** `"chunking_columns": ["date_column"]`
4. **Upgrade Snowflake warehouse:** `MEDIUM` recommended for large tables

**See [OPTIMIZATION.md](OPTIMIZATION.md) for detailed tuning.**

---

## Performance Tips

### For Small Tables (< 1M rows)
```json
{
  "parallel_threads": 3,
  "batch_size": 10000,
  "chunking_columns": ["id"],
  "chunking_column_types": ["numeric"]
}
```

### For Medium Tables (1-10M rows)
```json
{
  "parallel_threads": 5,
  "batch_size": 25000,
  "chunking_columns": ["created_date"],
  "chunking_column_types": ["timestamp"]
}
```

### For Large Tables (10M+ rows)
```json
{
  "parallel_threads": 10,
  "batch_size": 25000,
  "chunking_columns": ["created_date"],
  "chunking_column_types": ["timestamp"],
  "disable_index": true
}
```

**Remember:** Monitor memory usage and adjust threads accordingly.

---

## Command Reference

```bash
# Validate configuration
python scripts/lambda_handler.py validate_config

# Test connections
python scripts/lambda_handler.py test_connections

# Run migration for specific source
python scripts/lambda_handler.py migrate analytics

# Run migration for all sources
python scripts/lambda_handler.py migrate

# Check logs (verbose)
python scripts/lambda_handler.py migrate analytics 2>&1 | tee migration.log
```

---

## Next Steps

1. ✅ **Test with small table first** - Verify configuration works
2. ✅ **Review documentation:**
   - [CONFIGURATION.md](CONFIGURATION.md) - Complete config reference
   - [OPTIMIZATION.md](OPTIMIZATION.md) - Performance tuning
   - [MONITORING.md](MONITORING.md) - Progress tracking
3. ✅ **Deploy to AWS Lambda** - See [DEPLOYMENT.md](DEPLOYMENT.md)
4. ✅ **Setup monitoring** - Use queries from `sql/QUICK_MONITORING.sql`

---

## Getting Help

**For common issues:** See [TROUBLESHOOTING.md](TROUBLESHOOTING.md)

**For performance:** See [OPTIMIZATION.md](OPTIMIZATION.md)

**For AWS deployment:** See [DEPLOYMENT.md](DEPLOYMENT.md)

**For current state:** See [.context/PROJECT_CONTEXT.md](../.context/PROJECT_CONTEXT.md)

---

**Ready to migrate! 🚀**

For comprehensive documentation, see [README.md](../README.md)
