# Quick Start Guide

Get the PostgreSQL Migration Tool running in 5 minutes.

## Prerequisites

- Python 3.8+
- Snowflake access with RSA key
- PostgreSQL database
- Access credentials for both

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

## Step 2: Configure (2 minutes)

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
SNOWFLAKE_USER=myuser
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_RSA_KEY=/path/to/key.pem

POSTGRES_HOST=localhost
POSTGRES_USER=postgres
POSTGRES_PASSWORD=mypassword
```

### Configure `config.json`

**Minimal example:**
```json
{
  "snowflake": {
    "account": "${SNOWFLAKE_ACCOUNT}",
    "user": "${SNOWFLAKE_USER}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "rsa_key": "${SNOWFLAKE_RSA_KEY}"
  },
  "postgres": {
    "host": "${POSTGRES_HOST}",
    "user": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}"
  },
  "parallel_threads": 4,
  "batch_size": 10000,
  "sources": [
    {
      "enabled": true,
      "source_name": "my_source",
      "source_sf_database": "MY_DATABASE",
      "source_sf_schema": "PUBLIC",
      "target_pg_database": "my_target_db",
      "target_pg_schema": "public",
      "tables": [
        {
          "enabled": true,
          "source": "MY_TABLE",
          "target": "my_table",
          "source_filter": null,
          "chunking_columns": ["id"],
          "chunking_column_types": ["int"],
          "uniqueness_columns": ["id"],
          "sort_columns": ["id"],
          "source_watermark": null,
          "target_watermark": null,
          "truncate_onstart": true,
          "disable_index": false
        }
      ]
    }
  ]
}
```

## Step 3: Validate (<1 minute)

```bash
# Dry run to validate configuration
python migrate.py --dry-run
```

**Expected output:**
```
✓ Loaded environment variables from .env
✓ Configuration validation passed
✓ Connected to Snowflake account: myaccount
✓ PostgreSQL connection manager initialized

DRY RUN MODE - No data will be migrated
Configuration Summary:
  Total sources: 1
  Enabled sources: 1
  Tables: 1/1 enabled
  
✓ Ready to migrate!
```

## Step 4: Run Migration

```bash
# Start migration
python migrate.py
```

**Output:**
```
✓ Loaded environment variables from .env
✓ Configuration validation passed
✓ Connected to Snowflake account: myaccount
[MY_TABLE] Starting migration...
[MY_TABLE] Created 5 chunks, processing with 4 threads...
✓ [MY_TABLE] Completed: 50,000 rows migrated

✓ Migration completed successfully!
```

## Common Configuration Patterns

### Pattern 1: Simple Full Table Copy

```json
{
  "enabled": true,
  "source": "USERS",
  "target": "users",
  "chunking_columns": ["id"],
  "chunking_column_types": ["int"],
  "uniqueness_columns": ["id"],
  "truncate_onstart": true,         // Full reload
  "disable_index": false
}
```

### Pattern 2: Incremental Load with Watermark

```json
{
  "enabled": true,
  "source": "TRANSACTIONS",
  "target": "transactions",
  "source_filter": "\"created_at\" >= DATEADD(day, -7, CURRENT_DATE)",
  "chunking_columns": ["transaction_id"],
  "chunking_column_types": ["int"],
  "uniqueness_columns": ["transaction_id"],
  "source_watermark": "updated_at",     // Compare timestamps
  "target_watermark": "updated_at",
  "truncate_onstart": false,            // Upsert mode
  "disable_index": false
}
```

### Pattern 3: Large Table with Index Optimization

```json
{
  "enabled": true,
  "source": "BIG_TABLE",
  "target": "big_table",
  "chunking_columns": ["date_key"],
  "chunking_column_types": ["date"],
  "uniqueness_columns": ["composite_key_1", "composite_key_2"],
  "truncate_onstart": true,
  "disable_index": true                 // Disable for bulk load
}
```

### Pattern 4: Case-Sensitive Table Name

```json
{
  "enabled": true,
  "source": "\"lowercase_table\"",      // Quoted for Snowflake
  "target": "lowercase_table",
  "chunking_columns": null,             // Single chunk
  "chunking_column_types": null,
  "uniqueness_columns": null,
  "truncate_onstart": true
}
```

## Important Configuration Rules

### Column Names
**MUST match exact casing in PostgreSQL:**
```json
{
  "chunking_columns": ["Payer Id"],      // ✅ If PostgreSQL has "Payer Id"
  "chunking_columns": ["payer_id"]       // ❌ Will fail
}
```

### Table Names
**Default casing (no quotes needed unless exception):**
```json
{
  "source": "MY_TABLE",                  // ✅ Standard (Snowflake uppercase)
  "source": "\"mixed_Case_Table\"",      // ✅ Exception (requires quotes)
  "target": "my_table"                   // ✅ PostgreSQL lowercase
}
```

### Required Fields
- `enabled` - Must be `true` to process
- `source` - Snowflake table name
- `target` - PostgreSQL table name
- `chunking_columns` - Can be `null` for small tables
- `uniqueness_columns` - Required for upsert, can be `null` for truncate

## Troubleshooting Quick Fixes

### Issue: "Environment variable not set"
```bash
# Check .env file exists and has correct format
cat .env
# No quotes around values, no spaces around =
```

### Issue: "Configuration validation failed"
```bash
# Run dry-run to see detailed errors
python migrate.py --dry-run
```

### Issue: "column 'xxx' does not exist"
```bash
# Check PostgreSQL for actual column names
psql -h localhost -U postgres -d mydb -c "\d my_table"
# Update config with exact casing
```

### Issue: "Connection failed"
```bash
# Test Snowflake connection
python -c "
from snowflake.connector import connect
from dotenv import load_dotenv
import os
load_dotenv()
conn = connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER')
)
print('Connected!')
"
```

## Next Steps

1. **Review full documentation:** See [README.md](README.md) for comprehensive guide
2. **Add more tables:** Copy table configuration and modify
3. **Enable more sources:** Add additional sources to config
4. **Monitor progress:** Check `migration_status` schema in PostgreSQL
5. **Optimize performance:** Tune `batch_size` and `parallel_threads`

## Quick Reference

### Command Options
```bash
python migrate.py --dry-run              # Validate without running
python migrate.py --log-level DEBUG      # Detailed logging
python migrate.py --config custom.json   # Custom config file
```

### Monitoring
```sql
-- Check migration history
SELECT * FROM migration_status.migration_runs 
ORDER BY started_at DESC LIMIT 10;

-- View table progress
SELECT * FROM migration_status.v_table_progress;

-- Active migrations
SELECT * FROM migration_status.v_active_migrations;
```

### Performance Tips
- Start with `parallel_threads: 4` and `batch_size: 10000`
- Use `truncate_onstart: true` for full reloads (faster)
- Enable `disable_index: true` for large tables (>100K rows)
- Choose high-cardinality columns for `chunking_columns`

---

**Ready to migrate!** For more details, see [README.md](README.md)
