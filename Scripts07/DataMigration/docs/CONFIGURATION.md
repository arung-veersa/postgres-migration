# Configuration Reference

Complete reference for `config.json` configuration options.

---

## Table of Contents

1. [Overview](#overview)
2. [Connection Settings](#connection-settings)
3. [Global Settings](#global-settings)
4. [Source Configuration](#source-configuration)
5. [Table Configuration](#table-configuration)
6. [Chunking Strategies](#chunking-strategies)
7. [Watermark Configuration](#watermark-configuration)
8. [Performance Settings](#performance-settings)
9. [Migration Modes](#migration-modes)
10. [Examples by Use Case](#examples-by-use-case)
11. [Complete Settings Reference](#complete-settings-reference)

---

## Overview

The migration tool is entirely configuration-driven through `config.json`. All migration logic, chunking strategies, performance settings, and behavior are defined in this file.

**File Location:** `Scripts07/DataMigration/config.json`

**Basic Structure:**
```json
{
  "snowflake": {
    "account": "${SNOWFLAKE_ACCOUNT}",
    "user": "${SNOWFLAKE_USER}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "rsa_key": "${SNOWFLAKE_PRIVATE_KEY}"
  },
  "postgres": {
    "host": "${POSTGRES_HOST}",
    "user": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}"
  },
  "parallel_threads": 10,
  "batch_size": 25000,
  "max_retry_attempts": 3,
  "lambda_timeout_buffer_seconds": 90,
  "insert_only_mode": false,
  "sources": [...]
}
```

---

## Connection Settings

### Snowflake Configuration

Connection settings for Snowflake source database.

```json
{
  "snowflake": {
    "account": "${SNOWFLAKE_ACCOUNT}",
    "user": "${SNOWFLAKE_USER}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "rsa_key": "${SNOWFLAKE_PRIVATE_KEY}"
  }
}
```

#### `snowflake.account`
**Type:** String  
**Required:** Yes  
**Environment Variable:** `SNOWFLAKE_ACCOUNT`

Snowflake account identifier in format: `account.region`

**Examples:**
```json
"account": "abc12345.us-east-1"
"account": "mycompany.us-west-2"
```

---

#### `snowflake.user`
**Type:** String  
**Required:** Yes  
**Environment Variable:** `SNOWFLAKE_USER`

Snowflake user with permissions to read source tables.

**Example:**
```json
"user": "MIGRATION_USER"
```

**Required Permissions:**
```sql
GRANT USAGE ON WAREHOUSE migration_wh TO ROLE migration_role;
GRANT USAGE ON DATABASE analytics TO ROLE migration_role;
GRANT USAGE ON SCHEMA analytics.public TO ROLE migration_role;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.public TO ROLE migration_role;
```

---

#### `snowflake.warehouse`
**Type:** String  
**Required:** Yes  
**Environment Variable:** `SNOWFLAKE_WAREHOUSE`

Snowflake warehouse to use for queries.

**Example:**
```json
"warehouse": "COMPUTE_WH"
```

**Recommendations:**
- **Small migrations:** X-SMALL or SMALL
- **Medium migrations:** SMALL or MEDIUM
- **Large migrations (100M+ rows):** MEDIUM (fastest overall, lower total cost)

---

#### `snowflake.rsa_key`
**Type:** String  
**Required:** Yes  
**Environment Variable:** `SNOWFLAKE_PRIVATE_KEY` or `SNOWFLAKE_RSA_KEY`

RSA private key for authentication (PEM format).

**Format:**
```
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC...
-----END PRIVATE KEY-----
```

**In .env file:**
```bash
# Option 1: Path to file
SNOWFLAKE_PRIVATE_KEY=/path/to/rsa_key.pem

# Option 2: Inline (preserve newlines)
SNOWFLAKE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBg...
-----END PRIVATE KEY-----"
```

**In Lambda environment:**
```bash
# Newlines as \n
SNOWFLAKE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nMIIEvQ...\n-----END PRIVATE KEY-----"
```

---

### PostgreSQL Configuration

Connection settings for PostgreSQL target database.

```json
{
  "postgres": {
    "host": "${POSTGRES_HOST}",
    "user": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}"
  }
}
```

#### `postgres.host`
**Type:** String  
**Required:** Yes  
**Environment Variable:** `POSTGRES_HOST`

PostgreSQL server hostname or IP address.

**Examples:**
```json
"host": "localhost"
"host": "mydb.us-east-1.rds.amazonaws.com"
"host": "10.0.1.50"
```

---

#### `postgres.user`
**Type:** String  
**Required:** Yes  
**Environment Variable:** `POSTGRES_USER`

PostgreSQL user with permissions to write to target tables.

**Example:**
```json
"user": "migration_user"
```

**Required Permissions:**
```sql
GRANT CONNECT ON DATABASE target_db TO migration_user;
GRANT USAGE ON SCHEMA target_schema TO migration_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA target_schema TO migration_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA target_schema TO migration_user;

-- For status tracking
GRANT ALL ON SCHEMA migration_status TO migration_user;
GRANT ALL ON ALL TABLES IN SCHEMA migration_status TO migration_user;
```

---

#### `postgres.password`
**Type:** String  
**Required:** Yes  
**Environment Variable:** `POSTGRES_PASSWORD`

PostgreSQL user password.

**Example:**
```bash
# In .env file
POSTGRES_PASSWORD=SecurePassword123!
```

**Security Note:** Use environment variables or AWS Secrets Manager, never hardcode passwords in config.json.

---

## Global Settings

These settings apply to all sources and tables unless overridden.

### `parallel_threads`
**Type:** Integer  
**Default:** 6  
**Range:** 1-20

Number of concurrent threads for processing chunks.

**Guidelines:**
- Each thread: ~300-500 MB memory
- Monitor Lambda "Max Memory Used"
- Formula: `threads × 500MB < 80% of Lambda memory`

**Examples:**
```json
"parallel_threads": 10    // Balanced (6GB Lambda)
"parallel_threads": 20    // High performance (10GB Lambda)
"parallel_threads": 3     // Conservative (small tables)
```

---

### `batch_size`
**Type:** Integer  
**Default:** 10000  
**Range:** 1000-50000

Number of rows per chunk.

**Trade-offs:**
- **Larger:** Fewer chunks, more memory, slower Snowflake queries
- **Smaller:** More chunks, less memory, faster Snowflake queries

**Guidelines:**
- Large tables (100M+ rows): 25,000
- Medium tables (1-10M rows): 10,000-25,000
- Small tables (< 1M rows): 5,000-10,000

**Examples:**
```json
"batch_size": 25000   // Large tables
"batch_size": 10000   // Default
"batch_size": 5000    // Small tables or limited memory
```

---

### `max_retry_attempts`
**Type:** Integer  
**Default:** 3  
**Range:** 1-10

Maximum number of retry attempts for failed chunks before marking as permanently failed.

**How it works:**
- Chunks that fail due to transient errors (network, timeouts) are automatically retried
- Uses exponential backoff: 4s, 8s, 16s, 32s, 60s (max)
- After `max_retry_attempts`, chunk marked as `failed` in status table
- Failed chunks can be manually retried or investigated

**Examples:**
```json
"max_retry_attempts": 3     // Default (recommended)
"max_retry_attempts": 5     // More resilient to transient issues
"max_retry_attempts": 1     // Fail fast for debugging
```

**When to adjust:**
- **Increase to 5-10:** Unstable network, frequent transient errors
- **Decrease to 1-2:** Debugging, want to fail fast to see errors quickly
- **Keep at 3:** Most production scenarios

---

### `lambda_timeout_buffer_seconds`
**Type:** Integer  
**Default:** 120  
**Range:** 30-300

Time buffer (in seconds) before Lambda's 15-minute timeout to stop processing and save state gracefully.

**How it works:**
- Lambda has maximum 900 second (15 minute) timeout
- When remaining time < buffer, Lambda stops accepting new chunks
- In-progress chunks complete, status saved, Step Functions resumes
- Prevents abrupt timeouts that could corrupt data or lose progress

**Examples:**
```json
"lambda_timeout_buffer_seconds": 90     // Aggressive (more chunks per invocation)
"lambda_timeout_buffer_seconds": 120    // Default (balanced)
"lambda_timeout_buffer_seconds": 180    // Conservative (safer shutdown)
```

**Formula:**
```
Effective processing time = 900s - buffer
90s buffer  = 810s (13.5 min) processing
120s buffer = 780s (13 min) processing
180s buffer = 720s (12 min) processing
```

**When to adjust:**
- **Decrease to 60-90:** Small tables, fast chunks, want more throughput
- **Increase to 180-240:** Large chunks, slow Snowflake queries, need safe shutdown
- **Keep at 120:** Most scenarios

**Monitoring:**
Check CloudWatch logs for:
```
Lambda timeout approaching: 85.2s remaining (buffer: 120s)
```

---

### `insert_only_mode` (Global)
**Type:** Boolean  
**Default:** false

Global default for insert-only mode (skip duplicates instead of updating).

**Behavior:**
- `true`: Use `INSERT ... ON CONFLICT DO NOTHING` (skip duplicates)
- `false`: Use `INSERT ... ON CONFLICT DO UPDATE` (upsert)

**Can be overridden per table.**

**Examples:**
```json
"insert_only_mode": false    // Default (UPSERT mode)
"insert_only_mode": true     // All tables skip duplicates
```

**Use cases:**
- Set `true` globally if most tables are append-only or full-load
- Override per table for tables that need updates

**See:** [Migration Modes](#migration-modes) for detailed behavior

---

## Source Configuration

Each source represents a Snowflake-to-PostgreSQL mapping.

### Required Fields

```json
{
  "source_name": "analytics",
  "enabled": true,
  "source_sf_database": "ANALYTICS_DB",
  "source_sf_schema": "PUBLIC",
  "target_pg_database": "analytics",
  "target_pg_schema": "analytics_dev",
  "tables": [...]
}
```

### Field Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_name` | string | Yes | Unique identifier for this source |
| `enabled` | boolean | Yes | Enable/disable this source |
| `source_sf_database` | string | Yes | Snowflake database name |
| `source_sf_schema` | string | Yes | Snowflake schema name |
| `target_pg_database` | string | Yes | PostgreSQL database name |
| `target_pg_schema` | string | Yes | PostgreSQL schema name |
| `tables` | array | Yes | List of tables to migrate |

---

## Table Configuration

### Minimal Configuration

```json
{
  "enabled": true,
  "source": "SOURCE_TABLE_NAME",
  "target": "target_table_name"
}
```

### Complete Configuration

```json
{
  "enabled": true,
  "source": "FACTVISITCALLPERFORMANCE_CR",
  "target": "factvisitcallperformance_cr",
  
  "chunking_columns": ["Visit Updated Timestamp"],
  "chunking_column_types": ["timestamp"],
  "uniqueness_columns": ["Visit Id"],
  
  "source_watermark": "Visit Updated Timestamp",
  "target_watermark": "Visit Updated Timestamp",
  
  "source_filter": null,
  "truncate_onstart": true,
  "insert_only_mode": true,
  "disable_index": true,
  
  "parallel_threads": 20,
  "batch_size": 25000
}
```

### Field Reference

#### Basic Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enabled` | boolean | Yes | Enable/disable this table |
| `source` | string | Yes | Snowflake table name (case-sensitive) |
| `target` | string | Yes | PostgreSQL table name (lowercase) |

#### Chunking Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `chunking_columns` | array | No | Columns for chunking (default: primary key) |
| `chunking_column_types` | array | No | Data types: "numeric", "timestamp", "uuid", "varchar_numeric" |
| `uniqueness_columns` | array | No | Columns for deduplication (usually primary key) |

#### Watermark Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_watermark` | string | No | Source timestamp column for incremental loads |
| `target_watermark` | string | No | Target timestamp column for incremental loads |

#### Behavior Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `source_filter` | string | No | null | SQL WHERE clause for source data |
| `truncate_onstart` | boolean | No | false | Truncate target table before migration |
| `insert_only_mode` | boolean | No | false | Skip duplicates instead of updating |
| `disable_index` | boolean | No | false | Disable indexes during bulk load |

#### Performance Overrides

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `parallel_threads` | integer | No | Override global thread count for this table |
| `batch_size` | integer | No | Override global batch size for this table |

---

## Chunking Strategies

The tool automatically selects the best chunking strategy based on `chunking_columns` and `chunking_column_types`.

### 1. Date/Timestamp-Based (Recommended)

**Best for:** Large tables, natural time grouping

**Configuration:**
```json
{
  "chunking_columns": ["created_date"],
  "chunking_column_types": ["timestamp"]
}
```

**How it works:**
- Groups rows by date
- Creates chunks per date (or sub-chunks if date has many rows)
- Efficient filtering with `WHERE DATE(column) = '2025-01-15'`
- Supports resume at date level

**Advantages:**
- ✅ Predictable chunk sizes
- ✅ Works with sparse data
- ✅ Natural incremental load support
- ✅ Efficient Snowflake queries

**Use when:**
- Table has updated/created timestamp
- Data distribution relatively even by date
- Table size > 10M rows

---

### 2. Numeric ID-Based

**Best for:** Small-medium tables with sequential IDs

**Configuration:**
```json
{
  "chunking_columns": ["id"],
  "chunking_column_types": ["numeric"]
}
```

**How it works:**
- Divides ID range into equal segments
- Creates chunks: `WHERE id BETWEEN 1 AND 25000`

**Advantages:**
- ✅ Simple and fast
- ✅ Works with indexed ID columns

**Disadvantages:**
- ⚠️ Assumes uniform distribution
- ⚠️ Sparse IDs can cause missing data
- ⚠️ Deleted records create gaps

**Use when:**
- Sequential IDs with no large gaps
- Table size < 10M rows
- No suitable timestamp column

---

### 3. UUID-Based

**Configuration:**
```json
{
  "chunking_columns": ["uuid_column"],
  "chunking_column_types": ["uuid"]
}
```

**How it works:**
- Ranges based on UUID prefixes
- More complex, less predictable

**Advantages:**
- ✅ Works with UUID primary keys

**Disadvantages:**
- ⚠️ Less efficient than other strategies
- ⚠️ May use OFFSET (slow)

**Use when:**
- Only UUID column available
- No better option exists

---

### 4. VARCHAR as Numeric

**Configuration:**
```json
{
  "chunking_columns": ["id"],
  "chunking_column_types": ["varchar_numeric"]
}
```

**How it works:**
- Treats VARCHAR column as numeric (CAST)
- Useful for numeric IDs stored as VARCHAR

**Use when:**
- IDs are VARCHAR but contain only numbers

---

### 5. Offset-Based (Fallback)

**Auto-selected when:** No suitable column specified

**How it works:**
- Simple LIMIT/OFFSET pagination
- `SELECT * FROM table LIMIT 25000 OFFSET 0`

**Disadvantages:**
- ⚠️ Very slow for large tables
- ⚠️ Full table scans
- ⚠️ Not recommended

**Use only for:** Very small tables (< 100K rows)

---

## Watermark Configuration

### Incremental Loads

Watermarks enable incremental updates by tracking the last processed timestamp.

**Configuration:**
```json
{
  "source_watermark": "updated_timestamp",
  "target_watermark": "updated_timestamp",
  "truncate_onstart": false
}
```

**How it works:**
1. Query max watermark from target: `SELECT MAX(updated_timestamp) FROM target`
2. Filter source: `WHERE updated_timestamp > [max_watermark]`
3. Process only changed rows
4. UPSERT mode (insert new, update existing)

**Advantages:**
- ✅ Only process changed data
- ✅ Efficient for ongoing sync
- ✅ Reduces Snowflake costs

**Requirements:**
- Source column tracks last update time
- Column values never decrease
- `uniqueness_columns` defined for UPSERT

---

### Full Load (No Watermark)

**Configuration:**
```json
{
  "source_watermark": null,
  "target_watermark": null,
  "truncate_onstart": true
}
```

**How it works:**
1. Truncate target table
2. Copy all rows from source
3. COPY mode (fastest)

**Use for:**
- Initial loads
- Complete refreshes
- When incremental not needed

---

## Performance Settings

### Per-Table Overrides

Override global settings for specific tables:

```json
{
  "source": "LARGE_FACT_TABLE",
  "parallel_threads": 20,      // Override global
  "batch_size": 25000           // Override global
}
```

**When to override:**
- Large tables need more threads
- Small tables waste resources with high threads
- Memory-intensive tables need smaller batches

---

### Memory Calculation

**Formula:**
```
Memory per chunk = batch_size × columns × avg_row_size
Total memory = parallel_threads × memory_per_chunk
```

**Example:**
```
batch_size: 25,000 rows
columns: 250
avg_row_size: 200 bytes
Memory per chunk: 25,000 × 250 × 200 = 1.25 GB

parallel_threads: 10
Total memory: 10 × 125MB (after optimization) = 1.25 GB
```

**Safety margin:** Keep total memory < 80% of Lambda allocation

---

## Migration Modes

### Mode 1: Full Load (Truncate + Insert)

```json
{
  "truncate_onstart": true,
  "insert_only_mode": true,
  "source_watermark": null,
  "target_watermark": null
}
```

**Behavior:**
- Truncates target table
- Loads all source data
- Uses COPY mode (fastest)
- Skips duplicates if resume

**Use for:** Initial loads, complete refreshes

---

### Mode 2: Incremental Update (Watermark + Upsert)

```json
{
  "truncate_onstart": false,
  "insert_only_mode": false,
  "source_watermark": "updated_timestamp",
  "target_watermark": "updated_timestamp",
  "uniqueness_columns": ["id"]
}
```

**Behavior:**
- Queries max watermark from target
- Loads only new/changed rows
- Uses UPSERT mode (insert or update)
- Updates existing, inserts new

**Use for:** Ongoing sync, delta loads

---

### Mode 3: Append-Only (Insert + Skip Duplicates)

```json
{
  "truncate_onstart": false,
  "insert_only_mode": true,
  "source_filter": "created_date > '2025-01-01'"
}
```

**Behavior:**
- Inserts new rows
- Skips duplicate keys
- No updates
- Uses INSERT...ON CONFLICT DO NOTHING

**Use for:** Catch-up loads, append-only tables

---

### Mode 4: Targeted Load (Filter + Insert)

```json
{
  "truncate_onstart": false,
  "insert_only_mode": true,
  "source_filter": "id > 1000000"
}
```

**Behavior:**
- Filters source data
- Inserts matching rows
- Skips duplicates

**Use for:** Missing data recovery, specific date ranges

---

## Examples by Use Case

### Example 1: Large Fact Table (272M rows)

```json
{
  "enabled": true,
  "source": "FACTVISITCALLPERFORMANCE_CR",
  "target": "factvisitcallperformance_cr",
  "chunking_columns": ["Visit Updated Timestamp"],
  "chunking_column_types": ["timestamp"],
  "uniqueness_columns": ["Visit Id"],
  "truncate_onstart": true,
  "insert_only_mode": true,
  "disable_index": true,
  "parallel_threads": 20,
  "batch_size": 25000
}
```

**Rationale:**
- Date-based chunking (predictable, efficient)
- Full load (truncate + insert)
- High parallelism (20 threads with 10GB Lambda)
- Disabled indexes for speed

---

### Example 2: Dimension Table (Incremental)

```json
{
  "enabled": true,
  "source": "DIM_CUSTOMER",
  "target": "dim_customer",
  "chunking_columns": ["customer_id"],
  "chunking_column_types": ["numeric"],
  "uniqueness_columns": ["customer_id"],
  "source_watermark": "updated_date",
  "target_watermark": "updated_date",
  "truncate_onstart": false,
  "insert_only_mode": false,
  "parallel_threads": 5,
  "batch_size": 10000
}
```

**Rationale:**
- Numeric ID chunking (sequential IDs)
- Incremental with watermark
- UPSERT mode (update existing)
- Lower parallelism (small table)

---

### Example 3: Catch-up Load

```json
{
  "enabled": true,
  "source": "TRANSACTION_LOG",
  "target": "transaction_log",
  "source_filter": "transaction_date >= '2025-01-01'",
  "chunking_columns": ["transaction_date"],
  "chunking_column_types": ["timestamp"],
  "uniqueness_columns": ["transaction_id"],
  "truncate_onstart": false,
  "insert_only_mode": true,
  "parallel_threads": 10,
  "batch_size": 25000
}
```

**Rationale:**
- Source filter for specific date range
- Date-based chunking
- Insert-only (skip duplicates)
- No truncate (append to existing)

---

## Best Practices

### ✅ Do

1. **Use date-based chunking** for large tables (> 10M rows)
2. **Test with small batch** first (dry-run or small table)
3. **Monitor memory usage** in CloudWatch
4. **Disable indexes** for large bulk loads
5. **Use insert_only_mode** for full loads
6. **Set source_filter** to null when not needed
7. **Override per-table settings** for optimal performance

### ❌ Don't

1. **Don't use UUID chunking** unless necessary
2. **Don't set parallel_threads too high** (memory issues)
3. **Don't use numeric chunking** with sparse IDs
4. **Don't enable watermarks** for full loads
5. **Don't mix truncate_onstart=true** with watermarks
6. **Don't forget uniqueness_columns** for UPSERT mode

---

## Complete Settings Reference

### Connection Settings

| Setting | Level | Type | Default | Required | Description |
|---------|-------|------|---------|----------|-------------|
| `snowflake.account` | Global | string | - | Yes | Snowflake account identifier |
| `snowflake.user` | Global | string | - | Yes | Snowflake username |
| `snowflake.warehouse` | Global | string | - | Yes | Snowflake warehouse name |
| `snowflake.rsa_key` | Global | string | - | Yes | RSA private key (PEM format) |
| `postgres.host` | Global | string | - | Yes | PostgreSQL host |
| `postgres.user` | Global | string | - | Yes | PostgreSQL username |
| `postgres.password` | Global | string | - | Yes | PostgreSQL password |

### Global Settings

| Setting | Level | Type | Default | Range | Description |
|---------|-------|------|---------|-------|-------------|
| `parallel_threads` | Global | integer | 6 | 1-20 | Concurrent chunk processing threads |
| `batch_size` | Global | integer | 10000 | 1000-50000 | Rows per chunk |
| `max_retry_attempts` | Global | integer | 3 | 1-10 | Max retries for failed chunks |
| `lambda_timeout_buffer_seconds` | Global | integer | 120 | 30-300 | Graceful shutdown buffer (seconds) |
| `insert_only_mode` | Global | boolean | false | - | Global default for insert-only mode |

### Source Settings

| Setting | Level | Type | Required | Description |
|---------|-------|------|----------|-------------|
| `source_name` | Source | string | Yes | Unique identifier for source |
| `enabled` | Source | boolean | Yes | Enable/disable source |
| `source_sf_database` | Source | string | Yes | Snowflake database name |
| `source_sf_schema` | Source | string | Yes | Snowflake schema name |
| `target_pg_database` | Source | string | Yes | PostgreSQL database name |
| `target_pg_schema` | Source | string | Yes | PostgreSQL schema name |
| `tables` | Source | array | Yes | Array of table configurations |

### Table Settings (Basic)

| Setting | Level | Type | Required | Description |
|---------|-------|------|----------|-------------|
| `enabled` | Table | boolean | Yes | Enable/disable table |
| `source` | Table | string | Yes | Snowflake table name |
| `target` | Table | string | Yes | PostgreSQL table name |

### Table Settings (Chunking)

| Setting | Level | Type | Required | Description |
|---------|-------|------|----------|-------------|
| `chunking_columns` | Table | array | No | Columns for data partitioning |
| `chunking_column_types` | Table | array | No | Types: numeric, timestamp, uuid, varchar_numeric |
| `uniqueness_columns` | Table | array | No | Primary key columns (for UPSERT) |

### Table Settings (Watermark)

| Setting | Level | Type | Required | Description |
|---------|-------|------|----------|-------------|
| `source_watermark` | Table | string | No | Source timestamp column for incremental |
| `target_watermark` | Table | string | No | Target timestamp column for incremental |

### Table Settings (Behavior)

| Setting | Level | Type | Default | Description |
|---------|-------|------|---------|-------------|
| `source_filter` | Table | string | null | SQL WHERE clause for filtering |
| `truncate_onstart` | Table | boolean | false | Truncate before migration |
| `insert_only_mode` | Table | boolean | false | Skip duplicates (vs update) |
| `disable_index` | Table | boolean | false | Disable indexes during load |

### Table Settings (Performance Overrides)

| Setting | Level | Type | Default | Description |
|---------|-------|------|---------|-------------|
| `parallel_threads` | Table | integer | (global) | Override global thread count |
| `batch_size` | Table | integer | (global) | Override global batch size |

---

## Setting Precedence

**Override Order (highest to lowest priority):**

1. **Table-level** settings (`tables[].parallel_threads`)
2. **Global** settings (`parallel_threads`)
3. **Code defaults** (if not specified anywhere)

**Example:**
```json
{
  "parallel_threads": 10,        // Global default
  "sources": [{
    "tables": [
      {
        "source": "SMALL_TABLE"
        // Uses global: 10 threads
      },
      {
        "source": "LARGE_TABLE",
        "parallel_threads": 20   // Override: 20 threads for this table only
      }
    ]
  }]
}
```

---

## Environment Variable Substitution

Settings can reference environment variables using `${VAR_NAME}` syntax:

```json
{
  "snowflake": {
    "account": "${SNOWFLAKE_ACCOUNT}",     // ✅ Recommended
    "user": "${SNOWFLAKE_USER}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE}",
    "rsa_key": "${SNOWFLAKE_PRIVATE_KEY}"
  },
  "postgres": {
    "host": "${POSTGRES_HOST}",
    "user": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}"     // ✅ Never hardcode passwords
  }
}
```

**Benefits:**
- ✅ Secure (no credentials in git)
- ✅ Environment-specific (dev/prod)
- ✅ AWS Secrets Manager compatible

---

## Validation

The tool validates configuration on startup. Common errors:

**Missing required fields:**
```
Error: source_name is required
```

**Invalid parallel_threads:**
```
Error: parallel_threads must be between 1 and 20
```

**Watermark without uniqueness:**
```
Warning: Watermarks defined but no uniqueness_columns for UPSERT
```

**Conflicting settings:**
```
Warning: truncate_onstart=true with watermarks defined
```

Run validation:
```bash
python scripts/lambda_handler.py validate_config
```

---

## Configuration Templates

See `.context/PROJECT_CONTEXT.md` for current working configurations and examples.

---

**For performance tuning, see [OPTIMIZATION.md](OPTIMIZATION.md)**  
**For monitoring progress, see [MONITORING.md](MONITORING.md)**

