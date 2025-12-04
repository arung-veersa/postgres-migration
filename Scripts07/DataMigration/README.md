# PostgreSQL Migration Tool

A production-ready data migration tool that copies data from Snowflake to PostgreSQL with advanced features including adaptive chunking, parallel processing, watermark-based incremental loads, and comprehensive status tracking.

## Features

- ✅ **Configuration-driven**: Define all migration logic in `config.json`
- ✅ **Adaptive chunking**: Automatically determines optimal chunking strategy based on data distribution
- ✅ **Parallel processing**: Multi-threaded chunk processing for maximum throughput
- ✅ **Incremental loads**: Watermark-based incremental loading with upsert support
- ✅ **Resume capability**: Granular status tracking enables resuming failed migrations
- ✅ **Column filtering**: Automatically handles schema differences between Snowflake and PostgreSQL
- ✅ **Index management**: Optionally disable/restore indexes during bulk loads
- ✅ **Case preservation**: Maintains exact column name casing with double quotes
- ✅ **Retry logic**: Automatic retry with exponential backoff for transient failures
- ✅ **Configuration validation**: Upfront validation catches errors before migration starts
- ✅ **Dry-run mode**: Preview migrations without touching data
- ✅ **Lambda-ready**: Designed for AWS Lambda with timeout handling

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for a step-by-step guide to get started in 5 minutes.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up environment
cp env.example .env
# Edit .env with your credentials

# 3. Validate configuration
python migrate.py --dry-run

# 4. Run migration
python migrate.py
```

## Project Structure

```
Scripts07/DataMigration/
├── config.json              # Migration configuration
├── migrate.py               # Main entry point
├── requirements.txt         # Python dependencies
├── schema.sql              # Status tracking tables DDL
├── env.example             # Environment variables template
├── lib/
│   ├── config_loader.py    # Configuration loading & validation
│   ├── config_validator.py # Configuration validation logic
│   ├── connections.py      # Database connection management
│   ├── chunking.py         # Adaptive chunking strategies
│   ├── migration_worker.py # Chunk processing & data copying
│   ├── status_tracker.py   # Migration status tracking
│   ├── index_manager.py    # Index/constraint management
│   └── utils.py            # Logging and helper utilities
├── README.md               # This file
└── QUICKSTART.md          # Quick start guide
```

## Configuration

### Environment Variables (.env)

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=myaccount.us-east-1
SNOWFLAKE_USER=myuser
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_RSA_KEY=/path/to/key.pem  # or key content

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_USER=postgres
POSTGRES_PASSWORD=mypassword
```

### config.json Structure

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
      "source_name": "analytics",
      "source_sf_database": "ANALYTICS",
      "source_sf_schema": "BI",
      "target_pg_database": "conflict_management",
      "target_pg_schema": "analytics_dev",
      "tables": [
        {
          "enabled": true,
          "source": "DIMPAYER",
          "target": "dimpayer",
          "source_filter": "\"Is Active\" = TRUE",
          "chunking_columns": ["Payer Id"],
          "chunking_column_types": ["uuid"],
          "uniqueness_columns": ["Payer Id"],
          "sort_columns": ["Payer Id"],
          "source_watermark": "Updated Datatimestamp",
          "target_watermark": "Updated Datatimestamp",
          "truncate_onstart": false,
          "disable_index": false
        }
      ]
    }
  ]
}
```

### Configuration Options

#### Global Settings
- **parallel_threads**: Number of parallel threads (default: 4, recommended: 4-8)
- **batch_size**: Rows per chunk (default: 10000)
- **max_retry_attempts**: Retry attempts for failed chunks (default: 3)

#### Table Settings
- **enabled**: Process this table (true/false)
- **source**: Source table in Snowflake (e.g., `"DIMPAYER"` or `"\"lowercase_table\""`)
- **target**: Target table in PostgreSQL (lowercase by default)
- **source_filter**: WHERE clause to filter rows
- **chunking_columns**: Column(s) for chunking (must match exact casing)
- **chunking_column_types**: Data types (`int`, `uuid`, `date`, `varchar`)
- **uniqueness_columns**: Primary/unique key columns (must match exact casing)
- **sort_columns**: Ordering columns (optional, defaults to chunking_columns)
- **source_watermark**: Timestamp column for incremental loads
- **target_watermark**: Timestamp column in target
- **truncate_onstart**: Truncate before loading (faster than upsert)
- **disable_index**: Disable indexes during load (recommended for large tables)

### Important: Column Name Casing

**Column names must use exact casing as they appear in both databases:**
```json
{
  "chunking_columns": ["Payer Id"],      // ✅ Correct (matches PostgreSQL)
  "uniqueness_columns": ["Payer Id"],    // ✅ Correct
  "uniqueness_columns": ["payer_id"]     // ❌ Wrong (will fail)
}
```

**Table names use default casing (uppercase in Snowflake, lowercase in PostgreSQL):**
```json
{
  "source": "DIMPAYER",              // ✅ Standard (uppercase in Snowflake)
  "source": "\"multipayer_payer\"",  // ✅ Exception (lowercase in Snowflake, requires quotes)
  "target": "dimpayer"               // ✅ Standard (lowercase in PostgreSQL)
}
```

## Usage

### Basic Commands

```bash
# Validate configuration without migrating
python migrate.py --dry-run

# Run migration
python migrate.py

# Debug mode with detailed logging
python migrate.py --log-level DEBUG

# Use custom config
python migrate.py --config my_config.json --env-file .env.prod
```

### Command-Line Options

- `--config`: Path to configuration file (default: `config.json`)
- `--log-level`: Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`)
- `--env-file`: Path to .env file (default: `.env`)
- `--dry-run`: Preview migration without touching data

## Chunking Strategies

The tool automatically selects the optimal chunking strategy:

| Strategy | Data Types | Method | Example |
|----------|------------|--------|---------|
| **Numeric Range** | int, bigint | Divide range into equal chunks | ID 1-100K → [1-10K], [10K-20K]... |
| **Grouped Values** | uuid, varchar | Group distinct values | 50K UUIDs → batches of 10K |
| **Date Range** | date, timestamp | Group by date/time periods | 60 days → daily chunks |
| **Single Chunk** | Any (small tables) | Process entire table | <10K rows → 1 chunk |

## Monitoring & Status

### Status Tracking

The tool creates a `migration_status` schema with tracking tables:

```sql
-- View active migrations
SELECT * FROM migration_status.v_active_migrations;

-- View table progress
SELECT * FROM migration_status.v_table_progress;

-- View chunk details
SELECT * FROM migration_status.migration_chunk_status 
WHERE run_id = 'your-run-id'
ORDER BY chunk_id;
```

### Log Output

```
2024-12-04 10:00:00 | INFO | ✓ Configuration validation passed
2024-12-04 10:00:01 | INFO | ✓ Connected to Snowflake account: myaccount
2024-12-04 10:00:02 | INFO | ✓ Created migration run: 550e8400-e29b-41d4-a716...
2024-12-04 10:00:03 | INFO | [DIMPAYER] Created 5 chunks, processing with 4 threads...
2024-12-04 10:00:15 | INFO | ✓ [DIMPAYER] Completed: 50,000 rows migrated
```

## Performance & Optimization

### Current Performance
- **Small tables** (<1K rows): ~50-100 rows/sec
- **Medium tables** (1K-100K rows): ~500-1,000 rows/sec
- **Large tables** (>100K rows): ~1,000-2,000 rows/sec

### Optimization Features
- ✅ **Column metadata caching**: 99% reduction in metadata queries
- ✅ **Column filtering**: Automatically excludes Snowflake columns not in PostgreSQL
- ✅ **Watermark comparison**: Skips unchanged records
- ✅ **Parallel processing**: Multi-threaded chunk processing
- ✅ **Retry logic**: Exponential backoff for transient errors

### Performance Tuning

#### For Maximum Throughput:
```json
{
  "parallel_threads": 8,        // Increase for more parallelism
  "batch_size": 50000,          // Larger batches for large tables
  "truncate_onstart": true,     // Use COPY instead of UPSERT
  "disable_index": true         // Disable indexes during load
}
```

#### For Large Tables (>1M rows):
- Use `truncate_onstart: true` for full reloads
- Enable `disable_index: true`
- Increase `batch_size` to 50,000-100,000
- Choose high-cardinality chunking columns

#### For Small Tables (<10K rows):
- Use default settings
- Can disable parallel processing (overhead dominates)

#### For Incremental Updates:
- Set `source_watermark` and `target_watermark`
- Use appropriate `source_filter` to limit date ranges
- Keep `truncate_onstart: false`

### Troubleshooting Performance

**Slow chunks:**
- Check chunking column data distribution
- Try different `chunking_columns`
- Increase `batch_size`

**High memory usage:**
- Reduce `batch_size`
- Reduce `parallel_threads`

**Connection errors:**
- Reduce `parallel_threads`
- Check database connection limits

## Error Handling

### Automatic Recovery
- **Transient errors**: Automatic retry with exponential backoff (3 attempts)
- **Chunk failures**: Other chunks continue, failed chunks logged
- **Table failures**: Other tables continue processing
- **Resume capability**: Rerun to resume from last checkpoint

### Manual Recovery

```bash
# Check status
python -c "
from lib.connections import *
# Query migration_status tables
"

# Resume failed migration
python migrate.py  # Automatically resumes
```

## Troubleshooting

### Configuration Issues

**Error: "Environment variable 'XXX' is not set"**
- Check `.env` file exists and has all required variables

**Error: "Configuration validation failed"**
- Review error messages (points to exact issues)
- Run `python migrate.py --dry-run` to validate

**Error: "column 'xxx' does not exist"**
- Check column name casing in config matches PostgreSQL
- Use exact casing: `"Payer Id"` not `"payer_id"`

### Connection Issues

**Error: "Failed to connect to Snowflake"**
- Verify RSA key path/content
- Check Snowflake account name format: `account.region`
- Ensure user has warehouse access

**Error: "database 'xxx' does not exist"**
- Check `target_pg_database` in config
- Verify database exists in PostgreSQL

### Data Issues

**Warning: "Excluding N columns not in target table"**
- Normal: Snowflake has more columns than PostgreSQL
- Verify critical columns are present in target

**0 rows migrated (unexpected):**
- Check `source_filter` conditions
- Check watermark comparison (may skip unchanged data)
- Verify source table has data

## Development

### Running in Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Validate without running
python migrate.py --dry-run

# Test with debug logging
python migrate.py --log-level DEBUG
```

### Extending Functionality

**Add custom chunking strategy:**
1. Create class in `lib/chunking.py` inheriting from `ChunkingStrategy`
2. Implement `create_chunks()` method
3. Register in `ChunkingStrategyFactory`

**Add custom validation:**
1. Extend `ConfigValidator` in `lib/config_validator.py`
2. Add validation methods
3. Call from `validate()` method

## AWS Lambda Deployment

### Considerations
- Maximum runtime: 15 minutes
- Use `lambda_timeout_buffer_seconds` for graceful shutdown
- Implement checkpoint/resume for long migrations
- Use Lambda layers for dependencies
- Consider Step Functions for orchestration

### Configuration for Lambda
```json
{
  "parallel_threads": 2,                    // Lower for Lambda
  "batch_size": 5000,                       // Smaller batches
  "lambda_timeout_buffer_seconds": 120      // 2-minute buffer
}
```

## Best Practices

### Before Large Migrations
1. ✅ Run `--dry-run` to validate configuration
2. ✅ Test with small subset of tables first
3. ✅ Monitor database connections and memory
4. ✅ Schedule during low-traffic periods
5. ✅ Enable `disable_index` for large tables

### During Migration
1. Monitor logs for errors/warnings
2. Check status tracking tables
3. Watch database resource usage
4. Keep terminal session active or use screen/tmux

### After Migration
1. Verify row counts match
2. Check data quality in target
3. Re-enable any disabled indexes (automatic)
4. Review migration_runs table for metrics

## Support

For issues or questions:
1. Check this README and QUICKSTART.md
2. Review logs for detailed error messages
3. Check PostgreSQL `migration_status` schema for run history
4. Contact data engineering team

## License

Internal use only - Conflict Management System

---

**Version:** 1.0  
**Last Updated:** December 2024  
**Status:** Production Ready
