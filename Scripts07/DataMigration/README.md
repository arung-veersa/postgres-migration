# Snowflake to PostgreSQL Migration Tool

A production-ready data migration tool for copying data from Snowflake to PostgreSQL with advanced features including adaptive chunking, parallel processing, watermark-based incremental loads, and comprehensive status tracking.

**Version:** 2.3 | **Last Updated:** December 16, 2025

---

## 🚀 What's New in v2.3

**Performance & Reliability Improvements:**

- ⚡ **Chunking optimization**: 11 minutes → 0.3 seconds (2,200x speedup with aggregated queries)
- 📉 **Logging fix**: Eliminated duplicate CloudWatch entries (50% cost reduction)
- 🔧 **PostgreSQL optimization**: Session-level performance settings for faster bulk loads
- 💾 **Memory tuning**: Balanced configuration for 20 parallel threads with 10GB Lambda
- 📊 **Monitoring**: Comprehensive query library for concurrent migration tracking

**Result:** 272M record migration time reduced from 10 days to 8-12 hours.

See [.context/PROJECT_CONTEXT.md](.context/PROJECT_CONTEXT.md) for current state and recent changes.

---

## Core Features

### Data Migration
- ✅ **Configuration-driven** - All logic in `config.json`, no code changes needed
- ✅ **Smart COPY/UPSERT mode** - Auto-selects optimal strategy (8-10x faster for bulk loads)
- ✅ **Adaptive chunking** - Automatically determines best chunking strategy per table
- ✅ **Parallel processing** - Multi-threaded chunk execution for maximum throughput
- ✅ **Incremental loads** - Watermark-based incremental updates with upsert support
- ✅ **Insert-only mode** - Skip duplicates for fast catch-up/resume scenarios

### Reliability & Safety
- ✅ **Resume capability** - Granular status tracking enables resuming failed migrations
- ✅ **Concurrent execution isolation** - Multiple migrations run safely without interference
- ✅ **Truncation protection** - Dual-layer safety prevents accidental data loss
- ✅ **Tiered error handling** - Auto-retry, auto-fallback, adaptive batch sizing
- ✅ **Lambda timeout handling** - Graceful shutdown before 15-minute AWS limit

### Management
- ✅ **Index management** - Automatically disable/restore indexes during bulk loads
- ✅ **Column filtering** - Handles schema differences automatically
- ✅ **Configuration validation** - Upfront validation catches errors before migration
- ✅ **Enhanced diagnostics** - Comprehensive logging for troubleshooting
- ✅ **Per-table optimization** - Override threads/batch sizes per table

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for complete feature documentation.

---

## Quick Start

### Prerequisites
- Python 3.11+
- Snowflake account with RSA key authentication
- PostgreSQL database (local or RDS)
- AWS account (for Lambda deployment)

### Local Execution

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp env.example .env
# Edit .env with your credentials

# 3. Configure migration
# Edit config.json with your sources and tables

# 4. Validate
python scripts/lambda_handler.py validate_config

# 5. Run migration
python scripts/lambda_handler.py migrate analytics
```

### AWS Lambda Deployment

```powershell
# 1. Build Lambda package (one-time)
cd deploy
.\rebuild_layer.ps1      # Dependencies layer
.\rebuild_app_only.ps1   # Application code

# 2. Deploy to AWS
# Upload to Lambda via AWS Console or CLI
# See docs/DEPLOYMENT.md for details

# 3. Configure Step Functions
# Import workflow from aws/step_functions/
```

See [docs/QUICKSTART.md](docs/QUICKSTART.md) for detailed setup instructions.

---

## Documentation

### Getting Started
- **[Quick Start Guide](docs/QUICKSTART.md)** - 5-minute setup for local development
- **[Configuration Reference](docs/CONFIGURATION.md)** - Complete config.json documentation
- **[Deployment Guide](docs/DEPLOYMENT.md)** - AWS Lambda and Step Functions setup

### Operations
- **[Monitoring Guide](docs/MONITORING.md)** - Track progress, queries, CloudWatch
- **[Optimization Guide](docs/OPTIMIZATION.md)** - Performance tuning and best practices
- **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)** - Common issues and solutions

### Development Context
- **[Project Context](.context/PROJECT_CONTEXT.md)** - Current state, recent changes, decisions
- **[Codebase Map](.context/CODEBASE_MAP.md)** - Where to find what in the code
- **[TODO List](.context/TODO.md)** - Current tasks and backlog
- **[Decisions Log](.context/DECISIONS.md)** - Architectural decisions and rationale
- **[Historical Issues](.context/HISTORICAL_ISSUES.md)** - Resolved bugs reference

---

## Project Structure

```
Scripts07/DataMigration/
├── README.md                    # This file
├── config.json                  # Migration configuration
├── migrate.py                   # Main orchestrator
│
├── .context/                    # Development context (session preservation)
│   ├── PROJECT_CONTEXT.md       # Current state, recent changes
│   ├── CODEBASE_MAP.md          # Code organization guide
│   ├── TODO.md                  # Task tracking
│   └── ...
│
├── docs/                        # User documentation
│   ├── QUICKSTART.md            # Quick setup guide
│   ├── CONFIGURATION.md         # Config reference
│   ├── OPTIMIZATION.md          # Performance tuning
│   ├── MONITORING.md            # Progress tracking
│   ├── TROUBLESHOOTING.md       # Problem solving
│   └── DEPLOYMENT.md            # AWS deployment
│
├── lib/                         # Core migration logic
│   ├── chunking.py              # Chunking strategies
│   ├── migration_worker.py      # Chunk processing
│   ├── connections.py           # DB connections
│   ├── status_tracker.py        # Progress tracking
│   └── utils.py                 # Utilities, logging
│
├── scripts/                     # Entry points
│   ├── lambda_handler.py        # AWS Lambda handler
│   └── migration_orchestrator.py  # Run orchestration
│
├── sql/                         # SQL scripts
│   ├── migration_status_schema.sql   # Status tables DDL
│   ├── QUICK_MONITORING.sql          # Quick queries
│   └── ...
│
├── aws/                         # AWS deployment
│   └── step_functions/          # Step Function definitions
│
└── deploy/                      # Build scripts
    ├── rebuild_app_only.ps1     # App package
    └── rebuild_layer.ps1        # Dependencies layer
```

---

## Configuration Example

```json
{
  "parallel_threads": 10,
  "batch_size": 25000,
  "sources": [
    {
      "source_name": "analytics",
      "enabled": true,
      "source_sf_database": "ANALYTICS_DB",
      "source_sf_schema": "PUBLIC",
      "target_pg_database": "analytics",
      "target_pg_schema": "analytics_dev",
      "tables": [
        {
          "enabled": true,
          "source": "FACTVISITCALLPERFORMANCE_CR",
          "target": "factvisitcallperformance_cr",
          "chunking_columns": ["Visit Updated Timestamp"],
          "chunking_column_types": ["timestamp"],
          "uniqueness_columns": ["Visit Id"],
          "source_watermark": "Visit Updated Timestamp",
          "target_watermark": "Visit Updated Timestamp",
          "truncate_onstart": true,
          "insert_only_mode": true,
          "parallel_threads": 20,
          "batch_size": 25000
        }
      ]
    }
  ]
}
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for complete reference.

---

## Common Use Cases

### Full Table Load
```json
{
  "truncate_onstart": true,
  "insert_only_mode": true,
  "source_watermark": null,
  "target_watermark": null
}
```
**Use for:** Initial loads, complete refreshes

### Incremental Updates
```json
{
  "truncate_onstart": false,
  "insert_only_mode": false,
  "source_watermark": "updated_timestamp",
  "target_watermark": "updated_timestamp"
}
```
**Use for:** Ongoing sync, delta loads

### Catch-up/Resume
```json
{
  "truncate_onstart": false,
  "insert_only_mode": true,
  "source_filter": "id > 1000000"
}
```
**Use for:** Adding missing data, resuming failed loads

---

## Monitoring

### Quick Status Check
```sql
SELECT 
    target_schema,
    source_table,
    ROUND(completed_chunks::NUMERIC / total_chunks * 100, 1) || '%' as progress,
    total_rows_copied,
    status
FROM migration_status.migration_table_status
WHERE status IN ('in_progress', 'completed')
ORDER BY target_schema, source_table;
```

See [docs/MONITORING.md](docs/MONITORING.md) and [sql/QUICK_MONITORING.sql](sql/QUICK_MONITORING.sql) for complete query library.

---

## Performance

### Typical Benchmarks
- **Small tables** (< 1M rows): Minutes
- **Medium tables** (1-10M rows): 1-2 hours
- **Large tables** (10-100M rows): 2-6 hours
- **Very large tables** (100M+ rows): 8-24 hours

### Optimization Tips
1. Use date-based chunking for large tables
2. Tune `parallel_threads` and `batch_size` per table
3. Upgrade Snowflake warehouse for faster queries
4. Enable `insert_only_mode` for full loads
5. Monitor memory usage and adjust threads

See [docs/OPTIMIZATION.md](docs/OPTIMIZATION.md) for detailed tuning guide.

---

## Troubleshooting

### Common Issues

**Migration stuck/slow:**
- Check Snowflake warehouse size
- Monitor CloudWatch logs for errors
- Verify parallel threads not causing OOM

**Missing rows:**
- Check chunking strategy (prefer date-based over ID-based for sparse data)
- Verify source_filter conditions
- Compare row counts: source vs target

**Memory errors:**
- Reduce `parallel_threads`
- Reduce `batch_size`
- Increase Lambda memory allocation

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for complete guide.

---

## Support & Development

### Session Preservation
When working with AI assistants (like Cursor), start new sessions with:
```
Please read .context/PROJECT_CONTEXT.md for current state
```

This provides complete context including:
- Recent changes and versions
- Active work and decisions
- Configuration details
- Known issues

### Contributing
See `.context/` folder for:
- **CODEBASE_MAP.md** - Where to find what
- **DECISIONS.md** - Why things are the way they are
- **TODO.md** - Current tasks

---

## License

[Your License Here]

---

## Version History

### v2.3.0 (December 2025)
- Chunking optimization (2,200x faster startup)
- Duplicate logging fix (50% CloudWatch cost reduction)
- PostgreSQL connection optimizations
- Memory-balanced configuration (20 threads, 10GB Lambda)

### v2.2.0 (December 2025)
- Parallel thread scaling (3 → 15 threads)
- Batch size optimization (50K → 25K)
- Performance boost (170x for large tables)

### v2.1.0 (November 2025)
- Concurrent execution isolation
- Truncation protection
- Enhanced diagnostics
- Per-table optimization

See [.context/HISTORICAL_ISSUES.md](.context/HISTORICAL_ISSUES.md) for resolved bugs.

---

**For detailed current state, always check [.context/PROJECT_CONTEXT.md](.context/PROJECT_CONTEXT.md)**
