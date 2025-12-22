# S3-Based Migration System

Migrates data from Snowflake to PostgreSQL via Amazon S3 staging.

## Documentation

- **[Quick Start](docs/README_S3.md)** - Get started quickly
- **[Setup Guide](docs/MANUAL_SNOWFLAKE_SETUP.md)** - Complete Snowflake & AWS setup
- **[Testing Guide](docs/TESTING_PHASE1_TO_3.md)** - Testing instructions
- **[Commit Summary](docs/COMMIT_READY.md)** - What's included in this release

### Architecture & Design
- **[S3 Migration Guide](docs/S3_MIGRATION_GUIDE.md)** - Architecture overview
- **[Credentials Architecture](docs/AWS_CREDENTIALS_ARCHITECTURE.md)** - Authentication details
- **[File Management](docs/S3_FILE_MANAGEMENT.md)** - OVERWRITE strategies

## Quick Start

```powershell
# 1. Setup
cd Scripts08/S3DataMigration
py -3.11 -m venv s3venv
.\s3venv\Scripts\activate
pip install -r requirements_s3.txt

# 2. Configure
Copy-Item env.s3template .env
notepad .env  # Fill in credentials

# 3. Test
python tests/test_snowflake_unload.py --table DIMPAYER --rows 100
```

## Configuration Files

- `s3copyconfig.json` - Migration configuration
- `env.s3template` - Environment template
- `requirements_s3.txt` - Python dependencies

## Status

**Phase 1-3: ✅ Complete**
- Snowflake UNLOAD to S3 working
- DIMPAYER tested (100 + 160 rows)

**Phase 4-6: ⏳ Awaiting**
- PostgreSQL `aws_s3` extension
- S3 → PostgreSQL load

## Key Components

```
lib/
  snowflake_unloader.py  # Snowflake UNLOAD implementation
  s3_manager.py          # S3 operations
  
sql/
  s3_migration_tracking.sql  # PostgreSQL tracking schema
  
snowflake/
  s3_setup.sql          # Snowflake setup instructions
  
tests/
  test_snowflake_unload.py  # Test script
```
