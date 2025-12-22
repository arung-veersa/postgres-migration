# S3-Based Migration System

Migrates data from Snowflake to PostgreSQL via Amazon S3 staging.

## Quick Start

```powershell
# 1. Setup environment
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

## Documentation

- **[Setup Guide](MANUAL_SNOWFLAKE_SETUP.md)** - Complete Snowflake & AWS setup
- **[Testing Guide](TESTING_PHASE1_TO_3.md)** - Testing instructions
- **[S3 Migration Guide](docs/S3_MIGRATION_GUIDE.md)** - Architecture & usage
- **[Credentials Architecture](docs/AWS_CREDENTIALS_ARCHITECTURE.md)** - Authentication details
- **[File Management](docs/S3_FILE_MANAGEMENT.md)** - OVERWRITE strategies

## Configuration

### Required Environment Variables

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_PRIVATE_KEY_PATH=path/to/key.pem

# Snowflake Storage Integration
SNOWFLAKE_STORAGE_INTEGRATION=CM_S3_INTEGRATION
SNOWFLAKE_STAGE_NAME=cm_s3_stage
SNOWFLAKE_STAGE_DATABASE=CONFLICTREPORT_SANDBOX
SNOWFLAKE_STAGE_SCHEMA=PUBLIC
SNOWFLAKE_AWS_ROLE_ARN=arn:aws:iam::xxx:role/snowflake-role

# S3
AWS_REGION=us-east-1
AWS_S3_BUCKET=cm-migration-dev01
```

## Architecture

```
Snowflake → S3 (Staging) → PostgreSQL
   ↓           ↓              ↓
Storage    Parquet        aws_s3
Integration  Files       Extension
```

## Current Status

**Phase 1-3: ✅ Complete**
- Snowflake UNLOAD to S3 working
- File tracking in PostgreSQL
- Manual verification tested

**Phase 4-6: ⏳ Waiting**
- PostgreSQL aws_s3 extension required
- S3 → PostgreSQL load implementation
- Full automation

## Testing

```powershell
# 100-row test
python tests/test_snowflake_unload.py --table DIMPAYER --rows 100

# Full table
python tests/test_snowflake_unload.py --table DIMPAYER

# Verify in Snowflake
LIST @CONFLICTREPORT_SANDBOX.PUBLIC.cm_s3_stage/ANALYTICS/BI/DIMPAYER/;
```

## Key Files

```
s3copyconfig.json          # Migration configuration
env.s3template             # Environment template
lib/snowflake_unloader.py  # Snowflake UNLOAD implementation
lib/s3_manager.py          # S3 operations
sql/s3_migration_tracking.sql  # PostgreSQL tracking schema
snowflake/s3_setup.sql     # Snowflake setup instructions
```

## Notes

- Python 3.11 or 3.12 required (not 3.13)
- Stage must be fully qualified: DATABASE.SCHEMA.STAGE_NAME
- Files auto-created in S3 by Snowflake
- OVERWRITE=TRUE by default for testing

