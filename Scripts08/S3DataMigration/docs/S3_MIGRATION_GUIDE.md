# S3-Based Migration Guide

## Overview

This guide covers the S3-staged migration approach for copying data from Snowflake to PostgreSQL via Amazon S3.

### Architecture

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│              │         │              │         │              │
│  Snowflake   │ UNLOAD  │   Amazon S3  │  LOAD   │  PostgreSQL  │
│              ├────────>│              ├────────>│              │
│              │         │  (Staging)   │         │              │
└──────────────┘         └──────────────┘         └──────────────┘
```

### Key Benefits

1. **Bypasses Lambda Timeout** - Snowflake UNLOAD can run independently of Lambda's 15-minute limit
2. **Decoupled Processing** - Unload and load can happen at different times
3. **Resume Granularity** - Can resume at file level, not just chunk level
4. **Cost Optimization** - S3 staging may be cheaper for massive datasets
5. **Parallel Processing** - Multiple files can be loaded in parallel

### Comparison: Direct vs S3-Staged

| Aspect | Direct Copy | S3-Staged |
|--------|-------------|-----------|
| **Speed (Small Tables)** | Faster (no S3 overhead) | Slower |
| **Speed (Large Tables)** | May timeout | Reliable |
| **Lambda Timeout Risk** | High for large chunks | Low |
| **Complexity** | Simple | Moderate |
| **Resume Granularity** | Chunk level | File level |
| **S3 Costs** | None | Storage + transfer |

---

## Prerequisites

### 1. AWS S3 Setup

**Create S3 Bucket:**
```bash
aws s3 mb s3://cm-migration-dev01 --region us-east-1
```

**Create IAM User for Python S3 Access:**
- User name: `migration-python-s3-reader`
- Purpose: Used by Python `S3Manager` to verify/list files (read-only)
- Policy: Custom read-only policy (see below)
- Generate access key (AKIA..., NOT session token ASIA...)

**IAM Policy (Read-Only for Python):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::cm-migration-dev01",
        "arn:aws:s3:::cm-migration-dev01/*"
      ]
    }
  ]
}
```

**Note:** Python only needs READ access. Snowflake UNLOAD uses its own IAM role (configured separately).

### 2. Snowflake Storage Integration

**Purpose:** Allows Snowflake to write directly to S3 using an IAM role (no AWS keys in Snowflake!)

**See:** `snowflake/s3_setup.sql` for complete setup instructions.

**Quick Summary:**
1. Create IAM role for Snowflake with S3 write permissions and trust policy
2. Create storage integration in Snowflake (references IAM role ARN)
3. Get Snowflake IAM user ARN from `DESC STORAGE INTEGRATION`
4. Update IAM role trust policy with Snowflake IAM user ARN and external ID
5. Create external stage pointing to S3
6. Test with small UNLOAD

### 3. PostgreSQL aws_s3 Extension

**Check if installed:**
```sql
SELECT * FROM pg_available_extensions WHERE name = 'aws_s3';
```

**If available, enable it:**
```sql
CREATE EXTENSION IF NOT EXISTS aws_s3;
```

**If not available:**
- Request admin to enable it (RDS: modify parameter group)
- Or use fallback method (download from S3 → psycopg2 COPY)

---

## Configuration

### s3copyconfig.json Structure

```json
{
  "aws": {
    "access_key_id": "${AWS_ACCESS_KEY_ID}",
    "secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
    "region": "${AWS_REGION}",
    "s3_bucket": "${AWS_S3_BUCKET}"
  },
  "s3_staging": {
    "bucket": "cm-migration-dev01",
    "prefix_pattern": "{source_database}/{source_schema}/{source_table}/",
    "file_format": "parquet",
    "compression": "snappy",
    "max_file_size_mb": 100
  },
  "snowflake_unload": {
    "storage_integration": "CM_S3_INTEGRATION",
    "stage_name": "CM_S3_STAGE",
    "warehouse_size": "MEDIUM"
  }
}
```

### Key Settings

| Setting | Description | Default |
|---------|-------------|---------|
| `prefix_pattern` | S3 folder structure | `{source_database}/{source_schema}/{source_table}/` |
| `file_format` | Parquet or CSV | `parquet` |
| `compression` | Compression algorithm | `snappy` |
| `max_file_size_mb` | Max size per file | `100` |
| `cleanup_after_load` | Delete S3 files after load | `false` |

---

## Usage

### Phase 1: Test S3 Connectivity

```powershell
cd Scripts08/S3DataMigration

# Create virtual environment
python -m venv s3venv
.\s3venv\Scripts\activate

# Install dependencies
pip install -r requirements_s3.txt

# Test S3 connection
python tests/test_s3_connection.py
```

**Expected Output:**
```
✅ S3 connection successful
✅ Upload test file: test_upload.txt
✅ List files in bucket
✅ Download test file
✅ Delete test file
```

### Phase 2: Test Snowflake UNLOAD

```powershell
# Unload small dataset (100 rows) from DIMPAYER
python tests/test_snowflake_unload.py --table DIMPAYER --rows 100

# Check files in S3
aws s3 ls s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/ --recursive
```

**Expected Output:**
```
✅ Snowflake connection successful
✅ UNLOAD query generated
✅ Executing UNLOAD...
✅ UNLOAD completed: 1 file(s) created
Files:
  - s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/data_0_0_0.snappy.parquet (15.2 KB)
```

### Phase 3: Test PostgreSQL Load (When aws_s3 Available)

```powershell
# Load from S3 into PostgreSQL
python tests/test_postgres_s3_load.py --s3-key ANALYTICS/BI/DIMPAYER/data_0_0_0.snappy.parquet --table dimpayer
```

---

## File Naming Convention

### S3 Folder Structure

```
s3://cm-migration-dev01/
├── ANALYTICS/
│   └── BI/
│       ├── DIMPAYER/
│       │   ├── run_<run_id>/
│       │   │   ├── chunk_001/
│       │   │   │   ├── data_0_0_0.snappy.parquet
│       │   │   │   └── data_0_0_1.snappy.parquet
│       │   │   └── chunk_002/
│       │   │       └── data_0_0_0.snappy.parquet
│       │   └── test_100_rows/
│       │       └── data_0_0_0.snappy.parquet
│       └── FACTVISITCALLPERFORMANCE_CR/
│           └── run_<run_id>/
│               ├── chunk_001/
│               ├── chunk_002/
│               └── ...
```

### File Naming Pattern

- **Test runs:** `{table}/test_{rows}_rows/data_*.parquet`
- **Production runs:** `{table}/run_{run_id}/chunk_{chunk_id}/data_*.parquet`

---

## Troubleshooting

### S3 Access Denied

**Error:** `The AWS Access Key Id you provided is not valid`

**Cause:** Using session token (ASIA...) instead of permanent credentials (AKIA...)

**Fix:** Create IAM user with permanent credentials (see Prerequisites)

**Important:** Python needs READ access only. Snowflake writes using its own IAM role.

### TIMESTAMP_TZ Unload Error

**Error:** `TIMESTAMP_TZ types are not supported for unloading to Parquet`

**Cause:** Snowflake's TIMESTAMP_TZ type is not supported in Parquet format

**Fix:** Cast to TIMESTAMP_NTZ during UNLOAD (automatically handled by `snowflake_unloader.py`)

### Storage Integration Not Found

**Error:** `Storage integration 'CM_S3_INTEGRATION' does not exist`

**Cause:** Storage integration not created in Snowflake

**Fix:** Run `snowflake/s3_setup.sql` to create integration

---

## Performance Considerations

### When to Use S3-Staged Migration

**Use S3-Staged for:**
- ✅ Very large tables (> 100M rows)
- ✅ Tables with large row sizes (risk of Lambda timeout)
- ✅ Tables that need to be unloaded once, loaded multiple times
- ✅ Migration with strict audit trail requirements (files in S3)

**Use Direct Copy for:**
- ✅ Small to medium tables (< 10M rows)
- ✅ Tables with incremental updates
- ✅ Quick catch-up migrations
- ✅ Development/testing

### File Size Optimization

**Smaller files (50-100 MB):**
- ✅ Faster parallel loading
- ✅ Better resume granularity
- ✅ Lower memory footprint

**Larger files (500-1000 MB):**
- ✅ Fewer S3 operations
- ✅ Lower overhead
- ❌ Slower resume on failure

**Recommended:** 100 MB per file (configurable in `s3copyconfig.json`)

---

## Next Steps

1. ✅ Complete Phase 1-3 testing (up to Snowflake UNLOAD)
2. ⏳ Wait for aws_s3 extension in PostgreSQL
3. ⏳ Complete Phase 4-6 (PostgreSQL load, orchestration)
4. ⏳ Lambda deployment
5. ⏳ Production migration of FACTVISITCALLPERFORMANCE_CR

---

## Related Documentation

- **[Snowflake COPY INTO Documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html)**
- **[AWS S3 Documentation](https://docs.aws.amazon.com/s3/)**
- **[PostgreSQL aws_s3 Extension](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/postgresql-s3-export.html)**


