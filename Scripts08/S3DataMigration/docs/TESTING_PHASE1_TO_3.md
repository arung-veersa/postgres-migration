# 🚀 Testing Guide - Phases 1-3

## Quick Start

```powershell
# 1. Setup Python environment
cd Scripts08\S3DataMigration
py -3.11 -m venv s3venv
.\s3venv\Scripts\activate
pip install -r requirements_s3.txt

# 2. Configure environment
Copy-Item env.s3template .env
notepad .env  # Fill in all values

# 3. Setup Snowflake (one-time, see snowflake/s3_setup.sql)
# 4. Test UNLOAD
python tests/test_snowflake_unload.py --table DIMPAYER --rows 100
```

---

## Prerequisites

### Required Software
- Python 3.11 or 3.12 (NOT 3.13)
- AWS CLI (optional, for verification)
- Snowflake access (ACCOUNTADMIN for initial setup)
- PostgreSQL access (for tracking tables)

### Required AWS Resources
- S3 bucket: `cm-migration-dev01`
- IAM role for Snowflake with S3 write permissions

### Required Snowflake Resources
- Storage Integration: `CM_S3_INTEGRATION`
- Stage: `cm_s3_stage` (in CONFLICTREPORT_SANDBOX.PUBLIC)

---

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_PRIVATE_KEY_PATH=C:/Users/ArunGupta/.snowflake/cmsfkey.pem

# PostgreSQL
POSTGRES_HOST=your-postgres-host
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password

# AWS S3 (Used by Python S3Manager to verify/list files)
# NOTE: Snowflake UNLOAD uses Storage Integration (IAM role), NOT these keys
AWS_ACCESS_KEY_ID=AKIA...  # IMPORTANT: Must be AKIA, not ASIA
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
AWS_S3_BUCKET=cm-migration-dev01

# Snowflake Storage Integration (How Snowflake writes to S3 - uses IAM role)
SNOWFLAKE_STORAGE_INTEGRATION=CM_S3_INTEGRATION
SNOWFLAKE_STAGE_NAME=CM_S3_STAGE
SNOWFLAKE_AWS_ROLE_ARN=arn:aws:iam::354073143602:role/snowflake-temperarly-iamrole
```

---

## Step 3: Setup PostgreSQL Tracking Tables

```powershell
# Connect to PostgreSQL and run:
psql -h your-postgres-host -U your_user -d conflict_management

# In psql:
\i sql/migration_status_schema.sql  # If not already created
\i sql/s3_migration_tracking.sql    # New S3 tracking tables
```

**Expected output:**
```
✅ S3 migration tracking tables created successfully!
```

---

## Step 4: Setup Snowflake Storage Integration

**Option A: Run in DBeaver or Snowflake Web UI**

Open `snowflake/s3_setup.sql` and follow the steps carefully. This is a **multi-step process** that requires:
1. Creating IAM role in AWS
2. Creating storage integration in Snowflake
3. Getting Snowflake IAM user ARN
4. Updating AWS IAM trust policy
5. Creating external stage
6. Testing

**Option B: If already done**

Skip this step if your admin has already created `CM_S3_INTEGRATION` and `CM_S3_STAGE`.

**Verify setup:**
```sql
DESC STORAGE INTEGRATION CM_S3_INTEGRATION;
LIST @CM_S3_STAGE;
```

---

## Step 5: Test S3 Connectivity

```powershell
# Make sure virtual environment is activated
.\s3venv\Scripts\activate

# Run S3 connection test
python tests/test_s3_connection.py
```

**Expected output:**
```
======================================================================
S3 CONNECTION TEST
======================================================================

✅ Loaded environment variables from .env
✅ Configuration loaded successfully
✅ Bucket access verified
✅ Upload successful
✅ Found 1 file(s)
✅ File info retrieved
✅ Download successful
✅ Downloaded content matches uploaded content
✅ File deleted
✅ Verified: File no longer exists in S3

======================================================================
✅ ALL TESTS PASSED!
======================================================================
```

**If this fails:**
- Check AWS credentials (must be AKIA..., not ASIA...)
- Check S3 bucket name
- Check IAM user has S3 permissions

---

## Step 6: Test Snowflake UNLOAD (Small Dataset)

```powershell
# Test with 100 rows from DIMPAYER
python tests/test_snowflake_unload.py --table DIMPAYER --rows 100
```

**Expected output:**
```
======================================================================
SNOWFLAKE UNLOAD TEST
======================================================================

✅ Loaded environment variables from .env
✅ Configuration loaded successfully
✅ Snowflake connection established
✅ S3 manager initialized
✅ Snowflake unloader initialized

📦 S3 Destination:
   s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/test_100_rows/

🚀 Starting UNLOAD operation...
======================================================================

✅ UNLOAD completed successfully!
   Duration: 5.23 seconds
   Files created: 1
   Total rows: 100
   Total size: 15,234 bytes (0.01 MB)

📦 Files:
   - data_0_0_0.snappy.parquet: 100 rows, 0.01 MB

======================================================================
VERIFYING FILES IN S3
======================================================================

✅ Found 1 file(s) in S3:
   - ANALYTICS/BI/DIMPAYER/test_100_rows/data_0_0_0.snappy.parquet
     Size: 15,234 bytes (0.01 MB)
     Modified: 2025-12-21 10:30:45

======================================================================
✅ TEST COMPLETED SUCCESSFULLY!
======================================================================

📊 Summary:
   Files created: 1
   Total rows: 100
   Total size: 0.01 MB
   Duration: 5.23 seconds
   S3 Location: s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/test_100_rows/

✨ Next step: Once aws_s3 extension is available, test loading into PostgreSQL
```

---

## Step 7: Test Full DIMPAYER Unload (160 rows)

```powershell
# Unload all DIMPAYER data (with filters from config)
python tests/test_snowflake_unload.py --table DIMPAYER
```

**Expected output:**
```
📋 Table Details:
   Database: ANALYTICS
   Schema: BI
   Table: DIMPAYER
   Filter: "Is Active" = TRUE AND "Is Demo" = FALSE AND "Source System" = 'hha'

⏱️  Estimated:
   Rows: 160
   Size: 0.08 MB (0.00 GB)
   Files: ~1
   Time: ~1 minute(s)

✅ UNLOAD completed successfully!
   Total rows: 160
   Total size: 0.15 MB

✨ Next step: Once aws_s3 extension is available, test loading into PostgreSQL
```

---

## Step 8: Verify Files in S3

**Option A: AWS CLI**
```powershell
aws s3 ls s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/ --recursive
```

**Option B: AWS Console**
1. Go to S3 Console
2. Navigate to bucket: `cm-migration-dev01`
3. Browse: `ANALYTICS/BI/DIMPAYER/`
4. Verify `.parquet` files exist

---

## Troubleshooting

### Error: "Could not deserialize key data"

**Cause:** Python 3.13 with PKCS#1 key format

**Solution:** Use Python 3.11 or 3.12 for virtual environment (see Step 1)

---

### Error: "The AWS Access Key Id you provided is not valid"

**Cause:** Using session token (ASIA...) instead of permanent credentials (AKIA...)

**Solution:** 
1. Create IAM user with permanent credentials
2. Attach S3 read access policy (example below)
3. Generate access key
4. Update `.env` with AKIA... credentials

**IAM Policy for Python S3 Access (Read-Only):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::cm-migration-dev01",
        "arn:aws:s3:::cm-migration-dev01/*"
      ]
    }
  ]
}
```

**Note:** Python only needs READ access. Snowflake writes using its own IAM role.

---

### Error: "Storage integration does not exist"

**Cause:** Storage integration not created in Snowflake

**Solution:** Run `snowflake/s3_setup.sql` (all steps)

---

### Error: "Access Denied" when listing stage

**Cause:** AWS IAM trust policy not updated with Snowflake IAM user ARN

**Solution:**
1. Run `DESC STORAGE INTEGRATION CM_S3_INTEGRATION;` in Snowflake
2. Copy `STORAGE_AWS_IAM_USER_ARN` value
3. Update AWS IAM role trust policy (see Step 3 in `snowflake/s3_setup.sql`)

---

### Error: "TIMESTAMP_TZ types are not supported"

**Cause:** Snowflake TIMESTAMP_TZ columns can't be exported to Parquet directly

**Solution:** This is handled automatically by `snowflake_unloader.py` (casts to TIMESTAMP_NTZ)

---

## Success Criteria

✅ Phase 1-3 is complete when:
- [ ] S3 connection test passes
- [ ] DIMPAYER (100 rows) unload succeeds
- [ ] DIMPAYER (160 rows) unload succeeds
- [ ] Files are visible in S3 bucket
- [ ] File sizes and row counts match expectations

---

## Next Steps

**After Phase 1-3 completion:**

1. ⏳ **Wait for aws_s3 extension** in PostgreSQL
2. ⏳ **Phase 4:** Test loading from S3 into PostgreSQL
3. ⏳ **Phase 5:** Build S3 migration worker
4. ⏳ **Phase 6:** Build orchestrator
5. ⏳ **Phase 7:** Lambda deployment
6. ⏳ **Phase 8:** Production test with FACTVISITCALLPERFORMANCE_CR (272M rows)

---

## Files Created in This Phase

```
Scripts08/S3DataMigration/
├── s3copyconfig.json                    # S3 migration configuration
├── env.s3template                       # Environment template
├── requirements_s3.txt                  # Python dependencies
│
├── lib/
│   ├── s3_manager.py                    # S3 operations
│   └── snowflake_unloader.py            # Snowflake UNLOAD
│
├── tests/
│   ├── test_s3_connection.py            # S3 connectivity test
│   ├── test_snowflake_unload.py         # UNLOAD test
│   └── s3venv_setup.md                  # venv setup guide
│
├── snowflake/
│   └── s3_setup.sql                     # Snowflake storage integration
│
├── sql/
│   └── s3_migration_tracking.sql        # S3 tracking tables
│
└── docs/
    └── S3_MIGRATION_GUIDE.md            # Complete guide
```

---

## Questions?

If you encounter any issues not covered here, check:
- `docs/S3_MIGRATION_GUIDE.md` - Complete architecture and setup guide
- `snowflake/s3_setup.sql` - Detailed Snowflake setup steps
- `tests/s3venv_setup.md` - Virtual environment troubleshooting

---

**🎯 Goal:** Successfully unload DIMPAYER to S3, then wait for aws_s3 extension to complete the cycle.


