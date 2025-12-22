# 🔐 AWS Credentials Architecture

## Overview

This document clarifies when and where AWS credentials are used in the S3-based migration system.

---

## Two Authentication Methods

### 1️⃣ **Snowflake UNLOAD → S3** (Uses IAM Role, NO AWS Keys)

**Authentication Method:** Snowflake Storage Integration with IAM Role

```
┌──────────────┐                    ┌──────────────┐
│              │                    │              │
│  Snowflake   │  ──────────────>   │  Amazon S3   │
│              │  Storage Integration│              │
└──────────────┘  (IAM Role)        └──────────────┘
                  No AWS Keys!
```

**Configuration:**
```sql
-- In Snowflake
CREATE STORAGE INTEGRATION CM_S3_INTEGRATION
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://cm-migration-dev01/');
```

**No AWS access keys needed for this step!** Snowflake assumes the IAM role to write to S3.

---

### 2️⃣ **Python/Lambda → S3** (Uses AWS Access Keys)

**Authentication Method:** AWS IAM User with Access Keys

```
┌──────────────┐                    ┌──────────────┐
│              │                    │              │
│  Python      │  ──────────────>   │  Amazon S3   │
│  S3Manager   │  boto3 + AWS Keys  │              │
└──────────────┘                    └──────────────┘
                  Requires AKIA...
```

**Used For:**
- ✅ Verifying files after Snowflake UNLOAD
- ✅ Listing files in S3 bucket
- ✅ Getting file metadata (size, modified date)
- ✅ Downloading files (if needed for fallback load)
- ✅ Cleanup operations (deleting old files)

**Configuration:**
```bash
# In .env file
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=your_secret_key
```

---

### 3️⃣ **PostgreSQL → S3** (TBD - When aws_s3 Extension Available)

**Option A: AWS IAM Role (Preferred)**
```
┌──────────────┐                    ┌──────────────┐
│              │                    │              │
│  PostgreSQL  │  ──────────────>   │  Amazon S3   │
│  (RDS)       │  IAM Role          │              │
└──────────────┘                    └──────────────┘
                  No AWS Keys!
```

**Option B: AWS Access Keys (Alternative)**
```
┌──────────────┐                    ┌──────────────┐
│              │                    │              │
│  PostgreSQL  │  ──────────────>   │  Amazon S3   │
│  (Non-RDS)   │  AWS Keys          │              │
└──────────────┘                    └──────────────┘
                  Requires AKIA...
```

We'll implement both options when `aws_s3` extension becomes available.

---

## Required Credentials Summary

| Component | Auth Method | Credentials Needed |
|-----------|-------------|-------------------|
| **Snowflake UNLOAD** | IAM Role | ❌ No AWS keys needed |
| **Python S3Manager** | Access Keys | ✅ AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY |
| **Lambda Functions** | Access Keys or IAM Role | ✅ AWS credentials (via IAM role in production) |
| **PostgreSQL LOAD** | IAM Role or Access Keys | ⏳ TBD (prefer IAM role if RDS) |

---

## Environment Variables Required

### Snowflake Storage Integration
```bash
# These define HOW Snowflake connects to S3
SNOWFLAKE_STORAGE_INTEGRATION=CM_S3_INTEGRATION
SNOWFLAKE_STAGE_NAME=CM_S3_STAGE
SNOWFLAKE_AWS_ROLE_ARN=arn:aws:iam::123456789012:role/snowflake-s3-role
```

### Python S3 Access
```bash
# These are used by boto3 in Python for S3 operations
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
AWS_S3_BUCKET=cm-migration-dev01
```

---

## IAM Policies

### Snowflake IAM Role Policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::cm-migration-dev01/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::cm-migration-dev01"
    }
  ]
}
```

### Python IAM User Policy
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

**Note:** Python only needs READ access. Snowflake does all the writing.

---

## Why This Design?

### ✅ Security Benefits
1. **Least Privilege**: Python only needs read access, not write
2. **No Shared Keys**: Snowflake and Python use different authentication
3. **Key Rotation**: Can rotate Python keys without affecting Snowflake

### ✅ Operational Benefits
1. **Snowflake Performance**: Direct S3 write via IAM role is faster
2. **No Key Management in Snowflake**: IAM role is more secure
3. **Audit Trail**: Separate IAM entities = better CloudTrail logging

---

## Testing Impact

When testing locally:
- ✅ **S3 connection test** (`test_s3_connection.py`) → Needs AWS keys
- ✅ **Snowflake UNLOAD test** (`test_snowflake_unload.py`) → Snowflake uses IAM role, Python uses keys to verify

Both sets of credentials are needed for complete testing!

---

## Production Deployment

### Lambda Functions (Future)
In production, Lambda will use **IAM roles**, not access keys:

```
┌──────────────┐                    ┌──────────────┐
│              │                    │              │
│  Lambda      │  ──────────────>   │  Amazon S3   │
│  Function    │  IAM Role          │              │
└──────────────┘  (No keys!)        └──────────────┘
```

We'll configure this during Lambda deployment (Phase 7).

---

## Summary

**Your understanding is correct!** Snowflake Storage Integration with IAM role means:
- ✅ **Snowflake → S3**: No AWS keys needed (uses IAM role)
- ✅ **Python → S3**: AWS keys needed (for verification/listing)
- ✅ **Both** are required for the complete system

The code has been updated to clarify this in comments! 🎯

