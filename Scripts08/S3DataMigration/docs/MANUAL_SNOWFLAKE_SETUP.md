# 🔧 Manual Snowflake Storage Integration Setup

## Overview

This is a **one-time manual setup** that must be done by you (or your Snowflake admin) with ACCOUNTADMIN privileges.

**Time Required:** 15-30 minutes  
**Prerequisites:** ACCOUNTADMIN access to Snowflake, AWS Console access

---

## Step 1: Create IAM Role in AWS Console

### 1.1 Go to AWS IAM Console
- Navigate to: https://console.aws.amazon.com/iam/
- Click **Roles** → **Create role**

### 1.2 Select Trust Type
- Select: **Custom trust policy**
- Paste this **temporary** trust policy (we'll update it in Step 5):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::354073143602:root"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

- Click **Next**

### 1.3 Attach Permissions Policy
- Click **Create policy** (opens new tab)
- Select **JSON** tab
- Paste this policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SnowflakeS3Write",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion"
      ],
      "Resource": "arn:aws:s3:::cm-migration-dev01/*"
    },
    {
      "Sid": "SnowflakeS3List",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::cm-migration-dev01"
    }
  ]
}
```

- Click **Next: Tags** → **Next: Review**
- Name: `SnowflakeS3WritePolicy`
- Click **Create policy**
- Close the tab and return to role creation

### 1.4 Attach the Policy to Role
- Refresh the policy list
- Search for: `SnowflakeS3WritePolicy`
- Select it
- Click **Next**

### 1.5 Name the Role
- Role name: `snowflake-temperarly-iamrole` (or your preferred name)
- Description: `Allows Snowflake to write to S3 bucket cm-migration-dev01`
- Click **Create role**

### 1.6 Copy the Role ARN
- Click on the newly created role
- Copy the **ARN** (should look like: `arn:aws:iam::354073143602:role/snowflake-temperarly-iamrole`)
- **Save this ARN** - you'll need it in the next step!

---

## Step 2: Create Storage Integration in Snowflake

### 2.1 Open Snowflake
- Use DBeaver, Snowflake Web UI, or any SQL client
- Connect to your Snowflake account

### 2.2 Switch to ACCOUNTADMIN Role
```sql
USE ROLE ACCOUNTADMIN;
```

### 2.3 Create Storage Integration
**Replace the ARN with YOUR role ARN from Step 1.6:**

```sql
CREATE OR REPLACE STORAGE INTEGRATION CM_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::354073143602:role/snowflake-temperarly-iamrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://cm-migration-dev01/');
```

**Expected Result:**
```
Storage integration CM_S3_INTEGRATION successfully created.
```

---

## Step 3: Get Snowflake IAM User ARN

### 3.1 Describe the Storage Integration
```sql
DESC STORAGE INTEGRATION CM_S3_INTEGRATION;
```

### 3.2 Find These Two Properties
Look for these rows in the output:

| property | value |
|----------|-------|
| STORAGE_AWS_IAM_USER_ARN | `arn:aws:iam::123456789012:user/abc12345-s` |
| STORAGE_AWS_EXTERNAL_ID | `ABC12345_SFCRole=1_XXXXX` |

**📝 COPY BOTH VALUES** - you'll need them in the next step!

---

## Step 4: Update IAM Role Trust Policy in AWS

### 4.1 Go Back to AWS IAM Console
- Navigate to: https://console.aws.amazon.com/iam/
- Click **Roles**
- Find and click on: `snowflake-temperarly-iamrole`

### 4.2 Edit Trust Relationship
- Click **Trust relationships** tab
- Click **Edit trust policy** button

### 4.3 Replace Trust Policy
**Replace with this, using YOUR values from Step 3.2:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:user/abc12345-s"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "ABC12345_SFCRole=1_XXXXX"
        }
      }
    }
  ]
}
```

**Important:**
- Replace `arn:aws:iam::123456789012:user/abc12345-s` with YOUR `STORAGE_AWS_IAM_USER_ARN`
- Replace `ABC12345_SFCRole=1_XXXXX` with YOUR `STORAGE_AWS_EXTERNAL_ID`

- Click **Update policy**

---

## Step 5: Create External Stage in Snowflake

### 5.1 Switch to Your Working Database
```sql
USE DATABASE ANALYTICS;
USE SCHEMA BI;
```

### 5.2 Create the Stage
```sql
CREATE OR REPLACE STAGE CM_S3_STAGE
  STORAGE_INTEGRATION = CM_S3_INTEGRATION
  URL = 's3://cm-migration-dev01/'
  FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = SNAPPY
  );
```

**Expected Result:**
```
Stage CM_S3_STAGE successfully created.
```

---

## Step 6: Test the Setup

### 6.1 List the Stage (Should Not Error)
```sql
LIST @CM_S3_STAGE;
```

**Expected Result:**
- Empty list (no error) OR
- List of existing files in bucket

**If you get an error here**, the trust policy in Step 4 is likely incorrect.

### 6.2 Test Small UNLOAD
```sql
-- Create a test table
CREATE OR REPLACE TEMPORARY TABLE test_unload AS
SELECT 
  1 AS id, 
  'Test Row' AS name, 
  CURRENT_TIMESTAMP() AS created_at;

-- Unload to S3
COPY INTO @CM_S3_STAGE/test/test_unload.parquet
FROM test_unload
FILE_FORMAT = (
  TYPE = PARQUET
  COMPRESSION = SNAPPY
)
OVERWRITE = TRUE
MAX_FILE_SIZE = 104857600;
```

**Expected Result:**
```
files_created: 1
rows_unloaded: 1
```

### 6.3 Verify File in S3
**Option A: AWS Console**
- Go to S3 bucket: `cm-migration-dev01`
- Look for folder: `test/`
- Should see file: `test_unload_0_0_0.snappy.parquet`

**Option B: AWS CLI**
```bash
aws s3 ls s3://cm-migration-dev01/test/ --recursive
```

**✅ If you see the file, setup is complete!**

---

## Step 7: Grant Permissions (If Using Non-Admin User)

If your migration will run as a different user/role:

```sql
USE ROLE ACCOUNTADMIN;

-- Grant USAGE on integration
GRANT USAGE ON INTEGRATION CM_S3_INTEGRATION 
  TO ROLE YOUR_MIGRATION_ROLE;

-- Grant USAGE on stage
GRANT USAGE ON STAGE ANALYTICS.BI.CM_S3_STAGE 
  TO ROLE YOUR_MIGRATION_ROLE;
```

Replace `YOUR_MIGRATION_ROLE` with your actual role (e.g., `CONFLICTREPORT_USER`).

---

## ✅ Setup Complete!

You can now verify your setup is correct:

### Environment Variables to Update
```bash
SNOWFLAKE_STORAGE_INTEGRATION=CM_S3_INTEGRATION
SNOWFLAKE_STAGE_NAME=CM_S3_STAGE
SNOWFLAKE_AWS_ROLE_ARN=arn:aws:iam::354073143602:role/snowflake-temperarly-iamrole
```

---

## Troubleshooting

### Error: "Storage integration does not exist"
**Cause:** Not running as ACCOUNTADMIN  
**Fix:** Run `USE ROLE ACCOUNTADMIN;` before creating integration

### Error: "Access Denied" when listing stage
**Cause:** Trust policy not updated correctly  
**Fix:** 
1. Re-run `DESC STORAGE INTEGRATION CM_S3_INTEGRATION;`
2. Verify you copied the EXACT values to AWS trust policy
3. Make sure both `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` are correct

### Error: "Cannot assume role"
**Cause:** IAM role doesn't have S3 permissions  
**Fix:** Verify the policy from Step 1.3 is attached to the role

### Error: File not appearing in S3
**Cause:** Might be in a different location  
**Fix:** 
```bash
# List entire bucket
aws s3 ls s3://cm-migration-dev01/ --recursive
```

---

## Summary

This is a **one-time setup**. Once complete:
- ✅ Snowflake can UNLOAD to S3 automatically
- ✅ Python scripts will work without needing Snowflake admin access
- ✅ All future migrations use this integration

**Estimated Time:** 15-30 minutes  
**Difficulty:** Medium (requires AWS + Snowflake access)  
**Frequency:** One time only!

