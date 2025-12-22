-- ====================================================================
-- Snowflake S3 Storage Integration Setup
-- ====================================================================
-- This script sets up Snowflake to unload data to Amazon S3
-- ====================================================================

-- IMPORTANT: Follow these steps in order!

-- ====================================================================
-- STEP 1: Create IAM Role in AWS (Do this in AWS Console first!)
-- ====================================================================

/*
1. Go to AWS IAM Console > Roles > Create Role
2. Select "Custom trust policy"
3. Use this trust policy (temporary - will update in Step 3):

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {}
    }
  ]
}

4. Attach this permission policy:

{
  "Version": "2012-10-17",
  "Statement": [
    {
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
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::cm-migration-dev01"
    }
  ]
}

5. Name the role: snowflake-s3-integration-role
6. Copy the Role ARN (e.g., arn:aws:iam::123456789012:role/snowflake-s3-integration-role)
*/

-- ====================================================================
-- STEP 2: Create Storage Integration in Snowflake
-- ====================================================================

-- NOTE: You need ACCOUNTADMIN or CREATE INTEGRATION privilege to run this

USE ROLE ACCOUNTADMIN;  -- Or role with CREATE INTEGRATION privilege

CREATE OR REPLACE STORAGE INTEGRATION CM_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-s3-integration-role'  -- REPLACE with your Role ARN from Step 1
  STORAGE_ALLOWED_LOCATIONS = ('s3://cm-migration-dev01/');  -- REPLACE with your bucket name

-- ====================================================================
-- STEP 3: Get Snowflake IAM User ARN and Update AWS Trust Policy
-- ====================================================================

-- Run this to get Snowflake's IAM user ARN:
DESC STORAGE INTEGRATION CM_S3_INTEGRATION;

-- Look for these two properties in the output:
-- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::123456789012:user/abc12345-s
-- STORAGE_AWS_EXTERNAL_ID: ABC12345_SFCRole=1_XXXXX

/*
Now go back to AWS IAM Console and update the trust policy:

1. Go to IAM > Roles > snowflake-s3-integration-role
2. Click "Trust relationships" tab > "Edit trust policy"
3. Replace with this (use values from DESC STORAGE INTEGRATION above):

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:user/abc12345-s"  <-- STORAGE_AWS_IAM_USER_ARN
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "ABC12345_SFCRole=1_XXXXX"  <-- STORAGE_AWS_EXTERNAL_ID
        }
      }
    }
  ]
}

4. Save the trust policy
*/

-- ====================================================================
-- STEP 4: Create External Stage
-- ====================================================================

-- Switch to your working database/schema
USE DATABASE ANALYTICS;  -- REPLACE with your database
USE SCHEMA BI;           -- REPLACE with your schema

-- Create the external stage
CREATE OR REPLACE STAGE CM_S3_STAGE
  STORAGE_INTEGRATION = CM_S3_INTEGRATION
  URL = 's3://cm-migration-dev01/'  -- REPLACE with your bucket
  FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = SNAPPY
  );

-- ====================================================================
-- STEP 5: Verify Setup
-- ====================================================================

-- Test 1: List stage (should not error)
LIST @CM_S3_STAGE;

-- Test 2: Try a small unload
COPY INTO @CM_S3_STAGE/test/test_unload.parquet
FROM (
  SELECT 1 AS id, 'test' AS name, CURRENT_TIMESTAMP() AS created_at
)
FILE_FORMAT = (
  TYPE = PARQUET
  COMPRESSION = SNAPPY
)
OVERWRITE = TRUE
MAX_FILE_SIZE = 104857600;

-- Test 3: Verify file in S3
-- Go to AWS S3 Console and check: s3://cm-migration-dev01/test/
-- You should see test_unload_0_0_0.snappy.parquet

-- ====================================================================
-- STEP 6: Grant Permissions (if needed)
-- ====================================================================

-- Grant USAGE on integration to your migration user
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION CM_S3_INTEGRATION TO ROLE CONFLICTREPORT_USER;  -- REPLACE with your role

-- Grant USAGE on stage to your migration user
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON STAGE CM_S3_STAGE TO ROLE CONFLICTREPORT_USER;  -- REPLACE with your role

-- ====================================================================
-- SUCCESS!
-- ====================================================================

/*
✅ If all steps completed successfully, you're ready to use S3 unloading!

Update your .env file with:
  SNOWFLAKE_STORAGE_INTEGRATION=CM_S3_INTEGRATION
  SNOWFLAKE_STAGE_NAME=CM_S3_STAGE

Then proceed to test Snowflake UNLOAD:
  python tests/test_snowflake_unload.py --table DIMPAYER --rows 100
*/

-- ====================================================================
-- Troubleshooting
-- ====================================================================

-- Problem: "Storage integration does not exist"
-- Solution: Run STEP 2 again with ACCOUNTADMIN role

-- Problem: "Access Denied" when listing stage
-- Solution: Verify AWS trust policy is updated with correct IAM user ARN (STEP 3)

-- Problem: "The AWS Access Key Id you provided is not valid"
-- Solution: Verify IAM role permissions policy allows s3:ListBucket and s3:GetObject (STEP 1)

-- Problem: "Cannot assume role"
-- Solution: Verify external ID in AWS trust policy matches STORAGE_AWS_EXTERNAL_ID (STEP 3)

-- ====================================================================
-- Cleanup (if you need to start over)
-- ====================================================================

-- DROP STAGE CM_S3_STAGE;
-- DROP STORAGE INTEGRATION CM_S3_INTEGRATION;


