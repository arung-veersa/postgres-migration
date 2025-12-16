# AWS Deployment Guide

Complete guide to deploying the migration tool to AWS Lambda with Step Functions orchestration.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Architecture Overview](#architecture-overview)
3. [IAM Setup](#iam-setup)
4. [Lambda Setup](#lambda-setup)
5. [Step Functions Setup](#step-functions-setup)
6. [PostgreSQL Schema Setup](#postgresql-schema-setup)
7. [Environment Variables](#environment-variables)
8. [Deployment Process](#deployment-process)
9. [Testing](#testing)
10. [Troubleshooting Deployment](#troubleshooting-deployment)

---

## Prerequisites

### Required AWS Services
- AWS Lambda - Executes migration code
- AWS Step Functions - Orchestrates workflow
- VPC - For PostgreSQL connectivity (if RDS)
- IAM - Roles and permissions
- CloudWatch Logs - Monitoring

### Required Tools
- AWS CLI v2 ([Install](https://aws.amazon.com/cli/))
- PowerShell 7+ (Windows) or Bash (Linux/Mac)
- Python 3.11+

### AWS Account Requirements
- AWS Account with appropriate IAM permissions
- Ability to create Lambda functions, Step Functions, IAM roles
- VPC with subnets for Lambda (if using RDS PostgreSQL)

---

## Architecture Overview

```
User/Scheduler
    ↓
Step Functions (Orchestrator)
    ↓
    ├── ValidateConfig (Lambda)
    ├── TestConnections (Lambda)
    └── ExecuteMigration (Lambda)  ← Repeats on timeout
         ↓
         ├── Snowflake (source data)
         ├── PostgreSQL (target data + status tracking)
         └── Resume from checkpoint on timeout
```

### Lambda Configuration
- **Runtime:** Python 3.11
- **Memory:** 6-10GB (table-size dependent)
- **Timeout:** 900 seconds (15 minutes max)
- **Ephemeral Storage:** 10GB
- **VPC:** Enabled (for PostgreSQL)
- **Layers:** 2 (psycopg2 + dependencies)

### Key Features
- ✅ Auto-resume on 15-minute timeout
- ✅ Granular status tracking
- ✅ Concurrent execution support
- ✅ Retry with exponential backoff

---

## IAM Setup

### 1. Lambda Execution Role

**Trust Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
```

**Attach Managed Policies:**
- `AWSLambdaBasicExecutionRole` (CloudWatch Logs)
- `AWSLambdaVPCAccessExecutionRole` (VPC networking)

**Inline Policy (optional, for Secrets Manager):**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["secretsmanager:GetSecretValue"],
    "Resource": [
      "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:snowflake/*",
      "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:postgres/*"
    ]
  }]
}
```

**Create via AWS CLI:**
```bash
aws iam create-role \
  --role-name lambda-migration-execution-role \
  --assume-role-policy-document file://trust-policy.json

aws iam attach-role-policy \
  --role-name lambda-migration-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam attach-role-policy \
  --role-name lambda-migration-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
```

Save the Role ARN for later use.

---

### 2. Step Functions Execution Role

**Trust Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "states.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
```

**Inline Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "lambda:InvokeFunction"
    ],
    "Resource": [
      "arn:aws:lambda:REGION:ACCOUNT_ID:function:snowflake-postgres-migration"
    ]
  }]
}
```

---

## Lambda Setup

### Step 1: Build Layers

**Dependencies Layer (one-time):**
```powershell
cd Scripts07\DataMigration\deploy
.\rebuild_layer.ps1
```

This creates `dependencies_layer.zip` (~50MB) with:
- snowflake-connector-python
- numpy, pytz, cryptography
- Other heavy dependencies

**psycopg2 Layer (pre-built):**
- Use pre-built `psycopg2_layer.zip` (~1MB)
- Or download from AWS Lambda Layers

---

### Step 2: Build Application Package

```powershell
cd Scripts07\DataMigration\deploy
.\rebuild_app_only.ps1
```

This creates `lambda_deployment.zip` (~200KB) with:
- Application code (lib/, scripts/, migrate.py)
- config.json
- Excludes dependencies (in layers)

---

### Step 3: Create Lambda Function

**Via AWS Console:**
1. Go to Lambda → Create function
2. Author from scratch
3. Function name: `snowflake-postgres-migration`
4. Runtime: Python 3.11
5. Architecture: x86_64
6. Execution role: Use existing role (created in IAM Setup)

**Via AWS CLI:**
```bash
aws lambda create-function \
  --function-name snowflake-postgres-migration \
  --runtime python3.11 \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-migration-execution-role \
  --handler scripts/lambda_handler.lambda_handler \
  --timeout 900 \
  --memory-size 10240 \
  --ephemeral-storage Size=10240 \
  --region us-east-1 \
  --zip-file fileb://lambda_deployment.zip
```

---

### Step 4: Configure Lambda

**Memory & Timeout:**
```bash
aws lambda update-function-configuration \
  --function-name snowflake-postgres-migration \
  --memory-size 10240 \
  --timeout 900 \
  --region us-east-1
```

**VPC Configuration (if using RDS):**
```bash
aws lambda update-function-configuration \
  --function-name snowflake-postgres-migration \
  --vpc-config SubnetIds=subnet-xxx,subnet-yyy,SecurityGroupIds=sg-zzz \
  --region us-east-1
```

**Environment Variables:**
```bash
aws lambda update-function-configuration \
  --function-name snowflake-postgres-migration \
  --environment "Variables={ \
    SNOWFLAKE_ACCOUNT=your_account, \
    SNOWFLAKE_USER=your_user, \
    SNOWFLAKE_RSA_KEY=your_rsa_key, \
    SNOWFLAKE_WAREHOUSE=your_warehouse, \
    POSTGRES_HOST=your_host, \
    POSTGRES_USER=your_user, \
    POSTGRES_PASSWORD=your_password, \
    MIGRATION_VERSION=v2.3 \
  }" \
  --region us-east-1
```

---

### Step 5: Add Layers

**Upload dependencies layer:**
```bash
aws lambda publish-layer-version \
  --layer-name migration-dependencies \
  --zip-file fileb://dependencies_layer.zip \
  --compatible-runtimes python3.11 \
  --region us-east-1
```

**Upload psycopg2 layer:**
```bash
aws lambda publish-layer-version \
  --layer-name psycopg2 \
  --zip-file fileb://psycopg2_layer.zip \
  --compatible-runtimes python3.11 \
  --region us-east-1
```

**Attach layers to function:**
```bash
aws lambda update-function-configuration \
  --function-name snowflake-postgres-migration \
  --layers \
    arn:aws:lambda:us-east-1:ACCOUNT_ID:layer:migration-dependencies:1 \
    arn:aws:lambda:us-east-1:ACCOUNT_ID:layer:psycopg2:1 \
  --region us-east-1
```

---

## Step Functions Setup

### Step 1: Create State Machine

**Import workflow definition:**
- Use `aws/step_functions/migration_workflow_analytics.json`
- Replace placeholders:
  - `YOUR_LAMBDA_ARN` with actual Lambda ARN
  - `YOUR_REGION` with your AWS region

**Via AWS Console:**
1. Go to Step Functions → Create state machine
2. Choose "Write your workflow in code"
3. Paste workflow JSON
4. Name: `snowflake-postgres-migration-analytics`
5. Select execution role (created in IAM Setup)
6. Create

**Via AWS CLI:**
```bash
aws stepfunctions create-state-machine \
  --name snowflake-postgres-migration-analytics \
  --definition file://aws/step_functions/migration_workflow_analytics.json \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/step-functions-migration-role \
  --region us-east-1
```

---

### Step 2: Test Execution

**Start execution:**
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT_ID:stateMachine:snowflake-postgres-migration-analytics \
  --input '{"action":"validate_config","source_name":"analytics"}' \
  --region us-east-1
```

**Monitor execution:**
```bash
aws stepfunctions describe-execution \
  --execution-arn arn:aws:states:REGION:ACCOUNT_ID:execution:...:xxx \
  --region us-east-1
```

---

## PostgreSQL Schema Setup

### Step 1: Create Status Schema

Run the following on your PostgreSQL database:

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS migration_status;

-- Create runs table
CREATE TABLE IF NOT EXISTS migration_status.migration_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    config_hash VARCHAR(64),
    total_sources INTEGER DEFAULT 0,
    total_tables INTEGER DEFAULT 0,
    completed_tables INTEGER DEFAULT 0,
    failed_tables INTEGER DEFAULT 0,
    total_rows_copied BIGINT DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    created_by VARCHAR(255)
);

-- Create table status
CREATE TABLE IF NOT EXISTS migration_status.migration_table_status (
    run_id UUID NOT NULL REFERENCES migration_status.migration_runs(run_id) ON DELETE CASCADE,
    source_name VARCHAR(255) NOT NULL,
    source_database VARCHAR(255) NOT NULL,
    source_schema VARCHAR(255) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    target_database VARCHAR(255) NOT NULL,
    target_schema VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    total_chunks INTEGER DEFAULT 0,
    completed_chunks INTEGER DEFAULT 0,
    failed_chunks INTEGER DEFAULT 0,
    total_rows_copied BIGINT DEFAULT 0,
    indexes_disabled BOOLEAN DEFAULT FALSE,
    indexes_restored BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    metadata JSONB,
    PRIMARY KEY (run_id, source_database, source_schema, source_table)
);

-- Create chunk status
CREATE TABLE IF NOT EXISTS migration_status.migration_chunk_status (
    run_id UUID NOT NULL,
    source_database VARCHAR(255) NOT NULL,
    source_schema VARCHAR(255) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    chunk_id INTEGER NOT NULL,
    chunk_range JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    rows_copied INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    error_message TEXT,
    PRIMARY KEY (run_id, source_database, source_schema, source_table, chunk_id),
    FOREIGN KEY (run_id, source_database, source_schema, source_table) 
        REFERENCES migration_status.migration_table_status(run_id, source_database, source_schema, source_table) 
        ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX idx_migration_runs_status ON migration_status.migration_runs(status, started_at DESC);
CREATE INDEX idx_table_status_run_status ON migration_status.migration_table_status(run_id, status);
CREATE INDEX idx_chunk_status_run_status ON migration_status.migration_chunk_status(run_id, status);
```

**File:** `sql/migration_status_schema.sql`

---

### Step 2: Verify Schema

```sql
-- Check tables exist
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'migration_status';

-- Should show:
-- migration_runs
-- migration_table_status
-- migration_chunk_status
```

---

## Environment Variables

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Snowflake user | `MIGRATION_USER` |
| `SNOWFLAKE_RSA_KEY` | RSA private key (PEM format) | `-----BEGIN PRIVATE KEY-----\n...` |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse | `MIGRATION_WH` |
| `POSTGRES_HOST` | PostgreSQL host | `mydb.us-east-1.rds.amazonaws.com` |
| `POSTGRES_USER` | PostgreSQL user | `migration_user` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `SecurePassword123!` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MIGRATION_VERSION` | Version tag for logging | none |
| `LOG_LEVEL` | Logging level | `INFO` |

---

## Deployment Process

### Initial Deployment (One-Time)

1. **Build layers:**
   ```powershell
   .\deploy\rebuild_layer.ps1
   ```

2. **Upload layers to AWS:**
   ```bash
   # Upload dependencies layer
   aws lambda publish-layer-version ...
   
   # Upload psycopg2 layer
   aws lambda publish-layer-version ...
   ```

3. **Create Lambda function:**
   ```bash
   aws lambda create-function ...
   ```

4. **Configure Lambda:**
   - Memory: 10GB
   - Timeout: 900s
   - VPC settings
   - Environment variables
   - Attach layers

5. **Create Step Functions:**
   ```bash
   aws stepfunctions create-state-machine ...
   ```

6. **Setup PostgreSQL schema:**
   ```sql
   \i sql/migration_status_schema.sql
   ```

---

### Code Updates

**After code changes:**

1. **Rebuild package:**
   ```powershell
   .\deploy\rebuild_app_only.ps1
   ```

2. **Upload to Lambda:**
   ```bash
   aws lambda update-function-code \
     --function-name snowflake-postgres-migration \
     --zip-file fileb://deploy/lambda_deployment.zip \
     --region us-east-1
   ```

3. **Force container refresh:**
   ```bash
   aws lambda update-function-configuration \
     --function-name snowflake-postgres-migration \
     --environment "Variables={MIGRATION_VERSION=v2.3-$(date +%Y%m%d),...}" \
     --region us-east-1
   ```

---

### Configuration Updates

**After config.json changes:**

1. Rebuild package (includes config.json)
2. Upload to Lambda
3. Start new execution with `no_resume: true` if needed

---

## Testing

### 1. Test Lambda Directly

```bash
aws lambda invoke \
  --function-name snowflake-postgres-migration \
  --payload '{"action":"validate_config"}' \
  --region us-east-1 \
  response.json

cat response.json
```

**Expected:** `{"statusCode": 200, "body": {...}}`

---

### 2. Test Step Functions

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT_ID:stateMachine:migration-analytics \
  --input '{"action":"test_connections"}' \
  --region us-east-1
```

**Check execution:**
```bash
# Get execution ARN from previous command output
aws stepfunctions describe-execution \
  --execution-arn arn:aws:states:... \
  --region us-east-1
```

---

### 3. Test Small Migration

**Use small table first:**

```json
{
  "action": "migrate",
  "source_name": "analytics",
  "tables_filter": ["SMALL_TABLE_NAME"]
}
```

Monitor CloudWatch logs for:
- ✓ Connection success
- ✓ Chunking completes
- ✓ Data copied
- ✓ Status updated

---

## Troubleshooting Deployment

### Lambda Can't Connect to PostgreSQL

**Symptoms:** Timeout errors connecting to PostgreSQL

**Solutions:**
1. **Verify VPC configuration:**
   - Lambda in private subnets with NAT gateway OR
   - Lambda in public subnets (not recommended)
   - Security group allows outbound to PostgreSQL port

2. **Check security groups:**
   - PostgreSQL security group allows inbound from Lambda security group
   - Port 5432 (or custom port) open

3. **Test connectivity:**
   ```python
   # Test Lambda payload
   {
     "action": "test_connections"
   }
   ```

---

### Lambda Can't Connect to Snowflake

**Symptoms:** SSL or authentication errors

**Solutions:**
1. **Verify RSA key format:**
   - Must be PEM format
   - Include `-----BEGIN PRIVATE KEY-----` header/footer
   - Newlines replaced with `\n` in environment variable

2. **Check Snowflake user permissions:**
   ```sql
   SHOW GRANTS TO USER migration_user;
   ```

3. **Test authentication:**
   ```bash
   # Test via Lambda
   {"action": "test_connections"}
   ```

---

### Lambda Out of Memory

**Symptoms:** `Runtime.OutOfMemory` error

**Solutions:**
1. Reduce `parallel_threads` in config.json
2. Reduce `batch_size` in config.json
3. Increase Lambda memory allocation

See [OPTIMIZATION.md](OPTIMIZATION.md) for memory tuning.

---

### Step Functions Timeout

**Symptoms:** Execution fails after 1 hour (default timeout)

**Solution:**
Update state machine timeout:
```json
{
  "TimeoutSeconds": 86400,  // 24 hours
  ...
}
```

---

### Deployment Package Too Large

**Symptoms:** Error uploading zip file

**Solutions:**
1. Verify using layers (not including dependencies in main zip)
2. Check layer sizes:
   - dependencies_layer: ~50MB
   - psycopg2_layer: ~1MB
   - Application: ~200KB

3. If still too large, exclude test files, docs from build

---

## Post-Deployment Checklist

- [ ] Lambda function created and configured
- [ ] Layers attached successfully
- [ ] Environment variables set correctly
- [ ] VPC configuration correct (if using RDS)
- [ ] Step Functions state machine created
- [ ] PostgreSQL schema created
- [ ] Test connections successful
- [ ] Small table test migration successful
- [ ] CloudWatch logs showing expected output
- [ ] Monitoring queries working

---

**For ongoing monitoring, see [MONITORING.md](MONITORING.md)**  
**For performance tuning, see [OPTIMIZATION.md](OPTIMIZATION.md)**  
**For troubleshooting issues, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md)**

