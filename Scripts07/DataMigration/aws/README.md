# AWS Deployment Guide
## Snowflake to PostgreSQL Migration - Lambda & Step Functions

This guide covers deploying the migration tool to AWS Lambda with Step Functions orchestration.

---

## 📋 **Prerequisites**

### Required AWS Services:
- **AWS Lambda** - Executes migration code
- **AWS Step Functions** - Orchestrates migration workflow
- **VPC** - For PostgreSQL connectivity (if RDS)
- **IAM** - Roles and permissions
- **CloudWatch Logs** - Monitoring and debugging

### Required Tools:
- AWS CLI v2 ([Install](https://aws.amazon.com/cli/))
- Docker Desktop ([Install](https://www.docker.com/products/docker-desktop))
- PowerShell 7+ (Windows) or Bash (Linux/Mac)

### AWS Account Requirements:
- AWS Account ID
- Appropriate IAM permissions to create Lambda, Step Functions, IAM roles

---

## 🏗️ **Architecture**

```
Step Functions (Orchestrator)
    ↓
    ├── ValidateConfig (Lambda)
    ├── TestConnections (Lambda)
    └── ExecuteMigration (Lambda)
         ├── Snowflake (source data)
         ├── PostgreSQL (target data + status tracking)
         └── Auto-resume on timeout/failure
```

**Lambda Configuration:**
- Runtime: Python 3.11
- Memory: 3GB (configurable up to 10GB for very large datasets)
- Timeout: 900 seconds (15 minutes, Lambda maximum)
- Ephemeral Storage: 10GB (for large data buffering)
- VPC: Enabled (for PostgreSQL access)
- Layers: 2 (psycopg2 + heavy dependencies)

---

## 🔐 **IAM Roles & Permissions**

### 1. Lambda Execution Role

Create an IAM role with the following permissions:

**Trust Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**Managed Policies:**
- `AWSLambdaBasicExecutionRole` (CloudWatch Logs)
- `AWSLambdaVPCAccessExecutionRole` (VPC networking)

**Inline Policy (if using Secrets Manager):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": [
        "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:snowflake/*",
        "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:postgres/*"
      ]
    }
  ]
}
```

**Save the Role ARN:** `arn:aws:iam::ACCOUNT_ID:role/lambda-migration-execution-role`

---

### 2. Step Functions Execution Role

**Trust Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "states.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**Inline Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:REGION:ACCOUNT_ID:function:migration-lambda"
    }
  ]
}
```

**Save the Role ARN:** `arn:aws:iam::ACCOUNT_ID:role/stepfunctions-migration-execution-role`

---

## 🌐 **VPC Configuration**

### Requirements:
- Lambda must be in a VPC with:
  - **Private subnet(s)** with route to NAT Gateway (for Snowflake access)
  - **Security group** allowing outbound HTTPS (443) for Snowflake
  - **Security group** allowing outbound PostgreSQL (5432) to RDS

### Security Groups:

**Lambda Security Group (Outbound Rules):**
```
Type       | Protocol | Port Range | Destination       | Description
-----------+----------+------------+-------------------+-------------------
HTTPS      | TCP      | 443        | 0.0.0.0/0         | Snowflake access
PostgreSQL | TCP      | 5432       | <RDS-SG-ID>       | PostgreSQL access
```

**RDS Security Group (Inbound Rules):**
```
Type       | Protocol | Port Range | Source            | Description
-----------+----------+------------+-------------------+-------------------
PostgreSQL | TCP      | 5432       | <Lambda-SG-ID>    | Lambda access
```

---

## 🔧 **Deployment Steps**

### Step 1: Build Deployment Packages

```powershell
# Navigate to project directory
cd Scripts07/DataMigration

# Run build script (requires Docker running)
.\deploy\build_lambda.ps1
```

**Output:**
- `deploy/lambda_deployment.zip` - Application code (1-2 MB)
- `deploy/dependencies_layer.zip` - pandas, numpy, snowflake (150-200 MB)
- `deploy/psycopg2_layer.zip` - psycopg2 (5-10 MB)

---

### Step 2: Deploy to AWS

```powershell
# Navigate to aws directory
cd aws

# Run deployment script
.\deploy_step_functions.ps1
```

**Prompts:**
1. AWS Account ID: `123456789012`
2. Lambda Role ARN: `arn:aws:iam::123456789012:role/lambda-migration-execution-role`
3. Step Functions Role ARN: `arn:aws:iam::123456789012:role/stepfunctions-migration-execution-role`

---

### Step 3: Configure Lambda Environment Variables

#### Via AWS Console:
1. Go to Lambda → Functions → `migration-lambda`
2. Configuration → Environment variables → Edit
3. Add the following:

| Key | Value | Description |
|-----|-------|-------------|
| `SF_ACCOUNT` | `IKB38126.us-east-1` | Snowflake account |
| `SF_USER` | `migration_user` | Snowflake username |
| `SF_PASSWORD` | `***` | Snowflake password |
| `SF_WAREHOUSE` | `COMPUTE_WH` | Snowflake warehouse |
| `PG_HOST` | `mydb.abc123.us-east-1.rds.amazonaws.com` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_USER` | `postgres` | PostgreSQL username |
| `PG_PASSWORD` | `***` | PostgreSQL password |

#### Via AWS CLI:
```bash
aws lambda update-function-configuration \
  --function-name migration-lambda \
  --environment "Variables={
    SF_ACCOUNT=IKB38126.us-east-1,
    SF_USER=migration_user,
    SF_PASSWORD=***,
    SF_WAREHOUSE=COMPUTE_WH,
    PG_HOST=mydb.abc123.us-east-1.rds.amazonaws.com,
    PG_PORT=5432,
    PG_USER=postgres,
    PG_PASSWORD=***
  }" \
  --region us-east-1
```

---

### Step 4: Configure VPC

```bash
aws lambda update-function-configuration \
  --function-name migration-lambda \
  --vpc-config SubnetIds=subnet-abc123,subnet-def456,SecurityGroupIds=sg-xyz789 \
  --region us-east-1
```

---

## ✅ **Testing**

### Test 1: Validate Configuration

```bash
aws lambda invoke \
  --function-name migration-lambda \
  --payload '{"action": "validate_config"}' \
  --region us-east-1 \
  response.json

cat response.json
```

**Expected Output:**
```json
{
  "statusCode": 200,
  "body": {
    "status": "success",
    "message": "Configuration validated successfully",
    "sources": ["analytics", "aggregator", "conflict"]
  }
}
```

---

### Test 2: Test Connections

```bash
aws lambda invoke \
  --function-name migration-lambda \
  --payload '{"action": "test_connections"}' \
  --region us-east-1 \
  response.json
```

**Expected Output:**
```json
{
  "statusCode": 200,
  "body": {
    "status": "success",
    "message": "All connections successful",
    "snowflake": "IKB38126.us-east-1"
  }
}
```

---

### Test 3: Test Migration (Dry Run)

```bash
aws lambda invoke \
  --function-name migration-lambda \
  --payload '{
    "action": "migrate",
    "source_name": "analytics"
  }' \
  --region us-east-1 \
  response.json
```

**Monitor in CloudWatch Logs:**
```bash
aws logs tail /aws/lambda/migration-lambda --follow
```

---

## 🚀 **Execute Migration Workflow**

### Via AWS Console:
1. Go to Step Functions → State machines → `migration-state-machine`
2. Click "Start execution"
3. Input:
```json
{
  "source_name": "analytics",
  "no_resume": false,
  "resume_max_age": 12,
  "resume_attempt_count": 0
}
```
4. Click "Start execution"

### Via AWS CLI:
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:migration-state-machine \
  --input '{
    "source_name": "analytics",
    "no_resume": false,
    "resume_max_age": 12,
    "resume_attempt_count": 0
  }' \
  --region us-east-1
```

---

## 📊 **Monitoring**

### CloudWatch Logs:
- **Log Group:** `/aws/lambda/migration-lambda`
- View in real-time:
```bash
aws logs tail /aws/lambda/migration-lambda --follow
```

### Step Functions Execution:
- View in AWS Console → Step Functions → Executions
- Shows visual workflow progress
- Click on each state to see input/output

### PostgreSQL Status Tables:
```sql
-- Check migration run status
SELECT * FROM migration_status.migration_runs
ORDER BY started_at DESC
LIMIT 5;

-- Check table status
SELECT * FROM migration_status.migration_table_status
WHERE run_id = '<run-id>';

-- Check chunk status
SELECT status, COUNT(*) as count
FROM migration_status.migration_chunk_status
WHERE run_id = '<run-id>'
GROUP BY status;
```

---

## 🔄 **Resume Capability**

The migration automatically resumes from incomplete runs:

1. **Automatic Resume:** If a run times out or fails, the next execution will automatically detect and resume
2. **Status Tracking:** PostgreSQL status tables track completed/pending chunks
3. **Idempotent:** Safe to retry - completed work is skipped

**Force New Run:**
```json
{
  "source_name": "analytics",
  "no_resume": true
}
```

**Resume Specific Run:**
```json
{
  "source_name": "analytics",
  "resume_run_id": "efb5f5dd-e318-48f6-950d-f7a7df0e3b64"
}
```

---

## 🐛 **Troubleshooting**

### Issue: Lambda Timeout
**Symptom:** Function returns `status: "partial"`

**Solution:** This is expected! The Step Functions workflow will automatically resume.

---

### Issue: VPC Connection Timeout
**Symptom:** "Unable to connect to PostgreSQL" or "Connection timed out"

**Checks:**
1. Lambda in correct VPC and subnets?
2. Security groups allow outbound to PostgreSQL port?
3. RDS security group allows inbound from Lambda security group?
4. NAT Gateway exists for Snowflake access?

---

### Issue: Layer Size Limit
**Symptom:** "Unzipped size must be smaller than 262144000 bytes"

**Solution:** Dependencies are already split into 2 layers. If still too large:
1. Remove unused packages from `layer_requirements.txt`
2. Consider using EFS mount for very large dependencies

---

### Issue: Memory Errors
**Symptom:** "Runtime exited with error: signal: killed" or "MemoryError"

**Solution:**
1. Increase Lambda memory to 6GB or 10GB
2. Reduce `batch_size` in `config.json` (e.g., 25,000 instead of 50,000)
3. Reduce `parallel_threads` to 3 or 4

---

## 💰 **Cost Estimation**

### Lambda Costs (us-east-1):
- **Compute:** $0.0000166667 per GB-second
- **Requests:** $0.20 per 1M requests

**Example:**
- 10 GB dataset, 3GB memory, 30 minutes runtime
- Cost: ~$0.50 per run

### Step Functions:
- **State transitions:** $0.025 per 1,000 transitions
- Typical run: 10-20 transitions = $0.0005

### Data Transfer:
- Snowflake → Lambda: Free (inbound)
- Lambda → RDS (same region): Free

---

## 📚 **Additional Resources**

- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [AWS Step Functions Documentation](https://docs.aws.amazon.com/step-functions/)
- [VPC Configuration for Lambda](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)
- [Lambda Layers](https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html)

---

## 🆘 **Support**

For issues or questions:
1. Check CloudWatch Logs: `/aws/lambda/migration-lambda`
2. Check PostgreSQL status tables
3. Review Step Functions execution visual graph
4. See main project documentation

---

