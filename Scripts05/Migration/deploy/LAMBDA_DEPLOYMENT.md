# AWS Lambda Deployment Guide

## Quick Deployment (Current Setup)

### Prerequisites
- Docker Desktop installed and running
- AWS Lambda function created
- Lambda Runtime: Python 3.11

---

## Build & Deploy

### 1. Build Deployment Package
```powershell
cd Scripts05\Migration
.\deploy\build_lambda.ps1
```

**Output:** `deploy/lambda_deployment.zip` (~5 MB)

---

### 2. Upload to Lambda
**AWS Console:**
1. Go to Lambda Console → Your function
2. Click "Upload from" → ".zip file"
3. Upload `deploy/lambda_deployment.zip`
4. Wait for upload to complete

**AWS CLI:**
```powershell
aws lambda update-function-code `
    --function-name your-function-name `
    --zip-file fileb://deploy/lambda_deployment.zip
```

---

### 3. Configure Lambda

**Runtime Settings:**
- Runtime: `Python 3.11`
- Handler: `lambda_handler.lambda_handler`
- Memory: `512 MB` (minimum)
- Timeout: `15 minutes` (900s)

**Lambda Layers:**
- Attach psycopg2 layer (required for Postgres connectivity)
- Create from Docker or use existing layer

**VPC Configuration (if database in VPC):**
- Same VPC as RDS
- Private subnets with RDS access
- Security group allowing outbound to RDS port 5432

---

### 4. Environment Variables

```env
# Required for Postgres
POSTGRES_HOST=your-rds-endpoint.rds.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_CONFLICT_SCHEMA=conflict
POSTGRES_ANALYTICS_SCHEMA=analytics

# Required for Snowflake (optional)
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=ETL_USER
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=BI

# Optional
ENVIRONMENT=production
LOG_LEVEL=INFO
```

---

### 5. Test Lambda

**Validate Configuration:**
```json
{"action": "validate_config"}
```

**Test Postgres:**
```json
{"action": "test_postgres"}
```

**Test Snowflake:**
```json
{"action": "test_snowflake"}
```

---

## Creating psycopg2 Lambda Layer

If you need to create the psycopg2 layer:

```powershell
# Build layer
mkdir deploy/psycopg2_layer/python
docker run --rm `
    --entrypoint /bin/bash `
    -v "${PWD}/deploy/psycopg2_layer:/workspace" `
    public.ecr.aws/lambda/python:3.11 `
    -c "pip install psycopg2-binary==2.9.11 -t /workspace/python"

# Create ZIP
cd deploy/psycopg2_layer
Compress-Archive -Path python -DestinationPath ../psycopg2_layer.zip
```

**Upload Layer:**
1. Lambda Console → Layers → Create layer
2. Name: `psycopg2-layer`
3. Upload: `psycopg2_layer.zip`
4. Compatible runtimes: `Python 3.11`

**Attach to Function:**
1. Your Lambda function → Layers → Add a layer
2. Custom layers → `psycopg2-layer`

---

## Troubleshooting

### Import Errors
**Issue:** "Unable to import module 'lambda_handler'"
- Verify handler: `lambda_handler.lambda_handler`
- Check runtime: Python 3.11
- Ensure psycopg2 layer is attached

### Connection Timeout
**Issue:** Database connection fails
- Check VPC configuration
- Verify security groups (Lambda → RDS on port 5432)
- Ensure RDS accepts Lambda security group

### Configuration Missing
**Issue:** "Missing required configuration"
- Verify all environment variables are set
- Check for typos in variable names

---

## Monitoring

**CloudWatch Logs:**
- Log group: `/aws/lambda/your-function-name`
- All console output appears here

**CloudWatch Metrics:**
- Invocations, Duration, Errors, Throttles
- Set alarms for errors > 0

---

## Updates

To update Lambda after code changes:

```powershell
# 1. Rebuild
.\deploy\build_lambda.ps1

# 2. Upload
# Via Console or AWS CLI

# 3. Test
```

---

For implementation details, see `DEPLOYMENT_SUMMARY.md`.

