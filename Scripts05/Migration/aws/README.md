# AWS Deployment Guide for ETL Pipeline

This directory contains AWS deployment resources for the ETL Pipeline using Lambda and Step Functions.

## 📋 Prerequisites

Before deploying to AWS, ensure you have:

1. **AWS Account** with permissions to create:
   - Lambda functions
   - Step Functions state machines
   - IAM roles and policies
   - Secrets Manager secrets (for database credentials)

2. **AWS CLI** installed and configured:
   ```bash
   aws --version
   aws configure  # If not already configured
   ```

3. **Local Testing Completed**:
   ```bash
   # Test locally before deploying
   python scripts/simulate_step_functions.py
   ```

---

## 🚀 Deployment Steps

### Step 1: Create IAM Roles

#### Lambda Execution Role

Create file: `aws/iam/lambda_role.json`

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

Create the role:
```bash
aws iam create-role \
  --role-name etl-pipeline-lambda-role \
  --assume-role-policy-document file://aws/iam/lambda_role.json
```

Attach policies:
```bash
# Basic Lambda execution
aws iam attach-role-policy \
  --role-name etl-pipeline-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# VPC access (if your RDS is in a VPC)
aws iam attach-role-policy \
  --role-name etl-pipeline-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole

# Secrets Manager (for database credentials)
aws iam attach-role-policy \
  --role-name etl-pipeline-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite
```

#### Step Functions Execution Role

Create file: `aws/iam/stepfunctions_role.json`

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

Create the role:
```bash
aws iam create-role \
  --role-name etl-pipeline-stepfunctions-role \
  --assume-role-policy-document file://aws/iam/stepfunctions_role.json
```

Create inline policy for Lambda invocation:
```bash
aws iam put-role-policy \
  --role-name etl-pipeline-stepfunctions-role \
  --policy-name StepFunctionsInvokeLambda \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "lambda:InvokeFunction"
        ],
        "Resource": "arn:aws:lambda:*:*:function:etl-pipeline-function"
      }
    ]
  }'
```

---

### Step 2: Store Database Credentials in Secrets Manager

```bash
aws secretsmanager create-secret \
  --name etl-pipeline/postgres \
  --description "PostgreSQL credentials for ETL pipeline" \
  --secret-string '{
    "host": "your-rds-endpoint.amazonaws.com",
    "port": "5432",
    "database": "conflictreport",
    "user": "your_username",
    "password": "your_password"
  }'
```

**Note:** Update Lambda handler to read from Secrets Manager (optional improvement).

---

### Step 3: Package Lambda Function

```bash
cd Scripts05/Migration

# Create package directory
mkdir -p lambda_package

# Install dependencies
pip install -r requirements.txt -t lambda_package/

# Copy source code
cp -r src/ lambda_package/
cp -r config/ lambda_package/
cp -r sql/ lambda_package/
cp scripts/lambda_handler.py lambda_package/
cp scripts/mock_postgres_connector.py lambda_package/

# Create deployment package
cd lambda_package
zip -r ../lambda_function.zip .
cd ..

# Verify package size (should be < 50MB uncompressed)
ls -lh lambda_function.zip
```

---

### Step 4: Deploy Lambda Function

```bash
# Get the Lambda role ARN
LAMBDA_ROLE_ARN=$(aws iam get-role --role-name etl-pipeline-lambda-role --query 'Role.Arn' --output text)

# Create Lambda function
aws lambda create-function \
  --function-name etl-pipeline-function \
  --runtime python3.11 \
  --role $LAMBDA_ROLE_ARN \
  --handler lambda_handler.lambda_handler \
  --zip-file fileb://lambda_function.zip \
  --timeout 900 \
  --memory-size 1024 \
  --environment Variables="{
    POSTGRES_HOST=your-rds-endpoint.amazonaws.com,
    POSTGRES_PORT=5432,
    POSTGRES_DATABASE=conflictreport,
    POSTGRES_USER=your_username,
    POSTGRES_PASSWORD=your_password,
    POSTGRES_CONFLICT_SCHEMA=conflict,
    POSTGRES_ANALYTICS_SCHEMA=analytics
  }"

# If your RDS is in a VPC, add VPC configuration
# --vpc-config SubnetIds=subnet-xxx,subnet-yyy,SecurityGroupIds=sg-xxx
```

**Update Lambda function (for subsequent deploys):**
```bash
aws lambda update-function-code \
  --function-name etl-pipeline-function \
  --zip-file fileb://lambda_function.zip
```

---

### Step 5: Test Lambda Function

```bash
# Test validate_config
aws lambda invoke \
  --function-name etl-pipeline-function \
  --payload '{"action": "validate_config"}' \
  response.json

cat response.json

# Test Task 01
aws lambda invoke \
  --function-name etl-pipeline-function \
  --payload '{"action": "task_01"}' \
  task01_response.json

cat task01_response.json

# Test Task 02
aws lambda invoke \
  --function-name etl-pipeline-function \
  --payload '{"action": "task_02"}' \
  task02_response.json

cat task02_response.json
```

---

### Step 6: Deploy Step Functions State Machine

First, update the state machine definition with your actual ARNs:

```bash
# Get your AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1  # Change to your region

# Update the JSON file (replace placeholders)
sed -i "s/REGION/$REGION/g" aws/step_functions/etl_pipeline.json
sed -i "s/ACCOUNT_ID/$ACCOUNT_ID/g" aws/step_functions/etl_pipeline.json
```

Create the state machine:

```bash
# Get the Step Functions role ARN
SF_ROLE_ARN=$(aws iam get-role --role-name etl-pipeline-stepfunctions-role --query 'Role.Arn' --output text)

# Create state machine
aws stepfunctions create-state-machine \
  --name etl-pipeline \
  --definition file://aws/step_functions/etl_pipeline.json \
  --role-arn $SF_ROLE_ARN
```

**Update state machine (for subsequent deploys):**
```bash
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='etl-pipeline'].stateMachineArn" --output text)

aws stepfunctions update-state-machine \
  --state-machine-arn $STATE_MACHINE_ARN \
  --definition file://aws/step_functions/etl_pipeline.json
```

---

### Step 7: Execute Step Functions

```bash
# Get state machine ARN
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='etl-pipeline'].stateMachineArn" --output text)

# Start execution
EXECUTION_ARN=$(aws stepfunctions start-execution \
  --state-machine-arn $STATE_MACHINE_ARN \
  --name "manual-execution-$(date +%Y%m%d-%H%M%S)" \
  --query 'executionArn' \
  --output text)

echo "Execution started: $EXECUTION_ARN"

# Check execution status
aws stepfunctions describe-execution \
  --execution-arn $EXECUTION_ARN
```

---

## 📊 Monitoring

### CloudWatch Logs

Lambda logs are automatically sent to CloudWatch:

```bash
# View recent logs
aws logs tail /aws/lambda/etl-pipeline-function --follow
```

### Step Functions Execution History

View in AWS Console:
1. Go to Step Functions console
2. Select `etl-pipeline` state machine
3. View execution history and details

### Metrics

Key metrics to monitor:
- Lambda duration (should be < 900 seconds)
- Lambda errors
- Step Functions execution failures
- Database connection errors

---

## 🔧 Troubleshooting

### Lambda Times Out

If Task 02 exceeds 15 minutes:
1. Check execution time locally: `python scripts/simulate_step_functions.py`
2. Consider implementing chunking (see Phase 2 in architecture docs)
3. Optimize SQL queries

### Database Connection Fails

1. Verify VPC configuration (if RDS is in VPC)
2. Check security group rules
3. Verify credentials in Secrets Manager
4. Test connection from Lambda:
   ```bash
   aws lambda invoke \
     --function-name etl-pipeline-function \
     --payload '{"action": "validate_config"}' \
     response.json
   ```

### Permission Errors

1. Verify IAM roles have correct policies
2. Check Lambda execution role has VPC permissions (if needed)
3. Verify Step Functions role can invoke Lambda

---

## 🧹 Cleanup (Optional)

To remove all AWS resources:

```bash
# Delete state machine
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='etl-pipeline'].stateMachineArn" --output text)
aws stepfunctions delete-state-machine --state-machine-arn $STATE_MACHINE_ARN

# Delete Lambda function
aws lambda delete-function --function-name etl-pipeline-function

# Delete IAM roles
aws iam delete-role --role-name etl-pipeline-lambda-role
aws iam delete-role --role-name etl-pipeline-stepfunctions-role

# Delete secret
aws secretsmanager delete-secret \
  --secret-id etl-pipeline/postgres \
  --force-delete-without-recovery
```

---

## 📚 Additional Resources

- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [AWS Step Functions Documentation](https://docs.aws.amazon.com/step-functions/)
- [Step Functions Best Practices](https://docs.aws.amazon.com/step-functions/latest/dg/bp-lambda-tasks.html)

