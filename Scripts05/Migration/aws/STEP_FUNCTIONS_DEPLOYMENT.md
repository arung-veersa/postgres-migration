# Step Functions Deployment Guide (Phase 1)

## Overview
Deploy AWS Step Functions to orchestrate your ETL pipeline with sequential task execution and automatic retry logic.

## Pipeline Flow

```
┌─────────────────────┐
│ ValidateConfig      │  (60s timeout)
│ - Check env vars    │
│ - Retry: 2x         │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│ ExecuteTask01       │  (15 min timeout)
│ - Copy to temp      │
│ - Retry: 2x         │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│ ExecuteTask02       │  (15 min timeout)
│ - Update conflicts  │
│ - Retry: 2x         │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│ Success             │
└─────────────────────┘
```

---

## Prerequisites

### 1. Lambda Function Deployed
Ensure your Lambda function is deployed and working:
```powershell
# Test Lambda function
aws lambda invoke --function-name your-function-name --payload '{"action":"validate_config"}' response.json
```

### 2. Get Lambda Function ARN
```powershell
# Get Lambda ARN
aws lambda get-function --function-name your-function-name --query 'Configuration.FunctionArn' --output text
```

Save this ARN - you'll need it for deployment.

---

## Step 1: Create IAM Role for Step Functions

### Option A: AWS Console

1. Go to IAM Console → Roles → Create role
2. **Trust Policy:**
   - Trusted entity type: AWS service
   - Use case: Step Functions
3. **Permissions:**
   - Attach policy: `AWSLambdaRole` (or create custom policy below)
4. **Name:** `cm-step-function-lambda-role`
5. **Create role**

### Option B: AWS CLI

Create trust policy file `trust-policy.json`:
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

Create permissions policy file `permissions-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:REGION:ACCOUNT:function:your-function-name"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogDelivery",
        "logs:GetLogDelivery",
        "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies",
        "logs:DescribeLogGroups"
      ],
      "Resource": "*"
    }
  ]
}
```

Create the role:
```bash
# Create role
aws iam create-role \
  --role-name cm-step-function-lambda-role \
  --assume-role-policy-document file://trust-policy.json

# Attach inline policy
aws iam put-role-policy \
  --role-name cm-step-function-lambda-role \
  --policy-name StepFunctionsETLPolicy \
  --policy-document file://permissions-policy.json

# Get role ARN
aws iam get-role --role-name cm-step-function-lambda-role --query 'Role.Arn' --output text
```

Save the role ARN for the next step.

---

## Step 2: Deploy State Machine

### PowerShell (Windows):
```powershell
cd Scripts05\Migration\aws

.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:us-east-1:123456789:function:your-function" `
  -RoleArn "arn:aws:iam::123456789:role/cm-step-function-lambda-role"
```

### Bash (Linux/Mac):
```bash
cd Scripts05/Migration/aws

chmod +x deploy_step_functions.sh

./deploy_step_functions.sh \
  --lambda-arn "arn:aws:lambda:us-east-1:123456789:function:your-function" \
  --role-arn "arn:aws:iam::123456789:role/cm-step-function-lambda-role"
```

### Expected Output:
```
=============================================
Deploying Step Functions State Machine
=============================================

Configuration:
  State Machine Name: cm-etl-pipeline-state-machine
  Lambda Function ARN: arn:aws:lambda:...

[1/3] Checking if state machine exists...
[2/3] Creating state machine...
[3/3] Verifying deployment...

=============================================
✓ Deployment Successful
=============================================

State Machine ARN: arn:aws:states:us-east-1:...
```

---

## Step 3: Test Execution

### Option A: AWS Console

1. Go to Step Functions Console
2. Find: `cm-etl-pipeline-state-machine`
3. Click "Start execution"
4. Input: `{}` (empty JSON)
5. Click "Start execution"
6. Monitor the visual workflow

### Option B: AWS CLI

```bash
# Start execution
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:REGION:ACCOUNT:stateMachine:cm-etl-pipeline-state-machine" \
  --input "{}"

# Check execution status
aws stepfunctions describe-execution \
  --execution-arn "arn:aws:states:REGION:ACCOUNT:execution:..."
```

---

## Monitoring

### View Execution History:
```bash
# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:cm-etl-pipeline-state-machine" \
  --max-results 10
```

### View Execution Details:
1. Go to Step Functions Console
2. Click on your state machine
3. Click on execution ARN
4. View:
   - Visual workflow progress
   - Each step's input/output
   - Timing information
   - Error details (if failed)

### CloudWatch Logs:
- Step Functions automatically logs to CloudWatch
- Lambda logs: `/aws/lambda/your-function-name`
- Step Functions logs: `/aws/states/cm-etl-pipeline-state-machine`

---

## Understanding the Workflow

### State: ValidateConfig
- **Purpose:** Verify environment variables and connectivity
- **Timeout:** 60 seconds
- **Retry:** 2 attempts with exponential backoff
- **On Failure:** Pipeline stops, returns error

### State: ExecuteTask01
- **Purpose:** Copy data to temp table
- **Timeout:** 15 minutes (900s)
- **Retry:** 2 attempts with exponential backoff
- **On Success:** Proceeds to Task 02
- **On Timeout:** Suggests implementing chunking

### State: ExecuteTask02
- **Purpose:** Update conflict visit maps
- **Timeout:** 15 minutes (900s)
- **Retry:** 2 attempts with exponential backoff
- **On Success:** Pipeline completes
- **On Timeout:** Suggests implementing chunking

---

## Retry Logic

Each task has automatic retry:
```
Attempt 1: Immediate
   ↓ (fails)
Wait: 5 seconds
   ↓
Attempt 2: Retry
   ↓ (fails)
Wait: 10 seconds (5s × 2.0 backoff)
   ↓
Attempt 3: Final retry
   ↓ (fails)
Error: Task Failed
```

---

## Cost Estimation

**Step Functions:**
- State transitions: $0.025 per 1,000 transitions
- 3 states per execution = 3 transitions
- Cost per execution: ~$0.000075
- Monthly cost (daily runs): ~$0.002

**Lambda:**
- See existing Lambda cost estimation
- Step Functions doesn't add significant Lambda cost

**Total:** ~$0.08/month for daily pipeline runs

---

## Troubleshooting

### Issue: State machine creation fails
**Check:**
- IAM role has correct trust policy
- IAM role has Lambda invoke permissions
- Lambda function ARN is correct

### Issue: Execution fails at ValidateConfig
**Check:**
- Lambda environment variables are set
- Database credentials are correct
- VPC/Security group configuration

### Issue: Task timeout
**Monitor:**
- CloudWatch logs for actual execution time
- If consistently >12 minutes: Consider Phase 2 (chunking)

### Issue: Permission denied
**Check:**
- Step Functions role can invoke Lambda
- Lambda execution role has database access

---

## Update State Machine

To update the definition after changes:

```powershell
# Just run deployment again with same parameters
.\deploy_step_functions.ps1 -LambdaFunctionArn "arn:..."
```

The script automatically updates if state machine exists.

---

## Schedule Execution (Optional)

### Using EventBridge:

1. Go to EventBridge Console → Rules
2. Create rule:
   - Name: `cm-etl-pipeline-daily`
   - Schedule: `cron(0 2 * * ? *)` (2 AM daily)
   - Target: Step Functions state machine
   - Input: `{}`

---

## Next Steps

### Current Capabilities:
✅ Sequential execution (Task 01 → Task 02)  
✅ Automatic retry on failure  
✅ Visual monitoring  
✅ Error handling with clear messages  

### When to Consider Phase 2:
⚠️ If Task 02 consistently takes >12 minutes  
⚠️ If you need to process 100K+ rows  
⚠️ If you want parallel processing for speed  

Monitor your execution times first, then decide if chunking is needed.

---

For implementation details, see `../deploy/DEPLOYMENT_SUMMARY.md`

