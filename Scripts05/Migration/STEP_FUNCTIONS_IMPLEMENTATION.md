# Step Functions Implementation - Complete Guide

## 📋 Project Overview

This implementation provides a complete AWS Step Functions-based ETL pipeline that can be:
- ✅ **Tested 100% locally** before AWS deployment
- ✅ **Executed in AWS Lambda** with 15-minute timeout handling
- ✅ **Monitored and debugged** through CloudWatch
- ✅ **Extended with chunking** if tasks exceed timeout

---

## 🎯 What Was Created

### **Core Lambda Handler**
- `scripts/lambda_handler.py` - Main Lambda function
  - Handles: `validate_config`, `task_01`, `task_02`
  - Works in AWS Lambda and locally
  - Supports mock mode for fast testing

### **Local Testing Tools**
- `scripts/simulate_step_functions.py` - Full pipeline simulator
- `scripts/test_lambda_locally.py` - Interactive test script
- `scripts/mock_postgres_connector.py` - Mock database for unit tests

### **Test Suites**
- `tests/test_lambda_handler.py` - Unit tests (mock-based, fast)
- `tests/integration/test_pipeline_integration.py` - Integration tests (real DB)

### **AWS Deployment**
- `aws/step_functions/etl_pipeline.json` - State machine definition
- `aws/README.md` - Complete deployment guide

### **Documentation**
- `LOCAL_TESTING_GUIDE.md` - How to test locally
- `STEP_FUNCTIONS_IMPLEMENTATION.md` - This file

---

## 🚀 How It Works

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                  AWS Step Functions                          │
│                                                               │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐│
│  │  Validate    │────▶│  Execute     │────▶│  Execute     ││
│  │  Config      │     │  Task 01     │     │  Task 02     ││
│  └──────────────┘     └──────────────┘     └──────────────┘│
│         │                     │                     │        │
└─────────┼─────────────────────┼─────────────────────┼────────┘
          │                     │                     │
          ▼                     ▼                     ▼
    ┌─────────────────────────────────────────────────────┐
    │          AWS Lambda: etl-pipeline-function          │
    │                                                       │
    │  Input: {"action": "validate_config|task_01|task_02"}│
    │                                                       │
    │  ┌────────────────────────────────────────────────┐ │
    │  │  Lambda Handler (lambda_handler.py)            │ │
    │  │                                                  │ │
    │  │  • Validates configuration                       │ │
    │  │  • Connects to PostgreSQL                        │ │
    │  │  • Executes Task 01 or Task 02                   │ │
    │  │  • Returns results                               │ │
    │  └────────────────────────────────────────────────┘ │
    └───────────────────────┬──────────────────────────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │  AWS RDS          │
                  │  PostgreSQL       │
                  │                   │
                  │  • conflictreport │
                  │  • conflict schema │
                  │  • analytics schema│
                  └──────────────────┘
```

### Execution Flow

1. **Step Functions** starts execution
2. **State 1: ValidateConfig**
   - Lambda validates environment variables
   - Checks database connectivity
   - Proceeds to Task 01 or fails
3. **State 2: ExecuteTask01**
   - Lambda executes Task 01 SQL
   - Copies data to temp table
   - Returns row count
4. **State 3: ExecuteTask02**
   - Lambda executes Task 02 SQL
   - Updates conflict visit maps
   - Returns row count
5. **Success** - Pipeline completes

### Error Handling

- **Retry Logic**: Each task retries up to 2 times with exponential backoff
- **Timeout Detection**: Fails fast if task exceeds 15 minutes
- **Error States**: Specific error states for each failure type

---

## 🧪 Testing Strategy

### Phase 1: Local Development (Current)

```bash
# 1. Quick mock tests (5 seconds)
pytest tests/test_lambda_handler.py -v

# 2. Single task test
python scripts/lambda_handler.py task_01

# 3. Full pipeline simulation
python scripts/simulate_step_functions.py

# 4. Integration tests
pytest tests/integration/ -v -m integration
```

**Goal:** Validate logic works and measure timing

### Phase 2: AWS Deployment (Future)

```bash
# 1. Deploy Lambda
# (See aws/README.md)

# 2. Deploy Step Functions
# (See aws/README.md)

# 3. Test in AWS
aws stepfunctions start-execution --state-machine-arn <arn> --name test-run
```

**Goal:** Validate AWS integration works

---

## ⏱️ Performance Expectations

### Current Performance (Based on Local Testing)

**Expected Timings:**
- Task 01: ~1-2 minutes (simple operations)
- Task 02: ~3-10 minutes (complex joins and calculations)
- **Total: 4-12 minutes** ✅ Within Lambda limit

**Lambda Timeout:** 15 minutes (900 seconds)
- ✅ **Safe Zone:** < 12 minutes (720 seconds)
- ⚠️ **Warning Zone:** 12-15 minutes
- ❌ **Timeout Risk:** > 15 minutes

### What to Do if Timeout Occurs

**Scenario A: Task 01 times out** (unlikely)
- Task 01 is simple and fast
- If it times out, check:
  - Database performance
  - Network latency
  - SQL query optimization

**Scenario B: Task 02 times out** (possible with large data)
- Task 02 is complex with joins and calculations
- **Solution:** Implement chunking (Phase 2)
  - Split by date ranges
  - Process chunks in parallel
  - Use Step Functions Map state

---

## 🔄 Future Enhancements (Phase 2)

### If Task 02 Exceeds 15 Minutes

**Option 1: Chunking with Parallel Execution**

Modify `task_02_update_conflicts.sql` to accept date parameters:
```sql
WHERE CR1."Visit Date"::date
    BETWEEN '{start_date}'::date
        AND '{end_date}'::date
```

Update Step Functions to use Map state:
```json
{
  "ExecuteTask02Parallel": {
    "Type": "Map",
    "ItemsPath": "$.date_chunks",
    "MaxConcurrency": 5,
    "Iterator": {
      "StartAt": "ProcessChunk",
      "States": {
        "ProcessChunk": {
          "Type": "Task",
          "Resource": "arn:aws:lambda:...",
          "Parameters": {
            "action": "task_02_chunk",
            "start_date.$": "$.start_date",
            "end_date.$": "$.end_date"
          }
        }
      }
    }
  }
}
```

Benefits:
- Each chunk completes in < 15 minutes
- Parallel execution = faster overall
- Resumable on failure

**Option 2: Use ECS Fargate**

If chunking is complex:
- Run Task 02 in ECS Fargate (no time limit)
- Step Functions waits for ECS task completion
- More infrastructure but simpler code

---

## 📊 Monitoring and Observability

### CloudWatch Logs

Lambda automatically logs to CloudWatch:
```
/aws/lambda/etl-pipeline-function
```

Key log entries:
- `Lambda invoked with action: task_01`
- `Task 01 completed successfully in X.XXs`
- `Rows affected: XXXXX`

### Step Functions Execution History

View in AWS Console:
- Execution status (RUNNING, SUCCEEDED, FAILED)
- Duration of each state
- Input/output of each state
- Error details if failed

### Custom Metrics (Optional)

Add CloudWatch custom metrics:
```python
import boto3

cloudwatch = boto3.client('cloudwatch')

cloudwatch.put_metric_data(
    Namespace='ETL/Pipeline',
    MetricData=[
        {
            'MetricName': 'Task01Duration',
            'Value': duration_seconds,
            'Unit': 'Seconds'
        }
    ]
)
```

---

## 🛠️ Maintenance

### Updating Lambda Code

```bash
# 1. Make changes locally
# 2. Test locally
python scripts/simulate_step_functions.py

# 3. Run tests
pytest tests/ -v

# 4. Package and deploy
cd Scripts05/Migration
./package_lambda.sh  # Create this script
aws lambda update-function-code \
  --function-name etl-pipeline-function \
  --zip-file fileb://lambda_function.zip
```

### Updating Step Functions

```bash
# 1. Edit aws/step_functions/etl_pipeline.json
# 2. Validate locally
aws stepfunctions validate-state-machine-definition \
  --definition file://aws/step_functions/etl_pipeline.json

# 3. Update in AWS
STATE_MACHINE_ARN=<your-arn>
aws stepfunctions update-state-machine \
  --state-machine-arn $STATE_MACHINE_ARN \
  --definition file://aws/step_functions/etl_pipeline.json
```

### Updating SQL Scripts

```bash
# 1. Modify sql/task_01_copy_to_temp.sql or sql/task_02_update_conflicts.sql
# 2. Test locally
python scripts/lambda_handler.py task_01

# 3. Deploy Lambda (SQL files are included in package)
```

---

## 🔒 Security Best Practices

### Database Credentials

**Option 1: Environment Variables** (Current)
```python
POSTGRES_PASSWORD=xxx  # In Lambda environment variables
```

**Option 2: AWS Secrets Manager** (Recommended)
```python
import boto3
import json

def get_db_credentials():
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId='etl-pipeline/postgres')
    return json.loads(response['SecretString'])
```

### Network Security

- Place Lambda in VPC with RDS
- Use security groups to restrict access
- Use private subnets for Lambda
- NAT Gateway for internet access

### IAM Permissions

Principle of least privilege:
- Lambda role: Only permissions needed for execution
- Step Functions role: Only Lambda:InvokeFunction
- No wildcard permissions

---

## 📈 Cost Estimation

### AWS Lambda
- **Request Cost:** $0.20 per 1M requests
- **Duration Cost:** $0.0000166667 per GB-second
- **Example:** 
  - 1 execution/day × 30 days = 30 requests
  - Duration: 10 min @ 1GB memory = 600 GB-seconds
  - **Cost:** ~$0.01/month

### Step Functions
- **State Transitions:** $0.025 per 1,000 transitions
- **Example:**
  - 3 states per execution × 30 executions = 90 transitions
  - **Cost:** ~$0.00/month (under free tier)

### Total
- **Estimated: < $1/month** for daily execution

---

## ❓ FAQ

### Q: Can I run this without AWS permissions?
**A:** Yes! All testing can be done locally. You only need AWS permissions when ready to deploy.

### Q: What if my task takes more than 15 minutes?
**A:** Implement chunking (Phase 2) or use ECS Fargate for long-running tasks.

### Q: How do I debug Lambda failures?
**A:** Check CloudWatch logs: `/aws/lambda/etl-pipeline-function`

### Q: Can I run tasks in parallel?
**A:** Yes, but current implementation is sequential. Use Step Functions Parallel state for concurrent execution.

### Q: How do I schedule this to run daily?
**A:** Use EventBridge (CloudWatch Events) to trigger Step Functions on a schedule:
```bash
aws events put-rule \
  --name etl-pipeline-daily \
  --schedule-expression "cron(0 2 * * ? *)"  # 2 AM daily
```

### Q: What if Step Functions fails mid-execution?
**A:** Step Functions maintains state. You can retry from the failed state or start a new execution.

---

## 🎓 Next Steps

1. ✅ **Run local tests** to validate everything works
2. ✅ **Measure performance** with your actual database
3. ✅ **Document timings** for future reference
4. ⏳ **Get AWS permissions** for deployment
5. ⏳ **Deploy to AWS** following `aws/README.md`
6. ⏳ **Monitor first executions** in CloudWatch
7. ⏳ **Set up scheduling** with EventBridge

---

## 📚 Additional Resources

- [AWS Lambda Best Practices](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)
- [Step Functions Service Integration](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-service-integrations.html)
- [Python Lambda Deployment Package](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html)
- [VPC Configuration for Lambda](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)

---

## 👥 Support

For issues or questions:
1. Check `LOCAL_TESTING_GUIDE.md` for local testing help
2. Check `aws/README.md` for deployment help
3. Review CloudWatch logs for error details
4. Check Step Functions execution history

