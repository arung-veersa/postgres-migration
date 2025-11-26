# Conflict Report Pipeline - Complete Documentation

## Table of Contents
1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Quick Start](#quick-start)
4. [Testing Guide](#testing-guide)
5. [Step Functions Architecture](#step-functions-architecture)
6. [Performance & Optimization](#performance--optimization)
7. [AWS Deployment](#aws-deployment)
8. [Troubleshooting](#troubleshooting)
9. [Maintenance & Operations](#maintenance--operations)

---

## Overview

A production-ready AWS Step Functions-based ETL pipeline for migrating Snowflake SQL procedures to PostgreSQL. The implementation provides:

- ✅ **100% Local Testing** - Test everything before AWS deployment
- ✅ **Real Database Support** - Connect to dev database for integration testing
- ✅ **Mock Mode** - Fast tests without database connections
- ✅ **Timeout Management** - 15-minute Lambda timeout monitoring
- ✅ **Comprehensive Tests** - Unit and integration test suites
- ✅ **Production Ready** - Retry logic and error handling
- ✅ **Deployment Ready** - Complete AWS deployment guides

### Migration Status

| Task | Status | Description | Duration |
|------|--------|-------------|----------|
| TASK_01 | ✅ Complete | Copy to Temp | 1-2 min |
| TASK_02 | ✅ Complete | Update Conflicts | 3-10 min |
| TASK_03 | ⏳ Pending | Insert from Main | TBD |

---

## Project Structure

```
Migration/
├── config/                     # Configuration files
├── src/                        # Source code
│   ├── connectors/            # Database connections
│   ├── tasks/                 # Task implementations
│   ├── loaders/               # Data loaders
│   └── utils/                 # Utilities
├── scripts/                   # Standalone scripts & Lambda handler
│   ├── lambda_handler.py              ⭐ Main Lambda function
│   ├── simulate_step_functions.py     ⭐ Local pipeline simulator
│   ├── test_lambda_locally.py         Interactive test script
│   ├── mock_postgres_connector.py     Mock database
│   ├── run_task_01.py                 Direct task execution
│   └── run_task_02.py                 Direct task execution
├── tests/                     # Test suite
│   ├── test_lambda_handler.py         Unit tests (12 passing)
│   └── integration/
│       └── test_pipeline_integration.py  Real DB tests
├── aws/                       # AWS deployment
│   ├── step_functions/
│   │   └── etl_pipeline.json          State machine definition
│   └── README.md                      Deployment guide
├── sql/                       # SQL scripts
│   ├── views/                         Database views
│   ├── task_01_copy_to_temp.sql      Task 01 SQL
│   └── task_02_update_conflicts.sql  Task 02 SQL
└── docs/                      # Additional documentation

```

---

## Quick Start

### 1. Environment Setup

**Windows (PowerShell):**
```powershell
cd Scripts05\Migration
py -3 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**Linux/Mac:**
```bash
cd Scripts05/Migration
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Credentials

Create and edit `.env` file:
```bash
# Database Configuration
POSTGRES_HOST=your-aws-rds-endpoint.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_DATABASE=conflictreport_dev
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password

# Schema Configuration
POSTGRES_CONFLICT_SCHEMA=conflict
POSTGRES_ANALYTICS_SCHEMA=analytics

# Other Settings
ENVIRONMENT=dev
LOG_LEVEL=INFO
MAX_WORKERS=4  # Adjust for performance (default: 4)
```

### 3. Create Postgres View (First Time Only)

```bash
psql -h localhost -U your_user -d conflictreport -f sql/views/vw_conflictvisitmaps_base.sql
```

### 4. Verify Database Connection

```bash
python scripts/test_connections.py
```

Expected output:
```
✅ Connected to Postgres: conflictreport_dev
```

### 5. Test the Pipeline

**Quick Test (30 seconds with mock):**
```bash
python scripts/simulate_step_functions.py --mock
```

**Full Test (5-10 minutes with real database):**
```bash
python scripts/simulate_step_functions.py
```

**Run All Tests:**
```bash
pytest tests/ -v
```

---

## Testing Guide

### Testing Methods Overview

| Test Method | Speed | Database | Use Case |
|-------------|-------|----------|----------|
| **Unit Tests** | ⚡ Very Fast (5s) | ❌ Mock | During development |
| **Mock Simulation** | ⚡ Fast (10s) | ❌ Mock | Quick validation |
| **Integration Tests** | 🐌 Slow (5min) | ✅ Real | Before commit |
| **Full Simulation** | 🐌 Slow (5min) | ✅ Real | Before deployment |

### Method 1: Full Pipeline Simulation (Recommended)

Simulates complete Step Functions execution with real database.

```bash
# Full pipeline with real database
python scripts/simulate_step_functions.py

# Fast test with mocks
python scripts/simulate_step_functions.py --mock
```

**Expected Output:**
```
======================================================================
AWS STEP FUNCTIONS - LOCAL SIMULATION
======================================================================
Started: 2025-01-20 14:30:00
Database: conflictreport_dev
======================================================================

[State 1/3] ValidateConfig
----------------------------------------------------------------------
✅ SUCCEEDED in 0.05s

[State 2/3] ExecuteTask01
----------------------------------------------------------------------
✅ SUCCEEDED in 45.23s
   Rows Affected: 15,234

[State 3/3] ExecuteTask02
----------------------------------------------------------------------
✅ SUCCEEDED in 187.56s
   Rows Updated: 8,432

======================================================================
✅ PIPELINE EXECUTION COMPLETED SUCCESSFULLY
======================================================================
Total Duration: 232.84s (3.88 min)

🔍 Lambda Timeout Analysis:
✅ Duration is safely within Lambda 15-minute timeout
   Buffer remaining: 667s (11.1 min)
======================================================================
```

### Method 2: Test Lambda Handler Directly

Test individual Lambda actions.

```bash
# Test Task 01
python scripts/lambda_handler.py task_01

# Test Task 02
python scripts/lambda_handler.py task_02

# Test config validation
python scripts/lambda_handler.py validate_config

# Test with mock (fast)
python scripts/lambda_handler.py task_01 --mock
```

### Method 3: Individual Task Execution

Run tasks using dedicated scripts.

```bash
# Windows
py scripts\run_task_01.py
py scripts\run_task_02.py

# Linux/Mac
python scripts/run_task_01.py
python scripts/run_task_02.py
```

### Method 4: Interactive Test Script

Guided testing with safety prompts.

```bash
# With prompts
python scripts/test_lambda_locally.py

# Skip real database tests
python scripts/test_lambda_locally.py --skip-real
```

### Method 5: Unit Tests (Fast, Mock-Based)

Quick feedback during development.

```bash
# All unit tests
pytest tests/test_lambda_handler.py -v

# Specific test
pytest tests/test_lambda_handler.py::test_lambda_handler_task_01_success_mock_mode -v

# With coverage
pytest tests/test_lambda_handler.py -v --cov=scripts.lambda_handler
```

### Method 6: Integration Tests (Real Database)

Thorough validation with real database.

```bash
# All integration tests
pytest tests/integration/ -v -m integration

# Specific test
pytest tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_full_pipeline_sequential -v

# With detailed output
pytest tests/integration/ -v -s -m integration
```

### Testing Workflow

**Daily Development:**
```bash
# 1. Quick mock test (5 seconds)
pytest tests/test_lambda_handler.py -v

# 2. If tests pass, test one task with real DB
python scripts/lambda_handler.py task_01
```

**Before Committing:**
```bash
# 1. All unit tests
pytest tests/ -v

# 2. Full pipeline simulation
python scripts/simulate_step_functions.py

# 3. Integration tests
pytest tests/integration/ -v -m integration
```

**Before AWS Deployment:**
```bash
# Full validation with timing measurement
python scripts/simulate_step_functions.py
```

### What to Look For

**✅ Success Indicators:**
- All tests pass (green checkmarks)
- Duration < 12 minutes (Lambda safe zone)
- Rows affected/updated match expectations
- No errors in logs

**⚠️ Warning Signs:**
- Duration 12-15 minutes - Close to timeout
- Warnings about execution time
- Unexpected row counts
- Database connection errors

**❌ Critical Issues:**
- Duration > 15 minutes - Will timeout in Lambda
- Tests fail consistently
- SQL errors
- Cannot connect to database

---

## Step Functions Architecture

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

### Lambda Handler Actions

The `lambda_handler.py` supports three actions:

1. **validate_config** - Validates environment and database connection
2. **task_01** - Executes Task 01 (Copy to Temp)
3. **task_02** - Executes Task 02 (Update Conflicts)

---

## Performance & Optimization

### Current Performance

Based on local testing with real database:

| Task | Expected Duration | Status |
|------|------------------|--------|
| Task 01 | 1-2 minutes | ✅ Fast |
| Task 02 | 3-10 minutes | ✅ Moderate |
| **Total** | **4-12 minutes** | ✅ Within Lambda limit |

**Lambda Timeout:** 15 minutes (900 seconds)
- ✅ **Safe Zone:** < 12 minutes (720 seconds)
- ⚠️ **Warning Zone:** 12-15 minutes
- ❌ **Timeout Risk:** > 15 minutes

### Performance Configuration

Adjust parallel workers in `.env`:
```bash
MAX_WORKERS=4  # Default: 4 workers
MAX_WORKERS=6  # Faster processing
```

Expected performance impact:
- Default (4 workers): ~20-30 minutes for full run
- Increased workers: Faster, but more database load

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

### Future Enhancements (Phase 2)

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

**Benefits:**
- Each chunk completes in < 15 minutes
- Parallel execution = faster overall
- Resumable on failure

**Option 2: Use ECS Fargate**

If chunking is complex:
- Run Task 02 in ECS Fargate (no time limit)
- Step Functions waits for ECS task completion
- More infrastructure but simpler code

### Performance Benchmarking

Track performance over time:

```bash
# Run multiple times and record durations
for i in {1..3}; do
  echo "Run $i:"
  python scripts/simulate_step_functions.py | grep "Total Duration"
done
```

Expected variance: ±10% between runs

---

## AWS Deployment

### Prerequisites

Before deploying to AWS, you need:

1. **AWS Permissions:**
   - AWS Lambda
   - Step Functions
   - IAM roles
   - Secrets Manager (optional)

2. **Local Testing Complete:**
   - [ ] Ran `python scripts/simulate_step_functions.py` successfully
   - [ ] Total duration < 12 minutes
   - [ ] All unit tests pass (12/12)
   - [ ] Integration tests pass
   - [ ] No errors in logs
   - [ ] Row counts match expectations
   - [ ] Documented timing results

3. **Results Documentation:**
   - Task 01 duration: _____ minutes
   - Task 02 duration: _____ minutes
   - Total duration: _____ minutes
   - Within Lambda timeout? ✅ Yes / ❌ No

### Deployment Steps

Detailed deployment instructions are available in `aws/README.md`.

**Quick Overview:**

1. **Package Lambda Function**
   ```bash
   cd Scripts05/Migration
   ./package_lambda.sh
   ```

2. **Create IAM Roles**
   - Lambda execution role
   - Step Functions execution role

3. **Deploy Lambda**
   ```bash
   aws lambda create-function \
     --function-name etl-pipeline-function \
     --runtime python3.9 \
     --role <lambda-role-arn> \
     --handler lambda_handler.lambda_handler \
     --zip-file fileb://lambda_function.zip \
     --timeout 900
   ```

4. **Deploy Step Functions**
   ```bash
   aws stepfunctions create-state-machine \
     --name etl-pipeline \
     --definition file://aws/step_functions/etl_pipeline.json \
     --role-arn <step-functions-role-arn>
   ```

5. **Test in AWS**
   ```bash
   aws stepfunctions start-execution \
     --state-machine-arn <arn> \
     --name test-run
   ```

### Security Best Practices

**Database Credentials:**

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

**Network Security:**
- Place Lambda in VPC with RDS
- Use security groups to restrict access
- Use private subnets for Lambda
- NAT Gateway for internet access

**IAM Permissions:**
- Lambda role: Only permissions needed for execution
- Step Functions role: Only Lambda:InvokeFunction
- No wildcard permissions

### Scheduling

Use EventBridge (CloudWatch Events) to trigger Step Functions on a schedule:

```bash
aws events put-rule \
  --name etl-pipeline-daily \
  --schedule-expression "cron(0 2 * * ? *)"  # 2 AM daily
```

### Cost Estimation

**AWS Lambda:**
- Request Cost: $0.20 per 1M requests
- Duration Cost: $0.0000166667 per GB-second
- Example: 
  - 1 execution/day × 30 days = 30 requests
  - Duration: 10 min @ 1GB memory = 600 GB-seconds
  - **Cost:** ~$0.01/month

**Step Functions:**
- State Transitions: $0.025 per 1,000 transitions
- Example:
  - 3 states per execution × 30 executions = 90 transitions
  - **Cost:** ~$0.00/month (under free tier)

**Total: < $1/month** for daily execution

---

## Troubleshooting

### "Database connection failed"

**Solution:**
1. Check `.env` file has correct credentials
2. Verify security groups allow your IP
3. Test connection: `python scripts/test_connections.py`

```bash
# Verify .env file
cat .env  # Linux/Mac
type .env  # Windows
```

### "Module not found"

**Solution:**
```bash
# Ensure you're in the correct directory
cd Scripts05/Migration

# Verify virtual environment is activated
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Reinstall dependencies
pip install -r requirements.txt
```

### "SQL file not found"

**Solution:**
1. Ensure you're running from project root
2. Verify `sql/` directory exists
3. Check file paths in error message

### "Tests fail"

**Solution:**
```bash
# Run with verbose output
pytest tests/test_lambda_handler.py -vv -s

# Check logs
cat logs/etl_pipeline.log  # Linux/Mac
type logs\etl_pipeline.log  # Windows
```

### "Task exceeds 15 minutes"

**Solution:**
1. This may require chunking implementation
2. See Phase 2 implementation plan for chunking
3. Review database performance
4. Optimize SQL queries

### Lambda Failures in AWS

**Solution:**
- Check CloudWatch logs: `/aws/lambda/etl-pipeline-function`
- Review Step Functions execution history
- Verify IAM permissions
- Check VPC/Security group configuration

---

## Maintenance & Operations

### Monitoring and Observability

**CloudWatch Logs:**

Lambda automatically logs to CloudWatch:
```
/aws/lambda/etl-pipeline-function
```

Key log entries:
- `Lambda invoked with action: task_01`
- `Task 01 completed successfully in X.XXs`
- `Rows affected: XXXXX`

**Step Functions Execution History:**

View in AWS Console:
- Execution status (RUNNING, SUCCEEDED, FAILED)
- Duration of each state
- Input/output of each state
- Error details if failed

**Custom Metrics (Optional):**

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

### Updating Lambda Code

```bash
# 1. Make changes locally
# 2. Test locally
python scripts/simulate_step_functions.py

# 3. Run tests
pytest tests/ -v

# 4. Package and deploy
cd Scripts05/Migration
./package_lambda.sh
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

### Validation Scripts

```bash
# View current errors (if any)
python scripts/view_errors.py

# Validate Task 01 results
python scripts/validate_task_01.py

# Validate Task 02 results
python scripts/validate_task_02.py
```

---

## Quick Command Reference

```bash
# Fastest test (mock, 5 seconds)
python scripts/simulate_step_functions.py --mock

# Real database test (5-10 minutes)
python scripts/simulate_step_functions.py

# Run unit tests
pytest tests/test_lambda_handler.py -v

# Run integration tests
pytest tests/integration/ -v -m integration

# Run all tests
pytest tests/ -v

# Test single task
python scripts/lambda_handler.py task_01
python scripts/lambda_handler.py task_02

# Interactive testing
python scripts/test_lambda_locally.py

# Individual task execution
python scripts/run_task_01.py
python scripts/run_task_02.py

# Validate connection
python scripts/test_connections.py
```

---

## FAQ

**Q: Can I run this without AWS permissions?**  
**A:** Yes! All testing can be done locally. You only need AWS permissions when ready to deploy.

**Q: What if my task takes more than 15 minutes?**  
**A:** Implement chunking (Phase 2) or use ECS Fargate for long-running tasks.

**Q: How do I debug Lambda failures?**  
**A:** Check CloudWatch logs: `/aws/lambda/etl-pipeline-function`

**Q: Can I run tasks in parallel?**  
**A:** Yes, but current implementation is sequential. Use Step Functions Parallel state for concurrent execution.

**Q: How do I schedule this to run daily?**  
**A:** Use EventBridge (CloudWatch Events) to trigger Step Functions on a schedule.

**Q: What if Step Functions fails mid-execution?**  
**A:** Step Functions maintains state. You can retry from the failed state or start a new execution.

**Q: Is the pipeline idempotent?**  
**A:** Yes, tasks can be safely run multiple times without issues.

---

## Learning Path

### Day 1: Understand the Architecture
1. Review this document
2. Understand project structure
3. Review Lambda handler: `scripts/lambda_handler.py`
4. Review Step Functions definition: `aws/step_functions/etl_pipeline.json`

### Day 2: Test Locally
1. Run `python scripts/simulate_step_functions.py --mock` (fast test)
2. Run `python scripts/simulate_step_functions.py` (real database)
3. Run `pytest tests/test_lambda_handler.py -v`
4. Document your results

### Day 3: Prepare for AWS
1. Request AWS permissions
2. Review `aws/README.md`
3. Prepare credentials (Secrets Manager)
4. Complete verification checklist

### Day 4+: Deploy to AWS
1. Follow `aws/README.md` step-by-step
2. Deploy Lambda function
3. Deploy Step Functions
4. Test in AWS
5. Set up monitoring and scheduling

---

## Additional Resources

- [AWS Lambda Best Practices](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)
- [Step Functions Service Integration](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-service-integrations.html)
- [Python Lambda Deployment Package](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html)
- [VPC Configuration for Lambda](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)

---

## Support

For issues or questions:
1. Review relevant sections in this documentation
2. Check `aws/README.md` for deployment-specific help
3. Review CloudWatch logs for error details (AWS deployment)
4. Check Step Functions execution history (AWS deployment)
5. Review local logs: `logs/etl_pipeline.log`

---

**Ready to Start Testing! 🚀**

Begin with:
```bash
cd Scripts05/Migration
python scripts/simulate_step_functions.py
```

Watch the magic happen! ✨

