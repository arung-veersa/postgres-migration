# Local Testing Guide for ETL Pipeline

This guide explains how to test the complete ETL pipeline locally before deploying to AWS.

---

## 🎯 Overview

You can test **100% of the functionality locally** before AWS deployment:
- ✅ Lambda handler execution
- ✅ Step Functions workflow simulation  
- ✅ Database operations (with real database)
- ✅ Mock testing (fast, no database)
- ✅ Unit tests
- ✅ Integration tests

---

## 🚀 Quick Start

### 1. Environment Setup

Ensure your `.env` file is configured:

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
```

### 2. Verify Database Connection

```bash
python scripts/test_connections.py
```

Expected output:
```
✅ Connected to Postgres: conflictreport_dev
```

---

## 🧪 Testing Methods

### Method 1: Full Pipeline Simulation (Recommended First Test)

**Simulates Step Functions execution with real database.**

```bash
python scripts/simulate_step_functions.py
```

**What it does:**
- Validates configuration
- Executes Task 01 (Copy to Temp)
- Executes Task 02 (Update Conflicts)
- Measures execution time
- Warns if Lambda timeout is at risk

**Expected output:**
```
======================================================================
AWS STEP FUNCTIONS - LOCAL SIMULATION
======================================================================
Started: 2025-01-20 10:30:00
Database: conflictreport_dev
Conflict Schema: conflict
Analytics Schema: analytics
Mock Mode: False
======================================================================

[State 1/3] ValidateConfig
----------------------------------------------------------------------
✅ SUCCEEDED in 0.05s

[State 2/3] ExecuteTask01
----------------------------------------------------------------------
✅ SUCCEEDED in 45.23s
   Rows Affected: 15,234
   Task Duration: 45.21s

[State 3/3] ExecuteTask02
----------------------------------------------------------------------
✅ SUCCEEDED in 187.56s
   Rows Updated: 8,432
   Task Duration: 187.53s

======================================================================
✅ PIPELINE EXECUTION COMPLETED SUCCESSFULLY
======================================================================

State Summary:
----------------------------------------------------------------------
1. ValidateConfig        ✅ SUCCEEDED     0.05s ( 0.00 min)
2. ExecuteTask01         ✅ SUCCEEDED    45.23s ( 0.75 min)
3. ExecuteTask02         ✅ SUCCEEDED   187.56s ( 3.13 min)
----------------------------------------------------------------------
Total Duration                          232.84s ( 3.88 min)
======================================================================

🔍 Lambda Timeout Analysis:
----------------------------------------------------------------------
✅ Duration is safely within Lambda 15-minute timeout
   Buffer remaining: 667s (11.1 min)
======================================================================
```

**Fast test with mocks (no database):**
```bash
python scripts/simulate_step_functions.py --mock
```

---

### Method 2: Test Lambda Handler Directly

**Test individual Lambda actions.**

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

**Expected output (Task 01):**
```
======================================================================
LAMBDA HANDLER - LOCAL EXECUTION
======================================================================
Action: task_01
Mock Mode: False
======================================================================

[Logs showing task execution...]

======================================================================
EXECUTION RESULT
======================================================================
Status Code: 200
Duration: 45.23s

Response Body:
{
  "status": "success",
  "task": "TASK_01",
  "start_time": "2025-01-20T10:30:00.123456",
  "end_time": "2025-01-20T10:30:45.356789",
  "duration_seconds": 45.21,
  "result": {
    "status": "success",
    "affected_rows": 15234,
    "duration_seconds": 45.21
  }
}
======================================================================
```

---

### Method 3: Interactive Test Script

**Guided testing with safety prompts.**

```bash
python scripts/test_lambda_locally.py
```

**What it does:**
1. Runs fast mock tests (no database)
2. Prompts before running real database tests
3. Shows summary of all test results

**Options:**
```bash
# Skip real database tests
python scripts/test_lambda_locally.py --skip-real
```

---

### Method 4: Unit Tests (Fast, Mock-Based)

**Quick feedback during development.**

```bash
# All unit tests
pytest tests/test_lambda_handler.py -v

# Specific test
pytest tests/test_lambda_handler.py::test_lambda_handler_task_01_success_mock_mode -v

# With coverage
pytest tests/test_lambda_handler.py -v --cov=scripts.lambda_handler
```

---

### Method 5: Integration Tests (Real Database)

**Thorough validation with real database.**

```bash
# All integration tests
pytest tests/integration/ -v -m integration

# Specific test
pytest tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_full_pipeline_sequential -v

# With detailed output
pytest tests/integration/ -v -s -m integration
```

**Expected output:**
```
tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_database_connection PASSED
tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_task_01_execution PASSED

Task 01 Performance:
  Duration: 45.23s (0.8 min)
  Rows affected: 15234

tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_task_02_execution PASSED

Task 02 Performance:
  Duration: 187.56s (3.1 min)
  Rows updated: 8432

tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_full_pipeline_sequential PASSED

======================================================================
FULL PIPELINE INTEGRATION TEST
======================================================================

Executing Task 01...
✅ Task 01 completed in 45.23s
   Rows affected: 15234

Executing Task 02...
✅ Task 02 completed in 187.56s
   Rows updated: 8432

----------------------------------------------------------------------
Total Pipeline Duration: 232.79s (3.9 min)
✅ Pipeline completes within Lambda timeout (buffer: 667s)
======================================================================
```

---

## 📊 Test Comparison

| Test Method | Speed | Database | Use Case |
|-------------|-------|----------|----------|
| **Unit Tests** | ⚡ Very Fast (5s) | ❌ Mock | During development |
| **Mock Simulation** | ⚡ Fast (10s) | ❌ Mock | Quick validation |
| **Integration Tests** | 🐌 Slow (5min) | ✅ Real | Before commit |
| **Full Simulation** | 🐌 Slow (5min) | ✅ Real | Before deployment |

---

## 🔍 What to Look For

### ✅ Success Indicators

1. **All tests pass** (green checkmarks)
2. **Duration < 12 minutes** (Lambda safe zone)
3. **Rows affected/updated** match expectations
4. **No errors in logs**

### ⚠️ Warning Signs

1. **Duration 12-15 minutes** - Close to timeout
2. **Warnings about execution time**
3. **Unexpected row counts**
4. **Database connection errors**

### ❌ Critical Issues

1. **Duration > 15 minutes** - Will timeout in Lambda
2. **Tests fail consistently**
3. **SQL errors**
4. **Cannot connect to database**

---

## 🛠️ Development Workflow

### Daily Development:
```bash
# 1. Quick mock test (5 seconds)
pytest tests/test_lambda_handler.py -v

# 2. If tests pass, test one task with real DB
python scripts/lambda_handler.py task_01
```

### Before Committing:
```bash
# 1. All unit tests
pytest tests/ -v

# 2. Full pipeline simulation
python scripts/simulate_step_functions.py

# 3. Integration tests
pytest tests/integration/ -v -m integration
```

### Before AWS Deployment:
```bash
# Full validation with timing measurement
python scripts/simulate_step_functions.py
```

---

## 🐛 Troubleshooting

### Issue: "Database connection failed"

**Solution:**
1. Check `.env` file has correct credentials
2. Verify security groups allow your IP
3. Test connection: `python scripts/test_connections.py`

### Issue: "SQL file not found"

**Solution:**
1. Ensure you're running from project root
2. Verify `sql/` directory exists
3. Check file paths in error message

### Issue: "Module not found"

**Solution:**
```bash
# Ensure you're in the correct directory
cd Scripts05/Migration

# Verify virtual environment is activated
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### Issue: "Task exceeds 15 minutes"

**Solution:**
1. This is expected - your current implementation may need chunking
2. See Phase 2 implementation plan for chunking
3. Contact team to discuss optimization

---

## 📈 Performance Benchmarking

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

## 🎓 Next Steps

1. **Run full simulation** to get baseline timings
2. **Review results** and ensure < 15 min total
3. **Fix any errors** found in local testing
4. **Document performance** for future reference
5. **Proceed to AWS deployment** (see `aws/README.md`)

---

## 📚 Additional Scripts

### View Current Errors (if any)
```bash
python scripts/view_errors.py
```

### Validate Task 01 Results
```bash
python scripts/validate_task_01.py
```

### Validate Task 02 Results
```bash
python scripts/validate_task_02.py
```

---

## 💡 Tips

1. **Start with mock tests** for fast iteration
2. **Run real database tests** before committing
3. **Monitor execution times** closely
4. **Use pytest markers** to run specific test groups:
   ```bash
   pytest -m integration  # Only integration tests
   pytest -m "not integration"  # Exclude integration tests
   ```
5. **Check logs** in `logs/etl_pipeline.log` for details

---

## ❓ Questions?

If you encounter issues not covered here:
1. Check CloudWatch logs (once deployed)
2. Review error messages carefully
3. Check database connectivity
4. Verify IAM permissions (for AWS deployment)

