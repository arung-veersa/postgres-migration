# Step Functions Workflow Files

This directory contains two Step Functions workflow definitions for the migration system:

## Files

### 1. `migration_workflow_analytics.json`
**Lambda Function**: `cm-datacopy-analytics`

**Purpose**: Dedicated workflow for analytics migration.

**Usage**: 
```json
{}                            // Uses default from Lambda's config
{"source_name": "analytics"}  // Explicit (recommended)
```

**Recommended Input**: 
```json
{"source_name": "analytics"}
```

---

### 2. `migration_workflow_conflict.json`
**Lambda Function**: `cm-datacopy-test01`

**Purpose**: Dedicated workflow for conflict migration.

**Usage**: 
```json
{}                           // Uses default from Lambda's config
{"source_name": "conflict"}  // Explicit (recommended)
```

**Recommended Input**: 
```json
{"source_name": "conflict"}
```

---

## Key Features (All Workflows)

✅ **Auto-Resume**: Automatically resumes after Lambda timeout  
✅ **No Data Loss**: Bulletproof truncation protection (dual-layer check)  
✅ **Resume Fix Applied**: `no_resume` defaults to `false` in all workflows  
✅ **Idempotent**: Safe to retry/restart at any time  
✅ **Progress Tracking**: All state stored in PostgreSQL `migration_status` schema  

## Deployment

### Create/Update State Machine via AWS Console

1. Go to: https://console.aws.amazon.com/states/
2. Click **Create state machine** (or select existing and click **Edit**)
3. Choose **Write your workflow in code**
4. Copy-paste the entire JSON from the appropriate file
5. Configure:
   - **Name**: `MigrationWorkflow_Analytics` (or similar)
   - **Type**: Standard
   - **Permissions**: Use existing role or create new with Lambda invoke permissions
6. Click **Create** (or **Save**)

### Create/Update State Machine via AWS CLI

```bash
# For analytics workflow:
aws stepfunctions create-state-machine \
  --name "MigrationWorkflow_Analytics" \
  --definition file://migration_workflow_analytics.json \
  --role-arn "arn:aws:iam::354073143602:role/StepFunctionsExecutionRole" \
  --region us-east-1

# For conflict workflow:
aws stepfunctions create-state-machine \
  --name "MigrationWorkflow_Conflict" \
  --definition file://migration_workflow_conflict.json \
  --role-arn "arn:aws:iam::354073143602:role/StepFunctionsExecutionRole" \
  --region us-east-1

# To update existing state machine:
aws stepfunctions update-state-machine \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:MigrationWorkflow_Analytics" \
  --definition file://migration_workflow_analytics.json \
  --region us-east-1
```

## Execution Examples

### Start via AWS Console
1. Go to Step Functions Console
2. Select your state machine
3. Click **Start execution**
4. Enter input JSON (see above)
5. Click **Start execution**

### Start via AWS CLI

```bash
# Analytics migration:
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:MigrationWorkflow_Analytics" \
  --input '{"source_name": "analytics"}' \
  --region us-east-1

# Conflict migration:
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:MigrationWorkflow_Conflict" \
  --input '{"source_name": "conflict"}' \
  --region us-east-1
```

## Parallel Execution

To run both migrations in parallel (recommended):

```bash
# Terminal 1 or separate execution:
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:MigrationWorkflow_Analytics" \
  --input '{"source_name": "analytics"}' \
  --region us-east-1

# Terminal 2 or separate execution:
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:MigrationWorkflow_Conflict" \
  --input '{"source_name": "conflict"}' \
  --region us-east-1
```

**Benefits**:
- Both migrations run simultaneously
- Separate Lambda functions → no resource contention
- Independent progress tracking
- Separate CloudWatch log groups

## Important Notes

### Resume Settings
All workflows default to:
```json
"defaults": {
  "resume_attempt_count": 0,
  "resume_max_age": 12,      // Resume runs started within last 12 hours
  "no_resume": false          // Resume enabled by default
}
```

**DO NOT** pass `{"no_resume": true}` unless you explicitly want to disable resume and start fresh!

### Execution Input Best Practices

✅ **Recommended** (minimal input):
```json
{"source_name": "analytics"}
```

✅ **Also OK** (empty input, uses Lambda's config):
```json
{}
```

❌ **Avoid** (unless you know what you're doing):
```json
{"no_resume": true}  // Disables resume - not recommended for Lambda!
```

### Monitoring

Check execution status:
```bash
aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:MigrationWorkflow_Analytics" \
  --region us-east-1
```

Get execution details:
```bash
aws stepfunctions describe-execution \
  --execution-arn "arn:aws:states:us-east-1:354073143602:execution:MigrationWorkflow_Analytics:abc-123" \
  --region us-east-1
```

## Troubleshooting

### Execution Failed at ConfigValidation
- Check Lambda function environment variables
- Verify `config.json` is included in Lambda package
- Check CloudWatch logs for Lambda function

### Execution Failed at TestConnections
- Verify Lambda is in correct VPC/subnets
- Check security groups allow outbound to Snowflake and PostgreSQL
- Verify database credentials in Lambda environment variables

### Execution Stuck in Resume Loop
- Check CloudWatch logs for errors
- Query `migration_status.migration_chunk_status` for failed chunks
- See `TROUBLESHOOTING.md` for diagnostic queries

### Multiple run_ids Created
- You likely passed `{"no_resume": true}` in execution input
- Stop execution, clean database, restart with `{"source_name": "analytics"}`
- See `RESUME_BUG_FIX.md` for details

## Related Documentation

- `../../docs/TROUBLESHOOTING.md` - Comprehensive troubleshooting guide
- `../../docs/FEATURES.md` - Detailed feature documentation
- `../README.md` - AWS infrastructure overview
- `SETUP.md` - Quick deployment guide

