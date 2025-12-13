# Step Functions Workflows - Final Setup

## What We Have Now

**Two separate workflow files, one for each Lambda function:**

### 1. Analytics Migration
- **File**: `migration_workflow_analytics.json`
- **Lambda**: `cm-datacopy-analytics`
- **State Machine Name**: `MigrationWorkflow_Analytics`

### 2. Conflict Migration
- **File**: `migration_workflow_conflict.json`
- **Lambda**: `cm-datacopy-test01`
- **State Machine Name**: `MigrationWorkflow_Conflict`

## Quick Deployment

### Step 1: Create Both State Machines

```bash
# Create Analytics State Machine
aws stepfunctions create-state-machine \
  --name "MigrationWorkflow_Analytics" \
  --definition file://Scripts07/DataMigration/aws/step_functions/migration_workflow_analytics.json \
  --role-arn "arn:aws:iam::354073143602:role/YourStepFunctionsRole" \
  --region us-east-1

# Create Conflict State Machine
aws stepfunctions create-state-machine \
  --name "MigrationWorkflow_Conflict" \
  --definition file://Scripts07/DataMigration/aws/step_functions/migration_workflow_conflict.json \
  --role-arn "arn:aws:iam::354073143602:role/YourStepFunctionsRole" \
  --region us-east-1
```

### Step 2: Start Both Migrations (Parallel)

```bash
# Start Analytics
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:MigrationWorkflow_Analytics" \
  --input '{"source_name": "analytics"}' \
  --region us-east-1

# Start Conflict
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:354073143602:stateMachine:MigrationWorkflow_Conflict" \
  --input '{"source_name": "conflict"}' \
  --region us-east-1
```

## What Changed

### Files Removed
- ❌ `migration_workflow.json` (redundant with conflict workflow)

### Files Created
- ✅ `migration_workflow_analytics.json` (targets `cm-datacopy-analytics`)
- ✅ `migration_workflow_conflict.json` (targets `cm-datacopy-test01`)
- ✅ `README.md` (comprehensive guide)

### Key Features
- ✅ Resume fix applied (`no_resume: false` default)
- ✅ State preservation across resume cycles
- ✅ Separate Lambda functions for parallel execution
- ✅ Independent progress tracking

## Benefits

1. **No Confusion**: Clear separation - one workflow per Lambda
2. **Parallel Execution**: Both migrations run simultaneously
3. **Independent Monitoring**: Separate CloudWatch logs
4. **Easier Management**: Clear naming convention
5. **Scalability**: Easy to add more sources in the future

## Next Steps

1. Deploy Lambda functions with updated config
2. Create/update both state machines
3. Start both migrations in parallel
4. Monitor progress in CloudWatch

See `README.md` for detailed instructions.

