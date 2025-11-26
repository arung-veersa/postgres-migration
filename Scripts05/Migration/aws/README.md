# AWS Step Functions (Future)

## Current Status
Lambda function deployed with connection testing actions.

## Future: Step Functions Integration

When ready to orchestrate full pipeline:

### State Machine Definition
See `step_functions/etl_pipeline.json` for state machine template.

### Steps to Deploy:
1. Update Lambda ARN in state machine definition
2. Create state machine in AWS Step Functions console
3. Configure IAM role for state machine
4. Test execution with sample input

### Pipeline Flow:
```
ValidateConfig → ExecuteTask01 → ExecuteTask02
```

For current Lambda deployment, see `../deploy/LAMBDA_DEPLOYMENT.md`.
