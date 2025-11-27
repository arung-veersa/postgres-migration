# AWS Step Functions - ETL Pipeline Orchestration

## Current Status
✅ Step Functions definition ready  
✅ Lambda function deployed with connection testing  
✅ Deployment scripts created  

---

## Quick Deployment

### Prerequisites:
- Lambda function deployed and tested
- AWS CLI configured
- IAM role for Step Functions

### Deploy:

**PowerShell:**
```powershell
.\deploy_step_functions.ps1 `
  -LambdaFunctionArn "arn:aws:lambda:us-east-1:354073143602:function:cm-task-ag-test01" `
  -RoleArn "arn:aws:iam::354073143602:role/cm-step-function-lambda-role"
```

**Bash:**
```bash
./deploy_step_functions.sh \
  --lambda-arn "arn:aws:lambda:us-east-1:354073143602:function:cm-task-ag-test01" \
  --role-arn "arn:aws:iam::354073143602:role/cm-step-function-lambda-role"
```

---

## Pipeline Flow

```
ValidateConfig → ExecuteTask01 → ExecuteTask02 → Success
   (60s)            (15 min)         (15 min)
   
Each step has:
- Automatic retry (2x)
- Error handling
- Timeout protection
```

---

## Documentation

See **[STEP_FUNCTIONS_DEPLOYMENT.md](STEP_FUNCTIONS_DEPLOYMENT.md)** for:
- Complete deployment guide
- IAM role creation
- Testing instructions
- Monitoring setup
- Troubleshooting

---

## State Machine Definition

See `step_functions/etl_pipeline.json` for the complete state machine definition.

---

For Lambda deployment, see `../deploy/LAMBDA_DEPLOYMENT.md`.
