#!/usr/bin/env pwsh
# Deploy AWS Step Functions State Machine
# This creates/updates the Step Functions state machine for ETL pipeline orchestration

param(
    [Parameter(Mandatory=$true)]
    [string]$LambdaFunctionArn,
    
    [Parameter(Mandatory=$false)]
    [string]$StateMachineName = "cm-etl-pipeline-state-machine",
    
    [Parameter(Mandatory=$false)]
    [string]$RoleArn
)

$ErrorActionPreference = "Stop"

Write-Host "=============================================" -ForegroundColor Blue
Write-Host "Deploying Step Functions State Machine" -ForegroundColor Blue
Write-Host "=============================================" -ForegroundColor Blue
Write-Host ""

# Validate inputs
if (-not $LambdaFunctionArn) {
    Write-Host "ERROR: Lambda Function ARN is required" -ForegroundColor Red
    Write-Host "Usage: .\deploy_step_functions.ps1 -LambdaFunctionArn 'arn:aws:lambda:...'" -ForegroundColor Yellow
    exit 1
}

Write-Host "Configuration:" -ForegroundColor Cyan
Write-Host "  State Machine Name: $StateMachineName"
Write-Host "  Lambda Function ARN: $LambdaFunctionArn"
Write-Host ""

# Read the state machine definition
$definitionFile = Join-Path $PSScriptRoot "step_functions\etl_pipeline.json"
if (-not (Test-Path $definitionFile)) {
    Write-Host "ERROR: State machine definition not found: $definitionFile" -ForegroundColor Red
    exit 1
}

$definition = Get-Content $definitionFile -Raw

# Replace placeholders with actual values
$definition = $definition -replace 'arn:aws:lambda:REGION:ACCOUNT_ID:function:etl-pipeline-function', $LambdaFunctionArn

Write-Host "[1/3] Checking if state machine exists..." -ForegroundColor Yellow
$existingStateMachine = aws stepfunctions list-state-machines --query "stateMachines[?name=='$StateMachineName'].stateMachineArn" --output text 2>$null

if ($existingStateMachine) {
    Write-Host "  State machine exists: $existingStateMachine" -ForegroundColor Gray
    
    # If RoleArn not provided, get it from existing state machine
    if (-not $RoleArn) {
        Write-Host "  Getting role ARN from existing state machine..." -ForegroundColor Gray
        $RoleArn = aws stepfunctions describe-state-machine --state-machine-arn $existingStateMachine --query "roleArn" --output text
        Write-Host "  Using existing role: $RoleArn" -ForegroundColor Gray
    }
    
    Write-Host "[2/3] Updating state machine definition..." -ForegroundColor Yellow
    
    # Save definition to temp file (AWS CLI doesn't accept from stdin easily)
    $tempFile = [System.IO.Path]::GetTempFileName()
    $definition | Out-File -FilePath $tempFile -Encoding UTF8 -NoNewline
    
    try {
        $updateResult = aws stepfunctions update-state-machine `
            --state-machine-arn $existingStateMachine `
            --definition file://$tempFile `
            --role-arn $RoleArn 2>&1
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host ""
            Write-Host "ERROR: Failed to update state machine" -ForegroundColor Red
            Write-Host $updateResult -ForegroundColor Red
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
            exit 1
        }
        
        Write-Host "  ✓ State machine updated successfully" -ForegroundColor Green
    }
    catch {
        Write-Host ""
        Write-Host "ERROR: Failed to update state machine" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        exit 1
    }
    finally {
        Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
    }
}
else {
    Write-Host "  State machine does not exist. Creating new..." -ForegroundColor Gray
    
    if (-not $RoleArn) {
        Write-Host ""
        Write-Host "ERROR: Role ARN is required for creating new state machine" -ForegroundColor Red
        Write-Host ""
        Write-Host "Please create an IAM role with Step Functions trust policy and pass it via -RoleArn parameter" -ForegroundColor Yellow
        Write-Host "See: aws/README.md for role creation instructions" -ForegroundColor Yellow
        exit 1
    }
    
    Write-Host "[2/3] Creating state machine..." -ForegroundColor Yellow
    
    # Save definition to temp file
    $tempFile = [System.IO.Path]::GetTempFileName()
    $definition | Out-File -FilePath $tempFile -Encoding UTF8 -NoNewline
    
    try {
        $result = aws stepfunctions create-state-machine `
            --name $StateMachineName `
            --definition file://$tempFile `
            --role-arn $RoleArn `
            --type STANDARD `
            --output json 2>&1
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host ""
            Write-Host "ERROR: Failed to create state machine" -ForegroundColor Red
            Write-Host $result -ForegroundColor Red
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
            exit 1
        }
        
        $resultObj = $result | ConvertFrom-Json
        $existingStateMachine = $resultObj.stateMachineArn
        Write-Host "  ✓ State machine created: $existingStateMachine" -ForegroundColor Green
    }
    catch {
        Write-Host ""
        Write-Host "ERROR: Failed to create state machine" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        exit 1
    }
    finally {
        Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "[3/3] Verifying deployment..." -ForegroundColor Yellow
try {
    $stateMachineInfo = aws stepfunctions describe-state-machine --state-machine-arn $existingStateMachine --output json 2>&1 | ConvertFrom-Json
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "ERROR: Failed to verify deployment" -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host ""
    Write-Host "ERROR: Failed to verify deployment" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=============================================" -ForegroundColor Green
Write-Host "✓ Deployment Successful" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""
Write-Host "State Machine Details:" -ForegroundColor Cyan
Write-Host "  Name: $($stateMachineInfo.name)"
Write-Host "  ARN: $($stateMachineInfo.stateMachineArn)"
Write-Host "  Status: $($stateMachineInfo.status)"
Write-Host "  Creation Date: $($stateMachineInfo.creationDate)"
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "  1. Go to AWS Step Functions Console"
Write-Host "  2. Find your state machine: $StateMachineName"
Write-Host "  3. Click 'Start execution'"
Write-Host "  4. Use input: {}"
Write-Host ""
Write-Host "Or execute via CLI:" -ForegroundColor Cyan
Write-Host "  aws stepfunctions start-execution --state-machine-arn $existingStateMachine" -ForegroundColor Gray
Write-Host ""
Write-Host "=============================================" -ForegroundColor Green

