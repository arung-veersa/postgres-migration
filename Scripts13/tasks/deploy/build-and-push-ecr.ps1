# Build, push, and optionally run Task 02 Conflict Updater on ECS
# ================================================================
#
# Interactive script -- prompts for what you want to do:
#   1. SSO login (optional)
#   2. Build Docker image
#   3. Push to ECR
#   4. Run ECS task (optional, with action selection)
#
# Usage:
#   cd Scripts13\tasks\deploy
#   .\build-and-push-ecr.ps1
#
# All settings are pre-configured below. Edit the CONFIGURATION section
# if your account, cluster, or networking details change.

$ErrorActionPreference = "Stop"

# =====================================================================
# CONFIGURATION -- edit these if your environment changes
# =====================================================================
$AccountId      = "354073143602"
$Region         = "us-east-1"
$Profile        = "HHA-DEV-CONFLICT-MGMT-354073143602"
$RepositoryName = "conflict-snowflake"
$Cluster        = "conflict-batch-1"
$TaskDefinition = "task02-conflict-updater"
$Subnets        = "subnet-0bf69a7a22a445997,subnet-07b70458a1e90f658,subnet-0e558920a2b805a7f,subnet-0213fe32b2341360b"
$SecurityGroups = "sg-0af84ea45bd095351"
$AssignPublicIp = "DISABLED"
# =====================================================================

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TasksDir  = Split-Path -Parent $ScriptDir
$EcrUri    = "$AccountId.dkr.ecr.$Region.amazonaws.com"
$RepoUri   = "$EcrUri/$RepositoryName"

function Show-Banner {
    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor Cyan
    Write-Host "  CONFLICT MANAGEMENT - ECS Deploy Tool" -ForegroundColor Cyan
    Write-Host ("=" * 70) -ForegroundColor Cyan
    Write-Host "  Account:    $AccountId"
    Write-Host "  Region:     $Region"
    Write-Host "  Profile:    $Profile"
    Write-Host "  Repository: $RepositoryName"
    Write-Host "  Cluster:    $Cluster"
    Write-Host ("=" * 70) -ForegroundColor Cyan
    Write-Host ""
}

function Do-SsoLogin {
    Write-Host "`n--- SSO Login ---" -ForegroundColor Yellow
    Write-Host "  Logging in via SSO..."
    aws sso login --profile $Profile
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  SSO login failed" -ForegroundColor Red
        return $false
    }

    # Verify
    Write-Host "  Verifying credentials..."
    aws sts get-caller-identity --profile $Profile | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Credential verification failed" -ForegroundColor Red
        return $false
    }
    Write-Host "  SSO login successful" -ForegroundColor Green
    return $true
}

function Do-Build {
    Write-Host "`n--- Building Docker Image ---" -ForegroundColor Yellow
    Write-Host "  Build context: $TasksDir"

    docker build -t "${RepositoryName}:latest" $TasksDir
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Build failed" -ForegroundColor Red
        return $false
    }

    $ImageSize = docker images "${RepositoryName}:latest" --format "{{.Size}}"
    Write-Host "  Image size: $ImageSize" -ForegroundColor Green
    Write-Host "  Build successful" -ForegroundColor Green
    return $true
}

function Do-Push {
    Write-Host "`n--- Pushing to ECR ---" -ForegroundColor Yellow

    # Authenticate
    Write-Host "  Authenticating to ECR..."
    aws ecr get-login-password --region $Region --profile $Profile | docker login --username AWS --password-stdin $EcrUri
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ECR authentication failed" -ForegroundColor Red
        return $false
    }

    # Tag
    $Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    docker tag "${RepositoryName}:latest" "${RepoUri}:latest"
    docker tag "${RepositoryName}:latest" "${RepoUri}:${Timestamp}"

    # Push
    Write-Host "  Pushing ${RepoUri}:latest ..."
    docker push "${RepoUri}:latest"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Push failed" -ForegroundColor Red
        return $false
    }

    Write-Host "  Pushing ${RepoUri}:${Timestamp} ..."
    docker push "${RepoUri}:${Timestamp}"

    Write-Host "  Push successful" -ForegroundColor Green
    Write-Host "  Tags: latest, $Timestamp" -ForegroundColor Green
    return $true
}

function Do-RunTask {
    param([string]$Action = "")

    $NetworkConfig = "awsvpcConfiguration={subnets=[$Subnets],securityGroups=[$SecurityGroups],assignPublicIp=$AssignPublicIp}"

    if ($Action) {
        Write-Host "`n--- Running ECS Task: $Action ---" -ForegroundColor Yellow

        # Write overrides to temp file (avoids PowerShell JSON escaping issues)
        $OverrideJson = @{
            containerOverrides = @(
                @{
                    name = $TaskDefinition
                    environment = @(
                        @{ name = "ACTION"; value = $Action }
                    )
                }
            )
        } | ConvertTo-Json -Depth 5 -Compress

        $OverrideFile = Join-Path $env:TEMP "ecs-override.json"
        $OverrideJson | Out-File -Encoding ascii -FilePath $OverrideFile

        aws ecs run-task `
            --cluster $Cluster `
            --task-definition $TaskDefinition `
            --launch-type FARGATE `
            --network-configuration $NetworkConfig `
            --overrides "file://$OverrideFile" `
            --profile $Profile
    }
    else {
        Write-Host "`n--- Running ECS Task: default pipeline ---" -ForegroundColor Yellow

        aws ecs run-task `
            --cluster $Cluster `
            --task-definition $TaskDefinition `
            --launch-type FARGATE `
            --network-configuration $NetworkConfig `
            --profile $Profile
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Task launch failed" -ForegroundColor Red
        return $false
    }

    Write-Host "  Task launched successfully" -ForegroundColor Green
    Write-Host "  View logs: CloudWatch > /ecs/task02-conflict-updater" -ForegroundColor Cyan
    return $true
}

# =====================================================================
# MAIN -- interactive menu
# =====================================================================

Show-Banner

# Step 1: SSO login?
$doLogin = Read-Host "SSO login needed? (y/N)"
if ($doLogin -eq 'y') {
    $ok = Do-SsoLogin
    if (-not $ok) { exit 1 }
}

# Step 2: Build?
$doBuild = Read-Host "Build Docker image? (Y/n)"
if ($doBuild -ne 'n') {
    $ok = Do-Build
    if (-not $ok) { exit 1 }
}

# Step 3: Push?
$doPush = Read-Host "Push to ECR? (Y/n)"
if ($doPush -ne 'n') {
    $ok = Do-Push
    if (-not $ok) { exit 1 }
}

# Step 4: Run?
Write-Host ""
Write-Host "Run ECS task?" -ForegroundColor Cyan
Write-Host "  0) Skip - don't run"
Write-Host "  1) Default pipeline (validate_config -> test_connections -> task02_00_run_conflict_update)"
Write-Host "  2) validate_config only"
Write-Host "  3) test_connections only"
Write-Host "  4) task02_00_run_conflict_update only"
Write-Host "  5) Custom action(s) - you type the action name(s)"
$runChoice = Read-Host "Choice (0-5)"

switch ($runChoice) {
    '0' { Write-Host "`nSkipping task run." }
    '1' { Do-RunTask -Action "" }
    '2' { Do-RunTask -Action "validate_config" }
    '3' { Do-RunTask -Action "test_connections" }
    '4' { Do-RunTask -Action "task02_00_run_conflict_update" }
    '5' {
        $customAction = Read-Host "Enter action name(s) (comma-separated)"
        Do-RunTask -Action $customAction
    }
    default { Write-Host "`nSkipping task run." }
}

# Done
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Green
Write-Host "  Done!" -ForegroundColor Green
Write-Host ("=" * 70) -ForegroundColor Green
Write-Host ""
