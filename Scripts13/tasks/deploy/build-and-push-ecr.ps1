# Build, push, register task definition, and optionally run on ECS
# ================================================================
#
# Interactive script -- prompts for what you want to do.
# Each prompt has a default shown in brackets [Y/n] or [y/N].
# Just press Enter to accept the default.
#
#   1. SSO login            [default: No]
#   2. Build Docker image   [default: Yes]
#   3. Push to ECR          [default: Yes]
#   4. Register task def    [default: No -- only needed when env vars or CPU/memory change]
#   5. Run ECS task         [default: Skip]
#
# Usage:
#   cd Scripts13\tasks\deploy
#   .\build-and-push-ecr.ps1
#
# Pre-requisites:
#   - PowerShell 5.1+ (Windows) or PowerShell 7+ (cross-platform)
#   - Docker Desktop installed and RUNNING (required for steps 2-3)
#       Download: https://www.docker.com/products/docker-desktop
#       Verify:   docker --version
#   - AWS CLI v2 installed and configured with an SSO profile (required for steps 1, 3-5)
#       Download: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
#       Verify:   aws --version
#   - AWS SSO profile configured matching the $Profile variable below
#       Setup:    aws configure sso --profile <profile-name>
#   - deploy/.env file with secret values (required for step 4)
#       Create:   copy deploy\.env.example deploy\.env
#       Then fill in real values for Snowflake, PostgreSQL, and email settings.
#       See .env.example for the full list of required variables.
#   - ECR repository must already exist in your AWS account
#       The repository name is set in $RepositoryName below.
#   - ECS cluster must already exist with Fargate capacity
#       The cluster name is set in $Cluster below.
#   - CloudWatch log group "/ecs/task02-conflict-updater" must exist
#       Create manually in the AWS Console or via:
#       aws logs create-log-group --log-group-name /ecs/task02-conflict-updater --profile <profile>
#   - IAM role "ecsTaskExecutionRole" must exist with permissions for:
#       ECR pull, CloudWatch Logs, and (if using Secrets Manager later) secrets access.
#   - Network: VPC subnets and security group configured in $Subnets / $SecurityGroups
#       Security group must allow outbound HTTPS (443) for Snowflake and ECR,
#       and outbound to PostgreSQL on port 5432.
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
$TaskFamily     = "task02-conflict-updater"
$Cpu            = "1024"        # Fargate vCPU units (256/512/1024/2048/4096)
$Memory         = "2048"        # Fargate memory in MB (must match CPU -- see AWS docs)
$Subnets        = "subnet-0bf69a7a22a445997,subnet-07b70458a1e90f658,subnet-0e558920a2b805a7f,subnet-0213fe32b2341360b"
$SecurityGroups = "sg-0af84ea45bd095351"
$AssignPublicIp = "DISABLED"
# =====================================================================

$ScriptDir          = Split-Path -Parent $MyInvocation.MyCommand.Path
$TasksDir           = Split-Path -Parent $ScriptDir
$EcrUri             = "$AccountId.dkr.ecr.$Region.amazonaws.com"
$RepoUri            = "$EcrUri/$RepositoryName"
$TemplatePath       = Join-Path $ScriptDir "ecs-task-definition.json"
$EnvFilePath        = Join-Path $ScriptDir ".env"
$GeneratedPath      = Join-Path $ScriptDir ".generated-taskdef.json"

# Task definition to use for run-task (updated by Do-RegisterTaskDef)
$script:TaskDefinition = $TaskFamily

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

# =====================================================================
# Task Definition Registration
# =====================================================================

function Read-EnvFile {
    <#
    .SYNOPSIS
        Parse a .env file into a hashtable. Skips comments and blank lines.
    #>
    param([string]$Path)

    $envVars = @{}
    if (-not (Test-Path $Path)) {
        return $envVars
    }

    foreach ($line in Get-Content $Path) {
        $trimmed = $line.Trim()
        # Skip comments and blank lines
        if ($trimmed -eq '' -or $trimmed.StartsWith('#')) { continue }

        # Split on first '=' only
        $eqIdx = $trimmed.IndexOf('=')
        if ($eqIdx -le 0) { continue }

        $key   = $trimmed.Substring(0, $eqIdx).Trim()
        $value = $trimmed.Substring($eqIdx + 1).Trim()

        # Strip surrounding quotes if present
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        $envVars[$key] = $value
    }
    return $envVars
}

function Mask-Secret {
    <#
    .SYNOPSIS
        Mask a secret value for display. Shows first 4 and last 4 chars.
    #>
    param([string]$Value)

    if ($Value.Length -le 12) {
        return ("*" * $Value.Length)
    }
    $first = $Value.Substring(0, 4)
    $last  = $Value.Substring($Value.Length - 4)
    $stars = "*" * [Math]::Min(($Value.Length - 8), 20)
    return "${first}${stars}${last}"
}

function Is-SensitiveKey {
    <#
    .SYNOPSIS
        Check if an env var key contains sensitive data.
    #>
    param([string]$Key)

    $sensitivePatterns = @('PASSWORD', 'PRIVATE_KEY', 'SECRET', 'TOKEN')
    foreach ($pattern in $sensitivePatterns) {
        if ($Key.ToUpper().Contains($pattern)) { return $true }
    }
    return $false
}

function Do-RegisterTaskDef {
    Write-Host "`n--- Register ECS Task Definition ---" -ForegroundColor Yellow

    # Check template exists
    if (-not (Test-Path $TemplatePath)) {
        Write-Host "  Template not found: $TemplatePath" -ForegroundColor Red
        return $false
    }

    # Check .env exists
    if (-not (Test-Path $EnvFilePath)) {
        Write-Host "  .env file not found: $EnvFilePath" -ForegroundColor Red
        Write-Host "  Copy .env.example to .env and fill in real values:" -ForegroundColor Yellow
        Write-Host "    cp deploy\.env.example deploy\.env" -ForegroundColor Gray
        return $false
    }

    # Read template
    Write-Host "  Reading template: $TemplatePath"
    $templateJson = Get-Content $TemplatePath -Raw

    # Read .env
    Write-Host "  Reading .env: $EnvFilePath"
    $envVars = Read-EnvFile -Path $EnvFilePath

    if ($envVars.Count -eq 0) {
        Write-Host "  .env file is empty or has no valid entries" -ForegroundColor Red
        return $false
    }

    Write-Host "  Loaded $($envVars.Count) variable(s) from .env" -ForegroundColor Green

    # ---- Step 1: Replace infrastructure placeholders from PS1 config ----
    $resolved = $templateJson
    $resolved = $resolved.Replace('<ACCOUNT_ID>', $AccountId)
    $resolved = $resolved.Replace('<REGION>', $Region)
    $resolved = $resolved.Replace('<REPOSITORY_NAME>', $RepositoryName)
    $resolved = $resolved.Replace('<CPU>', $Cpu)
    $resolved = $resolved.Replace('<MEMORY>', $Memory)

    # ---- Step 2: Replace <YOUR_*> env var placeholders from .env ----
    # Mapping: template placeholder -> .env key
    $placeholderMap = @{
        '<YOUR_SNOWFLAKE_ACCOUNT>'     = 'SNOWFLAKE_ACCOUNT'
        '<YOUR_SNOWFLAKE_USER>'        = 'SNOWFLAKE_USER'
        '<YOUR_SNOWFLAKE_WAREHOUSE>'   = 'SNOWFLAKE_WAREHOUSE'
        '<YOUR_SNOWFLAKE_PRIVATE_KEY>' = 'SNOWFLAKE_PRIVATE_KEY'
        '<YOUR_POSTGRES_HOST>'         = 'POSTGRES_HOST'
        '<YOUR_POSTGRES_USER>'         = 'POSTGRES_USER'
        '<YOUR_POSTGRES_PASSWORD>'     = 'POSTGRES_PASSWORD'
        '<YOUR_AWS_REGION>'            = 'AWS_REGION'
        '<YOUR_EMAIL_SENDER>'          = 'EMAIL_SENDER'
        '<YOUR_EMAIL_RECIPIENTS>'      = 'EMAIL_RECIPIENTS'
    }

    $missingKeys = @()
    foreach ($placeholder in $placeholderMap.Keys) {
        $envKey = $placeholderMap[$placeholder]
        if ($envVars.ContainsKey($envKey)) {
            # JSON-escape the value:
            #   1. Convert literal \n (two chars) to real newlines (for RSA keys stored as single-line in .env)
            #   2. Escape double-quotes for JSON
            #   3. Convert real newlines back to JSON \n escape sequences
            #   4. Strip carriage returns (Windows line endings)
            $escapedValue = $envVars[$envKey] -replace '\\n', "`n"
            $escapedValue = $escapedValue -replace '"', '\"' -replace "`r", '' -replace "`n", '\n'
            $resolved = $resolved.Replace($placeholder, $escapedValue)
        }
        else {
            $missingKeys += $envKey
        }
    }

    if ($missingKeys.Count -gt 0) {
        Write-Host ""
        Write-Host "  WARNING: Missing .env keys:" -ForegroundColor Yellow
        foreach ($key in $missingKeys) {
            Write-Host "    - $key" -ForegroundColor Yellow
        }
        Write-Host "  Placeholders for these will remain unresolved." -ForegroundColor Yellow
    }

    # ---- Step 3: Validate JSON ----
    try {
        $parsedJson = $resolved | ConvertFrom-Json
    }
    catch {
        Write-Host "  Resolved JSON is invalid: $_" -ForegroundColor Red
        Write-Host "  Writing raw output to $GeneratedPath for debugging" -ForegroundColor Yellow
        $resolved | Out-File -Encoding utf8 -FilePath $GeneratedPath
        return $false
    }

    # ---- Step 4: Write generated file ----
    $resolved | Out-File -Encoding utf8 -FilePath $GeneratedPath
    Write-Host "  Generated: $GeneratedPath" -ForegroundColor Green

    # ---- Step 5: Display summary with masked secrets ----
    Write-Host ""
    Write-Host "  Task Definition Summary:" -ForegroundColor Cyan
    Write-Host "  -------------------------"
    Write-Host "  Family:          $($parsedJson.family)"
    Write-Host "  CPU:             $($parsedJson.cpu)"
    Write-Host "  Memory:          $($parsedJson.memory) MB"
    Write-Host "  Image:           $($parsedJson.containerDefinitions[0].image)"
    Write-Host "  Log group:       $($parsedJson.containerDefinitions[0].logConfiguration.options.'awslogs-group')"
    Write-Host "  Stop timeout:    $($parsedJson.containerDefinitions[0].stopTimeout)s"
    Write-Host ""
    Write-Host "  Environment Variables:" -ForegroundColor Cyan

    foreach ($envEntry in $parsedJson.containerDefinitions[0].environment) {
        $displayValue = $envEntry.value
        if ((Is-SensitiveKey -Key $envEntry.name) -and $displayValue.Length -gt 0) {
            $displayValue = Mask-Secret -Value $displayValue
        }
        # Truncate long values for display
        if ($displayValue.Length -gt 80) {
            $displayValue = $displayValue.Substring(0, 77) + "..."
        }
        $padding = " " * [Math]::Max(0, (24 - $envEntry.name.Length))
        Write-Host "    $($envEntry.name)${padding}= $displayValue"
    }

    # Check for unresolved placeholders
    $unresolvedMatches = [regex]::Matches($resolved, '<[A-Z_]+>')
    if ($unresolvedMatches.Count -gt 0) {
        Write-Host ""
        Write-Host "  WARNING: Unresolved placeholders found:" -ForegroundColor Yellow
        $unresolvedMatches | ForEach-Object { Write-Host "    - $($_.Value)" -ForegroundColor Yellow }
    }

    # ---- Step 6: Prompt for confirmation ----
    #   Default: Yes (just press Enter to register)
    Write-Host ""
    $confirm = Read-Host "  Register this task definition? [Y/n]"
    if ($confirm -eq 'n' -or $confirm -eq 'N') {
        Write-Host "  Skipping registration." -ForegroundColor Yellow
        return $true  # Not a failure, user chose to skip
    }

    # ---- Step 7: Register with ECS ----
    Write-Host "  Registering task definition..."
    $registerOutput = aws ecs register-task-definition `
        --cli-input-json "file://$GeneratedPath" `
        --profile $Profile 2>&1

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Registration failed:" -ForegroundColor Red
        Write-Host "  $registerOutput" -ForegroundColor Red
        return $false
    }

    # Parse the output to get the new revision
    try {
        $registerResult = $registerOutput | ConvertFrom-Json
        $newRevision = $registerResult.taskDefinition.revision
        $fullArn = $registerResult.taskDefinition.taskDefinitionArn
        Write-Host "  Registered: $TaskFamily`:$newRevision" -ForegroundColor Green
        Write-Host "  ARN: $fullArn" -ForegroundColor Gray

        # Update the task definition for subsequent run-task calls
        $script:TaskDefinition = "${TaskFamily}:${newRevision}"
        Write-Host "  Using $($script:TaskDefinition) for run-task" -ForegroundColor Green
    }
    catch {
        Write-Host "  Registered successfully (could not parse revision from output)" -ForegroundColor Green
    }

    return $true
}

# =====================================================================
# Run Task
# =====================================================================

function Do-RunTask {
    param([string]$Action = "")

    $NetworkConfig = "awsvpcConfiguration={subnets=[$Subnets],securityGroups=[$SecurityGroups],assignPublicIp=$AssignPublicIp}"
    $runOutput = $null

    if ($Action) {
        Write-Host "`n--- Running ECS Task: $Action ---" -ForegroundColor Yellow
        Write-Host "  Task definition: $($script:TaskDefinition)"

        # Write overrides to temp file (avoids PowerShell JSON escaping issues)
        $OverrideJson = @{
            containerOverrides = @(
                @{
                    name = $TaskFamily
                    environment = @(
                        @{ name = "ACTION"; value = $Action }
                    )
                }
            )
        } | ConvertTo-Json -Depth 5 -Compress

        $OverrideFile = Join-Path $env:TEMP "ecs-override.json"
        $OverrideJson | Out-File -Encoding ascii -FilePath $OverrideFile

        $runOutput = aws ecs run-task `
            --cluster $Cluster `
            --task-definition $script:TaskDefinition `
            --launch-type FARGATE `
            --network-configuration $NetworkConfig `
            --overrides "file://$OverrideFile" `
            --profile $Profile 2>&1
    }
    else {
        Write-Host "`n--- Running ECS Task: default pipeline ---" -ForegroundColor Yellow
        Write-Host "  Task definition: $($script:TaskDefinition)"

        $runOutput = aws ecs run-task `
            --cluster $Cluster `
            --task-definition $script:TaskDefinition `
            --launch-type FARGATE `
            --network-configuration $NetworkConfig `
            --profile $Profile 2>&1
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Task launch failed:" -ForegroundColor Red
        Write-Host "  $runOutput" -ForegroundColor Red
        return $false
    }

    # Parse the response and show a clean summary instead of raw JSON
    try {
        $runResult = $runOutput | ConvertFrom-Json
        $task = $runResult.tasks[0]
        $taskArn = $task.taskArn
        # Extract short task ID from ARN (last segment after /)
        $taskId = $taskArn.Split('/')[-1]
        Write-Host "  Task launched successfully" -ForegroundColor Green
        Write-Host "  Task ID:     $taskId" -ForegroundColor Green
        Write-Host "  Status:      $($task.lastStatus)" -ForegroundColor Green
        Write-Host "  Task def:    $($script:TaskDefinition)"
        Write-Host "  Cluster:     $Cluster"
        Write-Host ""
        Write-Host "  Monitor:" -ForegroundColor Cyan
        Write-Host "    Logs:    CloudWatch > /ecs/task02-conflict-updater"
        Write-Host "    Console: https://$Region.console.aws.amazon.com/ecs/v2/clusters/$Cluster/tasks/$taskId"

        # Show failures if any
        if ($runResult.failures -and $runResult.failures.Count -gt 0) {
            Write-Host ""
            Write-Host "  Failures:" -ForegroundColor Red
            foreach ($failure in $runResult.failures) {
                Write-Host "    - $($failure.reason)" -ForegroundColor Red
            }
        }
    }
    catch {
        # Fallback if JSON parsing fails
        Write-Host "  Task launched successfully" -ForegroundColor Green
        Write-Host "  View logs: CloudWatch > /ecs/task02-conflict-updater" -ForegroundColor Cyan
    }

    return $true
}

# =====================================================================
# MAIN -- interactive menu
# =====================================================================

Show-Banner

# Step 1: SSO login?
#   Default: No (just press Enter to skip)
$doLogin = Read-Host "SSO login needed? [y/N]"
if ($doLogin -eq 'y' -or $doLogin -eq 'Y') {
    $ok = Do-SsoLogin
    if (-not $ok) { exit 1 }
}

# Step 2: Build?
#   Default: Yes (just press Enter to build)
$didBuild = $false
$doBuild = Read-Host "Build Docker image? [Y/n]"
if ($doBuild -eq 'n' -or $doBuild -eq 'N') {
    Write-Host "  Skipping build." -ForegroundColor Gray
}
else {
    $ok = Do-Build
    if (-not $ok) { exit 1 }
    $didBuild = $true
}

# Step 3: Push?
#   Default: Yes if we just built, No if build was skipped
if ($didBuild) {
    $doPush = Read-Host "Push to ECR? [Y/n]"
    if ($doPush -eq 'n' -or $doPush -eq 'N') {
        Write-Host "  Skipping push." -ForegroundColor Gray
    }
    else {
        $ok = Do-Push
        if (-not $ok) { exit 1 }
    }
}
else {
    $doPush = Read-Host "Push to ECR? (no new build -- push existing image?) [y/N]"
    if ($doPush -eq 'y' -or $doPush -eq 'Y') {
        $ok = Do-Push
        if (-not $ok) { exit 1 }
    }
    else {
        Write-Host "  Skipping push." -ForegroundColor Gray
    }
}

# Step 4: Register task definition?
#   Default: No (just press Enter to skip -- only needed when env vars or CPU/memory change)
$doRegister = Read-Host "Register task definition from template + .env? [y/N]"
if ($doRegister -eq 'y' -or $doRegister -eq 'Y') {
    $ok = Do-RegisterTaskDef
    if (-not $ok) { exit 1 }
}
else {
    Write-Host "  Skipping registration (using existing: $($script:TaskDefinition))." -ForegroundColor Gray
}

# Step 5: Run?
#   Default: 0 (skip -- just press Enter)
Write-Host ""
Write-Host "Run ECS task?" -ForegroundColor Cyan
Write-Host "  Using: $($script:TaskDefinition)" -ForegroundColor Gray
Write-Host "  0) Skip - don't run [default]"
Write-Host "  1) Default pipeline (preflight -> task02 update -> postflight)"
Write-Host "  2) task00_preflight only"
Write-Host "  3) task02_00_run_conflict_update only"
Write-Host "  4) task99_postflight only"
Write-Host "  5) validate_config only (standalone)"
Write-Host "  6) test_connections only (standalone)"
Write-Host "  7) Custom action(s) - you type the action name(s)"
$runChoice = Read-Host "Choice [0-7]"

switch ($runChoice) {
    '1' { Do-RunTask -Action "" }
    '2' { Do-RunTask -Action "task00_preflight" }
    '3' { Do-RunTask -Action "task02_00_run_conflict_update" }
    '4' { Do-RunTask -Action "task99_postflight" }
    '5' { Do-RunTask -Action "validate_config" }
    '6' { Do-RunTask -Action "test_connections" }
    '7' {
        $customAction = Read-Host "Enter action name(s) (comma-separated)"
        if ($customAction) {
            Do-RunTask -Action $customAction
        }
        else {
            Write-Host "`nNo action entered -- skipping." -ForegroundColor Gray
        }
    }
    default { Write-Host "`nSkipping task run." -ForegroundColor Gray }
}

# Done
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Green
Write-Host "  Done!" -ForegroundColor Green
Write-Host ("=" * 70) -ForegroundColor Green
Write-Host ""
