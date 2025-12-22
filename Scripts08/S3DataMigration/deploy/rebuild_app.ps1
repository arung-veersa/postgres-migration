# Quick Rebuild Script - Application Code + All Dependencies
# Use this when you changed Python code or need to update the full package
# Matches Scripts05/Migration approach: Everything except psycopg2 in main ZIP

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Quick Rebuild - Full Package" -ForegroundColor Cyan
Write-Host "  (All dependencies included)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$ProjectRoot = $PSScriptRoot | Split-Path -Parent
$DeployDir = Join-Path $ProjectRoot "deploy"
$PackageDir = Join-Path $DeployDir "package"
$AppZip = Join-Path $DeployDir "lambda_deployment.zip"

# ===================================================================
# Step 1: Clean previous application package
# ===================================================================
Write-Host "[1/4] Cleaning previous application package..." -ForegroundColor Yellow

if (Test-Path $PackageDir) {
    Write-Host "  Removing: $PackageDir" -ForegroundColor Gray
    Remove-Item -Path $PackageDir -Recurse -Force
}

if (Test-Path $AppZip) {
    Write-Host "  Removing: $AppZip" -ForegroundColor Gray
    Remove-Item -Path $AppZip -Force
}

New-Item -ItemType Directory -Path $PackageDir -Force | Out-Null

Write-Host "  ✓ Cleanup complete" -ForegroundColor Green
Write-Host ""

# ===================================================================
# Step 2: Install ALL dependencies (pandas, numpy, snowflake, etc.)
# ===================================================================
Write-Host "[2/4] Installing ALL dependencies with Docker..." -ForegroundColor Yellow
Write-Host "  This includes: pandas, numpy, snowflake-connector" -ForegroundColor Gray
Write-Host "  May take 5-10 minutes..." -ForegroundColor Gray

docker run --rm `
    --entrypoint /bin/bash `
    -v "${DeployDir}:/workspace" `
    -w /workspace `
    public.ecr.aws/lambda/python:3.11 `
    -c "pip install --upgrade pip && pip install -r requirements_layer.txt -t package --no-cache-dir"

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ERROR: Docker install failed" -ForegroundColor Red
    exit 1
}

Write-Host "  ✓ Dependencies installed" -ForegroundColor Green
Write-Host ""

# ===================================================================
# Step 3: Copy application code
# ===================================================================
Write-Host "[3/4] Copying application code..." -ForegroundColor Yellow

# Copy lib directory
Copy-Item -Path (Join-Path $ProjectRoot "lib") -Destination $PackageDir -Recurse -Force
Write-Host "  ✓ lib/" -ForegroundColor Gray

# Copy scripts directory
Copy-Item -Path (Join-Path $ProjectRoot "scripts") -Destination $PackageDir -Recurse -Force
Write-Host "  ✓ scripts/" -ForegroundColor Gray

# Copy migrate.py (main orchestrator)
Copy-Item -Path (Join-Path $ProjectRoot "migrate.py") -Destination $PackageDir -Force
Write-Host "  ✓ migrate.py" -ForegroundColor Gray

# Copy config.json
Copy-Item -Path (Join-Path $ProjectRoot "config.json") -Destination $PackageDir -Force
Write-Host "  ✓ config.json" -ForegroundColor Gray

# Copy lambda_handler.py to root
Copy-Item -Path (Join-Path $ProjectRoot "scripts\lambda_handler.py") -Destination (Join-Path $PackageDir "lambda_handler.py") -Force
Write-Host "  ✓ lambda_handler.py (at root)" -ForegroundColor Gray

# Copy sql folder if exists
$SqlDir = Join-Path $ProjectRoot "sql"
if (Test-Path $SqlDir) {
    $TargetSqlDir = Join-Path $PackageDir "sql"
    New-Item -ItemType Directory -Path $TargetSqlDir -Force | Out-Null
    Copy-Item -Path (Join-Path $SqlDir "migration_status_schema.sql") -Destination $TargetSqlDir -Force -ErrorAction SilentlyContinue
    Write-Host "  ✓ sql/migration_status_schema.sql" -ForegroundColor Gray
}

# Clean cache files
Write-Host "  Cleaning cache files..." -ForegroundColor Gray
Get-ChildItem -Path $PackageDir -Include "__pycache__" -Recurse -Directory | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path $PackageDir -Include "*.pyc","*.pyo" -Recurse -File | Remove-Item -Force -ErrorAction SilentlyContinue

Write-Host "  ✓ Application code packaged" -ForegroundColor Green
Write-Host ""

# ===================================================================
# Step 4: Create deployment ZIP
# ===================================================================
Write-Host "[4/4] Creating deployment ZIP..." -ForegroundColor Yellow

Compress-Archive -Path (Join-Path $PackageDir "*") -DestinationPath $AppZip -Force

$AppSizeMB = [math]::Round((Get-Item $AppZip).Length / 1MB, 2)

Write-Host "  ✓ Package created" -ForegroundColor Green
Write-Host ""

# ===================================================================
# Summary
# ===================================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  ✓ REBUILD COMPLETE" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Package Created:" -ForegroundColor White
Write-Host "  $AppZip" -ForegroundColor Gray
Write-Host "  Size: $AppSizeMB MB" -ForegroundColor Gray

if ($AppSizeMB -gt 50) {
    Write-Host ""
    Write-Host "  ⚠️  WARNING: Package exceeds 50 MB Lambda limit!" -ForegroundColor Red
    Write-Host "  You may need to use S3 deployment" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Update Lambda function:" -ForegroundColor White
Write-Host ""
Write-Host "     aws lambda update-function-code \" -ForegroundColor Cyan
Write-Host "       --function-name migration-lambda \" -ForegroundColor Cyan
Write-Host "       --zip-file fileb://deploy/lambda_deployment.zip \" -ForegroundColor Cyan
Write-Host "       --region us-east-1" -ForegroundColor Cyan
Write-Host ""
Write-Host "  2. Wait for update:" -ForegroundColor White
Write-Host ""
Write-Host "     aws lambda wait function-updated-v2 \" -ForegroundColor Cyan
Write-Host "       --function-name migration-lambda \" -ForegroundColor Cyan
Write-Host "       --region us-east-1" -ForegroundColor Cyan
Write-Host ""
Write-Host "  3. Verify layers attached:" -ForegroundColor White
Write-Host "     - Only psycopg2-layer should be attached" -ForegroundColor Gray
Write-Host "     - All other dependencies are now in the main ZIP" -ForegroundColor Gray
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

