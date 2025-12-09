# Quick Rebuild Script - Application Code ONLY
# Use this for daily development when you change:
#   - config.json
#   - Python code in lib/, scripts/, migrate.py
# 
# Dependencies are in Lambda layers:
#   - psycopg2-layer (existing)
#   - dependencies-layer (new - build once with rebuild_layer.ps1)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Quick Rebuild - App Code Only" -ForegroundColor Cyan
Write-Host "  (No dependencies - uses layers)" -ForegroundColor Cyan
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
# Step 2: Install lightweight app dependencies (if any)
# ===================================================================
Write-Host "[2/4] Installing lightweight dependencies..." -ForegroundColor Yellow
Write-Host "  (python-dotenv, tenacity - very fast)" -ForegroundColor Gray

docker run --rm `
    --entrypoint /bin/bash `
    -v "${DeployDir}:/workspace" `
    -w /workspace `
    public.ecr.aws/lambda/python:3.11 `
    -c "pip install --upgrade pip && pip install python-dotenv==1.0.0 tenacity==8.2.3 -t package --no-cache-dir"

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ERROR: Docker install failed" -ForegroundColor Red
    exit 1
}

Write-Host "  ✓ Lightweight dependencies installed" -ForegroundColor Green
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

# Copy schema.sql if exists
if (Test-Path (Join-Path $ProjectRoot "schema.sql")) {
    Copy-Item -Path (Join-Path $ProjectRoot "schema.sql") -Destination $PackageDir -Force
    Write-Host "  ✓ schema.sql" -ForegroundColor Gray
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
$AppSizeKB = [math]::Round((Get-Item $AppZip).Length / 1KB, 0)

Write-Host "  ✓ Package created" -ForegroundColor Green
Write-Host ""

# ===================================================================
# Summary
# ===================================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  ✓ QUICK REBUILD COMPLETE" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Package Created:" -ForegroundColor White
Write-Host "  $AppZip" -ForegroundColor Gray

if ($AppSizeMB -lt 1) {
    Write-Host "  Size: $AppSizeKB KB" -ForegroundColor Green
} else {
    Write-Host "  Size: $AppSizeMB MB" -ForegroundColor Green
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Next Steps (Manual - AWS Console):" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Go to AWS Lambda Console" -ForegroundColor White
Write-Host ""
Write-Host "2. Open your function: cm-datacopy-test01" -ForegroundColor White
Write-Host ""
Write-Host "3. Click 'Code' tab → 'Upload from' → '.zip file'" -ForegroundColor White
Write-Host ""
Write-Host "4. Upload:" -ForegroundColor White
Write-Host "   $AppZip" -ForegroundColor Cyan
Write-Host ""
Write-Host "5. Wait for upload and deployment (5-10 seconds)" -ForegroundColor White
Write-Host ""
Write-Host "6. Verify in 'Layers' section you have:" -ForegroundColor White
Write-Host "   ✓ psycopg2-layer" -ForegroundColor Green
Write-Host "   ✓ migration-dependencies" -ForegroundColor Green
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "⚡ Future deploys: Just run this script!" -ForegroundColor Cyan
Write-Host "   No Docker, no pip, just copy + zip + upload" -ForegroundColor Gray
Write-Host "   Takes only 10-20 seconds!" -ForegroundColor Gray
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

