# Build Dependencies Lambda Layer
# This script creates a Lambda Layer with all heavy dependencies
# Run this ONCE or when you need to upgrade dependency versions

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Building Dependencies Layer" -ForegroundColor Cyan
Write-Host "  (Run this ONCE, reuse forever)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$DeployDir = $PSScriptRoot
$LayerDir = Join-Path $DeployDir "dependencies_layer"
$LayerZip = Join-Path $DeployDir "dependencies_layer.zip"

# ===================================================================
# Step 1: Clean previous layer
# ===================================================================
Write-Host "[1/3] Cleaning previous layer..." -ForegroundColor Yellow

if (Test-Path $LayerDir) {
    Write-Host "  Removing: $LayerDir" -ForegroundColor Gray
    Remove-Item -Path $LayerDir -Recurse -Force
}

if (Test-Path $LayerZip) {
    Write-Host "  Removing: $LayerZip" -ForegroundColor Gray
    Remove-Item -Path $LayerZip -Force
}

New-Item -ItemType Directory -Path (Join-Path $LayerDir "python") -Force | Out-Null

Write-Host "  ✓ Cleanup complete" -ForegroundColor Green
Write-Host ""

# ===================================================================
# Step 2: Install dependencies with Docker
# ===================================================================
Write-Host "[2/3] Installing dependencies with Docker..." -ForegroundColor Yellow
Write-Host "  Installing: pandas, numpy, snowflake-connector, etc." -ForegroundColor Gray
Write-Host "  This may take 5-10 minutes..." -ForegroundColor Gray
Write-Host ""

docker run --rm `
    --entrypoint /bin/bash `
    -v "${DeployDir}:/workspace" `
    -w /workspace `
    public.ecr.aws/lambda/python:3.11 `
    -c "pip install --upgrade pip && pip install -r requirements_layer.txt -t dependencies_layer/python --no-cache-dir"

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ERROR: Docker install failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "  ✓ Dependencies installed" -ForegroundColor Green
Write-Host ""

# Clean cache files
Write-Host "  Cleaning cache files..." -ForegroundColor Gray
Get-ChildItem -Path $LayerDir -Include "__pycache__" -Recurse -Directory | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path $LayerDir -Include "*.pyc","*.pyo" -Recurse -File | Remove-Item -Force -ErrorAction SilentlyContinue
Write-Host "  ✓ Cache cleaned" -ForegroundColor Green
Write-Host ""

# ===================================================================
# Step 3: Create layer ZIP
# ===================================================================
Write-Host "[3/3] Creating layer ZIP..." -ForegroundColor Yellow

Compress-Archive -Path (Join-Path $LayerDir "*") -DestinationPath $LayerZip -Force

$LayerSizeMB = [math]::Round((Get-Item $LayerZip).Length / 1MB, 2)

Write-Host "  ✓ Layer ZIP created" -ForegroundColor Green
Write-Host ""

# ===================================================================
# Summary
# ===================================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  ✓ LAYER BUILD COMPLETE" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Layer Created:" -ForegroundColor White
Write-Host "  $LayerZip" -ForegroundColor Gray
Write-Host "  Size: $LayerSizeMB MB" -ForegroundColor Gray
Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Next Steps (Manual - AWS Console):" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Go to AWS Lambda Console → Layers" -ForegroundColor White
Write-Host ""
Write-Host "2. Create new layer:" -ForegroundColor White
Write-Host "   - Name: migration-dependencies" -ForegroundColor Gray
Write-Host "   - Upload: dependencies_layer.zip" -ForegroundColor Gray
Write-Host "   - Compatible runtimes: Python 3.11" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Note the Layer ARN (you'll need it)" -ForegroundColor White
Write-Host "   Example: arn:aws:lambda:us-east-1:ACCOUNT:layer:migration-dependencies:1" -ForegroundColor Gray
Write-Host ""
Write-Host "4. Go to your Lambda function configuration" -ForegroundColor White
Write-Host "   - Scroll to 'Layers' section" -ForegroundColor Gray
Write-Host "   - Click 'Add a layer'" -ForegroundColor Gray
Write-Host "   - Select 'Custom layers'" -ForegroundColor Gray
Write-Host "   - Choose: migration-dependencies (version 1)" -ForegroundColor Gray
Write-Host ""
Write-Host "5. Verify your function has 2 layers:" -ForegroundColor White
Write-Host "   ✓ psycopg2-layer (existing)" -ForegroundColor Green
Write-Host "   ✓ migration-dependencies (new)" -ForegroundColor Green
Write-Host ""
Write-Host "6. Then run rebuild_app_only.ps1 for quick deployments!" -ForegroundColor Cyan
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

