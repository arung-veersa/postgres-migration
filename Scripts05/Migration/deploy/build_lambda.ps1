# AWS Lambda Deployment Script
# Builds deployment package for connection testing (test_postgres, test_snowflake)
# Uses Lambda Layer for psycopg2 to avoid binary compatibility issues

$ErrorActionPreference = "Stop"

Write-Host "==============================================" -ForegroundColor Blue
Write-Host "Building Lambda Deployment Package" -ForegroundColor Blue
Write-Host "==============================================" -ForegroundColor Blue

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$DeployDir = Join-Path $ProjectRoot "deploy"
$PackageDir = Join-Path $DeployDir "package"
$ZipFile = Join-Path $DeployDir "lambda_deployment.zip"

# Clean previous builds
Write-Host "[1/5] Cleaning previous builds..." -ForegroundColor Yellow
if (Test-Path $PackageDir) {
    Remove-Item -Path $PackageDir -Recurse -Force
}
if (Test-Path $ZipFile) {
    Remove-Item -Path $ZipFile -Force
}
New-Item -ItemType Directory -Path $PackageDir -Force | Out-Null

# Create requirements (psycopg2 via Lambda Layer)
Write-Host "[2/5] Creating requirements..." -ForegroundColor Yellow
$RequirementsContent = @"
python-dotenv==1.0.0
snowflake-connector-python==3.7.0
cryptography==41.0.7
cffi==1.16.0
"@
$RequirementsFile = Join-Path $DeployDir "requirements_prod.txt"
Set-Content -Path $RequirementsFile -Value $RequirementsContent

# Build with Docker (Linux binaries)
Write-Host "[3/5] Installing dependencies with Docker..." -ForegroundColor Yellow

docker run --rm `
    --entrypoint /bin/bash `
    -v "${DeployDir}:/workspace" `
    -w /workspace `
    public.ecr.aws/lambda/python:3.11 `
    -c "pip install --upgrade pip && pip install -r requirements_prod.txt -t package --no-cache-dir"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker build failed" -ForegroundColor Red
    exit 1
}

# Copy application code
Write-Host "[4/5] Copying application code..." -ForegroundColor Yellow
Copy-Item -Path (Join-Path $ProjectRoot "config") -Destination $PackageDir -Recurse -Force
Copy-Item -Path (Join-Path $ProjectRoot "src") -Destination $PackageDir -Recurse -Force
Copy-Item -Path (Join-Path $ProjectRoot "scripts") -Destination $PackageDir -Recurse -Force
Copy-Item -Path (Join-Path $ProjectRoot "sql") -Destination $PackageDir -Recurse -Force
Copy-Item -Path (Join-Path $ProjectRoot "scripts\lambda_handler.py") -Destination (Join-Path $PackageDir "lambda_handler.py") -Force

# Clean up
Write-Host "  Cleaning cache files..." -ForegroundColor Gray
Get-ChildItem -Path $PackageDir -Include "__pycache__" -Recurse -Directory | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path $PackageDir -Include "*.pyc","*.pyo" -Recurse -File | Remove-Item -Force -ErrorAction SilentlyContinue

# Create ZIP
Write-Host "[5/5] Creating deployment ZIP..." -ForegroundColor Yellow
Compress-Archive -Path (Join-Path $PackageDir "*") -DestinationPath $ZipFile -Force

$SizeMB = [math]::Round((Get-Item $ZipFile).Length / 1MB, 2)

Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "✓ SUCCESS: Lambda package created" -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green
Write-Host "Location: $ZipFile" -ForegroundColor White
Write-Host "Size: $SizeMB MB" -ForegroundColor White
Write-Host ""
Write-Host "Next: Upload to AWS Lambda + Attach psycopg2 layer" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Green

