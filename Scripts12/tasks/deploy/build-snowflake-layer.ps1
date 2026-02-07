# Build Snowflake Layer for AWS Lambda
# This layer contains snowflake-connector-python and dependencies
# Only needs to be created once and can be reused across multiple Lambda functions

param(
    [string]$OutputDir = ".\layer-snowflake",
    [string]$ZipName = "snowflake-layer.zip"
)

Write-Host "=" * 70
Write-Host "Building Snowflake Lambda Layer"
Write-Host "=" * 70

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Clean and create output directory
if (Test-Path $OutputDir) {
    Write-Host "Cleaning existing layer directory..."
    Remove-Item -Recurse -Force $OutputDir
}

Write-Host "Creating layer directory structure..."
New-Item -ItemType Directory -Force -Path "$OutputDir\python" | Out-Null

# Create requirements for snowflake layer
Write-Host "`nPreparing dependencies list..."
$Requirements = @"
snowflake-connector-python>=3.6.0
python-dotenv>=1.0.0
cryptography>=41.0.0
"@

$RequirementsPath = Join-Path $ScriptDir "requirements-snowflake-layer.txt"
$Requirements | Out-File -FilePath $RequirementsPath -Encoding UTF8

# Install dependencies
Write-Host "`nInstalling Snowflake connector and dependencies..."
Write-Host "  This may take several minutes..."

pip install --target "$OutputDir\python" -r $RequirementsPath --upgrade --platform manylinux2014_x86_64 --only-binary=:all:

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Failed to install dependencies" -ForegroundColor Red
    Write-Host "Try running: pip install --upgrade pip" -ForegroundColor Yellow
    exit 1
}

# Check installed size
$InstalledSize = (Get-ChildItem -Path "$OutputDir\python" -Recurse | Measure-Object -Property Length -Sum).Sum / 1MB
Write-Host "`n  Installed size: $([math]::Round($InstalledSize, 2)) MB"

# Create ZIP
Write-Host "`nCreating layer ZIP..."
$ZipPath = Join-Path $ScriptDir $ZipName

if (Test-Path $ZipPath) {
    Remove-Item $ZipPath -Force
}

Add-Type -Assembly System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory($OutputDir, $ZipPath)

$ZipSize = (Get-Item $ZipPath).Length / 1MB
Write-Host "  ZIP created: $ZipPath"
Write-Host "  Size: $([math]::Round($ZipSize, 2)) MB"

# Verify layer structure
Write-Host "`nVerifying layer structure..."
$PythonDir = Join-Path $OutputDir "python"
$SnowflakeExists = Test-Path (Join-Path $PythonDir "snowflake")
$CryptographyExists = Test-Path (Join-Path $PythonDir "cryptography")

if ($SnowflakeExists) {
    Write-Host "  ✓ snowflake-connector-python" -ForegroundColor Green
} else {
    Write-Host "  ✗ snowflake-connector-python (missing)" -ForegroundColor Red
}

if ($CryptographyExists) {
    Write-Host "  ✓ cryptography" -ForegroundColor Green
} else {
    Write-Host "  ✗ cryptography (missing)" -ForegroundColor Red
}

# Summary
Write-Host "`n" + ("=" * 70)
Write-Host "Snowflake Layer built successfully!" -ForegroundColor Green
Write-Host ("=" * 70)
Write-Host "`nLayer package: $ZipPath"
Write-Host "Layer size: $([math]::Round($ZipSize, 2)) MB"
Write-Host "`nNext steps:"
Write-Host "  1. Upload to AWS Lambda Layers:"
Write-Host ""
Write-Host "     aws lambda publish-layer-version \"
Write-Host "       --layer-name snowflake-connector \"
Write-Host "       --description ""Snowflake connector with dependencies"" \"
Write-Host "       --zip-file fileb://$ZipName \"
Write-Host "       --compatible-runtimes python3.11 python3.10"
Write-Host ""
Write-Host "  2. Note the Layer ARN from the response"
Write-Host "  3. Use ARN when creating/updating Lambda function"
Write-Host ""
Write-Host "Alternative: Upload via AWS Console"
Write-Host "  - Navigate to Lambda > Layers"
Write-Host "  - Create layer with $ZipName"
Write-Host "  - Compatible runtimes: Python 3.11, 3.10"
Write-Host

# Cleanup temporary files
Write-Host "Cleaning up temporary files..."
Remove-Item $RequirementsPath -Force -ErrorAction SilentlyContinue

# Cleanup layer directory automatically
Write-Host "Cleaning up temporary layer directory..."
Remove-Item -Recurse -Force $OutputDir
Write-Host "Layer directory cleaned up (ZIP file preserved)" -ForegroundColor Green

Write-Host "`nDone!" -ForegroundColor Green
