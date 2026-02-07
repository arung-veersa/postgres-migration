# Build minimal code package for AWS Lambda (uses layers for dependencies)
# This package only contains application code (~100KB), not dependencies (~50MB)
# Dependencies are provided via Lambda layers

param(
    [string]$OutputDir = ".\package",
    [string]$ZipName = "task02-conflict-updater-code.zip"
)

Write-Host "=" * 70
Write-Host "Building Lambda Code Package (Minimal - No Dependencies)"
Write-Host "=" * 70

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TasksDir = Split-Path -Parent $ScriptDir

# Make OutputDir absolute
if (-not [System.IO.Path]::IsPathRooted($OutputDir)) {
    $OutputDir = Join-Path $ScriptDir $OutputDir
}

Write-Host "Tasks directory: $TasksDir"
Write-Host "Output directory: $OutputDir"

# Clean output directory
if (Test-Path $OutputDir) {
    Write-Host "`nCleaning existing package directory..."
    Remove-Item -Recurse -Force $OutputDir
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

# Copy ONLY application code (no dependencies)
Write-Host "`nCopying application code (no dependencies)..."
$Folders = @("config", "lib", "sql", "scripts")
$TotalFiles = 0

foreach ($Folder in $Folders) {
    $SourcePath = Join-Path $TasksDir $Folder
    $DestPath = Join-Path $OutputDir $Folder
    
    if (Test-Path $SourcePath) {
        Write-Host "  Copying $Folder..."
        Copy-Item -Path $SourcePath -Destination $DestPath -Recurse -Force
        
        # Remove Python cache directories
        Get-ChildItem -Path $DestPath -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
        
        # Count files
        $FileCount = (Get-ChildItem -Path $DestPath -Recurse -File).Count
        $TotalFiles += $FileCount
        Write-Host "    Files: $FileCount"
    } else {
        Write-Host "  Warning: $Folder not found" -ForegroundColor Yellow
    }
}

Write-Host "`nTotal files copied: $TotalFiles"

# Calculate size before zipping
$SizeKB = (Get-ChildItem -Path $OutputDir -Recurse -File | Measure-Object -Property Length -Sum).Sum / 1KB
Write-Host "Total code size: $([math]::Round($SizeKB, 2)) KB"

# List key files
Write-Host "`nKey files included:"
Write-Host "  - config/config.json"
Write-Host "  - config/settings.py"
Write-Host "  - lib/connections.py"
Write-Host "  - lib/query_builder.py"
Write-Host "  - lib/conflict_processor.py"
Write-Host "  - lib/utils.py"
Write-Host "  - sql/sf_task02_conflict_detection.sql"
Write-Host "  - sql/fetch_*.sql (4 files)"
Write-Host "  - scripts/lambda_handler.py"

# Create ZIP file
Write-Host "`nCreating ZIP archive..."
$ZipPath = Join-Path $ScriptDir $ZipName

if (Test-Path $ZipPath) {
    Remove-Item $ZipPath -Force
}

Add-Type -Assembly System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory($OutputDir, $ZipPath)

$ZipSizeKB = (Get-Item $ZipPath).Length / 1KB
Write-Host "  ZIP created: $ZipPath"
Write-Host "  Compressed size: $([math]::Round($ZipSizeKB, 2)) KB"

# Size comparison
$WithDeps = 50 * 1024  # ~50MB with dependencies
$Savings = $WithDeps - $ZipSizeKB
$SavingsPercent = ($Savings / $WithDeps) * 100

Write-Host "`n" + ("-" * 70)
Write-Host "Size Comparison:" -ForegroundColor Cyan
Write-Host "  Without layers: ~50 MB"
Write-Host "  With layers:    $([math]::Round($ZipSizeKB, 2)) KB"
Write-Host "  Savings:        $([math]::Round($Savings, 0)) KB ($([math]::Round($SavingsPercent, 1))%)"
Write-Host ("-" * 70)

# Summary
Write-Host "`n" + ("=" * 70)
Write-Host "Code package built successfully!" -ForegroundColor Green
Write-Host ("=" * 70)
Write-Host "`nPackage: $ZipPath"
Write-Host "Size: $([math]::Round($ZipSizeKB, 2)) KB (compressed)"
Write-Host "`nThis package contains ONLY application code."
Write-Host "Dependencies must be provided via Lambda layers:"
Write-Host "  - Layer 1: psycopg2 (your existing layer)"
Write-Host "  - Layer 2: snowflake-connector (build with build-snowflake-layer.ps1)"
Write-Host "`nDeploy with AWS CLI:"
Write-Host "  aws lambda update-function-code \"
Write-Host "    --function-name task02-conflict-updater \"
Write-Host "    --zip-file fileb://$ZipName"
Write-Host "`nOr use the deploy-to-lambda.ps1 script:"
Write-Host "  .\deploy-to-lambda.ps1 -FunctionName task02-conflict-updater \"
Write-Host "    -PsycopgLayerArn <your-arn> -SnowflakeLayerArn <your-arn> -UpdateCode"
Write-Host

# Cleanup package directory automatically
Write-Host "`nCleaning up temporary package directory..."
Remove-Item -Recurse -Force $OutputDir
Write-Host "Package directory cleaned up (ZIP file preserved)" -ForegroundColor Green

Write-Host "`nDone!" -ForegroundColor Green
