#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Quick Start - Run all local test steps

.DESCRIPTION
    This script automates the entire local testing process
#>

param(
    [string]$GzipFile,
    [int]$NumRows = 20
)

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  PostgreSQL Local Test - Quick Start" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if .gz file was provided
if (-not $GzipFile) {
    Write-Host "ERROR: Please provide the path to your .gz file" -ForegroundColor Red
    Write-Host ""
    Write-Host "Usage:" -ForegroundColor Yellow
    Write-Host "  .\quick-start.ps1 -GzipFile 'path\to\file.csv.gz'" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Example:" -ForegroundColor Cyan
    Write-Host "  .\quick-start.ps1 -GzipFile 'C:\data\factvisitcallperformance_cr.csv.gz_0_0_0.csv.gz'" -ForegroundColor Cyan
    Write-Host ""
    exit 1
}

if (-not (Test-Path $GzipFile)) {
    Write-Host "ERROR: File not found: $GzipFile" -ForegroundColor Red
    exit 1
}

Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Source file: $GzipFile"
Write-Host "  Sample rows: $NumRows"
Write-Host ""

# Step 1: Create test CSV
Write-Host "Step 1: Creating test CSV..." -ForegroundColor Cyan
Write-Host "----------------------------------------"
.\create-test-csv.ps1 -GzipFile $GzipFile -NumRows $NumRows

# Check if file was created (more reliable than exit code)
if (-not (Test-Path "test_sample.csv")) {
    Write-Host ""
    Write-Host "Failed at Step 1 - test_sample.csv was not created" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Step 2: Analyzing CSV structure..." -ForegroundColor Cyan
Write-Host "----------------------------------------"

if (Test-Path "test_sample.csv") {
    $header = Get-Content "test_sample.csv" -First 1
    $columns = $header -split ','
    
    Write-Host "Found $($columns.Count) columns:" -ForegroundColor Green
    Write-Host ""
    
    for ($i = 0; $i -lt [Math]::Min($columns.Count, 20); $i++) {
        Write-Host "  $($i+1). $($columns[$i])" -ForegroundColor White
    }
    
    if ($columns.Count -gt 20) {
        Write-Host "  ... and $($columns.Count - 20) more columns" -ForegroundColor Gray
    }
    
    Write-Host ""
    Write-Host "Sample data (first row):" -ForegroundColor Yellow
    $firstDataRow = Get-Content "test_sample.csv" -Skip 1 -First 1
    Write-Host $firstDataRow -ForegroundColor Gray
    Write-Host ""
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Test CSV Created Successfully!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Review the columns above" -ForegroundColor Cyan
Write-Host ""
Write-Host "2. Edit create-test-table.ps1 to match these columns:" -ForegroundColor Cyan
Write-Host "   - Open: create-test-table.ps1" -ForegroundColor White
Write-Host "   - Find: CREATE TABLE section (line ~44)" -ForegroundColor White
Write-Host "   - Add: Your actual column definitions" -ForegroundColor White
Write-Host ""
Write-Host "3. Set your local database credentials in test-import-local.ps1:" -ForegroundColor Cyan
Write-Host "   - `$DB_NAME = 'your_database'" -ForegroundColor White
Write-Host "   - `$DB_USER = 'your_username'" -ForegroundColor White
Write-Host "   - `$DB_PASSWORD = 'your_password'" -ForegroundColor White
Write-Host ""
Write-Host "4. Run the table creation script:" -ForegroundColor Cyan
Write-Host "   .\create-test-table.ps1" -ForegroundColor White
Write-Host ""
Write-Host "5. Run the import test:" -ForegroundColor Cyan
Write-Host "   .\test-import-local.ps1" -ForegroundColor White
Write-Host ""
Write-Host "Or see LOCAL-TESTING-README.md for detailed instructions" -ForegroundColor Gray
Write-Host ""

