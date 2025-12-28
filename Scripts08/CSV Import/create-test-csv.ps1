#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Extract a small test dataset from a gzipped CSV file

.DESCRIPTION
    This script decompresses a .gz file and creates a small CSV with only N rows
    for testing purposes.
#>

param(
    [string]$GzipFile = "factvisitcallperformance_cr.csv.gz_0_0_0.csv.gz",
    [int]$NumRows = 20,
    [string]$OutputFile = "test_sample.csv"
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  CSV Test Data Extractor" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if input file exists
if (-not (Test-Path $GzipFile)) {
    Write-Host "ERROR: File not found: $GzipFile" -ForegroundColor Red
    Write-Host "Please specify the correct path to your .gz file" -ForegroundColor Yellow
    exit 1
}

$fileInfo = Get-Item $GzipFile
Write-Host "Input file: $($fileInfo.Name)" -ForegroundColor Green
Write-Host "File size: $([math]::Round($fileInfo.Length / 1MB, 2)) MB (compressed)" -ForegroundColor Green
Write-Host ""

# Decompress to temp file
Write-Host "Step 1: Decompressing file..." -ForegroundColor Cyan
$tempCsv = [System.IO.Path]::GetTempFileName() -replace '\.tmp$', '.csv'

try {
    $inputStream = New-Object System.IO.FileStream(
        $GzipFile, 
        [System.IO.FileMode]::Open, 
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::Read
    )
    $gzipStream = New-Object System.IO.Compression.GzipStream($inputStream, [System.IO.Compression.CompressionMode]::Decompress)
    $outputStream = New-Object System.IO.FileStream($tempCsv, [System.IO.FileMode]::Create)
    
    $gzipStream.CopyTo($outputStream)
    
    $outputStream.Close()
    $gzipStream.Close()
    $inputStream.Close()
    
    $decompressedSize = (Get-Item $tempCsv).Length
    Write-Host "Decompressed size: $([math]::Round($decompressedSize / 1MB, 2)) MB" -ForegroundColor Green
    Write-Host ""
    
    # Extract header + N rows
    Write-Host "Step 2: Extracting first $NumRows rows..." -ForegroundColor Cyan
    
    $reader = New-Object System.IO.StreamReader($tempCsv)
    $writer = New-Object System.IO.StreamWriter($OutputFile)
    
    try {
        $lineCount = 0
        $header = $reader.ReadLine()
        
        if ($null -eq $header) {
            throw "File appears to be empty"
        }
        
        # Write header
        $writer.WriteLine($header)
        Write-Host "Header columns: $($header.Split(',').Count)" -ForegroundColor Green
        
        # Write N data rows
        while (($line = $reader.ReadLine()) -and ($lineCount -lt $NumRows)) {
            $writer.WriteLine($line)
            $lineCount++
        }
        
        Write-Host "Extracted rows: $lineCount" -ForegroundColor Green
    }
    finally {
        $reader.Close()
        $writer.Close()
    }
    
    Write-Host ""
    Write-Host "Step 3: Cleaning up..." -ForegroundColor Cyan
    Remove-Item $tempCsv -Force -ErrorAction SilentlyContinue
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  SUCCESS!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Test file created: $OutputFile" -ForegroundColor Yellow
    Write-Host "File size: $([math]::Round((Get-Item $OutputFile).Length / 1KB, 2)) KB" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "You can now use this file for testing!" -ForegroundColor Cyan
    Write-Host ""
}
catch {
    Write-Host ""
    Write-Host "ERROR: $_" -ForegroundColor Red
    
    # Cleanup on error
    if (Test-Path $tempCsv) {
        Remove-Item $tempCsv -Force -ErrorAction SilentlyContinue
    }
    exit 1
}

