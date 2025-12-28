#!/usr/bin/env pwsh
<#
.SYNOPSIS
    LOCAL TEST VERSION - Robust Append Import with Schema Validation
#>

#region ============= LOCAL TEST CONFIGURATION =============
$PSQL_PATH = "C:\Program Files\PostgreSQL\16\bin" 
$SEVEN_ZIP_PATH = "C:\Program Files\7-Zip\7z.exe"

# Database Connection Settings
$DB_HOST = "pgsbconflictmanagement.hhaexchange.local"
$DB_PORT = 5432
$DB_NAME = "conflict_management"
$DB_USER = "cm_user"
$DB_PASSWORD = 'uGStSD2hgf&l16$1Bh' # Use single quotes if your password has special characters

$SCHEMA_NAME = "analytics_dev2"
$TABLE_NAME = "factvisitcallperformance_deleted_cr"

$INPUT_FOLDER = Join-Path $PSScriptRoot "OneDrive_4_12-28-2025"
$CSV_OUTPUT_FOLDER = Join-Path $PSScriptRoot "CSV"
$LOG_FILE = Join-Path $PSScriptRoot "test-import-log.txt"
#endregion ========================================================

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    $color = switch ($Level) { "ERROR" {"Red"} "SUCCESS" {"Green"} "WARNING" {"Yellow"} default {"White"} }
    Write-Host $logMessage -ForegroundColor $color
    Add-Content -Path $LOG_FILE -Value $logMessage
}

function Test-TableSchema {
    param([string]$CsvHeaderLine)
    
    Write-Log "Verifying schema for table: $SCHEMA_NAME.$TABLE_NAME..."
    
    # 1. Get Column Names from Database
    $sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = '$SCHEMA_NAME' AND table_name = '$TABLE_NAME' ORDER BY ordinal_position;"
    $dbColumns = psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c $sql
    $dbColumnList = $dbColumns | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
    
    # 2. Get Column Names from CSV Header
    $csvColumnList = $CsvHeaderLine.Replace('"', '').Split(',').ForEach({$_.Trim()})

    # 3. Check for Mismatch
    if ($dbColumnList.Count -ne $csvColumnList.Count) {
        Write-Log "--------------------------------------------------------" "ERROR"
        Write-Log "CRITICAL SCHEMA MISMATCH DETECTED" "ERROR"
        Write-Log "  DB Column Count: $($dbColumnList.Count)" "ERROR"
        Write-Log "  CSV Column Count: $($csvColumnList.Count)" "ERROR"
        Write-Log "--------------------------------------------------------"
        Write-Log "  SIDE-BY-SIDE COMPARISON:"
        
        $max = [Math]::Max($dbColumnList.Count, $csvColumnList.Count)
        for ($i = 0; $i -lt $max; $i++) {
            $dbCol = if ($i -lt $dbColumnList.Count) { $dbColumnList[$i] } else { "[MISSING]" }
            $csvCol = if ($i -lt $csvColumnList.Count) { $csvColumnList[$i] } else { "[MISSING]" }
            
            $normDb = $dbCol.Replace("_", "").ToLower()
            $normCsv = $csvCol.Replace(" ", "").ToLower()
            
            $status = if ($normDb -eq $normCsv) { "[OK]" } else { "[!! MISMATCH !!]" }
            $level = if ($normDb -eq $normCsv) { "INFO" } else { "ERROR" }
            
            Write-Log ("  {0,-3} | DB: {1,-30} | CSV: {2,-30} | {3}" -f ($i+1), $dbCol, $csvCol, $status) $level
        }
        Write-Log "--------------------------------------------------------"
        Write-Log "ABORTING: Program stopped due to schema mismatch on first occurrence." "ERROR"
        
        # Clean up password and exit the entire script
        Remove-Item env:PGPASSWORD -ErrorAction SilentlyContinue
        exit 1
    }
    
    Write-Log "  Schema check passed ($($dbColumnList.Count) columns)." "SUCCESS"
}

function Main {
    Write-Log "========================================================"
    Write-Log "   LOCAL TEST - Robust Append Import with Schema Check"
    Write-Log "========================================================"

    if (-not (Test-Path $INPUT_FOLDER)) { Write-Log "ERROR: Input folder not found" "ERROR"; exit 1 }
    if (-not (Test-Path $CSV_OUTPUT_FOLDER)) { New-Item -ItemType Directory -Path $CSV_OUTPUT_FOLDER | Out-Null }
    if ($PSQL_PATH -and (Test-Path $PSQL_PATH)) { $env:PATH = "$PSQL_PATH;$env:PATH" }

    $compressedFiles = Get-ChildItem -Path $INPUT_FOLDER -Filter "*.gz"
    if ($compressedFiles.Count -eq 0) { Write-Log "No .gz files found." "WARNING"; exit 0 }

    $env:PGPASSWORD = $DB_PASSWORD

    foreach ($gzFile in $compressedFiles) {
        Write-Log "--------------------------------------------------------"
        Write-Log "Processing: $($gzFile.Name)"
        
        try {
            # Capture folder state for decompression
            $filesBefore = Get-ChildItem $CSV_OUTPUT_FOLDER | Select-Object -ExpandProperty FullName

            Write-Log "  Decompressing..."
            & $SEVEN_ZIP_PATH e "$($gzFile.FullName)" -o"$CSV_OUTPUT_FOLDER" -y | Out-Null
            
            # Identify extracted file
            $tempCsvPath = $null
            $retryCount = 0
            while ($null -eq $tempCsvPath -and $retryCount -lt 5) {
                if ($retryCount -gt 0) { Start-Sleep -Seconds 1 }
                $filesAfter = Get-ChildItem $CSV_OUTPUT_FOLDER | Select-Object -ExpandProperty FullName
                $tempCsvPath = $filesAfter | Where-Object { $_ -notin $filesBefore } | Select-Object -First 1
                $retryCount++
            }

            if (-not $tempCsvPath) { throw "Decompression failed." }

            # --- NEW: SCHEMA CHECK ---
            $headerLine = Get-Content $tempCsvPath -First 1
            Test-TableSchema -CsvHeaderLine $headerLine
            # -------------------------

            # Preprocessing NULLs
            $processedCsv = Join-Path $CSV_OUTPUT_FOLDER "current_processing.csv"
            Write-Log "  Cleaning NULL markers..."
            $content = Get-Content $tempCsvPath
            $content = $content -replace '"\\N"', ''
            $content = $content -replace ',\x5CN,', ',,' 
            $content | Set-Content $processedCsv

            # Import
            Write-Log "  Executing \copy..."
            $countRaw = psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT count(*) FROM $SCHEMA_NAME.$TABLE_NAME;"
            $rowsBefore = [int]([string[]]$countRaw | Where-Object { $_ -match '\d' } | Select-Object -First 1).Trim()

            $psqlScript = @"
\set ON_ERROR_STOP on
SET maintenance_work_mem = '1GB';
SET synchronous_commit = OFF;
\copy $SCHEMA_NAME.$TABLE_NAME FROM '$($processedCsv.Replace('\','/'))' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
"@
            $result = $psqlScript | psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -v ON_ERROR_STOP=1 2>&1

            if ($LASTEXITCODE -eq 0) {
                $countRawAfter = psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT count(*) FROM $SCHEMA_NAME.$TABLE_NAME;"
                $rowsAfter = [int]([string[]]$countRawAfter | Where-Object { $_ -match '\d' } | Select-Object -First 1).Trim()
                Write-Log "  SUCCESS: Added $($rowsAfter - $rowsBefore) rows." "SUCCESS"
            } else {
                Write-Log "  FAILED: $result" "ERROR"
            }
        }
        catch {
            Write-Log "  CRITICAL ERROR: $_" "ERROR"
        }
        finally {
            Get-ChildItem $CSV_OUTPUT_FOLDER | Remove-Item -Force -ErrorAction SilentlyContinue
        }
    }

    Write-Log "--------------------------------------------------------"
    Write-Log "Finalizing: Reindexing..."
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "REINDEX TABLE $SCHEMA_NAME.$TABLE_NAME;" | Out-Null
    Remove-Item env:PGPASSWORD -ErrorAction SilentlyContinue
    Write-Log "Import Complete." "SUCCESS"
}

Main