#!/usr/bin/env pwsh
<#
.SYNOPSIS
    LOCAL TEST VERSION - Import CSV files into local PostgreSQL

.DESCRIPTION
    Simplified version for local testing with small datasets
#>

#region ============= LOCAL TEST CONFIGURATION =============

# PostgreSQL Installation Path (if psql is not in PATH)
# Set this to your PostgreSQL bin folder, or leave empty if psql is in PATH
$PSQL_PATH = "C:\Program Files\PostgreSQL\18\bin"  # Adjust version number as needed
# Examples:
# $PSQL_PATH = "C:\Program Files\PostgreSQL\16\bin"
# $PSQL_PATH = "C:\Program Files\PostgreSQL\15\bin"
# $PSQL_PATH = ""  # Leave empty if psql is in PATH

# Database Connection Settings - LOCAL POSTGRES
$DB_HOST = "localhost"
$DB_PORT = 5432
$DB_NAME = "postgres"           # Change to your local database name
$DB_USER = "postgres"          # Change to your local username
$DB_PASSWORD = "admin"      # Change to your local password

# Target Table Settings
$SCHEMA_NAME = "analytics_dev2"        # Usually 'public' for local testing
$TABLE_NAME = "factvisitcallperformance_cr" # Test table name

# CSV File Settings
$CSV_PATH = "test_sample.csv"  # The small test file we'll create

# Import Options
$CSV_DELIMITER = ","
$CSV_HAS_HEADER = $true
$CSV_NULL_STRING = ""  # Empty string for NULL
$CSV_ENCODING = "UTF8"

# Table Options - For testing, we'll keep it simple
$TRUNCATE_TABLE = $true        # Clean table before each test
$DISABLE_INDEXES = $false      # Keep simple for testing
$DISABLE_TRIGGERS = $false
$DISABLE_PRIMARY_KEY = $false
$DISABLE_FOREIGN_KEYS = $false
$DISABLE_AUTOVACUUM = $false
$DISABLE_SYNCHRONOUS_COMMIT = $false
$WORK_MEM_MB = 64
$MAINTENANCE_WORK_MEM_MB = 128

# Logging
$LOG_FILE = "test-import-log.txt"

#endregion ========================================================

#region ============= SCRIPT CODE =============

# Simple logging
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    
    $color = switch ($Level) {
        "ERROR" { "Red" }
        "SUCCESS" { "Green" }
        "WARNING" { "Yellow" }
        default { "White" }
    }
    
    Write-Host $logMessage -ForegroundColor $color
    Add-Content -Path $LOG_FILE -Value $logMessage
}

# Check if psql is available
function Test-PsqlAvailable {
    # If PSQL_PATH is specified, add it to PATH for this session
    if ($PSQL_PATH -and (Test-Path $PSQL_PATH)) {
        $env:PATH = "$PSQL_PATH;$env:PATH"
        Write-Log "  Added PostgreSQL bin to PATH: $PSQL_PATH" "DEBUG"
    }
    
    try {
        $null = Get-Command psql -ErrorAction Stop
        return $true
    }
    catch {
        return $false
    }
}

# Helper function to call psql
function Invoke-Psql {
    param([string[]]$Arguments)
    
    # If PSQL_PATH is specified, use full path
    if ($PSQL_PATH -and (Test-Path $PSQL_PATH)) {
        $psqlExe = Join-Path $PSQL_PATH "psql.exe"
        & $psqlExe @Arguments
    }
    else {
        & psql @Arguments
    }
}

# Main function
function Main {
    Write-Log ""
    Write-Log "========================================================"
    Write-Log "   LOCAL TEST - PostgreSQL CSV Import"
    Write-Log "========================================================"
    Write-Log ""
    Write-Log "Configuration:"
    Write-Log "  Database: $DB_NAME @ $DB_HOST"
    Write-Log "  Table: $SCHEMA_NAME.$TABLE_NAME"
    Write-Log "  CSV File: $CSV_PATH"
    Write-Log ""
    
    # Check if psql is available
    if (-not (Test-PsqlAvailable)) {
        Write-Log "ERROR: psql command not found!" "ERROR"
        Write-Log "Please install PostgreSQL client tools" "ERROR"
        exit 1
    }
    Write-Log "[OK] psql found" "SUCCESS"
    
    # Check if CSV file exists
    if (-not (Test-Path $CSV_PATH)) {
        Write-Log "ERROR: CSV file not found: $CSV_PATH" "ERROR"
        Write-Log "Please run create-test-csv.ps1 first to create the test file" "ERROR"
        exit 1
    }
    Write-Log "[OK] CSV file found" "SUCCESS"
    
    $fileSize = (Get-Item $CSV_PATH).Length
    Write-Log "  File size: $([math]::Round($fileSize / 1KB, 2)) KB"
    Write-Log ""
    
    # Count rows in CSV (excluding header)
    $lineCount = (Get-Content $CSV_PATH | Measure-Object -Line).Lines - 1
    Write-Log "  Rows to import: $lineCount"
    Write-Log ""
    
    # Set password
    if ($DB_PASSWORD) {
        $env:PGPASSWORD = $DB_PASSWORD
    }
    
    # Test connection
    Write-Log "Testing database connection..."
    $testArgs = @("-h", $DB_HOST, "-p", $DB_PORT, "-U", $DB_USER, "-d", $DB_NAME, "-c", "SELECT version();")
    $testResult = Invoke-Psql -Arguments $testArgs 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR: Cannot connect to database!" "ERROR"
        Write-Log "Error: $testResult" "ERROR"
        Write-Log "" 
        Write-Log "Please check:" "WARNING"
        Write-Log "  1. PostgreSQL is running on $DB_HOST" "WARNING"
        Write-Log "  2. Database '$DB_NAME' exists" "WARNING"
        Write-Log "  3. Username/password are correct" "WARNING"
        Write-Log "  4. User has access to the database" "WARNING"
        exit 1
    }
    Write-Log "[OK] Database connection successful" "SUCCESS"
    Write-Log ""
    
    # Truncate table if requested
    if ($TRUNCATE_TABLE) {
        Write-Log "Truncating table $SCHEMA_NAME.$TABLE_NAME..."
        $truncateCmd = "TRUNCATE TABLE $SCHEMA_NAME.$TABLE_NAME;"
        $truncateArgs = @("-h", $DB_HOST, "-p", $DB_PORT, "-U", $DB_USER, "-d", $DB_NAME, "-c", $truncateCmd)
        $result = Invoke-Psql -Arguments $truncateArgs 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Log "[OK] Table truncated" "SUCCESS"
        }
        else {
            Write-Log "WARNING: Could not truncate table (it might not exist yet)" "WARNING"
            Write-Log "  This is OK if this is the first import" "WARNING"
        }
        Write-Log ""
    }
    
    # Preprocess CSV to handle \N null values
    Write-Log "Preprocessing CSV to handle NULL values..."
    $processedCsvPath = [System.IO.Path]::GetTempFileName() -replace '\.tmp$', '.csv'
    
    try {
        # Read CSV line by line and replace \N with empty (no quotes)
        $reader = New-Object System.IO.StreamReader($CSV_PATH)
        $writer = New-Object System.IO.StreamWriter($processedCsvPath)
        
        $lineCount = 0
        while ($null -ne ($line = $reader.ReadLine())) {
            # Replace the pattern ","\N"," with ",," (removes the field entirely)
            # Also handle "\N" at start: "...,"\N"," becomes "...,,""
            $cleanedLine = $line.Replace(',"\N",', ',,')
            # Handle "\N" at end of line: ...,"\N" becomes ...,
            $cleanedLine = $cleanedLine.Replace(',"\N"', ',')
            # Handle "\N" at start: "\N",... becomes ,... (edge case)
            if ($cleanedLine.StartsWith('"\N",')) {
                $cleanedLine = ',' + $cleanedLine.Substring(5)
            }
            $writer.WriteLine($cleanedLine)
            $lineCount++
        }
        
        $reader.Close()
        $writer.Close()
        
        Write-Log "  Processed $lineCount lines, replaced NULL markers"
        $csvToImport = $processedCsvPath
    }
    catch {
        Write-Log "  Warning: Could not preprocess CSV: $_" "WARNING"
        $csvToImport = $CSV_PATH
        
        # Clean up if failed
        if ($reader) { $reader.Close() }
        if ($writer) { $writer.Close() }
    }
    
    # Build \copy command
    Write-Log "Starting import..."
    $escapedFilePath = (Resolve-Path $csvToImport).Path -replace "'", "''"
    $copyCommand = "\copy $SCHEMA_NAME.$TABLE_NAME FROM '$escapedFilePath' WITH (FORMAT csv"
    
    if ($CSV_HAS_HEADER) {
        $copyCommand += ", HEADER true"
    }
    
    if ($CSV_DELIMITER -ne ",") {
        $copyCommand += ", DELIMITER '$CSV_DELIMITER'"
    }
    
    if ($CSV_NULL_STRING) {
        $copyCommand += ", NULL '$CSV_NULL_STRING'"
    }
    
    $copyCommand += ", ENCODING '$CSV_ENCODING')"
    
    Write-Log "  Command: $copyCommand" "DEBUG"
    Write-Log ""
    
    # Execute import
    $psqlArgs = @(
        "-h", $DB_HOST,
        "-p", $DB_PORT,
        "-U", $DB_USER,
        "-d", $DB_NAME,
        "-c", $copyCommand
    )
    
    $startTime = Get-Date
    $result = Invoke-Psql -Arguments $psqlArgs 2>&1
    $duration = (Get-Date) - $startTime
    
    if ($LASTEXITCODE -eq 0) {
        $rowCount = "unknown"
        if ($result -match "COPY (\d+)") {
            $rowCount = $matches[1]
        }
        
        Write-Log ""
        Write-Log "========================================================"
        Write-Log "   SUCCESS!" "SUCCESS"
        Write-Log "========================================================"
        Write-Log "Rows imported: $rowCount" "SUCCESS"
        Write-Log "Duration: $([math]::Round($duration.TotalSeconds, 2)) seconds" "SUCCESS"
        Write-Log ""
        
        # Verify the import
        Write-Log "Verifying import..."
        $countCmd = "SELECT COUNT(*) FROM $SCHEMA_NAME.$TABLE_NAME;"
        $countArgs = @("-h", $DB_HOST, "-p", $DB_PORT, "-U", $DB_USER, "-d", $DB_NAME, "-t", "-c", $countCmd)
        $countResult = Invoke-Psql -Arguments $countArgs 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            $actualCount = $countResult.Trim()
            Write-Log "Table row count: $actualCount" "SUCCESS"
            
            # Show sample data
            Write-Log ""
            Write-Log "Sample data (first 3 rows):"
            $sampleCmd = "SELECT * FROM $SCHEMA_NAME.$TABLE_NAME LIMIT 3;"
            $sampleArgs = @("-h", $DB_HOST, "-p", $DB_PORT, "-U", $DB_USER, "-d", $DB_NAME, "-c", $sampleCmd)
            $sampleResult = Invoke-Psql -Arguments $sampleArgs 2>&1
            $sampleResult | ForEach-Object { Write-Host $_ }
        }
        
        Write-Log ""
        Write-Log "Test import completed successfully!" "SUCCESS"
        Write-Log "Log file: $LOG_FILE"
        
        # Cleanup preprocessed CSV
        if ($processedCsvPath -and (Test-Path $processedCsvPath)) {
            Remove-Item $processedCsvPath -Force -ErrorAction SilentlyContinue
        }
        
        exit 0
    }
    else {
        Write-Log ""
        Write-Log "========================================================"
        Write-Log "   FAILED!" "ERROR"
        Write-Log "========================================================"
        Write-Log "Error: $result" "ERROR"
        Write-Log ""
        
        # Cleanup preprocessed CSV
        if ($processedCsvPath -and (Test-Path $processedCsvPath)) {
            Remove-Item $processedCsvPath -Force -ErrorAction SilentlyContinue
        }
        
        exit 1
    }
}

# Clear password on exit
try {
    Main
}
finally {
    if ($DB_PASSWORD) {
        Remove-Item env:PGPASSWORD -ErrorAction SilentlyContinue
    }
}

#endregion ========================================================

