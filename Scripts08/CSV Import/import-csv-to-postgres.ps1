#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Import multiple .csv.gz files into a PostgreSQL table using psql.

.DESCRIPTION
    This standalone script decompresses and imports multiple .csv.gz files into 
    a specified PostgreSQL table. Just configure the settings below and run!

.NOTES
    Requirements: 
    - PostgreSQL client tools (psql) must be installed and in PATH
    - PowerShell 5.1 or higher
#>

#region ============= CONFIGURATION - EDIT THESE VALUES =============

# Database Connection Settings
$DB_HOST = "pgsbconflictmanagement.hhaexchange.local"
$DB_PORT = 5432
$DB_NAME = "conflict_management"
$DB_USER = "cm_user"
$DB_PASSWORD = "uGStSD2hgf&l16$1Bh"

# Target Table Settings
$SCHEMA_NAME = "analytics_dev2"
$TABLE_NAME = "FACTVISITCALLPERFORMANCE_CR"

# CSV File Settings
$CSV_PATH = "D:\agupta.c\downloads\OneDrive_1_12-23-2025"

# Import Options
$CSV_DELIMITER = ","
$CSV_HAS_HEADER = $true
$CSV_NULL_STRING = ""
$CSV_ENCODING = "UTF8"

# Table Options
$TRUNCATE_TABLE = $false

# Performance Options
# Disable indexes during import for faster loading (they'll be recreated after)
$DISABLE_INDEXES = $true
# Disable triggers during import
$DISABLE_TRIGGERS = $true
# Disable primary key constraint during import (RISKY - only if you trust source data!)
# This can provide 3-5x speed improvement but if data has duplicates, PK won't be recreatable
$DISABLE_PRIMARY_KEY = $true
# Disable foreign key constraints during import (RISKY - only if you trust referential integrity!)
$DISABLE_FOREIGN_KEYS = $true
# Disable autovacuum during import (prevents background vacuum from interfering)
# Provides 10-20% speed improvement
$DISABLE_AUTOVACUUM = $true

# PostgreSQL Session-Level Performance Settings (No admin access required)
# Disable synchronous commit for massive speed improvement (2-3x faster)
# Slightly increases risk of data loss if server crashes during import, but data will be consistent
$DISABLE_SYNCHRONOUS_COMMIT = $true
# Increase work memory for sorting/hashing operations (in MB)
# Higher values speed up index creation and sorting
$WORK_MEM_MB = 256
# Increase maintenance work memory for index creation (in MB)
$MAINTENANCE_WORK_MEM_MB = 512

# Logging
$LOG_FILE = "import-log.txt"

# Temporary Files Directory
# If specified, decompressed files will be stored here instead of the system temp folder
# Example: "D:\temp" (must exist and have sufficient space)
$TEMP_DIRECTORY = "D:\agupta.c\downloads\temp"

#endregion ========================================================

#region ============= SCRIPT CODE - DO NOT EDIT BELOW =============

# Function to write log messages
function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    
    $color = switch ($Level) {
        "ERROR" { "Red" }
        "SUCCESS" { "Green" }
        "WARNING" { "Yellow" }
        "PROGRESS" { "Cyan" }
        default { "White" }
    }
    
    Write-Host $logMessage -ForegroundColor $color
    Add-Content -Path $LOG_FILE -Value $logMessage
}

# Function to write progress updates without logging to file (to avoid clutter)
function Write-Progress-Status {
    param(
        [string]$Message
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [PROGRESS] $Message"
    Write-Host $logMessage -ForegroundColor Cyan
}

# Function to check if psql is available
function Test-PsqlAvailable {
    try {
        $null = Get-Command psql -ErrorAction Stop
        return $true
    }
    catch {
        return $false
    }
}

# Function to execute a PostgreSQL command
function Invoke-PsqlCommand {
    param(
        [string]$Command,
        [switch]$SuppressErrors
    )
    
    $psqlArgs = @(
        "-h", $DB_HOST,
        "-p", $DB_PORT,
        "-U", $DB_USER,
        "-d", $DB_NAME,
        "-c", $Command
    )
    
    # Set password environment variable
    if ($DB_PASSWORD) {
        $env:PGPASSWORD = $DB_PASSWORD
    }
    
    try {
        # Use PowerShell's call operator for cleaner execution
        $result = & psql @psqlArgs 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            return @{
                Success = $true
                Output = $result
            }
        }
        else {
            if (-not $SuppressErrors) {
                Write-Log "  Command failed: $Command" "ERROR"
                Write-Log "  Error: $result" "ERROR"
            }
            return @{
                Success = $false
                Output = $result
            }
        }
    }
    catch {
        if (-not $SuppressErrors) {
            Write-Log "  Exception executing command: $_" "ERROR"
        }
        return @{
            Success = $false
            Output = $_.Exception.Message
        }
    }
}

# Function to disable indexes and constraints
function Disable-TableOptimizations {
    param(
        [string]$SchemaName,
        [string]$TableName
    )
    
    $fullTableName = "$SchemaName.$TableName"
    Write-Log "  Disabling optimizations for faster import..."
    
    $script:savedIndexes = @()
    $script:savedPrimaryKey = $null
    $script:savedForeignKeys = @()
    $script:triggersDisabled = $false
    $script:autovacuumDisabled = $false
    
    # Disable primary key constraint if requested
    if ($DISABLE_PRIMARY_KEY) {
        Write-Log "  Retrieving primary key definition..."
        $pkQuery = @"
SELECT c.conname, pg_get_constraintdef(c.oid) as definition
FROM pg_constraint c
JOIN pg_namespace n ON n.oid = c.connamespace
JOIN pg_class t ON t.oid = c.conrelid
WHERE n.nspname = '$SchemaName'
  AND t.relname = '$TableName'
  AND c.contype = 'p';
"@
        
        $result = Invoke-PsqlCommand -Command $pkQuery
        if ($result.Success) {
            $lines = $result.Output | Where-Object { $_ -match '\S+' }
            foreach ($line in $lines) {
                # Parse: constraint_name | PRIMARY KEY (column_name)
                if ($line -match '^\s*(\S+)\s+\|\s+(.+)$') {
                    $script:savedPrimaryKey = @{
                        Name = $matches[1].Trim()
                        Definition = $matches[2].Trim()
                    }
                    break
                }
            }
            
            if ($script:savedPrimaryKey) {
                Write-Log "  Found primary key: $($script:savedPrimaryKey.Name)"
                Write-Log "  Dropping primary key constraint..."
                $dropCmd = "ALTER TABLE $fullTableName DROP CONSTRAINT IF EXISTS $($script:savedPrimaryKey.Name);"
                $result = Invoke-PsqlCommand -Command $dropCmd
                if ($result.Success) {
                    Write-Log "  Primary key constraint dropped" "SUCCESS"
                }
                else {
                    Write-Log "  Warning: Failed to drop primary key" "WARNING"
                    $script:savedPrimaryKey = $null
                }
            }
            else {
                Write-Log "  No primary key found on table" "WARNING"
            }
        }
    }
    
    # Disable foreign key constraints if requested
    if ($DISABLE_FOREIGN_KEYS) {
        Write-Log "  Retrieving foreign key definitions..."
        $fkQuery = @"
SELECT c.conname, pg_get_constraintdef(c.oid) as definition
FROM pg_constraint c
JOIN pg_namespace n ON n.oid = c.connamespace
JOIN pg_class t ON t.oid = c.conrelid
WHERE n.nspname = '$SchemaName'
  AND t.relname = '$TableName'
  AND c.contype = 'f';
"@
        
        $result = Invoke-PsqlCommand -Command $fkQuery
        if ($result.Success) {
            $lines = $result.Output | Where-Object { $_ -match '\S+' }
            foreach ($line in $lines) {
                if ($line -match '^\s*(\S+)\s+\|\s+(.+)$') {
                    $script:savedForeignKeys += @{
                        Name = $matches[1].Trim()
                        Definition = $matches[2].Trim()
                    }
                }
            }
            
            Write-Log "  Found $($script:savedForeignKeys.Count) foreign keys to drop"
            
            # Drop each foreign key
            foreach ($fk in $script:savedForeignKeys) {
                Write-Log "  Dropping foreign key: $($fk.Name)"
                $dropCmd = "ALTER TABLE $fullTableName DROP CONSTRAINT IF EXISTS $($fk.Name);"
                $result = Invoke-PsqlCommand -Command $dropCmd
                if (-not $result.Success) {
                    Write-Log "  Warning: Failed to drop foreign key $($fk.Name)" "WARNING"
                }
            }
        }
    }
    
    # Get all indexes (except primary key) for the table
    if ($DISABLE_INDEXES) {
        Write-Log "  Retrieving index definitions..."
        $indexQuery = @"
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE schemaname = '$SchemaName' 
  AND tablename = '$TableName'
  AND indexname NOT LIKE '%_pkey';
"@
        
        $result = Invoke-PsqlCommand -Command $indexQuery
        if ($result.Success) {
            # Parse output to get index definitions
            $lines = $result.Output | Where-Object { $_ -match 'CREATE' }
            foreach ($line in $lines) {
                if ($line -match '^(\S+)\s+\|\s+(.+)$') {
                    $script:savedIndexes += @{
                        Name = $matches[1].Trim()
                        Definition = $matches[2].Trim()
                    }
                }
            }
            
            Write-Log "  Found $($script:savedIndexes.Count) indexes to drop"
            
            # Drop each index
            foreach ($index in $script:savedIndexes) {
                Write-Log "  Dropping index: $($index.Name)"
                $dropCmd = "DROP INDEX IF EXISTS $SchemaName.$($index.Name);"
                $result = Invoke-PsqlCommand -Command $dropCmd
                if (-not $result.Success) {
                    Write-Log "  Warning: Failed to drop index $($index.Name)" "WARNING"
                }
            }
        }
    }
    
    # Disable triggers
    if ($DISABLE_TRIGGERS) {
        Write-Log "  Disabling triggers..."
        $disableTriggersCmd = "ALTER TABLE $fullTableName DISABLE TRIGGER ALL;"
        $result = Invoke-PsqlCommand -Command $disableTriggersCmd
        if ($result.Success) {
            $script:triggersDisabled = $true
            Write-Log "  Triggers disabled"
        }
        else {
            Write-Log "  Warning: Failed to disable triggers" "WARNING"
        }
    }
    
    # Disable autovacuum
    if ($DISABLE_AUTOVACUUM) {
        Write-Log "  Disabling autovacuum..."
        $disableAutovacuumCmd = "ALTER TABLE $fullTableName SET (autovacuum_enabled = false);"
        $result = Invoke-PsqlCommand -Command $disableAutovacuumCmd
        if ($result.Success) {
            $script:autovacuumDisabled = $true
            Write-Log "  Autovacuum disabled"
        }
        else {
            Write-Log "  Warning: Failed to disable autovacuum" "WARNING"
        }
    }
    
    Write-Log "  Optimizations disabled successfully" "SUCCESS"
}

# Function to re-enable indexes and constraints
function Enable-TableOptimizations {
    param(
        [string]$SchemaName,
        [string]$TableName
    )
    
    $fullTableName = "$SchemaName.$TableName"
    Write-Log "  Re-enabling optimizations..."
    
    # Re-enable triggers
    if ($script:triggersDisabled) {
        Write-Log "  Re-enabling triggers..."
        $enableTriggersCmd = "ALTER TABLE $fullTableName ENABLE TRIGGER ALL;"
        $result = Invoke-PsqlCommand -Command $enableTriggersCmd
        if ($result.Success) {
            Write-Log "  Triggers re-enabled"
        }
        else {
            Write-Log "  Warning: Failed to re-enable triggers" "WARNING"
        }
        $script:triggersDisabled = $false
    }
    
    # Re-enable autovacuum
    if ($script:autovacuumDisabled) {
        Write-Log "  Re-enabling autovacuum..."
        $enableAutovacuumCmd = "ALTER TABLE $fullTableName SET (autovacuum_enabled = true);"
        $result = Invoke-PsqlCommand -Command $enableAutovacuumCmd
        if ($result.Success) {
            Write-Log "  Autovacuum re-enabled"
        }
        else {
            Write-Log "  Warning: Failed to re-enable autovacuum" "WARNING"
        }
        $script:autovacuumDisabled = $false
    }
    
    # Recreate primary key
    if ($DISABLE_PRIMARY_KEY -and $script:savedPrimaryKey) {
        Write-Log "  Recreating primary key: $($script:savedPrimaryKey.Name)"
        $addPkCmd = "ALTER TABLE $fullTableName ADD CONSTRAINT $($script:savedPrimaryKey.Name) $($script:savedPrimaryKey.Definition);"
        $result = Invoke-PsqlCommand -Command $addPkCmd
        if ($result.Success) {
            Write-Log "  Primary key constraint recreated successfully" "SUCCESS"
        }
        else {
            Write-Log "  CRITICAL: Failed to recreate primary key!" "ERROR"
            Write-Log "  This likely means there are duplicate values in the primary key column." "ERROR"
            Write-Log "  You will need to investigate and manually fix the data." "ERROR"
        }
        $script:savedPrimaryKey = $null
    }
    
    # Recreate indexes
    if ($DISABLE_INDEXES -and $script:savedIndexes.Count -gt 0) {
        Write-Log "  Recreating $($script:savedIndexes.Count) indexes (this may take 10-20 minutes)..."
        
        # Set maintenance work memory for faster index creation
        if ($MAINTENANCE_WORK_MEM_MB -gt 0) {
            $setMemCmd = "SET maintenance_work_mem = '${MAINTENANCE_WORK_MEM_MB}MB';"
            Invoke-PsqlCommand -Command $setMemCmd -SuppressErrors | Out-Null
        }
        
        $indexNum = 0
        $indexStartTime = Get-Date
        foreach ($index in $script:savedIndexes) {
            $indexNum++
            $indexItemStart = Get-Date
            Write-Progress-Status "  [$indexNum/$($script:savedIndexes.Count)] Creating index: $($index.Name)..."
            $result = Invoke-PsqlCommand -Command $index.Definition
            $indexItemDuration = ((Get-Date) - $indexItemStart).TotalSeconds
            if ($result.Success) {
                Write-Log "  [$indexNum/$($script:savedIndexes.Count)] Index $($index.Name) created in $([math]::Round($indexItemDuration, 1))s" "SUCCESS"
            }
            else {
                Write-Log "  Failed to recreate index $($index.Name)" "ERROR"
            }
        }
        $totalIndexTime = ((Get-Date) - $indexStartTime).TotalSeconds
        Write-Log "  All indexes recreated in $([math]::Round($totalIndexTime, 1)) seconds" "SUCCESS"
        $script:savedIndexes = @()
    }
    
    # Recreate foreign keys
    if ($DISABLE_FOREIGN_KEYS -and $script:savedForeignKeys.Count -gt 0) {
        Write-Log "  Recreating $($script:savedForeignKeys.Count) foreign keys..."
        $fkNum = 0
        foreach ($fk in $script:savedForeignKeys) {
            $fkNum++
            Write-Log "  [$fkNum/$($script:savedForeignKeys.Count)] Creating foreign key: $($fk.Name)"
            $addFkCmd = "ALTER TABLE $fullTableName ADD CONSTRAINT $($fk.Name) $($fk.Definition);"
            $result = Invoke-PsqlCommand -Command $addFkCmd
            if ($result.Success) {
                Write-Log "  Foreign key $($fk.Name) created successfully" "SUCCESS"
            }
            else {
                Write-Log "  CRITICAL: Failed to recreate foreign key $($fk.Name)!" "ERROR"
                Write-Log "  This likely means there are referential integrity violations." "ERROR"
            }
        }
        $script:savedForeignKeys = @()
    }
    
    Write-Log "  Optimizations re-enabled" "SUCCESS"
}

# Function to decompress .gz file and return temp CSV path
function Expand-GzipFile {
    param(
        [string]$GzipPath,
        [string]$TempDirectory = ""
    )
    
    # Determine temp file location
    if ($TempDirectory -and (Test-Path $TempDirectory -PathType Container)) {
        # Use custom temp directory
        $tempFileName = "tmp_" + ([System.IO.Path]::GetRandomFileName() -replace '\.', '') + ".csv"
        $tempCsvPath = Join-Path $TempDirectory $tempFileName
    }
    else {
        # Use system temp directory
        $tempCsvPath = [System.IO.Path]::GetTempFileName()
        $tempCsvPath = $tempCsvPath -replace '\.tmp$', '.csv'
    }
    
    try {
        Write-Log "  Decompressing to temp file..."
        Write-Log "  Temp location: $tempCsvPath" "DEBUG"
        
        # Retry logic for file access (in case file is locked)
        $maxRetries = 3
        $retryCount = 0
        $success = $false
        
        while (-not $success -and $retryCount -lt $maxRetries) {
            try {
                # Try to open the file with shared read access
                $inputStream = New-Object System.IO.FileStream(
                    $GzipPath, 
                    [System.IO.FileMode]::Open, 
                    [System.IO.FileAccess]::Read,
                    [System.IO.FileShare]::Read
                )
                $success = $true
            }
            catch {
                $retryCount++
                if ($retryCount -lt $maxRetries) {
                    Write-Log "  File is locked, waiting 2 seconds before retry $retryCount/$maxRetries..." "WARNING"
                    Start-Sleep -Seconds 2
                }
                else {
                    throw "Failed to access file after $maxRetries attempts: $_"
                }
            }
        }
        
        $gzipStream = New-Object System.IO.Compression.GzipStream($inputStream, [System.IO.Compression.CompressionMode]::Decompress)
        $outputStream = New-Object System.IO.FileStream($tempCsvPath, [System.IO.FileMode]::Create)
        
        $gzipStream.CopyTo($outputStream)
        
        $outputStream.Close()
        $gzipStream.Close()
        $inputStream.Close()
        
        $fileSize = (Get-Item $tempCsvPath).Length
        $fileSizeMB = [math]::Round($fileSize / 1MB, 2)
        Write-Log "  Decompressed size: $fileSizeMB MB"
        
        return $tempCsvPath
    }
    catch {
        Write-Log "  Error decompressing file: $_" "ERROR"
        if (Test-Path $tempCsvPath) {
            Remove-Item $tempCsvPath -Force -ErrorAction SilentlyContinue
        }
        throw
    }
}

# Function to import CSV to PostgreSQL
function Import-CsvToPostgres {
    param(
        [string]$CsvFilePath,
        [string]$SourceFileName
    )
    
    Write-Log "  Building COPY command..."
    
    # Build the \copy command (psql meta-command, more efficient than COPY FROM STDIN)
    $fullTableName = $SCHEMA_NAME + "." + $TABLE_NAME
    
    # Escape single quotes in file path for psql
    $escapedFilePath = $CsvFilePath -replace "'", "''"
    
    # Build \copy command
    $copyCommand = "\copy $fullTableName FROM '$escapedFilePath' WITH (FORMAT csv"
    
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
    
    Write-Log "  Executing: $copyCommand" "DEBUG"
    
    # Build session settings as separate commands
    $sessionCommands = @()
    if ($DISABLE_SYNCHRONOUS_COMMIT) {
        $sessionCommands += "SET synchronous_commit = off;"
    }
    if ($WORK_MEM_MB -gt 0) {
        $sessionCommands += "SET work_mem = '${WORK_MEM_MB}MB';"
    }
    if ($MAINTENANCE_WORK_MEM_MB -gt 0) {
        $sessionCommands += "SET maintenance_work_mem = '${MAINTENANCE_WORK_MEM_MB}MB';"
    }
    
    # Build psql command arguments
    # Apply session settings first, then run \copy
    $psqlArgs = @(
        "-h", $DB_HOST,
        "-p", $DB_PORT,
        "-U", $DB_USER,
        "-d", $DB_NAME
    )
    
    # Add session settings if any
    foreach ($cmd in $sessionCommands) {
        $psqlArgs += "-c"
        $psqlArgs += $cmd
    }
    
    # Add the \copy command
    $psqlArgs += "-c"
    $psqlArgs += $copyCommand
    
    # Set password if provided
    if ($DB_PASSWORD) {
        $env:PGPASSWORD = $DB_PASSWORD
    }
    
    try {
        # Execute psql with \copy command (reads file directly, more efficient)
        Write-Log "  Running psql import..."
        Write-Log "  This may take 30-60 minutes for large files. Progress updates every 30 seconds..."
        
        # Start a background job to monitor progress
        $progressJob = Start-Job -ScriptBlock {
            param($interval)
            $elapsed = 0
            while ($true) {
                Start-Sleep -Seconds $interval
                $elapsed += $interval
                $minutes = [math]::Floor($elapsed / 60)
                $seconds = $elapsed % 60
                Write-Output "Still importing... Elapsed: ${minutes}m ${seconds}s"
            }
        } -ArgumentList 30
        
        try {
            # Use PowerShell native execution instead of cmd to avoid password escaping issues
            $startTime = Get-Date
            $result = & psql @psqlArgs 2>&1
            $duration = (Get-Date) - $startTime
            
            # Stop the progress job
            Stop-Job -Job $progressJob -ErrorAction SilentlyContinue
            Remove-Job -Job $progressJob -Force -ErrorAction SilentlyContinue
            
            # Display any progress messages from the job
            $progressMessages = Receive-Job -Job $progressJob -ErrorAction SilentlyContinue
            foreach ($msg in $progressMessages) {
                Write-Progress-Status "  $msg"
            }
            
            Write-Log "  Import command completed in $([math]::Round($duration.TotalSeconds, 2)) seconds"
            
            if ($LASTEXITCODE -eq 0) {
                # Try to extract row count from result
                $rowCount = "unknown"
                if ($result -match "COPY (\d+)") {
                    $rowCount = $matches[1]
                }
                Write-Log "  [OK] Successfully imported $rowCount rows from: $SourceFileName" "SUCCESS"
                return $true
            }
            else {
                Write-Log "  [FAIL] Failed to import $SourceFileName" "ERROR"
                Write-Log "  Error details: $result" "ERROR"
                return $false
            }
        }
        finally {
            # Ensure progress job is cleaned up
            if ($progressJob) {
                Stop-Job -Job $progressJob -ErrorAction SilentlyContinue
                Remove-Job -Job $progressJob -Force -ErrorAction SilentlyContinue
            }
        }
    }
    catch {
        Write-Log "  [FAIL] Exception during import of $SourceFileName : $_" "ERROR"
        return $false
    }
}

# Main script execution
function Main {
    try {
        Write-Log ""
        Write-Log "========================================================"
        Write-Log "   PostgreSQL CSV.GZ Import Script"
        Write-Log "========================================================"
        Write-Log ""
        Write-Log "Configuration:"
        Write-Log "  Database: $DB_NAME"
        Write-Log "  Host: ${DB_HOST}:${DB_PORT}"
        Write-Log "  User: $DB_USER"
        Write-Log "  Schema: $SCHEMA_NAME"
        $fullTableName = $SCHEMA_NAME + "." + $TABLE_NAME
        Write-Log "  Table: $fullTableName"
        Write-Log "  CSV Path: $CSV_PATH"
        Write-Log "  Delimiter: '$CSV_DELIMITER'"
        Write-Log "  Has Header: $CSV_HAS_HEADER"
        Write-Log "  Truncate: $TRUNCATE_TABLE"
        Write-Log "  Disable Indexes: $DISABLE_INDEXES"
        Write-Log "  Disable Triggers: $DISABLE_TRIGGERS"
        Write-Log "  Disable Primary Key: $DISABLE_PRIMARY_KEY"
        Write-Log "  Disable Foreign Keys: $DISABLE_FOREIGN_KEYS"
        Write-Log "  Disable Autovacuum: $DISABLE_AUTOVACUUM"
        Write-Log "  Disable Sync Commit: $DISABLE_SYNCHRONOUS_COMMIT"
        if ($WORK_MEM_MB -gt 0) {
            Write-Log "  Work Memory: ${WORK_MEM_MB}MB"
        }
        if ($MAINTENANCE_WORK_MEM_MB -gt 0) {
            Write-Log "  Maintenance Work Memory: ${MAINTENANCE_WORK_MEM_MB}MB"
        }
        if ($TEMP_DIRECTORY) {
            Write-Log "  Temp Directory: $TEMP_DIRECTORY"
        } else {
            Write-Log "  Temp Directory: System default"
        }
        Write-Log ""
        
        # Validate configuration
        if ($DB_NAME -eq "your_database_name" -or $TABLE_NAME -eq "your_table_name") {
            Write-Log "ERROR: Please configure the database and table names in the script!" "ERROR"
            Write-Log "Edit the CONFIGURATION section at the top of this script." "ERROR"
            exit 1
        }
        
        if ($CSV_PATH -eq "C:\path\to\csv\files") {
            Write-Log "ERROR: Please configure the CSV_PATH in the script!" "ERROR"
            Write-Log "Edit the CONFIGURATION section at the top of this script." "ERROR"
            exit 1
        }
        
        # Check if psql is available
        Write-Log "Checking prerequisites..."
        if (-not (Test-PsqlAvailable)) {
            Write-Log "ERROR: psql command not found!" "ERROR"
            Write-Log "Please install PostgreSQL client tools and ensure psql is in your PATH." "ERROR"
            Write-Log "Download from: https://www.postgresql.org/download/windows/" "ERROR"
            exit 1
        }
        Write-Log "[OK] psql found" "SUCCESS"
        
        # Validate temp directory if specified
        if ($TEMP_DIRECTORY) {
            if (-not (Test-Path $TEMP_DIRECTORY -PathType Container)) {
                Write-Log "ERROR: Temp directory does not exist: $TEMP_DIRECTORY" "ERROR"
                Write-Log "Please create the directory or leave TEMP_DIRECTORY empty to use system default." "ERROR"
                exit 1
            }
            Write-Log "[OK] Temp directory validated" "SUCCESS"
        }
        
        # Determine if CSV_PATH is a file or directory
        Write-Log ""
        Write-Log "Scanning for CSV files..."
        if (Test-Path $CSV_PATH -PathType Leaf) {
            # Single file
            if ($CSV_PATH -notlike "*.csv.gz") {
                Write-Log "ERROR: File must have .csv.gz extension: $CSV_PATH" "ERROR"
                exit 1
            }
            $files = @(Get-Item $CSV_PATH)
        }
        elseif (Test-Path $CSV_PATH -PathType Container) {
            # Directory - get all .csv.gz files
            $files = Get-ChildItem -Path $CSV_PATH -Filter "*.csv.gz" | Sort-Object Name
            if ($files.Count -eq 0) {
                Write-Log "ERROR: No .csv.gz files found in directory: $CSV_PATH" "ERROR"
                exit 1
            }
        }
        else {
            Write-Log "ERROR: Path not found: $CSV_PATH" "ERROR"
            exit 1
        }
        
        Write-Log "[OK] Found $($files.Count) file(s) to process" "SUCCESS"
        foreach ($file in $files) {
            Write-Log "  - $($file.Name)"
        }
        
        # Truncate table if requested
        if ($TRUNCATE_TABLE) {
            Write-Log ""
            $fullTableName = $SCHEMA_NAME + "." + $TABLE_NAME
            Write-Log "Truncating table: $fullTableName"
            
            $truncateCommand = "TRUNCATE TABLE $fullTableName;"
            $psqlArgs = @(
                "-h", $DB_HOST,
                "-p", $DB_PORT,
                "-U", $DB_USER,
                "-d", $DB_NAME,
                "-c", $truncateCommand
            )
            
            if ($DB_PASSWORD) {
                $env:PGPASSWORD = $DB_PASSWORD
            }
            
            $result = psql @psqlArgs 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Log "[OK] Table truncated successfully" "SUCCESS"
            }
            else {
                Write-Log "[FAIL] Failed to truncate table" "ERROR"
                Write-Log "Error: $result" "ERROR"
                exit 1
            }
        }
        
        # Process each file
        Write-Log ""
        Write-Log "========================================================"
        Write-Log "Starting file processing..."
        Write-Log "========================================================"
        
        # Disable indexes and triggers if requested (before processing any files)
        if ($DISABLE_INDEXES -or $DISABLE_TRIGGERS -or $DISABLE_PRIMARY_KEY -or $DISABLE_FOREIGN_KEYS -or $DISABLE_AUTOVACUUM) {
            Write-Log ""
            Write-Log "Disabling optimizations for bulk import..."
            try {
                Disable-TableOptimizations -SchemaName $SCHEMA_NAME -TableName $TABLE_NAME
            }
            catch {
                Write-Log "Warning: Failed to disable optimizations: $_" "WARNING"
                Write-Log "Continuing with import anyway..." "WARNING"
            }
        }
        
        $successCount = 0
        $failCount = 0
        $fileNumber = 0
        
        foreach ($file in $files) {
            $fileNumber++
            Write-Log ""
            Write-Log "========================================" "PROGRESS"
            $progressMsg = "[$fileNumber/$($files.Count)] Processing: $($file.Name)"
            Write-Log $progressMsg "PROGRESS"
            Write-Log "========================================" "PROGRESS"
            Write-Log "  File size: $([math]::Round($file.Length / 1MB, 2)) MB (compressed)"
            
            $tempCsvPath = $null
            $startTime = Get-Date
            
            try {
                # Decompress the .gz file
                Write-Log "  [Step 1/3] Decompressing file..."
                $tempCsvPath = Expand-GzipFile -GzipPath $file.FullName -TempDirectory $TEMP_DIRECTORY
                Write-Log "  [Step 1/3] Decompression complete" "SUCCESS"
                
                # Import to PostgreSQL
                Write-Log "  [Step 2/3] Importing to database..."
                $success = Import-CsvToPostgres -CsvFilePath $tempCsvPath -SourceFileName $file.Name
                
                if ($success) {
                    Write-Log "  [Step 2/3] Import complete" "SUCCESS"
                }
                
                $duration = (Get-Date) - $startTime
                $durationMinutes = [math]::Floor($duration.TotalMinutes)
                $durationSeconds = [math]::Round($duration.TotalSeconds % 60, 0)
                Write-Log "  Total duration for this file: ${durationMinutes}m ${durationSeconds}s"
                
                if ($success) {
                    $successCount++
                    Write-Log "  Status: SUCCESS [OK]" "SUCCESS"
                }
                else {
                    $failCount++
                    Write-Log "  Status: FAILED [X]" "ERROR"
                }
                
                # Show overall progress
                $remainingFiles = $files.Count - $fileNumber
                if ($remainingFiles -gt 0) {
                    Write-Log ""
                    Write-Log "Progress: $successCount succeeded, $failCount failed, $remainingFiles remaining" "PROGRESS"
                }
            }
            catch {
                Write-Log "  [FAIL] Error processing $($file.Name): $_" "ERROR"
                $failCount++
            }
            finally {
                # Clean up temp file
                Write-Log "  [Step 3/3] Cleaning up temporary files..."
                if ($tempCsvPath -and (Test-Path $tempCsvPath)) {
                    Remove-Item $tempCsvPath -Force -ErrorAction SilentlyContinue
                    Write-Log "  [Step 3/3] Cleanup complete" "SUCCESS"
                }
            }
        }
        
        # Re-enable indexes and triggers after all files are processed
        if ($DISABLE_INDEXES -or $DISABLE_TRIGGERS -or $DISABLE_PRIMARY_KEY -or $DISABLE_FOREIGN_KEYS -or $DISABLE_AUTOVACUUM) {
            Write-Log ""
            Write-Log "Re-enabling optimizations..."
            try {
                Enable-TableOptimizations -SchemaName $SCHEMA_NAME -TableName $TABLE_NAME
            }
            catch {
                Write-Log "ERROR: Failed to re-enable optimizations: $_" "ERROR"
                Write-Log "You may need to manually recreate indexes/constraints!" "ERROR"
            }
        }
        
        # Summary
        Write-Log ""
        Write-Log "========================================================"
        Write-Log "   IMPORT SUMMARY"
        Write-Log "========================================================"
        Write-Log "Total files processed: $($files.Count)"
        Write-Log "Successful imports: $successCount" "SUCCESS"
        if ($failCount -gt 0) {
            Write-Log "Failed imports: $failCount" "ERROR"
        } else {
            Write-Log "Failed imports: $failCount"
        }
        Write-Log "Log file: $LOG_FILE"
        Write-Log "========================================================"
        Write-Log ""
        
        if ($failCount -gt 0) {
            Write-Log "[WARNING] Import completed with errors! Check log for details." "WARNING"
            exit 1
        }
        
        Write-Log "[OK] All imports completed successfully!" "SUCCESS"
        exit 0
    }
    catch {
        Write-Log "" 
        Write-Log "========================================================"
        Write-Log "FATAL ERROR" "ERROR"
        Write-Log "========================================================"
        Write-Log "Error: $_" "ERROR"
        Write-Log $_.ScriptStackTrace "ERROR"
        Write-Log "========================================================"
        exit 1
    }
    finally {
        # Clear password from environment
        if ($DB_PASSWORD) {
            Remove-Item env:PGPASSWORD -ErrorAction SilentlyContinue
        }
    }
}

#endregion ========================================================

# Run the main function
Main
