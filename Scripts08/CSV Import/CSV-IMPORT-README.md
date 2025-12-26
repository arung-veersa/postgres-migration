# PostgreSQL CSV.GZ Import Script

A standalone PowerShell script to import multiple compressed CSV files (.csv.gz) into PostgreSQL tables using the `psql` command-line tool.

## Features

- ✅ **Single File** - No dependencies, just one PowerShell script
- ✅ **Easy Configuration** - Edit settings at the top of the script and run
- ✅ **Automatic Decompression** - Handles .csv.gz files automatically
- ✅ **Batch Import** - Process entire directories of CSV files
- ✅ **Efficient** - Uses PostgreSQL's native COPY command
- ✅ **Detailed Logging** - Complete log file with timestamps
- ✅ **Progress Tracking** - See what's happening in real-time
- ✅ **Error Handling** - Robust error handling and recovery
- ✅ **Automatic Cleanup** - Removes temporary files

## Prerequisites

1. **PostgreSQL Client Tools** - `psql` must be installed and in your PATH
   ```powershell
   # Install using winget
   winget install PostgreSQL.PostgreSQL
   
   # Or download from: https://www.postgresql.org/download/windows/
   ```

2. **PowerShell** - Windows PowerShell 5.1+ or PowerShell Core 7+

3. **Network Access** - Ensure you can connect to your PostgreSQL database

## Quick Start

### Step 1: Download the Script

Save `import-csv-to-postgres.ps1` to your computer.

### Step 2: Configure the Script

Open `import-csv-to-postgres.ps1` in a text editor and modify the configuration section at the top:

```powershell
#region ============= CONFIGURATION - EDIT THESE VALUES =============

# Database Connection Settings
$DB_HOST = "localhost"              # PostgreSQL server hostname
$DB_PORT = 5432                     # PostgreSQL server port
$DB_NAME = "your_database_name"     # Database name
$DB_USER = "postgres"               # PostgreSQL username
$DB_PASSWORD = ""                   # Password (or leave empty to use env var)

# Target Table Settings
$SCHEMA_NAME = "public"             # Schema name
$TABLE_NAME = "your_table_name"     # Table name

# CSV File Settings
$CSV_PATH = "C:\path\to\csv\files"  # Path to CSV files

# Import Options
$CSV_DELIMITER = ","                # Field delimiter
$CSV_HAS_HEADER = $true             # Does CSV have header row?
$CSV_NULL_STRING = ""               # String representing NULL
$CSV_ENCODING = "UTF8"              # File encoding

# Table Options
$TRUNCATE_TABLE = $false            # Truncate before import?

# Logging
$LOG_FILE = "import-log.txt"        # Log file path

#endregion
```

### Step 3: Run the Script

```powershell
.\import-csv-to-postgres.ps1
```

That's it! The script will process all .csv.gz files and show you the progress.

## Example Output

```
========================================================
   PostgreSQL CSV.GZ Import Script
========================================================

Configuration:
  Database: analytics_db
  Host: localhost:5432
  User: postgres
  Schema: public
  Table: public.sales_data
  CSV Path: C:\data\sales
  Delimiter: ','
  Has Header: True
  Truncate: False

Checking prerequisites...
✓ psql found

Scanning for CSV files...
✓ Found 3 file(s) to process
  - sales_2024_01.csv.gz
  - sales_2024_02.csv.gz
  - sales_2024_03.csv.gz

========================================================
Starting file processing...
========================================================

[1/3] Processing: sales_2024_01.csv.gz
  File size: 15.32 MB (compressed)
  Decompressing to temp file...
  Decompressed size: 89.45 MB
  Building COPY command...
  Running psql import...
  ✓ Successfully imported 125000 rows from: sales_2024_01.csv.gz
  Duration: 4.52 seconds
  Cleaned up temp file

[2/3] Processing: sales_2024_02.csv.gz
  File size: 14.88 MB (compressed)
  Decompressing to temp file...
  Decompressed size: 87.21 MB
  Building COPY command...
  Running psql import...
  ✓ Successfully imported 118500 rows from: sales_2024_02.csv.gz
  Duration: 4.38 seconds
  Cleaned up temp file

[3/3] Processing: sales_2024_03.csv.gz
  File size: 16.12 MB (compressed)
  Decompressing to temp file...
  Decompressed size: 92.33 MB
  Building COPY command...
  Running psql import...
  ✓ Successfully imported 132000 rows from: sales_2024_03.csv.gz
  Duration: 4.71 seconds
  Cleaned up temp file

========================================================
   IMPORT SUMMARY
========================================================
Total files processed: 3
Successful imports: 3
Failed imports: 0
Log file: import-log.txt
========================================================

✓ All imports completed successfully!
```

## Configuration Options

### Database Connection

| Setting | Description | Example |
|---------|-------------|---------|
| `$DB_HOST` | PostgreSQL server hostname or IP | `"localhost"`, `"db.example.com"` |
| `$DB_PORT` | PostgreSQL server port | `5432` |
| `$DB_NAME` | Database name | `"mydb"`, `"analytics"` |
| `$DB_USER` | PostgreSQL username | `"postgres"`, `"etl_user"` |
| `$DB_PASSWORD` | Password (optional) | `"mypassword"` or `""` |

### Target Table

| Setting | Description | Example |
|---------|-------------|---------|
| `$SCHEMA_NAME` | Schema containing the table | `"public"`, `"staging"`, `"raw"` |
| `$TABLE_NAME` | Target table name | `"sales_data"`, `"events"` |

### CSV File Settings

| Setting | Description | Example |
|---------|-------------|---------|
| `$CSV_PATH` | Directory or single file | `"C:\data"`, `"C:\data\file.csv.gz"` |
| `$CSV_DELIMITER` | Field separator | `","`, `"|"`, `"\t"` (tab) |
| `$CSV_HAS_HEADER` | First row is header? | `$true` or `$false` |
| `$CSV_NULL_STRING` | String meaning NULL | `""`, `"NULL"`, `"NA"` |
| `$CSV_ENCODING` | Character encoding | `"UTF8"`, `"LATIN1"`, `"WIN1252"` |

### Options

| Setting | Description | Example |
|---------|-------------|---------|
| `$TRUNCATE_TABLE` | Clear table before import? | `$true` or `$false` |
| `$LOG_FILE` | Path to log file | `"import-log.txt"` |

## Authentication Methods

### Method 1: Configure in Script
```powershell
$DB_PASSWORD = "your_password"
```

### Method 2: Environment Variable (Recommended)
Leave `$DB_PASSWORD = ""` in the script, then set:
```powershell
$env:PGPASSWORD = "your_password"
.\import-csv-to-postgres.ps1
```

### Method 3: .pgpass File (Most Secure)
Leave `$DB_PASSWORD = ""` and create file at `%APPDATA%\postgresql\pgpass.conf`:
```
hostname:port:database:username:password
```

## Common Scenarios

### Scenario 1: Import all files from a directory
```powershell
# Edit script:
$CSV_PATH = "C:\data\monthly_exports"
$TRUNCATE_TABLE = $false
```

### Scenario 2: Replace all data with new import
```powershell
# Edit script:
$CSV_PATH = "C:\data\new_data"
$TRUNCATE_TABLE = $true  # Clears table first
```

### Scenario 3: Import a single file
```powershell
# Edit script:
$CSV_PATH = "C:\data\january_sales.csv.gz"
```

### Scenario 4: Pipe-delimited files without headers
```powershell
# Edit script:
$CSV_DELIMITER = "|"
$CSV_HAS_HEADER = $false
```

### Scenario 5: Remote database
```powershell
# Edit script:
$DB_HOST = "prod-db.example.com"
$DB_PORT = 5432
$DB_USER = "etl_service"
# Use environment variable for password
```

## Table Requirements

The target PostgreSQL table must:
1. Already exist in the database
2. Have columns matching the CSV structure
3. Have compatible data types

Example table creation:
```sql
CREATE TABLE public.sales_data (
    id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    product_name VARCHAR(255),
    quantity INTEGER,
    price DECIMAL(10,2),
    customer_id INTEGER,
    notes TEXT
);
```

## CSV File Format

### Requirements
- Files must be gzip compressed with `.csv.gz` extension
- Standard CSV format (quoted fields, escaped quotes)
- Consistent column structure across all files

### Example CSV
```csv
sale_date,product_name,quantity,price,customer_id,notes
2024-01-01,Widget A,5,19.99,1001,"First sale of the year"
2024-01-02,Widget B,3,29.99,1002,"Regular customer"
2024-01-03,Widget C,10,9.99,1003,""
```

### Handling NULL Values
Configure `$CSV_NULL_STRING` to match your data:
```powershell
# Empty string is NULL
$CSV_NULL_STRING = ""

# "NULL" string is NULL
$CSV_NULL_STRING = "NULL"

# "NA" or "N/A" is NULL
$CSV_NULL_STRING = "NA"
```

## Troubleshooting

### Error: "psql command not found"
**Solution:** Install PostgreSQL client tools and add to PATH
```powershell
# Add PostgreSQL bin directory to PATH
$env:PATH += ";C:\Program Files\PostgreSQL\16\bin"
```

### Error: "Connection refused" or "could not connect"
**Check:**
1. Database is running: `psql -h localhost -U postgres -c "SELECT 1;"`
2. Host and port are correct
3. Firewall allows connection
4. `pg_hba.conf` allows your IP address

### Error: "permission denied for table"
**Solution:** Grant permissions to user
```sql
GRANT INSERT ON schema_name.table_name TO username;
```

### Error: "column 'x' does not exist"
**Check:**
1. CSV columns match table structure
2. Header row matches table column names (if `$CSV_HAS_HEADER = $true`)
3. Column order is correct

### Error: "invalid input syntax for type"
**Check:**
1. Data types in CSV match table definition
2. Date formats are compatible
3. Number formats don't have unexpected characters
4. NULL handling is configured correctly

### Error: "Please configure the database and table names"
**Solution:** Edit the configuration section at the top of the script with your actual values

## Performance Tips

### For Large Imports
1. **Drop indexes** before import, recreate after:
   ```sql
   DROP INDEX IF EXISTS idx_table_column;
   -- Run import
   CREATE INDEX idx_table_column ON table_name(column_name);
   ```

2. **Disable triggers** (if safe):
   ```sql
   ALTER TABLE table_name DISABLE TRIGGER ALL;
   -- Run import
   ALTER TABLE table_name ENABLE TRIGGER ALL;
   ```

3. **Increase maintenance_work_mem**:
   ```sql
   SET maintenance_work_mem = '2GB';
   ```

### For Multiple Tables
Run parallel imports in separate PowerShell windows, each with its own configured script.

### For Network Transfers
Run the script on the database server or a machine with high-speed connection to reduce network latency.

## Logging

All operations are logged to the file specified in `$LOG_FILE` (default: `import-log.txt`).

The log includes:
- Timestamp for each operation
- Configuration used
- Files discovered and processed
- Row counts imported
- Processing duration for each file
- Errors and warnings
- Summary statistics

## Security Best Practices

1. **Don't hardcode passwords** - Use environment variables or .pgpass file
2. **Restrict file permissions** - Keep script readable only by authorized users
3. **Use dedicated accounts** - Create a PostgreSQL user specifically for imports
4. **Audit logs** - Review log files regularly
5. **Secure transfer** - Use SSL connections for remote databases

## Advanced Usage

### Running on Schedule
Use Windows Task Scheduler:
```powershell
# Create a scheduled task
$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -File C:\scripts\import-csv-to-postgres.ps1"
$trigger = New-ScheduledTaskTrigger -Daily -At 2am
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "Daily CSV Import"
```

### Email Notifications
Add at the end of the script:
```powershell
if ($failCount -gt 0) {
    Send-MailMessage -To "admin@example.com" `
        -From "script@example.com" `
        -Subject "CSV Import Failed" `
        -Body (Get-Content $LOG_FILE | Out-String) `
        -SmtpServer "smtp.example.com"
}
```

### Processing Multiple Directories
Create multiple copies of the script with different configurations, or use a wrapper script to change `$CSV_PATH`.

## Exit Codes

- `0` - Success (all files imported)
- `1` - Error occurred (check log file for details)

## Support

For issues:
1. Check the log file (`import-log.txt`) for detailed error messages
2. Verify your configuration settings
3. Test `psql` connection manually:
   ```powershell
   psql -h localhost -U postgres -d mydb -c "SELECT 1;"
   ```

## License

This script is provided as-is for use in your PostgreSQL data import projects.

## Version History

- **v2.0** - Standalone single-file script with embedded configuration
- **v1.0** - Original version with separate batch file wrapper
