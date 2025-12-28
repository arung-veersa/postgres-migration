# Local PostgreSQL Testing Guide

## 🎯 Purpose
Test the CSV import script locally with a small dataset before running on AWS with large files.

## 📋 Prerequisites

1. **PostgreSQL installed locally** (with psql in PATH)
2. **Local database created** (e.g., `testdb`)
3. **PowerShell 5.1+**
4. **Your .gz.csv file** (e.g., `factvisitcallperformance_cr.csv.gz_0_0_0.csv.gz`)

---

## 🚀 Step-by-Step Instructions

### Step 1: Create Test CSV (20 rows)

```powershell
# Navigate to the scripts directory
cd "C:\Users\ArunGupta\Repos\postgres-migration\Scripts08\CSV Import"

# Extract 20 rows from your gzipped file
.\create-test-csv.ps1 -GzipFile "path\to\your\factvisitcallperformance_cr.csv.gz_0_0_0.csv.gz" -NumRows 20

# Output: test_sample.csv
```

**What this does:**
- Decompresses the .gz file
- Extracts header + 20 data rows
- Creates `test_sample.csv`

---

### Step 2: Check the CSV Structure

```powershell
# View the header
Get-Content test_sample.csv -Head 2

# This shows you what columns exist
```

**Example output:**
```
visit_id,visit_date,call_performance,status,agency_id
12345,2024-01-15,95.5,completed,AG001
```

---

### Step 3: Set Up Local PostgreSQL

#### Option A: Create Database (if needed)
```powershell
# Connect to PostgreSQL
psql -U postgres

# In psql:
CREATE DATABASE testdb;
\c testdb
\q
```

#### Option B: Use Existing Database
Skip to Step 4 if you already have a database.

---

### Step 4: Configure Local Test Scripts

Edit **`test-import-local.ps1`** (lines 11-16):

```powershell
$DB_HOST = "localhost"
$DB_PORT = 5432
$DB_NAME = "testdb"          # ← Your local database
$DB_USER = "postgres"         # ← Your local username
$DB_PASSWORD = "postgres"     # ← Your local password
```

Edit **`create-test-table.ps1`** (lines 44-56) to match your CSV columns:

```powershell
CREATE TABLE public.test_factvisit (
    visit_id VARCHAR(100),
    visit_date DATE,
    call_performance NUMERIC,
    status VARCHAR(50),
    agency_id VARCHAR(50)
    -- Add all your actual columns here
);
```

---

### Step 5: Create the Test Table

```powershell
# Default settings (edit params as needed)
.\create-test-table.ps1

# Or with custom parameters:
.\create-test-table.ps1 -DB_NAME "mydb" -DB_USER "myuser" -DB_PASSWORD "mypass"
```

---

### Step 6: Run Test Import

```powershell
.\test-import-local.ps1
```

**What you'll see:**
```
[INFO] Testing database connection...
[SUCCESS] [OK] Database connection successful
[INFO] Rows to import: 20
[INFO] Starting import...
[SUCCESS] Rows imported: 20
[SUCCESS] Duration: 0.15 seconds
[SUCCESS] Table row count: 20

Sample data (first 3 rows):
┌──────────┬─────────────┬──────────────────┐
│ visit_id │ visit_date  │ call_performance │
├──────────┼─────────────┼──────────────────┤
│ 12345    │ 2024-01-15  │ 95.5             │
...
```

---

## ✅ Verification

After successful import:

```powershell
# Connect to your local database
psql -U postgres -d testdb

# In psql, run:
SELECT COUNT(*) FROM public.test_factvisit;
SELECT * FROM public.test_factvisit LIMIT 5;
```

---

## 🔧 Troubleshooting

### Issue: "psql command not found"
**Fix:** Add PostgreSQL bin folder to PATH
```powershell
$env:PATH += ";C:\Program Files\PostgreSQL\16\bin"
```

### Issue: "password authentication failed"
**Fix:** Check your local PostgreSQL username/password in `test-import-local.ps1`

### Issue: "relation does not exist"
**Fix:** 
1. Make sure you ran `create-test-table.ps1` first
2. Check the schema name (usually `public` for local)

### Issue: "columns don't match"
**Fix:**
1. Look at CSV header: `Get-Content test_sample.csv -Head 1`
2. Update table structure in `create-test-table.ps1`
3. Recreate table

---

## 📊 Once Local Testing Works

After successful local testing, you can:

1. ✅ Update the main `import-csv-to-postgres.ps1` with any fixes
2. ✅ Test with progressively larger datasets (100 rows, 1000 rows)
3. ✅ Then try on AWS with full files

---

## 🔄 Quick Re-test

After making changes:

```powershell
# Clean and re-import
.\create-test-table.ps1   # Recreates table
.\test-import-local.ps1   # Re-imports data
```

---

## 📝 Files Created

- `create-test-csv.ps1` - Extracts small CSV from .gz file
- `test-import-local.ps1` - Simplified import script for local testing
- `create-test-table.ps1` - Creates local test table
- `test_sample.csv` - Your 20-row test file (generated)
- `test-import-log.txt` - Import log (generated)

---

## 💡 Tips

- Start with 20 rows to test quickly
- Increase to 1000 rows to test performance
- Keep optimizations OFF for local testing (easier to debug)
- Once it works locally, apply same logic to AWS script

---

## ❓ Need Help?

If something doesn't work:
1. Check `test-import-log.txt` for detailed errors
2. Verify PostgreSQL is running: `pg_isready`
3. Test connection manually: `psql -U postgres -d testdb`

