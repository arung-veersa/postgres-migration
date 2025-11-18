# Analytics Data Migration Tool

Migrates data from Snowflake Analytics views to PostgreSQL tables using MERGE logic (UPSERT with optional deletes).

## Features

- ✅ **MERGE Logic**: Conditionally INSERT, UPDATE, or DELETE rows
- ✅ **Incremental & Full Load**: Support for both one-time and incremental sync
- ✅ **Column Auto-Detection**: Automatically detects columns and primary keys from target table
- ✅ **Watermark-Based Sync**: Uses timestamp columns for incremental updates
- ✅ **Column Mapping**: Handles different column names between source and target
- ✅ **Parallel Execution**: Process multiple tables concurrently
- ✅ **Flexible Filtering**: Custom WHERE clauses for source data

---

## Quick Start

### 1. Install Dependencies

```bash
cd Scripts06-Analytics/Program
pip install -r requirements.txt
```

### 2. Configure Credentials

Create `.env` file with your database credentials:

```bash
# Copy from Scripts04 if credentials are the same
copy ..\..\Scripts04\Program\.env .env
```

Or create manually with:
```ini
SNOWFLAKE_USER=your_user
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_RSA_KEY=your_base64_key_without_headers

POSTGRES_HOST=your_host
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_SCHEMA=public
```

### 3. Validate Setup

```bash
python test_setup.py      # Full pre-flight checks
python test_snowflake.py  # Snowflake connection only
```

### 4. Run Migration

```bash
python migrate.py
```

---

## Configuration (`config.json`)

### Basic Structure

```json
{
  "max_parallel_jobs": 2,
  "tables": [
    {
      "source_view": "DIMPAYER",
      "target_table": "dimpayer",
      "load_type": "incremental",
      "watermark_column": "Updated Datatimestamp",
      "incremental_days_back": 30,
      "source_where_clause": "\"Is Active\" = TRUE AND \"Is Demo\" = FALSE",
      "perform_deletes": false,
      "batch_size": 10000
    }
  ]
}
```

### Configuration Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `source_view` | Yes | Snowflake view name | `"DIMPAYER"` |
| `target_table` | Yes | PostgreSQL table name | `"dimpayer"` |
| `load_type` | Yes | `"full"` or `"incremental"` | `"incremental"` |
| `watermark_column` | For incremental | Timestamp column (target name) | `"Updated Datatimestamp"` |
| `source_watermark_column` | If column mapping needed | Timestamp column in source | `"Updated Date"` |
| `target_watermark_column` | If column mapping needed | Timestamp column in target | `"Updated Datatimestamp"` |
| `incremental_days_back` | For incremental | Days of data to fetch | `30` |
| `source_where_clause` | Optional | Filter for source data | `"\"Is Active\" = TRUE"` |
| `perform_deletes` | Optional | Delete missing records | `false` |
| `batch_size` | Optional | Rows per batch | `10000` |

### Load Types

**Incremental Load:**
- Fetches only recent data based on watermark column
- Updates existing records if source timestamp > target timestamp
- Inserts new records
- Skips deletes (typically `perform_deletes: false`)

**Full Load:**
- Fetches all data matching WHERE clause
- Updates all existing records
- Inserts new records
- Can perform hard deletes (if `perform_deletes: true`)

### Column Mapping (When Source ≠ Target)

For tables where watermark column names differ:

```json
{
  "source_view": "FACTCAREGIVERABSENCE",
  "target_table": "factcaregiverabsence",
  "load_type": "incremental",
  "source_watermark_column": "Updated Date",
  "target_watermark_column": "Updated Datatimestamp",
  "incremental_days_back": 30
}
```

---

## Current Configuration (15 Tables)

### Dimension Tables (11)
1. **DIMPAYER** - Incremental (30 days), Active/Non-Demo/HHA
2. **DIMUSER** - Full load, HHA Payer users
3. **DIMPATIENTADDRESS** - Incremental (1000 days), Rockland County only, Column mapping
4. **DIMPATIENT** - Incremental (30 days), Active patients with visits
5. **DIMCAREGIVER** - Incremental (30 days), Registry checked
6. **DIMCONTRACT** - Incremental (30 days), HHA system
7. **DIMOFFICE** - Incremental (30 days), HHA system
8. **DIMPAYERPROVIDER** - Full load, All records
9. **DIMPROVIDER** - Incremental (30 days), HHA system
10. **DIMSERVICECODE** - Incremental (30 days), HHA system
11. **DIMUSEROFFICES** - Full load, HHA system (excludes NULL Office Ids)

### Fact Tables (4)
12. **FACTCAREGIVERABSENCE** - Incremental (30 days), Active, Column mapping
13. **FACTCAREGIVERINSERVICE** - Incremental (30 days), HHA system, Column mapping
14. **FACTVISITCALLPERFORMANCE_CR** - Incremental (30 days), Single Payer
15. **FACTVISITCALLPERFORMANCE_DELETED_CR** - Incremental (30 days), Single Payer

---

## How It Works

### MERGE Process

1. **Auto-detect schema**: Queries target table for columns and primary keys
2. **Fetch from Snowflake**: Retrieves data based on configuration
3. **Stage data**: Loads into temporary staging table
4. **MERGE operations**:
   - **UPDATE**: Updates rows where PK matches AND source watermark > target watermark
   - **INSERT**: Inserts rows where PK doesn't exist in target
   - **DELETE**: (Optional) Removes rows from target not in source
5. **Commit**: Commits transaction and reports summary

### Example Generated SQL

```sql
-- Source query (with incremental filter)
SELECT "Payer Id", "Application Payer Id", "Payer Name", ...
FROM "DIMPAYER"
WHERE ("Is Active" = TRUE AND "Is Demo" = FALSE)
  AND "Updated Datatimestamp" >= DATEADD(day, -30, CURRENT_DATE());

-- UPDATE (only if source is newer)
UPDATE public.dimpayer t
SET "Application Payer Id" = s."Application Payer Id", ...
FROM staging_dimpayer s
WHERE t."Payer Id" = s."Payer Id"
  AND s."Updated Datatimestamp" > t."Updated Datatimestamp";

-- INSERT (new records)
INSERT INTO public.dimpayer (...)
SELECT ... FROM staging_dimpayer s
WHERE NOT EXISTS (
    SELECT 1 FROM public.dimpayer t WHERE t."Payer Id" = s."Payer Id"
);
```

---

## Performance & Optimization

### Parallel Execution
- **Sequential**: `max_parallel_jobs: 1` (safer, ~15-30 min)
- **Parallel**: `max_parallel_jobs: 2-4` (faster, ~10-15 min)

### Batch Sizes
- **Dimension Tables**: 10,000 rows per batch
- **Fact Tables**: 10,000-50,000 rows per batch

### Incremental Windows
- **Standard**: 30 days (most dimensions and facts)
- **Long history**: 1000 days (DIMPATIENTADDRESS)
- **Short window**: 7-30 days (high-volume facts)

### Estimated Runtime
- **First run**: 15-30 minutes (all inserts)
- **Subsequent runs**: 5-10 minutes (only changed data)

---

## Troubleshooting

### Connection Issues
```bash
# Test connections
python test_snowflake.py  # Snowflake only
python test_setup.py      # Both databases
```

### No Rows Migrated
- Check WHERE clause filters
- Verify watermark date range (`incremental_days_back`)
- Confirm data exists in source view

### NULL Primary Key Errors
- Add filter to exclude NULLs: `"\"Primary Key Col\" IS NOT NULL"`
- Example: DIMUSEROFFICES filters `"Office Id" IS NOT NULL`

### Column Name Errors
- Use column mapping for different names between source/target
- Set `source_watermark_column` and `target_watermark_column`

### RSA Key Errors
- Ensure key is base64 encoded WITHOUT header/footer lines
- Check for literal `\n` characters (should be actual newlines)

---

## Files

```
Scripts06-Analytics/Program/
├── migrate.py           # Main migration script
├── config.json          # Table configurations (15 tables)
├── requirements.txt     # Python dependencies
├── test_setup.py        # Pre-flight validation
├── test_snowflake.py    # Snowflake connection test
├── README.md            # This file
└── .env                 # Database credentials (create manually)
```

---

## Migration Summary Report

After each run, you'll see:

```
============================================================
Migration Summary for dimpayer:
  Updated: 45
  Inserted: 10
  Deleted: 0
  Total: 55
============================================================
```

### Interpretation
- **Updated**: Existing rows with newer source timestamp
- **Inserted**: New rows not in target
- **Deleted**: Rows removed (only if `perform_deletes: true`)
- **Total**: Sum of all operations

---

## Support

### Pre-flight Checks
```bash
python test_setup.py      # Full validation
python test_snowflake.py  # Snowflake only
```

### Common Commands
```bash
# Install dependencies
pip install -r requirements.txt

# Run migration
python migrate.py

# Check specific table (edit config.json to comment out others)
python migrate.py
```

### Logs
- Real-time progress bars for data loading
- Row counts at each step
- Detailed error messages with context

---

## Notes

- **First Run**: All matching rows are inserted
- **Subsequent Runs**: Only updates/inserts changed rows
- **Full Load Tables**: Always compare all matching rows
- **Idempotent**: Safe to run multiple times
- **Column Detection**: Automatic from target PostgreSQL schema
- **Primary Keys**: Auto-detected from PostgreSQL constraints

---

## Version History

- **v1.0**: Initial implementation with 15 tables
- Supports incremental and full loads
- Column name mapping
- Parallel execution
- Comprehensive error handling
