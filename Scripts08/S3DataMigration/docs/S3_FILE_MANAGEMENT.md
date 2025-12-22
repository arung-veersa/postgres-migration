# 🗑️ **S3 File Management Strategies**

## 🎯 **Default Behavior: OVERWRITE = TRUE**

**Current implementation:** Files are **overwritten** on subsequent runs to the same location.

---

## 📋 **Behavior Explained**

### **Scenario 1: Same Path, OVERWRITE = TRUE (Default)**

#### **First Run:**
```sql
COPY INTO 's3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/'
FROM (SELECT * FROM ANALYTICS.BI.DIMPAYER)
OVERWRITE = TRUE;
```

**Result:**
```
s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/
└── data_0_0_0.snappy.parquet  (160 rows, created at 10:00 AM)
```

#### **Second Run (Same Path):**
```sql
-- Same path again
COPY INTO 's3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/'
FROM (SELECT * FROM ANALYTICS.BI.DIMPAYER)
OVERWRITE = TRUE;
```

**Result:**
```
s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/
└── data_0_0_0.snappy.parquet  (160 rows, REPLACED at 11:00 AM)
```

**✅ Old file deleted, new file created**

---

### **Scenario 2: Same Path, OVERWRITE = FALSE**

If you set `overwrite: false` in config:

#### **First Run:**
```sql
COPY INTO 's3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/'
FROM (SELECT * FROM ANALYTICS.BI.DIMPAYER)
OVERWRITE = FALSE;
```

**Result:**
```
s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/
└── data_0_0_0.snappy.parquet  (created)
```

#### **Second Run (Same Path):**
```sql
COPY INTO 's3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/'
FROM (SELECT * FROM ANALYTICS.BI.DIMPAYER)
OVERWRITE = FALSE;
```

**Result:**
```
❌ ERROR: Files already exist in the path
```

**Snowflake will fail with error:** `Files already exist and OVERWRITE is set to FALSE`

---

### **Scenario 3: Different Path Each Run (Recommended)**

Use unique run_id for each migration:

#### **Run 1:**
```python
s3_path = "s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/"
```

#### **Run 2:**
```python
s3_path = "s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_110000/"
```

**Result:**
```
s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/
├── run_20251221_100000/
│   └── data_0_0_0.snappy.parquet  (kept)
└── run_20251221_110000/
    └── data_0_0_0.snappy.parquet  (new)
```

**✅ Both runs preserved, no conflicts**

---

## 🛠️ **Configuration Options**

### **Option 1: Always Overwrite (Current Default)**

**Best for:**
- Testing and development
- When you only need the latest data
- When storage cost is a concern

**Configuration:**
```json
{
  "snowflake_unload": {
    "overwrite": true
  },
  "s3_staging": {
    "cleanup_after_load": false
  }
}
```

**Behavior:**
- ✅ Second run to same path overwrites files
- ✅ No error on re-run
- ✅ Lower S3 storage costs

---

### **Option 2: Never Overwrite (Strict)**

**Best for:**
- Production with audit requirements
- When you need history of all runs
- When errors should be explicit

**Configuration:**
```json
{
  "snowflake_unload": {
    "overwrite": false
  }
}
```

**Behavior:**
- ❌ Second run to same path will ERROR
- ✅ Forces you to use unique paths (run_id)
- ✅ Complete history preserved

---

### **Option 3: Unique Paths + Cleanup (Recommended)**

**Best for:**
- Production migrations
- Balance between history and storage
- Clean S3 bucket

**Configuration:**
```json
{
  "snowflake_unload": {
    "overwrite": true,
    "include_query_id": true
  },
  "s3_staging": {
    "cleanup_after_load": true
  }
}
```

**Behavior:**
1. ✅ Each run uses unique path (timestamp-based run_id)
2. ✅ Files loaded into PostgreSQL
3. ✅ Python deletes S3 files after successful load
4. ✅ Keeps S3 clean, saves costs

---

## 🔧 **How to Change Behavior**

### **Method 1: Update Config File**

Edit `s3copyconfig.json`:

```json
{
  "snowflake_unload": {
    "overwrite": true,  ← Change to false
    "include_query_id": true
  },
  "s3_staging": {
    "cleanup_after_load": false  ← Change to true for auto-cleanup
  }
}
```

---

### **Method 2: Override in Code**

When calling the unload function:

```python
# In test_snowflake_unload.py or your script
result = unloader.unload_table(
    source_database="ANALYTICS",
    source_schema="BI",
    source_table="DIMPAYER",
    s3_path=s3_path,
    overwrite=False  ← Override here
)
```

---

## 🎯 **Recommended Strategy by Use Case**

### **For Testing (Phase 1-3, Current):**

```json
{
  "snowflake_unload": {
    "overwrite": true  ← Allow re-runs without errors
  }
}
```

**Why:**
- You'll re-run tests multiple times
- Don't want to create new folders each time
- Easy to iterate

**Path pattern:**
```
ANALYTICS/BI/DIMPAYER/test_100_rows/  ← Fixed path, overwrite each test
```

---

### **For Development (Iterating on Full DIMPAYER):**

```json
{
  "snowflake_unload": {
    "overwrite": true
  }
}
```

**Path pattern:**
```python
# Use fixed run_id for development
s3_path = "s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/dev_latest/"
```

**Benefit:** Always overwrite `dev_latest/` folder during development

---

### **For Production (Phase 8, Final):**

```json
{
  "snowflake_unload": {
    "overwrite": true,
    "include_query_id": true
  },
  "s3_staging": {
    "cleanup_after_load": true
  }
}
```

**Path pattern:**
```python
# Unique run_id each time
run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
s3_path = f"s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_{run_id}/"
```

**Workflow:**
1. Run 1: UNLOAD → `run_20251221_100000/` → Load to PG → Delete from S3
2. Run 2: UNLOAD → `run_20251221_110000/` → Load to PG → Delete from S3
3. S3 stays clean, each run has unique path

---

## 🗑️ **Cleanup Strategies**

### **Manual Cleanup (Current)**

```bash
# Delete specific run
aws s3 rm s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/run_20251221_100000/ --recursive

# Delete all DIMPAYER data
aws s3 rm s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/ --recursive

# Delete old test runs
aws s3 rm s3://cm-migration-dev01/ANALYTICS/BI/DIMPAYER/test_100_rows/ --recursive
```

---

### **Automated Cleanup (Future Phase 4-6)**

When PostgreSQL load is implemented:

```python
# After successful load to PostgreSQL
if load_successful and config.get('cleanup_after_load'):
    s3_manager.delete_files(s3_keys_loaded)
    logger.info("✅ Cleaned up S3 files after successful load")
```

---

### **S3 Lifecycle Policy (Production)**

Set bucket lifecycle rule in AWS:

```json
{
  "Rules": [
    {
      "Id": "DeleteOldMigrationFiles",
      "Status": "Enabled",
      "Prefix": "ANALYTICS/BI/",
      "Expiration": {
        "Days": 7
      }
    }
  ]
}
```

**Benefit:** Auto-delete files older than 7 days

---

## 📊 **Behavior Summary Table**

| Scenario | OVERWRITE | run_id | Behavior | Use Case |
|----------|-----------|--------|----------|----------|
| **Test Runs** | TRUE | Fixed (`test_100_rows`) | Overwrites each time | Testing |
| **Dev Runs** | TRUE | Fixed (`dev_latest`) | Overwrites each time | Development |
| **Prod (No Cleanup)** | TRUE | Unique (timestamp) | New folder each run | Audit trail |
| **Prod (With Cleanup)** | TRUE | Unique (timestamp) | New folder + auto-delete | Clean S3 |
| **Strict Mode** | FALSE | Unique (timestamp) | Error if exists | High security |

---

## 🎯 **Recommended: Current Setup**

For your testing phase, the **current default is perfect**:

```json
{
  "snowflake_unload": {
    "overwrite": true
  }
}
```

**Why:**
- ✅ Test runs won't error on re-run
- ✅ Can iterate quickly
- ✅ Fixed paths like `test_100_rows/` get overwritten
- ✅ Easy to verify results (same location each time)

---

## 🚀 **When You Deploy to Production**

Switch to unique paths + cleanup:

```json
{
  "snowflake_unload": {
    "overwrite": true,
    "include_query_id": true
  },
  "s3_staging": {
    "cleanup_after_load": true
  }
}
```

Plus add S3 lifecycle policy for safety net.

---

## ✅ **Quick Answers**

**Q: Are files deleted on second run?**  
**A:** With default `overwrite: true` - **YES** ✅

**Q: Can I keep multiple runs?**  
**A:** **YES** - Use unique run_id for each run ✅

**Q: Can I prevent overwrite?**  
**A:** **YES** - Set `overwrite: false` in config ✅

**Q: Can I auto-cleanup after load?**  
**A:** **YES** - Set `cleanup_after_load: true` (Phase 4+) ✅

---

## 📝 **For Your Current Testing**

**Keep the defaults!** Your current config is perfect for testing:

```json
{
  "snowflake_unload": {
    "overwrite": true  ← Perfect for testing
  }
}
```

**This means:**
- ✅ Run `test_snowflake_unload.py --table DIMPAYER --rows 100` multiple times
- ✅ Each run overwrites `DIMPAYER/test_100_rows/` folder
- ✅ No errors, clean iteration
- ✅ Easy to verify same location each time

**Later for production:** Switch to unique paths + cleanup strategy!

