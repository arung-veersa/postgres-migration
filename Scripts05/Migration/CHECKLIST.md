# TASK_01 Implementation Checklist

## 📋 Pre-Execution Checklist

### Environment Setup
- [ ] Python 3.8+ installed
- [ ] Virtual environment created
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] `.env` file created with credentials
- [ ] Logs directory will be auto-created

### Database Prerequisites
- [ ] Snowflake Analytics database accessible (read-only)
- [ ] Postgres ConflictReport database accessible (read-write)
- [ ] Required Postgres tables exist:
  - [ ] `PAYER_PROVIDER_REMINDERS`
  - [ ] `CONFLICTVISITMAPS`
  - [ ] `CONFLICTVISITMAPS_TEMP`
  - [ ] `CONFLICTS`
  - [ ] `SETTINGS`

### Connection Tests
- [ ] Snowflake connection successful (`python scripts/test_connections.py`)
- [ ] Postgres connection successful
- [ ] Can read from Analytics database
- [ ] Can write to ConflictReport database

### Code Quality
- [ ] All unit tests pass (`pytest tests/unit/ -v`)
- [ ] Test coverage >90% (`pytest --cov=src`)
- [ ] No linter errors (`flake8 src/`)

## 🚀 Execution Checklist

### Before Running
- [ ] Backup `CONFLICTVISITMAPS_TEMP` if it contains important data
- [ ] Note current row count in `PAYER_PROVIDER_REMINDERS`
- [ ] Verify date range configuration is correct
- [ ] Check disk space (temp table may be large)

### During Execution
- [ ] Monitor logs (`tail -f logs/etl_pipeline.log`)
- [ ] Watch for errors or warnings
- [ ] Check memory usage if dataset is large
- [ ] Note execution time

### After Execution
- [ ] Task completed with status='success'
- [ ] No errors in log file
- [ ] Duration is reasonable (<10 minutes)
- [ ] All 4 steps completed:
  - [ ] Payer-provider reminders synced
  - [ ] Temp table truncated
  - [ ] Data copied to temp
  - [ ] Settings flag updated

## ✅ Validation Checklist

### Run Validation Script
- [ ] Execute: `python scripts/validate_task_01.py`
- [ ] All validations pass (3/3)

### Manual Validation (Optional)

**PAYER_PROVIDER_REMINDERS**
```sql
SELECT COUNT(*) FROM "public"."PAYER_PROVIDER_REMINDERS";
-- Should match Analytics dimension count
```

**CONFLICTVISITMAPS_TEMP**
```sql
-- Check row count
SELECT COUNT(*) FROM "public"."CONFLICTVISITMAPS_TEMP";

-- Verify date range
SELECT MIN("VisitDate"), MAX("VisitDate") 
FROM "public"."CONFLICTVISITMAPS_TEMP";

-- Sample data
SELECT * FROM "public"."CONFLICTVISITMAPS_TEMP" LIMIT 10;
```

**SETTINGS**
```sql
SELECT "InProgressFlag" FROM "public"."SETTINGS";
-- Should be 1
```

### Data Quality Checks
- [ ] No NULL values in key columns
- [ ] Date range matches expected filter
- [ ] StatusFlag values are valid ('U', 'R', 'D', etc.)
- [ ] VisitID and ConVisitID are populated
- [ ] CreatedDate is current timestamp

## 📊 Results Documentation

### Record Metrics
- [ ] Payer-Provider Reminders inserted: _______
- [ ] Payer-Provider Reminders updated: _______
- [ ] Temp table rows: _______
- [ ] Execution duration: _______ seconds
- [ ] Any warnings: _______

### Compare with Original SQL
- [ ] Row counts match historical runs (±5%)
- [ ] No data loss
- [ ] Performance is acceptable

## 🔧 Troubleshooting Checklist

### If Connection Fails
- [ ] Check `.env` credentials
- [ ] Verify network access
- [ ] Check firewall rules
- [ ] Verify database permissions
- [ ] Test with `psql` or `snowsql` CLI tools

### If Tests Fail
- [ ] Check Python version (3.8+)
- [ ] Reinstall dependencies
- [ ] Clear `__pycache__` directories
- [ ] Check for conflicting packages

### If Execution Fails
- [ ] Check logs for error details
- [ ] Verify all tables exist
- [ ] Check disk space
- [ ] Verify date range is reasonable
- [ ] Check for database locks

### If Validation Fails
- [ ] Check if TASK_01 completed successfully
- [ ] Verify source data exists
- [ ] Check date filter
- [ ] Compare row counts manually
- [ ] Review logs for clues

## 🎯 Success Criteria

All of the following must be true:
- [x] Unit tests pass (95%+ coverage)
- [ ] Connection tests pass
- [ ] TASK_01 executes without errors
- [ ] Validation script passes (3/3)
- [ ] Row counts are reasonable
- [ ] Execution time <10 minutes
- [ ] No data quality issues
- [ ] Logs show no errors

## 📝 Sign-Off

**Executed By**: _______________________

**Date**: _______________________

**Environment**: [ ] Dev [ ] Staging [ ] Prod

**Result**: [ ] Success [ ] Failed

**Notes**:
_____________________________________________
_____________________________________________
_____________________________________________

## 🚦 Next Steps After Success

- [ ] Document any issues encountered
- [ ] Update team on completion
- [ ] Archive logs for reference
- [ ] Proceed to Phase 2 (TASK_02)
- [ ] Schedule regular runs if applicable

