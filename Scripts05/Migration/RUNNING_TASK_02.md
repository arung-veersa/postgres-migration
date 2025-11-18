# Running Task 02 - Quick Guide

## 🚀 How to Run

### Simple Run
```powershell
cd C:\Users\ArunGupta\Repos\postgres-migration\Scripts05\Migration
py -B scripts\run_task_02.py
```

### Monitor in Real-Time (2 Terminal Windows)

**Terminal 1 - Run the script:**
```powershell
cd C:\Users\ArunGupta\Repos\postgres-migration\Scripts05\Migration
py -B scripts\run_task_02.py
```

**Terminal 2 - Watch logs:**
```powershell
cd C:\Users\ArunGupta\Repos\postgres-migration\Scripts05\Migration
Get-Content -Path .\logs\etl_pipeline.log -Wait -Tail 50
```

---

## 🛑 How to Stop

### Normal Stop
- Press **Ctrl+C** in the terminal running the script
- The script will gracefully shut down parallel workers

### Force Stop (if Ctrl+C doesn't work)
```powershell
# In a NEW PowerShell window:
taskkill /F /IM python.exe
```

---

## 📋 View Errors

### After a Run
```powershell
# View last 100 lines (includes errors)
py scripts\view_errors.py

# View last 200 lines
py scripts\view_errors.py --lines 200

# View ONLY error lines
py scripts\view_errors.py --errors-only
```

### Manual Log File Review
Log file location:
```
C:\Users\ArunGupta\Repos\postgres-migration\Scripts05\Migration\logs\etl_pipeline.log
```

Open in any text editor (VS Code, Notepad++, etc.)

---

## ⚙️ Configuration

### Adjust Parallel Workers

Edit `config/settings.py`:
```python
MAX_WORKERS = 2  # Reduce to 1 or 2 for easier debugging
MAX_WORKERS = 4  # Default for balanced performance
MAX_WORKERS = 6  # Increase for faster execution (if system allows)
```

**After changing:** Clear Python cache and restart:
```powershell
Remove-Item -Path .\src\__pycache__ -Recurse -Force -ErrorAction SilentlyContinue
py -B scripts\run_task_02.py
```

---

## 🔍 Troubleshooting

### Logs Scrolling Too Fast
1. **Reduce workers** to 1 or 2 (see Configuration above)
2. **Use 2-terminal approach** (Terminal 1: run, Terminal 2: watch logs)
3. **Review log file after** using `py scripts\view_errors.py`

### Ctrl+C Not Working
- Wait a few seconds (it needs to finish current batch)
- If still stuck, use force stop command above

### Want to See What Went Wrong
```powershell
# Quick check - last 100 lines
py scripts\view_errors.py

# Only errors
py scripts\view_errors.py --errors-only

# More detail
py scripts\view_errors.py --lines 500
```

---

## 📊 Expected Output

### Success Indicators
```
INFO - Processing 97 SSN batches with 2 parallel workers
INFO - [1/97] Batch 5 (SSN 04) complete: 849 records updated
INFO - [2/97] Batch 1 (SSN 00) complete: 696 records updated
INFO - All batches complete: 50000 total records updated
INFO - Success rate: 97/97 batches
```

### Warning Signs
```
ERROR - [5/97] Batch 12 (SSN prefix '11') FAILED: Connection timeout
WARNING - Completed with 3 failed batches
```

If you see errors, review the log file for full details:
```powershell
py scripts\view_errors.py --errors-only
```

---

## 💡 Performance Tips

1. **Optimal workers:** Start with `MAX_WORKERS = 4`, adjust based on system resources
2. **Monitor system:** Watch CPU/Memory usage, reduce workers if system struggles
3. **Network issues:** If Snowflake queries fail, reduce workers to avoid connection pool exhaustion
4. **Database load:** Check Postgres CPU usage, scale up if needed

---

## 📞 Common Issues

### Issue: "No module named 'src'"
**Solution:**
```powershell
# Make sure you're in the Migration directory
cd C:\Users\ArunGupta\Repos\postgres-migration\Scripts05\Migration
py -B scripts\run_task_02.py
```

### Issue: Python using old cached code
**Solution:**
```powershell
# Clear cache
Remove-Item -Path .\src\__pycache__ -Recurse -Force -ErrorAction SilentlyContinue
# Run with cache bypass
py -B scripts\run_task_02.py
```

### Issue: Snowflake connection errors
**Solution:**
1. Check `config/.env` file has correct Snowflake credentials
2. Test Snowflake connection: `py scripts\test_connections.py`
3. Check Snowflake warehouse is running

---

## 🎯 Quick Reference

| Task | Command |
|------|---------|
| Run script | `py -B scripts\run_task_02.py` |
| Stop script | `Ctrl+C` or `taskkill /F /IM python.exe` |
| View errors | `py scripts\view_errors.py` |
| View log file | Open `logs\etl_pipeline.log` |
| Clear cache | `Remove-Item -Path .\src\__pycache__ -Recurse -Force` |
| Change workers | Edit `config/settings.py` → `MAX_WORKERS = X` |

