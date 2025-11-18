# Conflict Report Pipeline Migration

## Overview
Python-based migration of Snowflake SQL procedures to a modern, maintainable ETL pipeline.

## Project Structure
```
Migration/
├── config/                     # Configuration files
├── src/                        # Source code
│   ├── connectors/            # Database connections
│   ├── tasks/                 # Task implementations
│   ├── loaders/               # Data loaders
│   └── utils/                 # Utilities
├── tests/                     # Test suite
├── scripts/                   # Standalone scripts
└── docs/                      # Documentation
```

## Quick Start

### 🪟 **Windows Users** → See [`WINDOWS_SETUP.md`](WINDOWS_SETUP.md) for detailed setup!

### 1. Setup Environment

**Windows (PowerShell):**
```powershell
cd Scripts05\Migration
py -3 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**Linux/Mac:**
```bash
cd Scripts05/Migration
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Credentials
```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 3. Run Tests
```bash
pytest tests/ -v
```

### 4. Create Postgres View (First Time Only)

```bash
psql -h localhost -U your_user -d conflictreport -f sql/views/vw_conflictvisitmaps_base.sql
```

### 5. Run Tasks

**TASK_01:**
```bash
# Windows
py scripts\run_task_01.py

# Linux/Mac
python scripts/run_task_01.py
```

**TASK_02:**
```bash
# Windows
py scripts\run_task_02.py

# Linux/Mac
python scripts/run_task_02.py
```

**Performance Configuration:**
- Default: 4 parallel workers (~20-30 minutes for full run)
- Adjust in `.env`: `MAX_WORKERS=6` for faster processing
- See `docs/task_02_phase1_optimizations_implemented.md` for details

### 6. Validate Results

**Windows:** `py scripts\validate_task_01.py`  
**Linux/Mac:** `python scripts/validate_task_01.py`

## Migration Status

| Task | Status | Description | Documentation |
|------|--------|-------------|---------------|
| TASK_01 | ✅ Complete | Copy to Temp | [Phase 1 Guide](docs/phase1_guide.md) |
| TASK_02 | ✅ Complete | Update Conflicts | [Task 02 Guide](docs/task_02_implementation.md) |
| TASK_03 | ⏳ Pending | Insert from Main | - |

## Documentation
- [Phase 1 Guide](docs/phase1_guide.md) - TASK_01 implementation
- [Task 02 Implementation](docs/task_02_implementation.md) - TASK_02 implementation
- [Testing Strategy](docs/testing_strategy.md)
- [Deployment Guide](docs/deployment.md)

