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

### 4. Run TASK_01

**Windows:** `py scripts\run_task_01.py`  
**Linux/Mac:** `python scripts/run_task_01.py`

### 5. Validate Results

**Windows:** `py scripts\validate_task_01.py`  
**Linux/Mac:** `python scripts/validate_task_01.py`

## Migration Status

| Task | Status | Coverage | Validated |
|------|--------|----------|-----------|
| TASK_01 | ✅ Complete | 95% | ✅ Yes |
| TASK_02 | ⏳ Pending | - | - |
| TASK_03 | ⏳ Pending | - | - |

## Documentation
- [Phase 1 Guide](docs/phase1_guide.md)
- [Testing Strategy](docs/testing_strategy.md)
- [Deployment Guide](docs/deployment.md)

