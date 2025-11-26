# Conflict Report Pipeline Migration

## Overview
Production-ready AWS Lambda-based ETL pipeline for migrating Snowflake SQL procedures to PostgreSQL with connection testing.

## Quick Start

### 1. Setup Environment
```powershell
cd Scripts05\Migration
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure Credentials
Create `.env` file with database credentials:
```env
# Postgres
POSTGRES_HOST=your-rds-endpoint
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

# Snowflake (optional)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
# ... see DEPLOYMENT_SUMMARY.md for complete list
```

### 3. Test Locally
```powershell
# Test Postgres connection
python scripts/lambda_handler.py test_postgres

# Test Snowflake connection
python scripts/lambda_handler.py test_snowflake
```

## AWS Deployment

### Build Package:
```powershell
.\deploy\build_lambda.ps1
```

### Deploy:
1. Upload `deploy/lambda_deployment.zip` to Lambda
2. Set handler: `lambda_handler.lambda_handler`
3. Attach psycopg2 Lambda Layer
4. Configure environment variables
5. Test: `{"action": "test_postgres"}`

## Actions Supported

| Action | Description | Status |
|--------|-------------|--------|
| `validate_config` | Validate environment variables | ✅ Working |
| `test_postgres` | Test Postgres connectivity | ✅ Working |
| `test_snowflake` | Test Snowflake connectivity | ✅ Working |
| `task_01` | Copy to temp table | 🔄 Future |
| `task_02` | Update conflicts | 🔄 Future |

## Documentation

- **[deploy/LAMBDA_DEPLOYMENT.md](deploy/LAMBDA_DEPLOYMENT.md)** - AWS Lambda deployment guide
- **[deploy/DEPLOYMENT_SUMMARY.md](deploy/DEPLOYMENT_SUMMARY.md)** - Implementation fixes & notes
- **[COMPLETE_DOCUMENTATION.md](COMPLETE_DOCUMENTATION.md)** - Full project documentation (legacy)


