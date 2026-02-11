# Task 02 Conflict Detection - ECS/Fargate Container
# ===================================================
# Snowflake + PostgreSQL cross-database conflict detection pipeline
#
# Build:  docker build -t task02-conflict-updater .
# Run:    docker run --env-file .env task02-conflict-updater
#
# All secrets (DB credentials, RSA key) are injected as environment
# variables by the ECS task definition from AWS Secrets Manager.

FROM python:3.11-slim AS base

WORKDIR /app

# Install Python dependencies (layer cached separately from app code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY config/ config/
COPY lib/ lib/
COPY scripts/ scripts/
COPY sql/ sql/

# Default environment variables (overridden by ECS task definition)
ENV ACTION="" \
    LOG_LEVEL=INFO \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "scripts/main.py"]
