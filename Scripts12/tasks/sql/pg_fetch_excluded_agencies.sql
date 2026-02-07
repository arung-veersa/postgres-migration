-- Fetch excluded agencies from Postgres
-- Used to filter out agencies from conflict detection
-- Parameters: {pg_database}, {pg_schema}

SELECT "ProviderID"
FROM {pg_database}.{pg_schema}.excluded_agency
WHERE "ProviderID" IS NOT NULL
