-- Fetch settings from Postgres
-- Contains ExtraDistancePer parameter for geospatial calculations
-- Parameters: {pg_database}, {pg_schema}

SELECT "ExtraDistancePer"
FROM {pg_database}.{pg_schema}.settings
LIMIT 1
