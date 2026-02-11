-- Fetch MPH (Miles Per Hour) lookup table from Postgres
-- Used to map distance ranges to average travel speeds
-- Parameters: {pg_database}, {pg_schema}

SELECT "From", "To", "AverageMilesPerHour"
FROM {pg_database}.{pg_schema}.mph
ORDER BY "From"
