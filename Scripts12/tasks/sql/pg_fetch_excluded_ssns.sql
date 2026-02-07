-- Fetch excluded SSNs from Postgres
-- Used to filter out specific caregivers from conflict detection
-- Parameters: {pg_database}, {pg_schema}
SELECT "SSN"
FROM {pg_database}.{pg_schema}.excluded_ssn
WHERE "SSN" IS NOT NULL
  AND TRIM("SSN") != ''