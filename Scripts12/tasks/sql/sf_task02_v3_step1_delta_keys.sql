-- ============================================================================
-- Task 02 v3: Step 1 - Create Delta Keys Temp Table (ASYMMETRIC MODE ONLY)
-- ============================================================================
-- 
-- PURPOSE: Extract unique (VisitDate, SSN) combinations from recently updated visits
--          to identify which caregivers and dates need comprehensive conflict checking
--
-- PERFORMANCE: Fast (~10-20 seconds) - processes only 32-hour window
-- OUTPUT: Temp table with thousands of rows (one per unique date+SSN combo)
--
-- This temp table is used in Step 2 to expand the search scope beyond just delta
-- records to include ALL visits on the same date with the same SSN.
-- ============================================================================

CREATE TEMPORARY TABLE IF NOT EXISTS delta_keys AS
SELECT DISTINCT 
  DATE(CR1."Visit Date") AS visit_date,
  TRIM(CAR."SSN") AS ssn
FROM 
  {sf_database}.{sf_schema}.FACTVISITCALLPERFORMANCE_CR AS CR1
  INNER JOIN {sf_database}.{sf_schema}.DIMCAREGIVER AS CAR 
    ON CAR."Caregiver Id" = CR1."Caregiver Id"
    AND TRIM(CAR."SSN") IS NOT NULL 
    AND TRIM(CAR."SSN") != ''
WHERE 
  CR1."Visit Updated Timestamp" >= DATEADD(HOUR, -{lookback_hours}, GETDATE())
  AND DATE(CR1."Visit Date") BETWEEN DATEADD(YEAR, -{lookback_years}, GETDATE()) 
                                 AND DATEADD(DAY, {lookforward_days}, GETDATE())
  AND CR1."Provider Id" NOT IN ({excluded_agencies})
  AND TRIM(CAR."SSN") NOT IN (SELECT ssn FROM excluded_ssns_temp);
