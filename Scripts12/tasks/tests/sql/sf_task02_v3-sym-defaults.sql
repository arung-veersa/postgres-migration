-- ============================================================================
-- Task 02 v3: SYMMETRIC MODE - Test Query with Default Values
-- ============================================================================
-- Generated for direct Snowflake execution via DBeaver or similar client
-- 
-- MODE: SYMMETRIC (enable_asymmetric_join = false)
-- DESCRIPTION: Only processes visits updated in last 36 hours
-- EXPECTED ROWS IN base_visits: ~70,000
-- EXPECTED RUNTIME: 30-40 seconds
-- 
-- NOTE: This query uses temp tables and must be run in a single session.
--       All steps below must be executed sequentially.
-- ============================================================================

-- STEP 0: Create excluded SSNs temp table
-- ============================================================================
-- Populate this from the Postgres excluded_ssn table.
-- Run: SELECT "SSN" FROM conflict_dev.excluded_ssn WHERE "SSN" IS NOT NULL
-- Then paste the values below.
-- ============================================================================
CREATE TEMPORARY TABLE IF NOT EXISTS excluded_ssns_temp (ssn VARCHAR);
-- INSERT INTO excluded_ssns_temp (ssn) VALUES ('ssn1'),('ssn2'),('ssn3');


-- ============================================================================
-- STEP 1: Create delta_keys temp table (used for stale cleanup scoping)
-- ============================================================================
-- In symmetric mode, delta_keys is used to collect distinct SSNs for
-- scoping stale cleanup marking in Postgres.
-- ============================================================================
CREATE TEMPORARY TABLE IF NOT EXISTS delta_keys AS
SELECT DISTINCT 
  DATE(CR1."Visit Date") AS visit_date,
  TRIM(CAR."SSN") AS ssn
FROM 
  ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
  INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR 
    ON CAR."Caregiver Id" = CR1."Caregiver Id"
    AND TRIM(CAR."SSN") IS NOT NULL 
    AND TRIM(CAR."SSN") != ''
WHERE 
  CR1."Visit Updated Timestamp" >= DATEADD(HOUR, -36, GETDATE())
  AND DATE(CR1."Visit Date") BETWEEN DATEADD(YEAR, -2, GETDATE()) 
                                 AND DATEADD(DAY, 45, GETDATE())
  AND CR1."Provider Id" NOT IN ('d65b83d5-eb23-4a20-9e64-288ddcd7e0f1','e1391287-3089-4db6-9603-0357ce908b67','aaa64975-c24a-421b-8239-f148001873e0')
  AND TRIM(CAR."SSN") NOT IN (SELECT ssn FROM excluded_ssns_temp);

-- Verify delta_keys count:
-- SELECT COUNT(*) FROM delta_keys;


-- ============================================================================
-- STEP 2d: Collect actual (visit_date, ssn) pairs for stale cleanup scoping
-- ============================================================================
-- Lambda streams these exact pairs to PostgreSQL _tmp_delta_pairs via COPY.
-- This gives pair-precise stale scoping (avoids the cross-product problem
-- of separate DISTINCT SSN + DISTINCT date lists).
-- SELECT visit_date, ssn FROM delta_keys;
-- Expected: ~12M pairs (~507K distinct SSNs, ~597 distinct dates)


-- ============================================================================
-- STEP 2: Create base_visits with delta rows only (SYMMETRIC MODE)
-- ============================================================================
-- In symmetric mode there is only one step: all rows come from the 36-hour
-- delta window and get is_delta = 1. No Part B INSERT needed.
-- Result: ~70K rows, ~30-40 seconds
-- ============================================================================

CREATE TEMPORARY TABLE IF NOT EXISTS base_visits AS
SELECT 
  DISTINCT 
  CAST(NULL AS STRING) AS "CONFLICTID",
  1 AS "is_delta",  -- All rows are delta in symmetric mode
  CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
  CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth",
  TRIM(CAR."SSN") AS "SSN",
  CAST(NULL AS STRING) AS "PStatus",
  CAR."Status" AS "AideStatus",
  CR1."Missed Visit Reason" AS "MissedVisitReason",
  CR1."Is Missed" AS "IsMissed",
  CR1."Call Out Device Type" AS "EVVType",
  CR1."Billed Rate" AS "BilledRate",
  CR1."Total Billed Amount" AS "TotalBilledAmount",
  CR1."Provider Id" AS "ProviderID",
  CR1."Application Provider Id" AS "AppProviderID",
  DPR."Provider Name" AS "ProviderName",
  CAST(NULL AS STRING) AS "AgencyContact",
  DPR."Phone Number 1" AS "AgencyPhone",
  DPR."Federal Tax Number" AS "FederalTaxNumber",
  CR1."Visit Id" AS "VisitID",
  CR1."Application Visit Id" AS "AppVisitID",
  DATE(CR1."Visit Date") AS "VisitDate",
  CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime",
  CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime",
  CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime",
  CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime",
  CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime",
  CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime",
  CR1."Caregiver Id" AS "CaregiverID",
  CR1."Application Caregiver Id" AS "AppCaregiverID",
  CAR."Caregiver Code" AS "AideCode",
  CAR."Caregiver Fullname" AS "AideName",
  CAR."Caregiver Firstname" AS "AideFName",
  CAR."Caregiver Lastname" AS "AideLName",
  TRIM(CAR."SSN") AS "AideSSN",
  CR1."Office Id" AS "OfficeID",
  CR1."Application Office Id" AS "AppOfficeID",
  DOF."Office Name" AS "Office",
  CR1."Payer Patient Id" AS "PA_PatientID",
  CR1."Application Payer Patient Id" AS "PA_AppPatientID",
  CR1."Provider Patient Id" AS "P_PatientID",
  CR1."Application Provider Patient Id" AS "P_AppPatientID",
  CR1."Patient Id" AS "PatientID",
  CR1."Application Patient Id" AS "AppPatientID",
  CAST(NULL AS STRING) AS "PAdmissionID",
  CAST(NULL AS STRING) AS "PName",
  CAST(NULL AS STRING) AS "PFName",
  CAST(NULL AS STRING) AS "PLName",
  CAST(NULL AS STRING) AS "PMedicaidNumber",
  CAST(NULL AS STRING) AS "PAddressID",
  CAST(NULL AS STRING) AS "PAppAddressID",
  CAST(NULL AS STRING) AS "PAddressL1",
  CAST(NULL AS STRING) AS "PAddressL2",
  CAST(NULL AS STRING) AS "PCity",
  CAST(NULL AS STRING) AS "PAddressState",
  CAST(NULL AS STRING) AS "PZipCode",
  CAST(NULL AS STRING) AS "PCounty",
  -- Coordinate priority: Call Out > Call In > Provider Address
  CASE 
    WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != ',' 
    THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", ',')[1], '"', CAST(NULL AS NUMBER))
    WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != ',' 
    THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", ',')[1], '"', CAST(NULL AS NUMBER))
    ELSE DPAD_P."Provider_Longitude" 
  END AS "Longitude",
  CASE 
    WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != ',' 
    THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", ',')[0], '"', CAST(NULL AS NUMBER))
    WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != ',' 
    THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", ',')[0], '"', CAST(NULL AS NUMBER))
    ELSE DPAD_P."Provider_Latitude" 
  END AS "Latitude",
  CR1."Payer Id" AS "PayerID",
  CR1."Application Payer Id" AS "AppPayerID",
  COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract",
  SPA."Payer State" AS "PayerState",
  CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate",
  CR1."Billed Hours" AS "BilledHours",
  CR1."Billed" AS "Billed",
  DSC."Service Code Id" AS "ServiceCodeID",
  DSC."Application Service Code Id" AS "AppServiceCodeID",
  CR1."Bill Type" AS "RateType",
  DSC."Service Code" AS "ServiceCode",
  CAST(CR1."Visit Updated Timestamp" AS timestamp) AS "LastUpdatedDate",
  DUSR."User Fullname" AS "LastUpdatedBy",
  DPA_P."Admission Id" AS "P_PAdmissionID",
  DPA_P."Patient Name" AS "P_PName",
  DPA_P."Patient Firstname" AS "P_PFName",
  DPA_P."Patient Lastname" AS "P_PLName",
  DPA_P."Medicaid Number" AS "P_PMedicaidNumber",
  DPA_P."Status" AS "P_PStatus",
  DPAD_P."Patient Address Id" AS "P_PAddressID",
  DPAD_P."Application Patient Address Id" AS "P_PAppAddressID",
  DPAD_P."Address Line 1" AS "P_PAddressL1",
  DPAD_P."Address Line 2" AS "P_PAddressL2",
  DPAD_P."City" AS "P_PCity",
  DPAD_P."Address State" AS "P_PAddressState",
  DPAD_P."Zip Code" AS "P_PZipCode",
  DPAD_P."County" AS "P_PCounty",
  DPA_PA."Admission Id" AS "PA_PAdmissionID",
  DPA_PA."Patient Name" AS "PA_PName",
  DPA_PA."Patient Firstname" AS "PA_PFName",
  DPA_PA."Patient Lastname" AS "PA_PLName",
  DPA_PA."Medicaid Number" AS "PA_PMedicaidNumber",
  DPA_PA."Status" AS "PA_PStatus",
  DPAD_PA."Patient Address Id" AS "PA_PAddressID",
  DPAD_PA."Application Patient Address Id" AS "PA_PAppAddressID",
  DPAD_PA."Address Line 1" AS "PA_PAddressL1",
  DPAD_PA."Address Line 2" AS "PA_PAddressL2",
  DPAD_PA."City" AS "PA_PCity",
  DPAD_PA."Address State" AS "PA_PAddressState",
  DPAD_PA."Zip Code" AS "PA_PZipCode",
  DPAD_PA."County" AS "PA_PCounty",
  CASE 
    WHEN CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" != '0' THEN 'Internal'
    WHEN CR1."Application Payer Id" != '0' AND CR1."Application Contract Id" != '0' THEN 'UPR'
    WHEN CR1."Application Payer Id" != '0' AND CR1."Application Contract Id" = '0' THEN 'Payer'
  END AS "ContractType"
FROM 
  ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
  INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR 
    ON CAR."Caregiver Id" = CR1."Caregiver Id"
    AND TRIM(CAR."SSN") IS NOT NULL 
    AND TRIM(CAR."SSN") != ''
  LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF 
    ON DOF."Office Id" = CR1."Office Id" AND DOF."Is Active" = TRUE
  LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_P 
    ON DPA_P."Patient Id" = CR1."Provider Patient Id"
  LEFT JOIN (
    SELECT 
      "Patient Address Id", "Application Patient Address Id", "Address Line 1", "Address Line 2",
      "City", "Address State", "Zip Code", "County", "Patient Id", "Application Patient Id",
      "Longitude" AS "Provider_Longitude", "Latitude" AS "Provider_Latitude",
      ROW_NUMBER() OVER (PARTITION BY "Patient Id" ORDER BY "Application Created UTC Timestamp" DESC) AS rn
    FROM ANALYTICS.BI.DIMPATIENTADDRESS
    WHERE "Primary Address" = TRUE AND "Address Type" LIKE '%GPS%'
  ) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" AND DPAD_P.RN = 1
  LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_PA 
    ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
  LEFT JOIN (
    SELECT 
      "Patient Address Id", "Application Patient Address Id", "Address Line 1", "Address Line 2",
      "City", "Address State", "Zip Code", "County", "Patient Id", "Application Patient Id",
      ROW_NUMBER() OVER (PARTITION BY "Patient Id" ORDER BY "Application Created UTC Timestamp" DESC) AS rn
    FROM ANALYTICS.BI.DIMPATIENTADDRESS
    WHERE "Primary Address" = TRUE AND "Address Type" LIKE '%GPS%'
  ) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" AND DPAD_PA.RN = 1
  LEFT JOIN ANALYTICS.BI.DIMPAYER AS SPA 
    ON SPA."Payer Id" = CR1."Payer Id" AND SPA."Is Active" = TRUE AND SPA."Is Demo" = FALSE
  LEFT JOIN ANALYTICS.BI.DIMCONTRACT AS DCON 
    ON DCON."Contract Id" = CR1."Contract Id" AND DCON."Is Active" = TRUE
  INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR 
    ON DPR."Provider Id" = CR1."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
  LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC 
    ON DSC."Service Code Id" = CR1."Service Code Id"
  LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR 
    ON DUSR."User Id" = CR1."Visit Updated User Id"
WHERE 
  DATE(CR1."Visit Date") BETWEEN DATEADD(YEAR, -2, GETDATE()) 
                             AND DATEADD(DAY, 45, GETDATE())
  AND CR1."Provider Id" NOT IN ('d65b83d5-eb23-4a20-9e64-288ddcd7e0f1','e1391287-3089-4db6-9603-0357ce908b67','aaa64975-c24a-421b-8239-f148001873e0')
  AND TRIM(CAR."SSN") NOT IN (SELECT ssn FROM excluded_ssns_temp)
  AND CR1."Visit Updated Timestamp" >= DATEADD(HOUR, -36, GETDATE());


-- STEP 3: Final conflict detection query
-- ============================================================================
-- Task 02 v3: Step 3 - Final Conflict Detection Query (Self-Join on base_visits)
-- ============================================================================
--
-- PURPOSE: Self-join the base_visits temp table to find conflict pairs,
--          apply 7 conflict detection rules, and return conflicts
--
-- In SYMMETRIC mode, the join condition is unconstrained (all-vs-all within ~70K rows).
-- In ASYMMETRIC mode, an additional join condition (V1."is_delta" = 1) ensures at least
-- one side of each pair is a delta visit, avoiding the expensive all-vs-all on 9.6M rows.
--
-- SYMMETRIC MODE: base_visits contains ~70K rows (36-hour delta only)
-- ASYMMETRIC MODE: base_visits contains ~9.6M rows (delta + related records)
--
-- PERFORMANCE: 
--   - Symmetric: ~30-40 seconds
--   - Asymmetric: ~3-5 minutes (estimated)
-- ============================================================================

WITH
-- CTE 1: MPH lookup data (hardcoded reference data for distance-based conflict detection)
mph_data AS (
  SELECT 0 AS "From", 2 AS "To", 2 AS "AverageMilesPerHour"
  UNION ALL
  SELECT 2 AS "From", 5 AS "To", 6 AS "AverageMilesPerHour"
  UNION ALL
  SELECT 5 AS "From", 10 AS "To", 15 AS "AverageMilesPerHour"
  UNION ALL
  SELECT 10 AS "From", 25 AS "To", 25 AS "AverageMilesPerHour"
  UNION ALL
  SELECT 25 AS "From", 50 AS "To", 45 AS "AverageMilesPerHour"
  UNION ALL
  SELECT 50 AS "From", 100 AS "To", 55 AS "AverageMilesPerHour"
  UNION ALL
  SELECT 100 AS "From", 500 AS "To", 60 AS "AverageMilesPerHour"
),

-- CTE 2: Conflict pairs - Self-join base_visits temp table
conflict_pairs AS (
  SELECT 
    V1."CONFLICTID",
    V1."SSN",
    V1."ProviderID", V1."AppProviderID", V1."ProviderName",
    V1."VisitID", V1."AppVisitID", V1."VisitDate",
    V1."SchStartTime", V1."SchEndTime",
    V1."VisitStartTime", V1."VisitEndTime",
    V1."EVVStartTime", V1."EVVEndTime",
    V1."CaregiverID", V1."AppCaregiverID", V1."AideCode", V1."AideName", V1."AideFName", V1."AideLName", V1."AideSSN", V1."AideStatus",
    V1."OfficeID", V1."AppOfficeID", V1."Office",
    V1."PatientID", V1."AppPatientID", V1."PAdmissionID", V1."PName", V1."PFName", V1."PLName", V1."PMedicaidNumber",
    V1."PAddressID", V1."PAppAddressID", V1."PAddressL1", V1."PAddressL2", V1."PCity", V1."PAddressState", V1."PZipCode", V1."PCounty",
    V1."Longitude", V1."Latitude",
    V1."PayerID", V1."AppPayerID", V1."Contract", V1."PayerState",
    V1."BilledDate", V1."BilledHours", V1."Billed", V1."BilledRate", V1."TotalBilledAmount",
    V1."ServiceCodeID", V1."AppServiceCodeID", V1."RateType", V1."ServiceCode",
    V1."IsMissed", V1."MissedVisitReason", V1."EVVType",
    V1."PStatus",
    V1."P_PatientID", V1."P_AppPatientID", V1."P_PAdmissionID", V1."P_PName", V1."P_PFName", V1."P_PLName", V1."P_PMedicaidNumber", V1."P_PStatus",
    V1."P_PAddressID", V1."P_PAppAddressID", V1."P_PAddressL1", V1."P_PAddressL2", V1."P_PCity", V1."P_PAddressState", V1."P_PZipCode", V1."P_PCounty",
    V1."PA_PatientID", V1."PA_AppPatientID", V1."PA_PAdmissionID", V1."PA_PName", V1."PA_PFName", V1."PA_PLName", V1."PA_PMedicaidNumber", V1."PA_PStatus",
    V1."PA_PAddressID", V1."PA_PAppAddressID", V1."PA_PAddressL1", V1."PA_PAddressL2", V1."PA_PCity", V1."PA_PAddressState", V1."PA_PZipCode", V1."PA_PCounty",
    V1."ContractType", V1."BillRateNonBilled", V1."BillRateBoth", V1."FederalTaxNumber",
    V1."LastUpdatedDate", V1."LastUpdatedBy",
    V2."ProviderID" AS "ConProviderID", V2."AppProviderID" AS "ConAppProviderID", V2."ProviderName" AS "ConProviderName",
    V2."VisitID" AS "ConVisitID", V2."AppVisitID" AS "ConAppVisitID",
    V2."SchStartTime" AS "ConSchStartTime", V2."SchEndTime" AS "ConSchEndTime",
    V2."VisitStartTime" AS "ConVisitStartTime", V2."VisitEndTime" AS "ConVisitEndTime",
    V2."EVVStartTime" AS "ConEVVStartTime", V2."EVVEndTime" AS "ConEVVEndTime",
    V2."CaregiverID" AS "ConCaregiverID", V2."AppCaregiverID" AS "ConAppCaregiverID", V2."AideCode" AS "ConAideCode",
    V2."AideName" AS "ConAideName", V2."AideFName" AS "ConAideFName", V2."AideLName" AS "ConAideLName", V2."AideSSN" AS "ConAideSSN", V2."AideStatus" AS "ConAideStatus",
    V2."OfficeID" AS "ConOfficeID", V2."AppOfficeID" AS "ConAppOfficeID", V2."Office" AS "ConOffice",
    V2."PatientID" AS "ConPatientID", V2."AppPatientID" AS "ConAppPatientID", V2."PAdmissionID" AS "ConPAdmissionID", V2."PName" AS "ConPName",
    V2."PFName" AS "ConPFName", V2."PLName" AS "ConPLName", V2."PMedicaidNumber" AS "ConPMedicaidNumber",
    V2."PAddressID" AS "ConPAddressID", V2."PAppAddressID" AS "ConPAppAddressID",
    V2."PAddressL1" AS "ConPAddressL1", V2."PAddressL2" AS "ConPAddressL2", V2."PCity" AS "ConPCity",
    V2."PAddressState" AS "ConPAddressState", V2."PZipCode" AS "ConPZipCode", V2."PCounty" AS "ConPCounty",
    V2."Longitude" AS "ConLongitude", V2."Latitude" AS "ConLatitude",
    V2."PayerID" AS "ConPayerID", V2."AppPayerID" AS "ConAppPayerID", V2."Contract" AS "ConContract", V2."PayerState" AS "ConPayerState",
    V2."BilledDate" AS "ConBilledDate", V2."BilledHours" AS "ConBilledHours", V2."Billed" AS "ConBilled",
    V2."BilledRate" AS "ConBilledRate", V2."TotalBilledAmount" AS "ConTotalBilledAmount",
    V2."ServiceCodeID" AS "ConServiceCodeID", V2."AppServiceCodeID" AS "ConAppServiceCodeID", V2."RateType" AS "ConRateType", V2."ServiceCode" AS "ConServiceCode",
    V2."IsMissed" AS "ConIsMissed", V2."MissedVisitReason" AS "ConMissedVisitReason", V2."EVVType" AS "ConEVVType",
    V2."PStatus" AS "ConPStatus",
    V2."P_PatientID" AS "ConP_PatientID", V2."P_AppPatientID" AS "ConP_AppPatientID",
    V2."P_PAdmissionID" AS "ConP_PAdmissionID", V2."P_PName" AS "ConP_PName", V2."P_PFName" AS "ConP_PFName", V2."P_PLName" AS "ConP_PLName",
    V2."P_PMedicaidNumber" AS "ConP_PMedicaidNumber", V2."P_PStatus" AS "ConP_PStatus",
    V2."P_PAddressID" AS "ConP_PAddressID", V2."P_PAppAddressID" AS "ConP_PAppAddressID",
    V2."P_PAddressL1" AS "ConP_PAddressL1", V2."P_PAddressL2" AS "ConP_PAddressL2", V2."P_PCity" AS "ConP_PCity",
    V2."P_PAddressState" AS "ConP_PAddressState", V2."P_PZipCode" AS "ConP_PZipCode", V2."P_PCounty" AS "ConP_PCounty",
    V2."PA_PatientID" AS "ConPA_PatientID", V2."PA_AppPatientID" AS "ConPA_AppPatientID",
    V2."PA_PAdmissionID" AS "ConPA_PAdmissionID", V2."PA_PName" AS "ConPA_PName", V2."PA_PFName" AS "ConPA_PFName", V2."PA_PLName" AS "ConPA_PLName",
    V2."PA_PMedicaidNumber" AS "ConPA_PMedicaidNumber", V2."PA_PStatus" AS "ConPA_PStatus",
    V2."PA_PAddressID" AS "ConPA_PAddressID", V2."PA_PAppAddressID" AS "ConPA_PAppAddressID",
    V2."PA_PAddressL1" AS "ConPA_PAddressL1", V2."PA_PAddressL2" AS "ConPA_PAddressL2", V2."PA_PCity" AS "ConPA_PCity",
    V2."PA_PAddressState" AS "ConPA_PAddressState", V2."PA_PZipCode" AS "ConPA_PZipCode", V2."PA_PCounty" AS "ConPA_PCounty",
    V2."ContractType" AS "ConContractType", V2."BillRateNonBilled" AS "ConBillRateNonBilled", V2."BillRateBoth" AS "ConBillRateBoth",
    V2."FederalTaxNumber" AS "ConFederalTaxNumber",
    V2."LastUpdatedDate" AS "ConLastUpdatedDate", V2."LastUpdatedBy" AS "ConLastUpdatedBy"
  FROM base_visits V1
  INNER JOIN base_visits V2
    ON V1."VisitDate" = V2."VisitDate"
    AND V1."SSN" = V2."SSN"
    AND V1."ProviderID" != V2."ProviderID"
    AND V1."VisitID" != V2."VisitID"
),

-- CTE 3: Calculate geospatial metrics ONCE (optimization: was calculated 5+ times before)
spatial_calculations AS (
  SELECT 
    CP.*,
    -- Calculate distance once and reuse
    ROUND(
      (ST_DISTANCE(
        ST_MAKEPOINT(CP."Longitude", CP."Latitude"),
        ST_MAKEPOINT(CP."ConLongitude", CP."ConLatitude")
      ) / 1609) * 100,
      2
    ) AS "DistanceMiles",
    -- Calculate time difference once
    ABS(DATEDIFF(MINUTE, CP."VisitEndTime", CP."ConVisitStartTime")) AS "MinutesDiff"
  FROM conflict_pairs CP
),

-- CTE 4: Add MPH lookup and calculate ETA (reuses DistanceMiles)
conflict_with_eta AS (
  SELECT 
    SC.*,
    MPH."AverageMilesPerHour",
    -- Reuse DistanceMiles from previous CTE (no recalculation)
    ROUND((SC."DistanceMiles" / NULLIF(MPH."AverageMilesPerHour", 0)) * 60, 2) AS "ETATravelMinutes"
  FROM spatial_calculations SC
  LEFT JOIN mph_data AS MPH 
    ON SC."DistanceMiles" BETWEEN MPH."From" AND MPH."To"
),

-- CTE 5: Apply 7 conflict detection rules
-- NOTE: ProviderID != ConProviderID is already enforced by the conflict_pairs JOIN condition,
--       so it is not repeated in each rule below.
conflicts_with_flags AS (
  SELECT 
    CE.*,
    -- Rule 1: Same scheduled time (both visits not started)
    CASE 
      WHEN CE."VisitStartTime" IS NULL AND CE."VisitEndTime" IS NULL
           AND CE."ConVisitStartTime" IS NULL AND CE."ConVisitEndTime" IS NULL
           AND CE."SchStartTime" = CE."ConSchStartTime" AND CE."SchEndTime" = CE."ConSchEndTime"
      THEN 'Y' ELSE 'N' 
    END AS "SameSchTimeFlag",
    
    -- Rule 2: Same visit time (both visits completed)
    CASE 
      WHEN CE."VisitStartTime" IS NOT NULL AND CE."VisitEndTime" IS NOT NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND CE."VisitStartTime" = CE."ConVisitStartTime" AND CE."VisitEndTime" = CE."ConVisitEndTime"
      THEN 'Y' ELSE 'N' 
    END AS "SameVisitTimeFlag",
    
    -- Rule 3: Scheduled time matches conflicting visit time
    CASE 
      WHEN CE."VisitStartTime" IS NULL AND CE."VisitEndTime" IS NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND CE."SchStartTime" = CE."ConVisitStartTime" AND CE."SchEndTime" = CE."ConVisitEndTime"
      THEN 'Y' ELSE 'N' 
    END AS "SchVisitTimeSame",
    
    -- Rule 4: Scheduled time overlaps another scheduled time (excludes exact match = Rule 1)
    CASE 
      WHEN CE."VisitStartTime" IS NULL AND CE."VisitEndTime" IS NULL
           AND CE."ConVisitStartTime" IS NULL AND CE."ConVisitEndTime" IS NULL
           AND (CE."SchStartTime" < CE."ConSchEndTime" AND CE."SchEndTime" > CE."ConSchStartTime")
           AND NOT (CE."SchStartTime" = CE."ConSchStartTime" AND CE."SchEndTime" = CE."ConSchEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "SchOverAnotherSchTimeFlag",
    
    -- Rule 5: Visit time overlaps another visit time (excludes exact match = Rule 2)
    CASE 
      WHEN CE."VisitStartTime" IS NOT NULL AND CE."VisitEndTime" IS NOT NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND (CE."VisitStartTime" < CE."ConVisitEndTime" AND CE."VisitEndTime" > CE."ConVisitStartTime")
           AND NOT (CE."VisitStartTime" = CE."ConVisitStartTime" AND CE."VisitEndTime" = CE."ConVisitEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "VisitTimeOverAnotherVisitTimeFlag",
    
    -- Rule 6: Scheduled time overlaps visit time (excludes exact match = Rule 3)
    CASE 
      WHEN CE."VisitStartTime" IS NULL AND CE."VisitEndTime" IS NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND (CE."SchStartTime" < CE."ConVisitEndTime" AND CE."SchEndTime" > CE."ConVisitStartTime")
           AND NOT (CE."SchStartTime" = CE."ConVisitStartTime" AND CE."SchEndTime" = CE."ConVisitEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "SchTimeOverVisitTimeFlag",
    
    -- Rule 7: Distance flag (impossible travel distance - reuses ETATravelMinutes from CTE 4)
    CASE 
      WHEN CE."Longitude" IS NOT NULL AND CE."Latitude" IS NOT NULL
           AND CE."ConLongitude" IS NOT NULL AND CE."ConLatitude" IS NOT NULL
           AND CE."VisitStartTime" IS NOT NULL AND CE."VisitEndTime" IS NOT NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND ((CE."PZipCode" IS NOT NULL AND CE."ConPZipCode" IS NOT NULL AND CE."PZipCode" != CE."ConPZipCode")
                OR (CE."PZipCode" IS NULL OR CE."ConPZipCode" IS NULL))
           AND CE."AverageMilesPerHour" IS NOT NULL
           AND CE."MinutesDiff" >= 0
           AND CE."ETATravelMinutes" > CE."MinutesDiff"
      THEN 'Y' ELSE 'N' 
    END AS "DistanceFlag"
  FROM conflict_with_eta CE
),

-- CTE 6: Filter to actual conflicts (at least one rule triggered)
final_conflicts AS (
  SELECT * 
  FROM conflicts_with_flags
  WHERE 
    "SameSchTimeFlag" = 'Y'
    OR "SameVisitTimeFlag" = 'Y'
    OR "SchVisitTimeSame" = 'Y'
    OR "SchOverAnotherSchTimeFlag" = 'Y'
    OR "VisitTimeOverAnotherVisitTimeFlag" = 'Y'
    OR "SchTimeOverVisitTimeFlag" = 'Y'
    OR "DistanceFlag" = 'Y'
)

-- Final SELECT: Return all columns needed for UPDATE
SELECT 
  "CONFLICTID", "SSN",
  "ProviderID", "AppProviderID", "ProviderName", "VisitID", "AppVisitID",
  "ConProviderID", "ConAppProviderID", "ConProviderName", "ConVisitID", "ConAppVisitID",
  "VisitDate",
  "SchStartTime", "SchEndTime", "ConSchStartTime", "ConSchEndTime",
  "VisitStartTime", "VisitEndTime", "ConVisitStartTime", "ConVisitEndTime",
  "EVVStartTime", "EVVEndTime", "ConEVVStartTime", "ConEVVEndTime",
  "CaregiverID", "AppCaregiverID", "AideCode", "AideName", "AideSSN",
  "ConCaregiverID", "ConAppCaregiverID", "ConAideCode", "ConAideName", "ConAideSSN",
  "OfficeID", "AppOfficeID", "Office", "ConOfficeID", "ConAppOfficeID", "ConOffice",
  "PatientID", "AppPatientID", "PAdmissionID", "PName", "PAddressID", "PAppAddressID",
  "PAddressL1", "PAddressL2", "PCity", "PAddressState", "PZipCode", "PCounty",
  "Longitude" AS "PLongitude", "Latitude" AS "PLatitude",
  "ConPatientID", "ConAppPatientID", "ConPAdmissionID", "ConPName", "ConPAddressID", "ConPAppAddressID",
  "ConPAddressL1", "ConPAddressL2", "ConPCity", "ConPAddressState", "ConPZipCode", "ConPCounty",
  "ConLongitude" AS "ConPLongitude", "ConLatitude" AS "ConPLatitude",
  "PayerID", "AppPayerID", "Contract", "ConPayerID", "ConAppPayerID", "ConContract",
  "BilledDate", "ConBilledDate", "BilledHours", "ConBilledHours", "Billed", "ConBilled",
  "MinutesDiff" AS "MinuteDiffBetweenSch",
  "DistanceMiles" AS "DistanceMilesFromLatLng",
  "AverageMilesPerHour",
  "ETATravelMinutes" AS "ETATravleMinutes",
  "ServiceCodeID", "AppServiceCodeID", "RateType", "ServiceCode",
  "ConServiceCodeID", "ConAppServiceCodeID", "ConRateType", "ConServiceCode",
  "SameSchTimeFlag", "SameVisitTimeFlag", "SchVisitTimeSame" AS "SchAndVisitTimeSameFlag",
  "SchOverAnotherSchTimeFlag", "VisitTimeOverAnotherVisitTimeFlag", "SchTimeOverVisitTimeFlag", "DistanceFlag",
  "AideFName", "AideLName", "ConAideFName", "ConAideLName",
  "PFName", "PLName", "ConPFName", "ConPLName",
  "PMedicaidNumber", "ConPMedicaidNumber",
  "PayerState", "ConPayerState",
  "LastUpdatedBy", "ConLastUpdatedBy", "LastUpdatedDate", "ConLastUpdatedDate",
  "BilledRate", "TotalBilledAmount", "ConBilledRate", "ConTotalBilledAmount",
  "IsMissed", "MissedVisitReason", "EVVType",
  "ConIsMissed", "ConMissedVisitReason", "ConEVVType",
  "PStatus", "ConPStatus", "AideStatus", "ConAideStatus",
  "P_PatientID", "P_AppPatientID", "ConP_PatientID", "ConP_AppPatientID",
  "PA_PatientID", "PA_AppPatientID", "ConPA_PatientID", "ConPA_AppPatientID",
  "P_PAdmissionID", "P_PName", "P_PAddressID", "P_PAppAddressID", "P_PAddressL1", "P_PAddressL2",
  "P_PCity", "P_PAddressState", "P_PZipCode", "P_PCounty", "P_PFName", "P_PLName", "P_PMedicaidNumber",
  "ConP_PAdmissionID", "ConP_PName", "ConP_PAddressID", "ConP_PAppAddressID", "ConP_PAddressL1", "ConP_PAddressL2",
  "ConP_PCity", "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty", "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber",
  "PA_PAdmissionID", "PA_PName", "PA_PAddressID", "PA_PAppAddressID", "PA_PAddressL1", "PA_PAddressL2",
  "PA_PCity", "PA_PAddressState", "PA_PZipCode", "PA_PCounty", "PA_PFName", "PA_PLName", "PA_PMedicaidNumber",
  "ConPA_PAdmissionID", "ConPA_PName", "ConPA_PAddressID", "ConPA_PAppAddressID", "ConPA_PAddressL1", "ConPA_PAddressL2",
  "ConPA_PCity", "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty", "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber",
  "ContractType", "ConContractType",
  "P_PStatus", "ConP_PStatus", "PA_PStatus", "ConPA_PStatus",
  "BillRateNonBilled", "ConBillRateNonBilled", "BillRateBoth", "ConBillRateBoth",
  "FederalTaxNumber", "ConFederalTaxNumber"
FROM final_conflicts;

