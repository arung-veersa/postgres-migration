-- Task 02: Conflict Detection - Refactored with DRY Principle
-- Eliminates code duplication by using base_visits CTE
-- Supports both symmetric (fast) and asymmetric (comprehensive) join modes
--
-- Parameters (injected by Python):
--   {sf_database}, {sf_schema} - Snowflake analytics database/schema
--   {excluded_agencies} - Comma-separated quoted list of excluded provider IDs
--   {lookback_years} - Years in past for visit date filter (default: 2)
--   {lookforward_days} - Days in future for visit date filter (default: 45)
--   {lookback_hours} - Hours for updated timestamp filter (default: 32)
--   {extra_distance_per} - ExtraDistancePer value from settings table (default: 1.25)
--   [MPH lookup data] - MPH lookup data as inline SELECT statements
--
-- Conditional blocks (replaced by Python based on enable_asymmetric_join):
--   ASYMMETRIC_DELTA_KEYS placeholder - delta_conflict_keys CTE (empty if symmetric mode)
--   ASYMMETRIC_ALL_VISITS placeholder - all_visits CTE (empty if symmetric mode)
--   CONFLICT_PAIRS_JOIN placeholder - Join logic (symmetric self-join OR asymmetric UNION)

WITH
-- CTE 0: MPH Lookup (from PostgreSQL, injected as inline data)
mph_data AS (
{mph_lookup}
),

-- CTE 1: Base visit data - ALL visits in date window with ALL dimension joins
-- This is the single source of truth - no duplication!
base_visits AS (
  SELECT 
    DISTINCT 
    CAST(NULL AS STRING) AS "CONFLICTID",
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
    {sf_database}.{sf_schema}.FACTVISITCALLPERFORMANCE_CR AS CR1
    INNER JOIN {sf_database}.{sf_schema}.DIMCAREGIVER AS CAR 
      ON CAR."Caregiver Id" = CR1."Caregiver Id"
      AND TRIM(CAR."SSN") IS NOT NULL 
      AND TRIM(CAR."SSN") != ''
    LEFT JOIN {sf_database}.{sf_schema}.DIMOFFICE AS DOF 
      ON DOF."Office Id" = CR1."Office Id" AND DOF."Is Active" = TRUE
    LEFT JOIN {sf_database}.{sf_schema}.DIMPATIENT AS DPA_P 
      ON DPA_P."Patient Id" = CR1."Provider Patient Id"
    LEFT JOIN (
      SELECT 
        "Patient Address Id", "Application Patient Address Id", "Address Line 1", "Address Line 2",
        "City", "Address State", "Zip Code", "County", "Patient Id", "Application Patient Id",
        "Longitude" AS "Provider_Longitude", "Latitude" AS "Provider_Latitude",
        ROW_NUMBER() OVER (PARTITION BY "Patient Id" ORDER BY "Application Created UTC Timestamp" DESC) AS rn
      FROM {sf_database}.{sf_schema}.DIMPATIENTADDRESS
      WHERE "Primary Address" = TRUE AND "Address Type" LIKE '%GPS%'
    ) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" AND DPAD_P.RN = 1
    LEFT JOIN {sf_database}.{sf_schema}.DIMPATIENT AS DPA_PA 
      ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
    LEFT JOIN (
      SELECT 
        "Patient Address Id", "Application Patient Address Id", "Address Line 1", "Address Line 2",
        "City", "Address State", "Zip Code", "County", "Patient Id", "Application Patient Id",
        ROW_NUMBER() OVER (PARTITION BY "Patient Id" ORDER BY "Application Created UTC Timestamp" DESC) AS rn
      FROM {sf_database}.{sf_schema}.DIMPATIENTADDRESS
      WHERE "Primary Address" = TRUE AND "Address Type" LIKE '%GPS%'
    ) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" AND DPAD_PA.RN = 1
    LEFT JOIN {sf_database}.{sf_schema}.DIMPAYER AS SPA 
      ON SPA."Payer Id" = CR1."Payer Id" AND SPA."Is Active" = TRUE AND SPA."Is Demo" = FALSE
    LEFT JOIN {sf_database}.{sf_schema}.DIMCONTRACT AS DCON 
      ON DCON."Contract Id" = CR1."Contract Id" AND DCON."Is Active" = TRUE
    INNER JOIN {sf_database}.{sf_schema}.DIMPROVIDER AS DPR 
      ON DPR."Provider Id" = CR1."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
    LEFT JOIN {sf_database}.{sf_schema}.DIMSERVICECODE AS DSC 
      ON DSC."Service Code Id" = CR1."Service Code Id"
    LEFT JOIN {sf_database}.{sf_schema}.DIMUSER AS DUSR 
      ON DUSR."User Id" = CR1."Visit Updated User Id"
  WHERE 
    DATE(CR1."Visit Date") BETWEEN DATEADD(YEAR, -{lookback_years}, GETDATE()) AND DATEADD(DAY, {lookforward_days}, GETDATE())
    AND CR1."Provider Id" NOT IN ({excluded_agencies})
    {base_visits_timestamp_filter}
),

-- CTE 2: Delta visits - visits updated in the last {lookback_hours} hours
-- Simple filter on base_visits - no code duplication!
delta_visits AS (
  SELECT * 
  FROM base_visits
  {delta_visits_timestamp_filter}
),

-- ============================================================================
-- CONDITIONAL BLOCK: Asymmetric Join CTEs (only present if enabled)
-- ============================================================================
{ASYMMETRIC_DELTA_KEYS}
{ASYMMETRIC_ALL_VISITS}
-- ============================================================================

-- CTE 3: Conflict pairs (CONDITIONAL - symmetric OR asymmetric join)
conflict_pairs AS (
{CONFLICT_PAIRS_JOIN}
),

-- CTE 4: Calculate geospatial metrics ONCE (optimization: was calculated 5+ times before)
spatial_calculations AS (
  SELECT 
    CP.*,
    -- Calculate distance once and reuse
    ROUND(
      (ST_DISTANCE(
        ST_MAKEPOINT(CP."Longitude", CP."Latitude"),
        ST_MAKEPOINT(CP."ConLongitude", CP."ConLatitude")
      ) / 1609) * {extra_distance_per},
      2
    ) AS "DistanceMiles",
    -- Calculate time difference once
    ABS(DATEDIFF(MINUTE, CP."VisitEndTime", CP."ConVisitStartTime")) AS "MinutesDiff"
  FROM conflict_pairs CP
),

-- CTE 5: Add MPH lookup and calculate ETA (reuses DistanceMiles)
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

-- CTE 6: Apply 7 conflict detection rules
conflicts_with_flags AS (
  SELECT 
    CE.*,
    -- Rule 1: Same scheduled time (both visits not started)
    CASE 
      WHEN CE."ProviderID" != CE."ConProviderID"
           AND CE."VisitStartTime" IS NULL AND CE."VisitEndTime" IS NULL
           AND CE."ConVisitStartTime" IS NULL AND CE."ConVisitEndTime" IS NULL
           AND CONCAT(CE."SchStartTime", '~', CE."SchEndTime") = CONCAT(CE."ConSchStartTime", '~', CE."ConSchEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "SameSchTimeFlag",
    
    -- Rule 2: Same visit time (both visits completed)
    CASE 
      WHEN CE."ProviderID" != CE."ConProviderID"
           AND CE."VisitStartTime" IS NOT NULL AND CE."VisitEndTime" IS NOT NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND CONCAT(CE."VisitStartTime", '~', CE."VisitEndTime") = CONCAT(CE."ConVisitStartTime", '~', CE."ConVisitEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "SameVisitTimeFlag",
    
    -- Rule 3: Scheduled time matches conflicting visit time
    CASE 
      WHEN CE."ProviderID" != CE."ConProviderID"
           AND CE."VisitStartTime" IS NULL AND CE."VisitEndTime" IS NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND CONCAT(CE."SchStartTime", '~', CE."SchEndTime") = CONCAT(CE."ConVisitStartTime", '~', CE."ConVisitEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "SchVisitTimeSame",
    
    -- Rule 4: Scheduled time overlaps another scheduled time
    CASE 
      WHEN CE."ProviderID" != CE."ConProviderID"
           AND CE."VisitStartTime" IS NULL AND CE."VisitEndTime" IS NULL
           AND CE."ConVisitStartTime" IS NULL AND CE."ConVisitEndTime" IS NULL
           AND (CE."SchStartTime" < CE."ConSchEndTime" AND CE."SchEndTime" > CE."ConSchStartTime")
           AND CONCAT(CE."SchStartTime", '~', CE."SchEndTime") != CONCAT(CE."ConSchStartTime", '~', CE."ConSchEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "SchOverAnotherSchTimeFlag",
    
    -- Rule 5: Visit time overlaps another visit time
    CASE 
      WHEN CE."ProviderID" != CE."ConProviderID"
           AND CE."VisitStartTime" IS NOT NULL AND CE."VisitEndTime" IS NOT NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND (CE."VisitStartTime" < CE."ConVisitEndTime" AND CE."VisitEndTime" > CE."ConVisitStartTime")
           AND CONCAT(CE."VisitStartTime", '~', CE."VisitEndTime") != CONCAT(CE."ConVisitStartTime", '~', CE."ConVisitEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "VisitTimeOverAnotherVisitTimeFlag",
    
    -- Rule 6: Scheduled time overlaps visit time
    CASE 
      WHEN CE."ProviderID" != CE."ConProviderID"
           AND CE."VisitStartTime" IS NULL AND CE."VisitEndTime" IS NULL
           AND CE."ConVisitStartTime" IS NOT NULL AND CE."ConVisitEndTime" IS NOT NULL
           AND (CE."SchStartTime" < CE."ConVisitEndTime" AND CE."SchEndTime" > CE."ConVisitStartTime")
           AND CONCAT(CE."SchStartTime", '~', CE."SchEndTime") != CONCAT(CE."ConVisitStartTime", '~', CE."ConVisitEndTime")
      THEN 'Y' ELSE 'N' 
    END AS "SchTimeOverVisitTimeFlag",
    
    -- Rule 7: Distance flag (impossible travel distance - reuses ETATravelMinutes from CTE 5)
    CASE 
      WHEN CE."ProviderID" != CE."ConProviderID"
           AND CE."Longitude" IS NOT NULL AND CE."Latitude" IS NOT NULL
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

-- CTE 7: Filter to actual conflicts (at least one rule triggered)
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
FROM final_conflicts
