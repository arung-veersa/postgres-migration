-- ============================================================================
-- Task 02 v3: Step 3 - Final Conflict Detection Query (Self-Join on base_visits)
-- ============================================================================
--
-- PURPOSE: Self-join the base_visits temp table to find conflict pairs,
--          apply 7 conflict detection rules, and return conflicts
--
-- In SYMMETRIC mode, the join condition is unconstrained (all-vs-all within ~70K rows).
-- In ASYMMETRIC mode, base_visits is ~9.6M rows but an {ASYMMETRIC_JOIN_CONDITION}
-- ensures at least one side of each pair is a delta visit (is_delta=1), avoiding
-- the expensive all-vs-all self-join on the full 9.6M rows.
--
-- SYMMETRIC MODE: base_visits contains ~70K rows (lookback_hours delta only)
-- ASYMMETRIC MODE: base_visits contains ~9.6M rows (delta + related records)
--
-- PERFORMANCE: 
--   - Symmetric: ~30-40 seconds
--   - Asymmetric: ~3-5 minutes (estimated)
-- ============================================================================

WITH
-- CTE 1: MPH lookup data (hardcoded reference data for distance-based conflict detection)
mph_data AS (
{mph_lookup}
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
    {ASYMMETRIC_JOIN_CONDITION}
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
      ) / 1609) * {extra_distance_per},
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
    
    -- Rule 7: Distance flag (impossible travel distance - reuses ETATravelMinutes from CTE 4)
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
