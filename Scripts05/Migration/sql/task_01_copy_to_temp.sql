-- ============================================================================
-- TASK_01: Copy Data from ConflictVisitMaps to Temp
-- ============================================================================
-- Migrated from: Snowflake TASK_01_COPY_DATA_FROM_CONFLICTVISITMAPS_TO_TEMP.sql
--
-- Purpose:
--   1. Sync PAYER_PROVIDER_REMINDERS from Analytics (insert new, update existing)
--   2. Truncate CONFLICTVISITMAPS_TEMP
--   3. Copy filtered data from CONFLICTVISITMAPS to CONFLICTVISITMAPS_TEMP
--   4. Update SETTINGS.InProgressFlag to 1 (in progress)
--
-- Schema Placeholders:
--   {conflict_schema}  - Conflict data schema (e.g., conflict_dev)
--   {analytics_schema} - Analytics data schema (e.g., analytics_dev)
--
-- Note: This SQL is executed as a single transaction by the Python orchestrator
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Step 1: INSERT new payer-provider reminders that don't exist
-- ----------------------------------------------------------------------------
INSERT INTO {conflict_schema}.payer_provider_reminders (
    "PayerID", 
    "AppPayerID", 
    "Contract", 
    "ProviderID", 
    "AppProviderID", 
    "ProviderName", 
    "CreatedDateTime", 
    "NumberOfDays"
)
SELECT DISTINCT 
    DPP."Payer Id" AS "PayerID", 
    DPP."Application Payer Id" AS "AppPayerID", 
    DPA."Payer Name" AS "Contract", 
    DPP."Provider Id" AS "ProviderID", 
    DPP."Application Provider Id" AS "AppProviderID", 
    DP."Provider Name" AS "ProviderName", 
    CURRENT_TIMESTAMP AS "CreatedDateTime", 
    CAST(NULL AS NUMERIC) AS "NumberOfDays"
FROM {analytics_schema}.dimprovider AS DP
INNER JOIN {analytics_schema}.dimpayerprovider AS DPP 
    ON DPP."Provider Id" = DP."Provider Id"
INNER JOIN {analytics_schema}.dimpayer AS DPA 
    ON DPA."Payer Id" = DPP."Payer Id"
WHERE NOT EXISTS (
    SELECT 1 
    FROM {conflict_schema}.payer_provider_reminders AS PPR_N 
    WHERE PPR_N."PayerID" = DPP."Payer Id"
    AND PPR_N."ProviderID" = DPP."Provider Id"
);

-- ----------------------------------------------------------------------------
-- Step 2: UPDATE existing payer-provider reminders with latest names
-- ----------------------------------------------------------------------------
UPDATE {conflict_schema}.payer_provider_reminders AS PPR
SET 
    "Contract" = DPA."Payer Name",
    "ProviderName" = DP."Provider Name"
FROM {analytics_schema}.dimprovider AS DP
INNER JOIN {analytics_schema}.dimpayerprovider AS DPP 
    ON DPP."Provider Id" = DP."Provider Id"
INNER JOIN {analytics_schema}.dimpayer AS DPA 
    ON DPA."Payer Id" = DPP."Payer Id"
WHERE 
    PPR."PayerID" = DPP."Payer Id"
    AND PPR."ProviderID" = DPP."Provider Id";

-- ----------------------------------------------------------------------------
-- Step 3: TRUNCATE temp table
-- ----------------------------------------------------------------------------
TRUNCATE TABLE {conflict_schema}.conflictvisitmaps_temp;

-- ----------------------------------------------------------------------------
-- Step 4: INSERT data into temp table with date filtering
-- Note: Only inserting columns that exist in conflictvisitmaps_temp table
-- ----------------------------------------------------------------------------
INSERT INTO {conflict_schema}.conflictvisitmaps_temp (
    "ID", "CONFLICTID", "SSN",
    "ProviderID", "AppProviderID", "ProviderName", "FederalTaxNumber",
    "VisitID", "AppVisitID",
    "ConProviderID", "ConAppProviderID", "ConProviderName", "ConFederalTaxNumber",
    "ConVisitID", "ConAppVisitID",
    "VisitDate",
    "SchStartTime", "SchEndTime",
    "ConSchStartTime", "ConSchEndTime",
    "VisitStartTime", "VisitEndTime",
    "ConVisitStartTime", "ConVisitEndTime",
    "EVVStartTime", "EVVEndTime",
    "ConEVVStartTime", "ConEVVEndTime",
    "CaregiverID", "AppCaregiverID",
    "AideCode", "AideName", "AideSSN",
    "ConCaregiverID", "ConAppCaregiverID",
    "ConAideCode", "ConAideName", "ConAideSSN",
    "OfficeID", "AppOfficeID", "Office",
    "ConOfficeID", "ConAppOfficeID", "ConOffice",
    "PatientID", "AppPatientID", "PAdmissionID", "PName",
    "PAddressID", "PAppAddressID",
    "PAddressL1", "PAddressL2", "PCity",
    "PAddressState", "PZipCode", "PCounty",
    "PLongitude", "PLatitude",
    "ConPatientID", "ConAppPatientID", "ConPAdmissionID",
    "ConPName", "ConPAddressID", "ConPAppAddressID",
    "ConPAddressL1", "ConPAddressL2", "ConPCity",
    "ConPAddressState", "ConPZipCode", "ConPCounty",
    "ConPLongitude", "ConPLatitude",
    "PayerID", "AppPayerID", "Contract",
    "ConPayerID", "ConAppPayerID", "ConContract",
    "BilledDate", "ConBilledDate",
    "BilledHours", "ConBilledHours",
    "Billed", "ConBilled",
    "MinuteDiffBetweenSch",
    "DistanceMilesFromLatLng",
    "AverageMilesPerHour",
    "ETATravleMinutes",
    "InserviceStartDate", "InserviceEndDate",
    "PTOStartDate", "PTOEndDate",
    "ConInserviceStartDate", "ConInserviceEndDate",
    "ConPTOStartDate", "ConPTOEndDate",
    "ServiceCodeID", "AppServiceCodeID",
    "RateType", "ServiceCode",
    "ConServiceCodeID", "ConAppServiceCodeID",
    "ConRateType", "ConServiceCode",
    "SameSchTimeFlag", "SameVisitTimeFlag",
    "SchAndVisitTimeSameFlag",
    "SchOverAnotherSchTimeFlag",
    "VisitTimeOverAnotherVisitTimeFlag",
    "SchTimeOverVisitTimeFlag",
    "DistanceFlag", "InServiceFlag", "PTOFlag",
    "StatusFlag",
    "ConStatusFlag",
    "AideFName", "AideLName",
    "ConAideFName", "ConAideLName",
    "PFName", "PLName",
    "ConPFName", "ConPLName",
    "PMedicaidNumber", "ConPMedicaidNumber"
)
SELECT 
    CVM."ID", CVM."CONFLICTID", CVM."SSN",
    CVM."ProviderID", CVM."AppProviderID", CVM."ProviderName", CVM."FederalTaxNumber",
    CVM."VisitID", CVM."AppVisitID",
    CVM."ConProviderID", CVM."ConAppProviderID", CVM."ConProviderName", CVM."ConFederalTaxNumber",
    CVM."ConVisitID", CVM."ConAppVisitID",
    CVM."VisitDate",
    CVM."SchStartTime", CVM."SchEndTime",
    CVM."ConSchStartTime", CVM."ConSchEndTime",
    CVM."VisitStartTime", CVM."VisitEndTime",
    CVM."ConVisitStartTime", CVM."ConVisitEndTime",
    CVM."EVVStartTime", CVM."EVVEndTime",
    CVM."ConEVVStartTime", CVM."ConEVVEndTime",
    CVM."CaregiverID", CVM."AppCaregiverID",
    CVM."AideCode", CVM."AideName", CVM."AideSSN",
    CVM."ConCaregiverID", CVM."ConAppCaregiverID",
    CVM."ConAideCode", CVM."ConAideName", CVM."ConAideSSN",
    CVM."OfficeID", CVM."AppOfficeID", CVM."Office",
    CVM."ConOfficeID", CVM."ConAppOfficeID", CVM."ConOffice",
    CVM."PatientID", CVM."AppPatientID", CVM."PAdmissionID", CVM."PName",
    CVM."PAddressID", CVM."PAppAddressID",
    CVM."PAddressL1", CVM."PAddressL2", CVM."PCity",
    CVM."PAddressState", CVM."PZipCode", CVM."PCounty",
    CVM."PLongitude", CVM."PLatitude",
    CVM."ConPatientID", CVM."ConAppPatientID", CVM."ConPAdmissionID",
    CVM."ConPName", CVM."ConPAddressID", CVM."ConPAppAddressID",
    CVM."ConPAddressL1", CVM."ConPAddressL2", CVM."ConPCity",
    CVM."ConPAddressState", CVM."ConPZipCode", CVM."ConPCounty",
    CVM."ConPLongitude", CVM."ConPLatitude",
    CVM."PayerID", CVM."AppPayerID", CVM."Contract",
    CVM."ConPayerID", CVM."ConAppPayerID", CVM."ConContract",
    CVM."BilledDate", CVM."ConBilledDate",
    CVM."BilledHours", CVM."ConBilledHours",
    CVM."Billed", CVM."ConBilled",
    CVM."MinuteDiffBetweenSch",
    CVM."DistanceMilesFromLatLng",
    CVM."AverageMilesPerHour",
    CVM."ETATravleMinutes",
    CVM."InserviceStartDate", CVM."InserviceEndDate",
    CVM."PTOStartDate", CVM."PTOEndDate",
    CVM."ConInserviceStartDate", CVM."ConInserviceEndDate",
    CVM."ConPTOStartDate", CVM."ConPTOEndDate",
    CVM."ServiceCodeID", CVM."AppServiceCodeID",
    CVM."RateType", CVM."ServiceCode",
    CVM."ConServiceCodeID", CVM."ConAppServiceCodeID",
    CVM."ConRateType", CVM."ConServiceCode",
    CVM."SameSchTimeFlag", CVM."SameVisitTimeFlag",
    CVM."SchAndVisitTimeSameFlag",
    CVM."SchOverAnotherSchTimeFlag",
    CVM."VisitTimeOverAnotherVisitTimeFlag",
    CVM."SchTimeOverVisitTimeFlag",
    CVM."DistanceFlag", CVM."InServiceFlag", CVM."PTOFlag",
    C."StatusFlag",
    CVM."StatusFlag" AS "ConStatusFlag",
    CVM."AideFName", CVM."AideLName",
    CVM."ConAideFName", CVM."ConAideLName",
    CVM."PFName", CVM."PLName",
    CVM."ConPFName", CVM."ConPLName",
    CVM."PMedicaidNumber", CVM."ConPMedicaidNumber"
FROM {conflict_schema}.conflictvisitmaps AS CVM
INNER JOIN {conflict_schema}.conflicts AS C 
    ON C."CONFLICTID" = CVM."CONFLICTID"
WHERE DATE(CVM."VisitDate") BETWEEN 
    DATE(NOW() - INTERVAL '2 years') AND 
    DATE(NOW() + INTERVAL '45 days');

-- ----------------------------------------------------------------------------
-- Step 5: UPDATE settings to indicate processing is in progress
-- ----------------------------------------------------------------------------
UPDATE {conflict_schema}.settings 
SET "InProgressFlag" = 1;

