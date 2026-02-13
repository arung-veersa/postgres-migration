-- ============================================================================
-- Task 02.01 InService: Step 2 - Create InService Events Temp Table
-- ============================================================================
--
-- PURPOSE: Materialize InService events from FACTCAREGIVERINSERVICE with
--          synthetic VisitID (MD5 hash of 'I' + AppCaregiverInserviceId).
--          Only minimal dimension joins (caregiver, provider, office) since
--          InService events have no patient, payer, GPS, or billing data.
--
-- This template is formatted by Python .format() with these placeholders:
--   sf_database, sf_schema, lookback_years, lookforward_days, excluded_agencies
--
-- The excluded_ssns_temp table must already exist in the Snowflake session.
-- ============================================================================

CREATE OR REPLACE TEMPORARY TABLE inservice_events AS
SELECT
  DISTINCT
  CAST(NULL AS STRING) AS "CONFLICTID",
  CAST(NULL AS NUMBER) AS "BillRateNonBilled",
  CAST(NULL AS NUMBER) AS "BillRateBoth",
  TRIM(CAR."SSN") AS "SSN",
  CAST(NULL AS STRING) AS "PStatus",
  CAST(NULL AS STRING) AS "AideStatus",
  CAST(NULL AS STRING) AS "MissedVisitReason",
  CAST(NULL AS BOOLEAN) AS "IsMissed",
  CAST(NULL AS STRING) AS "EVVType",
  CAST(NULL AS NUMBER) AS "BilledRate",
  CAST(NULL AS NUMBER) AS "TotalBilledAmount",
  DPR."Provider Id" AS "ProviderID",
  DPR."Application Provider Id" AS "AppProviderID",
  DPR."Provider Name" AS "ProviderName",
  CAST(NULL AS STRING) AS "AgencyContact",
  DPR."Phone Number 1" AS "AgencyPhone",
  DPR."Federal Tax Number" AS "FederalTaxNumber",
  -- Synthetic VisitID: MD5 hash of 'I' + InService app ID
  MD5(CONCAT('I', CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR))) AS "VisitID",
  CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) AS "AppVisitID",
  CAST(FCS."Inservice start date" AS DATE) AS "VisitDate",
  CAST(NULL AS TIMESTAMP) AS "SchStartTime",
  CAST(NULL AS TIMESTAMP) AS "SchEndTime",
  CAST(NULL AS TIMESTAMP) AS "VisitStartTime",
  CAST(NULL AS TIMESTAMP) AS "VisitEndTime",
  CAST(NULL AS TIMESTAMP) AS "EVVStartTime",
  CAST(NULL AS TIMESTAMP) AS "EVVEndTime",
  CAR."Caregiver Id" AS "CaregiverID",
  CAR."Application Caregiver Id" AS "AppCaregiverID",
  CAR."Caregiver Code" AS "AideCode",
  CAR."Caregiver Fullname" AS "AideName",
  CAR."Caregiver Firstname" AS "AideFName",
  CAR."Caregiver Lastname" AS "AideLName",
  TRIM(CAR."SSN") AS "AideSSN",
  DOF."Office Id" AS "OfficeID",
  DOF."Application Office Id" AS "AppOfficeID",
  DOF."Office Name" AS "Office",
  CAST(NULL AS STRING) AS "PA_PatientID",
  CAST(NULL AS STRING) AS "PA_AppPatientID",
  CAST(NULL AS STRING) AS "P_PatientID",
  CAST(NULL AS STRING) AS "P_AppPatientID",
  CAST(NULL AS STRING) AS "PatientID",
  CAST(NULL AS STRING) AS "AppPatientID",
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
  CAST(NULL AS NUMBER) AS "Longitude",
  CAST(NULL AS NUMBER) AS "Latitude",
  CAST(NULL AS STRING) AS "PayerID",
  CAST(NULL AS STRING) AS "AppPayerID",
  CAST(NULL AS STRING) AS "Contract",
  CAST(NULL AS STRING) AS "PayerState",
  CAST(NULL AS TIMESTAMP) AS "BilledDate",
  CAST(NULL AS NUMBER) AS "BilledHours",
  CAST(NULL AS STRING) AS "Billed",
  -- InService date columns: populated for InService events
  CAST(FCS."Inservice start date" AS TIMESTAMP) AS "InserviceStartDate",
  CAST(FCS."Inservice end date" AS TIMESTAMP) AS "InserviceEndDate",
  CAST(NULL AS STRING) AS "ServiceCodeID",
  CAST(NULL AS STRING) AS "AppServiceCodeID",
  CAST(NULL AS STRING) AS "RateType",
  CAST(NULL AS STRING) AS "ServiceCode",
  CAST(NULL AS TIMESTAMP) AS "LastUpdatedDate",
  CAST(NULL AS STRING) AS "LastUpdatedBy",
  -- Provider patient: NULL (InService events have no patient)
  CAST(NULL AS STRING) AS "P_PAdmissionID",
  CAST(NULL AS STRING) AS "P_PName",
  CAST(NULL AS STRING) AS "P_PFName",
  CAST(NULL AS STRING) AS "P_PLName",
  CAST(NULL AS STRING) AS "P_PMedicaidNumber",
  CAST(NULL AS STRING) AS "P_PStatus",
  CAST(NULL AS STRING) AS "P_PAddressID",
  CAST(NULL AS STRING) AS "P_PAppAddressID",
  CAST(NULL AS STRING) AS "P_PAddressL1",
  CAST(NULL AS STRING) AS "P_PAddressL2",
  CAST(NULL AS STRING) AS "P_PCity",
  CAST(NULL AS STRING) AS "P_PAddressState",
  CAST(NULL AS STRING) AS "P_PZipCode",
  CAST(NULL AS STRING) AS "P_PCounty",
  -- Payer patient: NULL
  CAST(NULL AS STRING) AS "PA_PAdmissionID",
  CAST(NULL AS STRING) AS "PA_PName",
  CAST(NULL AS STRING) AS "PA_PFName",
  CAST(NULL AS STRING) AS "PA_PLName",
  CAST(NULL AS STRING) AS "PA_PMedicaidNumber",
  CAST(NULL AS STRING) AS "PA_PStatus",
  CAST(NULL AS STRING) AS "PA_PAddressID",
  CAST(NULL AS STRING) AS "PA_PAppAddressID",
  CAST(NULL AS STRING) AS "PA_PAddressL1",
  CAST(NULL AS STRING) AS "PA_PAddressL2",
  CAST(NULL AS STRING) AS "PA_PCity",
  CAST(NULL AS STRING) AS "PA_PAddressState",
  CAST(NULL AS STRING) AS "PA_PZipCode",
  CAST(NULL AS STRING) AS "PA_PCounty",
  CAST(NULL AS STRING) AS "ContractType"
FROM
  {sf_database}.{sf_schema}.FACTCAREGIVERINSERVICE AS FCS
  INNER JOIN {sf_database}.{sf_schema}.DIMCAREGIVER AS CAR
    ON CAR."Caregiver Id" = FCS."Caregiver Id"
    AND TRIM(CAR."SSN") IS NOT NULL
    AND TRIM(CAR."SSN") != ''
  INNER JOIN {sf_database}.{sf_schema}.DIMPROVIDER AS DPR
    ON DPR."Provider Id" = FCS."Provider Id"
    AND DPR."Is Active" = TRUE
    AND DPR."Is Demo" = FALSE
  LEFT JOIN {sf_database}.{sf_schema}.DIMOFFICE AS DOF
    ON DOF."Office Id" = FCS."Office Id"
    AND DOF."Is Active" = TRUE
WHERE
  CAST(FCS."Inservice start date" AS DATE) BETWEEN DATEADD(YEAR, -{lookback_years}, GETDATE())
                                                AND DATEADD(DAY, {lookforward_days}, GETDATE())
  AND DPR."Provider Id" NOT IN ({excluded_agencies})
  AND TRIM(CAR."SSN") NOT IN (SELECT ssn FROM excluded_ssns_temp);
