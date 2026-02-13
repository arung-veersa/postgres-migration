-- ============================================================================
-- Task 02.01 InService: Step 1 - Create Eligible Visits Temp Table
-- ============================================================================
--
-- PURPOSE: Materialize visits eligible for InService conflict detection.
--          These are regular visits (not InService events themselves) with
--          valid start/end times, that do NOT have a same-provider InService
--          overlap.  Almost identical to base_visits but:
--            - No delta_keys / is_delta / TIMESTAMP_CONDITION
--            - Added FCS LEFT JOIN + IS NULL to exclude same-provider inservice
--            - Added IsMissed=FALSE filter
--            - Added caregiver semi-join: only visits for caregivers who have
--              at least one InService event in the date window (most caregivers
--              have none, so this dramatically reduces the result set)
--
-- This template is formatted by Python .format() with these placeholders:
--   sf_database, sf_schema, lookback_years, lookforward_days, excluded_agencies
--
-- The excluded_ssns_temp table must already exist in the Snowflake session.
-- ============================================================================

CREATE OR REPLACE TEMPORARY TABLE inservice_visits AS
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
  CAST(CR1."Scheduled Start Time" AS TIMESTAMP) AS "SchStartTime",
  CAST(CR1."Scheduled End Time" AS TIMESTAMP) AS "SchEndTime",
  CAST(CR1."Visit Start Time" AS TIMESTAMP) AS "VisitStartTime",
  CAST(CR1."Visit End Time" AS TIMESTAMP) AS "VisitEndTime",
  CAST(CR1."Call In Time" AS TIMESTAMP) AS "EVVStartTime",
  CAST(CR1."Call Out Time" AS TIMESTAMP) AS "EVVEndTime",
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
  CAST(CR1."Invoice Date" AS TIMESTAMP) AS "BilledDate",
  CR1."Billed Hours" AS "BilledHours",
  CR1."Billed" AS "Billed",
  -- Visit records have NULL InService dates (FCS IS NULL by the WHERE clause)
  CAST(NULL AS TIMESTAMP) AS "InserviceStartDate",
  CAST(NULL AS TIMESTAMP) AS "InserviceEndDate",
  DSC."Service Code Id" AS "ServiceCodeID",
  DSC."Application Service Code Id" AS "AppServiceCodeID",
  CR1."Bill Type" AS "RateType",
  DSC."Service Code" AS "ServiceCode",
  CAST(CR1."Visit Updated Timestamp" AS TIMESTAMP) AS "LastUpdatedDate",
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
  -- Exclude visits that overlap with InService at the SAME provider
  -- (same-provider overlap is not a conflict; only cross-provider is)
  LEFT JOIN {sf_database}.{sf_schema}.FACTCAREGIVERINSERVICE AS FCS
    ON FCS."Caregiver Id" = CR1."Caregiver Id"
    AND CR1."Visit Start Time" IS NOT NULL
    AND CR1."Visit End Time" IS NOT NULL
    AND (CAST(CR1."Visit Start Time" AS TIMESTAMP) <= CAST(FCS."Inservice end date" AS TIMESTAMP)
         AND CAST(CR1."Visit End Time" AS TIMESTAMP) >= CAST(FCS."Inservice start date" AS TIMESTAMP))
    AND FCS."Provider Id" = CR1."Provider Id"
WHERE
  CR1."Is Missed" = FALSE
  AND CR1."Visit Start Time" IS NOT NULL
  AND CR1."Visit End Time" IS NOT NULL
  AND FCS."Application Caregiver Inservice Id" IS NULL
  AND DATE(CR1."Visit Date") BETWEEN DATEADD(YEAR, -{lookback_years}, GETDATE())
                                 AND DATEADD(DAY, {lookforward_days}, GETDATE())
  AND CR1."Provider Id" NOT IN ({excluded_agencies})
  AND TRIM(CAR."SSN") NOT IN (SELECT ssn FROM excluded_ssns_temp)
  -- Pre-filter: only caregivers who actually have InService events in the window
  AND CR1."Caregiver Id" IN (
      SELECT DISTINCT FCS2."Caregiver Id"
      FROM {sf_database}.{sf_schema}.FACTCAREGIVERINSERVICE AS FCS2
      WHERE CAST(FCS2."Inservice start date" AS DATE)
            BETWEEN DATEADD(YEAR, -{lookback_years}, GETDATE())
                AND DATEADD(DAY, {lookforward_days}, GETDATE())
  );
