-- ======================================================================
-- TASK 04 - STEP 6: Refresh Primary Visit Data
-- Converted from Snowflake UPDATE_DATA_CONFLICTVISITMAPS_3 procedure
-- 
-- This step refreshes primary visit data (non-Con* columns) from analytics
-- for conflicts that are Resolved or Deleted.
--
-- Queries:
-- 1. update_records_p - Refresh primary columns for Resolved conflicts
-- 2. update_deleted_records_p - Refresh primary columns for Deleted conflicts
-- ======================================================================

-- Query 1: Refresh primary visit columns for RESOLVED conflicts (StatusFlag = 'R')
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "SSN" = ALLDATA."SSN",
    "ProviderID" = ALLDATA."ProviderID",
    "AppProviderID" = ALLDATA."AppProviderID",
    "ProviderName" = ALLDATA."ProviderName",
    "VisitDate" = ALLDATA."VisitDate",
    "SchStartTime" = ALLDATA."SchStartTime",
    "SchEndTime" = ALLDATA."SchEndTime",
    "VisitStartTime" = ALLDATA."VisitStartTime",
    "VisitEndTime" = ALLDATA."VisitEndTime",
    "EVVStartTime" = ALLDATA."EVVStartTime",
    "EVVEndTime" = ALLDATA."EVVEndTime",
    "CaregiverID" = ALLDATA."CaregiverID",
    "AppCaregiverID" = ALLDATA."AppCaregiverID",
    "AideCode" = ALLDATA."AideCode",
    "AideName" = ALLDATA."AideName",
    "AideSSN" = ALLDATA."AideSSN",
    "OfficeID" = ALLDATA."OfficeID",
    "AppOfficeID" = ALLDATA."AppOfficeID",
    "Office" = ALLDATA."Office",
    "PatientID" = ALLDATA."PatientID",
    "AppPatientID" = ALLDATA."AppPatientID",
    "PAdmissionID" = ALLDATA."PAdmissionID",
    "PName" = ALLDATA."PName",
    "PAddressID" = ALLDATA."PAddressID",
    "PAppAddressID" = ALLDATA."PAppAddressID",
    "PAddressL1" = ALLDATA."PAddressL1",
    "PAddressL2" = ALLDATA."PAddressL2",
    "PCity" = ALLDATA."PCity",
    "PAddressState" = ALLDATA."PAddressState",
    "PZipCode" = ALLDATA."PZipCode",
    "PCounty" = ALLDATA."PCounty",
    "PLongitude" = ALLDATA."PLongitude",
    "PLatitude" = ALLDATA."PLatitude",
    "PayerID" = ALLDATA."PayerID",
    "AppPayerID" = ALLDATA."AppPayerID",
    "BilledDate" = ALLDATA."BilledDate",
    "BilledHours" = ALLDATA."BilledHours",
    "Billed" = ALLDATA."Billed",
    "ServiceCodeID" = ALLDATA."ServiceCodeID",
    "AppServiceCodeID" = ALLDATA."AppServiceCodeID",
    "RateType" = ALLDATA."RateType",
    "ServiceCode" = ALLDATA."ServiceCode",
    "AideFName" = ALLDATA."AideFName",
    "AideLName" = ALLDATA."AideLName",
    "PFName" = ALLDATA."PFName",
    "PLName" = ALLDATA."PLName",
    "PMedicaidNumber" = ALLDATA."PMedicaidNumber",
    "PayerState" = ALLDATA."PayerState",
    "LastUpdatedBy" = ALLDATA."LastUpdatedBy",
    "LastUpdatedDate" = ALLDATA."LastUpdatedDate",
    "BilledRate" = ALLDATA."BilledRate",
    "TotalBilledAmount" = ALLDATA."TotalBilledAmount",
    "IsMissed" = ALLDATA."IsMissed",
    "MissedVisitReason" = ALLDATA."MissedVisitReason",
    "EVVType" = ALLDATA."EVVType",
    "PStatus" = ALLDATA."PStatus",
    "AideStatus" = ALLDATA."AideStatus",
    "P_PatientID" = ALLDATA."P_PatientID",
    "P_AppPatientID" = ALLDATA."P_AppPatientID",
    "PA_PatientID" = ALLDATA."PA_PatientID",
    "PA_AppPatientID" = ALLDATA."PA_AppPatientID",
    "P_PAdmissionID" = ALLDATA."P_PAdmissionID",
    "P_PName" = ALLDATA."P_PName",
    "P_PAddressID" = ALLDATA."P_PAddressID",
    "P_PAppAddressID" = ALLDATA."P_PAppAddressID",
    "P_PAddressL1" = ALLDATA."P_PAddressL1",
    "P_PAddressL2" = ALLDATA."P_PAddressL2",
    "P_PCity" = ALLDATA."P_PCity",
    "P_PAddressState" = ALLDATA."P_PAddressState",
    "P_PZipCode" = ALLDATA."P_PZipCode",
    "P_PCounty" = ALLDATA."P_PCounty",
    "P_PFName" = ALLDATA."P_PFName",
    "P_PLName" = ALLDATA."P_PLName",
    "P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber",
    "PA_PAdmissionID" = ALLDATA."PA_PAdmissionID",
    "PA_PName" = ALLDATA."PA_PName",
    "PA_PAddressID" = ALLDATA."PA_PAddressID",
    "PA_PAppAddressID" = ALLDATA."PA_PAppAddressID",
    "PA_PAddressL1" = ALLDATA."PA_PAddressL1",
    "PA_PAddressL2" = ALLDATA."PA_PAddressL2",
    "PA_PCity" = ALLDATA."PA_PCity",
    "PA_PAddressState" = ALLDATA."PA_PAddressState",
    "PA_PZipCode" = ALLDATA."PA_PZipCode",
    "PA_PCounty" = ALLDATA."PA_PCounty",
    "PA_PFName" = ALLDATA."PA_PFName",
    "PA_PLName" = ALLDATA."PA_PLName",
    "PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber",
    "BillRateNonBilled" = ALLDATA."BillRateNonBilled",
    "BillRateBoth" = ALLDATA."BillRateBoth",
    "FederalTaxNumber" = ALLDATA."FederalTaxNumber"
FROM (
    SELECT DISTINCT
        CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
        CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth",
        TRIM(CAR."SSN") AS "SSN",
        CAST(NULL AS VARCHAR) AS "PStatus",
        CAR."Status" AS "AideStatus",
        CR1."Missed Visit Reason" AS "MissedVisitReason",
        CR1."Is Missed" AS "IsMissed",
        CR1."Call Out Device Type" AS "EVVType",
        CR1."Billed Rate" AS "BilledRate",
        CR1."Total Billed Amount" AS "TotalBilledAmount",
        CR1."Provider Id" AS "ProviderID",
        CR1."Application Provider Id" AS "AppProviderID",
        DPR."Provider Name" AS "ProviderName",
        DPR."Phone Number 1" AS "AgencyPhone",
        DPR."Federal Tax Number" AS "FederalTaxNumber",
        CR1."Visit Id" AS "VisitID",
        CR1."Application Visit Id" AS "AppVisitID",
        CR1."Visit Date"::date AS "VisitDate",
        CR1."Scheduled Start Time"::timestamp AS "SchStartTime",
        CR1."Scheduled End Time"::timestamp AS "SchEndTime",
        CR1."Visit Start Time"::timestamp AS "VisitStartTime",
        CR1."Visit End Time"::timestamp AS "VisitEndTime",
        CR1."Call In Time"::timestamp AS "EVVStartTime",
        CR1."Call Out Time"::timestamp AS "EVVEndTime",
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
        CAST(NULL AS VARCHAR) AS "PAdmissionID",
        CAST(NULL AS VARCHAR) AS "PName",
        CAST(NULL AS VARCHAR) AS "PFName",
        CAST(NULL AS VARCHAR) AS "PLName",
        CAST(NULL AS VARCHAR) AS "PMedicaidNumber",
        CAST(NULL AS VARCHAR) AS "PAddressID",
        CAST(NULL AS VARCHAR) AS "PAppAddressID",
        CAST(NULL AS VARCHAR) AS "PAddressL1",
        CAST(NULL AS VARCHAR) AS "PAddressL2",
        CAST(NULL AS VARCHAR) AS "PCity",
        CAST(NULL AS VARCHAR) AS "PAddressState",
        CAST(NULL AS VARCHAR) AS "PZipCode",
        CAST(NULL AS VARCHAR) AS "PCounty",
        CASE 
            WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != ',' 
                THEN CAST(SPLIT_PART(CR1."Call Out GPS Coordinates", ',', 2) AS DOUBLE PRECISION)
            WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != ',' 
                THEN CAST(SPLIT_PART(CR1."Call In GPS Coordinates", ',', 2) AS DOUBLE PRECISION)
            ELSE DPAD_P."Provider_Longitude"
        END AS "PLongitude",
        CASE 
            WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != ',' 
                THEN CAST(SPLIT_PART(CR1."Call Out GPS Coordinates", ',', 1) AS DOUBLE PRECISION)
            WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != ',' 
                THEN CAST(SPLIT_PART(CR1."Call In GPS Coordinates", ',', 1) AS DOUBLE PRECISION)
            ELSE DPAD_P."Provider_Latitude"
        END AS "PLatitude",
        CR1."Payer Id" AS "PayerID",
        CR1."Application Payer Id" AS "AppPayerID",
        SPA."Payer State" AS "PayerState",
        CR1."Invoice Date"::timestamp AS "BilledDate",
        CR1."Billed Hours" AS "BilledHours",
        CR1."Billed" AS "Billed",
        DSC."Service Code Id" AS "ServiceCodeID",
        DSC."Application Service Code Id" AS "AppServiceCodeID",
        CR1."Bill Type" AS "RateType",
        DSC."Service Code" AS "ServiceCode",
        CR1."Visit Updated Timestamp"::timestamp AS "LastUpdatedDate",
        DUSR."User Fullname" AS "LastUpdatedBy",
        DPA_P."Admission Id" AS "P_PAdmissionID",
        DPA_P."Patient Name" AS "P_PName",
        DPA_P."Patient Firstname" AS "P_PFName",
        DPA_P."Patient Lastname" AS "P_PLName",
        DPA_P."Medicaid Number" AS "P_PMedicaidNumber",
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
        DPAD_PA."Patient Address Id" AS "PA_PAddressID",
        DPAD_PA."Application Patient Address Id" AS "PA_PAppAddressID",
        DPAD_PA."Address Line 1" AS "PA_PAddressL1",
        DPAD_PA."Address Line 2" AS "PA_PAddressL2",
        DPAD_PA."City" AS "PA_PCity",
        DPAD_PA."Address State" AS "PA_PAddressState",
        DPAD_PA."Zip Code" AS "PA_PZipCode",
        DPAD_PA."County" AS "PA_PCounty",
        DPA_PA."Patient Firstname" AS "PA_PFName",
        DPA_PA."Patient Lastname" AS "PA_PLName",
        DPA_PA."Medicaid Number" AS "PA_PMedicaidNumber"
    FROM {analytics_schema}.factvisitcallperformance_cr AS CR1
    INNER JOIN {analytics_schema}.dimcaregiver AS CAR 
        ON CAR."Caregiver Id" = CR1."Caregiver Id" 
        AND TRIM(CAR."SSN") IS NOT NULL 
        AND TRIM(CAR."SSN") != ''
    LEFT JOIN {analytics_schema}.dimoffice AS DOF 
        ON DOF."Office Id" = CR1."Office Id" 
        AND DOF."Is Active" = TRUE
    LEFT JOIN {analytics_schema}.dimpatient AS DPA_P 
        ON DPA_P."Patient Id" = CR1."Provider Patient Id"
    LEFT JOIN (
        SELECT 
            DDD."Patient Address Id",
            DDD."Application Patient Address Id",
            DDD."Address Line 1",
            DDD."Address Line 2",
            DDD."City",
            DDD."Address State",
            DDD."Zip Code",
            DDD."County",
            DDD."Patient Id",
            DDD."Application Patient Id",
            DDD."Longitude" AS "Provider_Longitude",
            DDD."Latitude" AS "Provider_Latitude",
            ROW_NUMBER() OVER (PARTITION BY DDD."Patient Id" ORDER BY DDD."Application Created UTC Timestamp" DESC) AS rn
        FROM {analytics_schema}.dimpatientaddress AS DDD
        WHERE DDD."Primary Address" = TRUE 
          AND DDD."Address Type" LIKE '%GPS%'
    ) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" AND DPAD_P.rn = 1
    LEFT JOIN {analytics_schema}.dimpatient AS DPA_PA 
        ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
    LEFT JOIN (
        SELECT 
            DDD."Patient Address Id",
            DDD."Application Patient Address Id",
            DDD."Address Line 1",
            DDD."Address Line 2",
            DDD."City",
            DDD."Address State",
            DDD."Zip Code",
            DDD."County",
            DDD."Patient Id",
            DDD."Application Patient Id",
            ROW_NUMBER() OVER (PARTITION BY DDD."Patient Id" ORDER BY DDD."Application Created UTC Timestamp" DESC) AS rn
        FROM {analytics_schema}.dimpatientaddress AS DDD
        WHERE DDD."Primary Address" = TRUE 
          AND DDD."Address Type" LIKE '%GPS%'
    ) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" AND DPAD_PA.rn = 1
    LEFT JOIN {analytics_schema}.dimpayer AS SPA 
        ON SPA."Payer Id" = CR1."Payer Id" 
        AND SPA."Is Active" = TRUE 
        AND SPA."Is Demo" = FALSE
    INNER JOIN {analytics_schema}.dimprovider AS DPR 
        ON DPR."Provider Id" = CR1."Provider Id" 
        AND DPR."Is Active" = TRUE 
        AND DPR."Is Demo" = FALSE
    LEFT JOIN {analytics_schema}.dimservicecode AS DSC 
        ON DSC."Service Code Id" = CR1."Service Code Id"
    LEFT JOIN {analytics_schema}.dimuser AS DUSR 
        ON DUSR."User Id" = CR1."Visit Updated User Id"
    INNER JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
        ON CVM2."VisitID" = CR1."Visit Id"
    INNER JOIN {conflict_schema}.conflicts AS C 
        ON C."CONFLICTID" = CVM2."CONFLICTID" 
        AND C."StatusFlag" = 'R'
    WHERE CR1."Visit Date" >= '{start_date}'::timestamp
      AND CR1."Visit Date" < ('{end_date}'::date + INTERVAL '1 day')
) AS ALLDATA
WHERE CVM."VisitID" = ALLDATA."VisitID";


-- Query 2: Refresh primary visit columns for DELETED conflicts (StatusFlag = 'D')
-- Uses FACTVISITCALLPERFORMANCE_DELETED_CR table for deleted visits
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "SSN" = ALLDATA."SSN",
    "ProviderID" = ALLDATA."ProviderID",
    "AppProviderID" = ALLDATA."AppProviderID",
    "ProviderName" = ALLDATA."ProviderName",
    "VisitDate" = ALLDATA."VisitDate",
    "SchStartTime" = ALLDATA."SchStartTime",
    "SchEndTime" = ALLDATA."SchEndTime",
    "VisitStartTime" = ALLDATA."VisitStartTime",
    "VisitEndTime" = ALLDATA."VisitEndTime",
    "EVVStartTime" = ALLDATA."EVVStartTime",
    "EVVEndTime" = ALLDATA."EVVEndTime",
    "CaregiverID" = ALLDATA."CaregiverID",
    "AppCaregiverID" = ALLDATA."AppCaregiverID",
    "AideCode" = ALLDATA."AideCode",
    "AideName" = ALLDATA."AideName",
    "AideSSN" = ALLDATA."AideSSN",
    "OfficeID" = ALLDATA."OfficeID",
    "AppOfficeID" = ALLDATA."AppOfficeID",
    "Office" = ALLDATA."Office",
    "PatientID" = ALLDATA."PatientID",
    "AppPatientID" = ALLDATA."AppPatientID",
    "PAdmissionID" = ALLDATA."PAdmissionID",
    "PName" = ALLDATA."PName",
    "PAddressID" = ALLDATA."PAddressID",
    "PAppAddressID" = ALLDATA."PAppAddressID",
    "PAddressL1" = ALLDATA."PAddressL1",
    "PAddressL2" = ALLDATA."PAddressL2",
    "PCity" = ALLDATA."PCity",
    "PAddressState" = ALLDATA."PAddressState",
    "PZipCode" = ALLDATA."PZipCode",
    "PCounty" = ALLDATA."PCounty",
    "PLongitude" = ALLDATA."PLongitude",
    "PLatitude" = ALLDATA."PLatitude",
    "PayerID" = ALLDATA."PayerID",
    "AppPayerID" = ALLDATA."AppPayerID",
    "BilledDate" = ALLDATA."BilledDate",
    "BilledHours" = ALLDATA."BilledHours",
    "Billed" = ALLDATA."Billed",
    "ServiceCodeID" = ALLDATA."ServiceCodeID",
    "AppServiceCodeID" = ALLDATA."AppServiceCodeID",
    "RateType" = ALLDATA."RateType",
    "ServiceCode" = ALLDATA."ServiceCode",
    "AideFName" = ALLDATA."AideFName",
    "AideLName" = ALLDATA."AideLName",
    "PFName" = ALLDATA."PFName",
    "PLName" = ALLDATA."PLName",
    "PMedicaidNumber" = ALLDATA."PMedicaidNumber",
    "PayerState" = ALLDATA."PayerState",
    "LastUpdatedBy" = ALLDATA."LastUpdatedBy",
    "LastUpdatedDate" = ALLDATA."LastUpdatedDate",
    "BilledRate" = ALLDATA."BilledRate",
    "TotalBilledAmount" = ALLDATA."TotalBilledAmount",
    "IsMissed" = ALLDATA."IsMissed",
    "MissedVisitReason" = ALLDATA."MissedVisitReason",
    "EVVType" = ALLDATA."EVVType",
    "PStatus" = ALLDATA."PStatus",
    "AideStatus" = ALLDATA."AideStatus",
    "P_PatientID" = ALLDATA."P_PatientID",
    "P_AppPatientID" = ALLDATA."P_AppPatientID",
    "PA_PatientID" = ALLDATA."PA_PatientID",
    "PA_AppPatientID" = ALLDATA."PA_AppPatientID",
    "P_PAdmissionID" = ALLDATA."P_PAdmissionID",
    "P_PName" = ALLDATA."P_PName",
    "P_PAddressID" = ALLDATA."P_PAddressID",
    "P_PAppAddressID" = ALLDATA."P_PAppAddressID",
    "P_PAddressL1" = ALLDATA."P_PAddressL1",
    "P_PAddressL2" = ALLDATA."P_PAddressL2",
    "P_PCity" = ALLDATA."P_PCity",
    "P_PAddressState" = ALLDATA."P_PAddressState",
    "P_PZipCode" = ALLDATA."P_PZipCode",
    "P_PCounty" = ALLDATA."P_PCounty",
    "P_PFName" = ALLDATA."P_PFName",
    "P_PLName" = ALLDATA."P_PLName",
    "P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber",
    "PA_PAdmissionID" = ALLDATA."PA_PAdmissionID",
    "PA_PName" = ALLDATA."PA_PName",
    "PA_PAddressID" = ALLDATA."PA_PAddressID",
    "PA_PAppAddressID" = ALLDATA."PA_PAppAddressID",
    "PA_PAddressL1" = ALLDATA."PA_PAddressL1",
    "PA_PAddressL2" = ALLDATA."PA_PAddressL2",
    "PA_PCity" = ALLDATA."PA_PCity",
    "PA_PAddressState" = ALLDATA."PA_PAddressState",
    "PA_PZipCode" = ALLDATA."PA_PZipCode",
    "PA_PCounty" = ALLDATA."PA_PCounty",
    "PA_PFName" = ALLDATA."PA_PFName",
    "PA_PLName" = ALLDATA."PA_PLName",
    "PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber",
    "BillRateNonBilled" = ALLDATA."BillRateNonBilled",
    "BillRateBoth" = ALLDATA."BillRateBoth",
    "FederalTaxNumber" = ALLDATA."FederalTaxNumber"
FROM (
    SELECT DISTINCT
        CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
        CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth",
        TRIM(CAR."SSN") AS "SSN",
        CAST(NULL AS VARCHAR) AS "PStatus",
        CAR."Status" AS "AideStatus",
        CR1."Missed Visit Reason" AS "MissedVisitReason",
        CR1."Is Missed" AS "IsMissed",
        CR1."Call Out Device Type" AS "EVVType",
        CR1."Billed Rate" AS "BilledRate",
        CR1."Total Billed Amount" AS "TotalBilledAmount",
        CR1."Provider Id" AS "ProviderID",
        CR1."Application Provider Id" AS "AppProviderID",
        DPR."Provider Name" AS "ProviderName",
        DPR."Phone Number 1" AS "AgencyPhone",
        DPR."Federal Tax Number" AS "FederalTaxNumber",
        CR1."Visit Id" AS "VisitID",
        CR1."Application Visit Id" AS "AppVisitID",
        CR1."Visit Date"::date AS "VisitDate",
        CR1."Scheduled Start Time"::timestamp AS "SchStartTime",
        CR1."Scheduled End Time"::timestamp AS "SchEndTime",
        CR1."Visit Start Time"::timestamp AS "VisitStartTime",
        CR1."Visit End Time"::timestamp AS "VisitEndTime",
        CR1."Call In Time"::timestamp AS "EVVStartTime",
        CR1."Call Out Time"::timestamp AS "EVVEndTime",
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
        CAST(NULL AS VARCHAR) AS "PAdmissionID",
        CAST(NULL AS VARCHAR) AS "PName",
        CAST(NULL AS VARCHAR) AS "PFName",
        CAST(NULL AS VARCHAR) AS "PLName",
        CAST(NULL AS VARCHAR) AS "PMedicaidNumber",
        CAST(NULL AS VARCHAR) AS "PAddressID",
        CAST(NULL AS VARCHAR) AS "PAppAddressID",
        CAST(NULL AS VARCHAR) AS "PAddressL1",
        CAST(NULL AS VARCHAR) AS "PAddressL2",
        CAST(NULL AS VARCHAR) AS "PCity",
        CAST(NULL AS VARCHAR) AS "PAddressState",
        CAST(NULL AS VARCHAR) AS "PZipCode",
        CAST(NULL AS VARCHAR) AS "PCounty",
        CASE 
            WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != ',' 
                THEN CAST(SPLIT_PART(CR1."Call Out GPS Coordinates", ',', 2) AS DOUBLE PRECISION)
            WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != ',' 
                THEN CAST(SPLIT_PART(CR1."Call In GPS Coordinates", ',', 2) AS DOUBLE PRECISION)
            ELSE DPAD_P."Provider_Longitude"
        END AS "PLongitude",
        CASE 
            WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != ',' 
                THEN CAST(SPLIT_PART(CR1."Call Out GPS Coordinates", ',', 1) AS DOUBLE PRECISION)
            WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != ',' 
                THEN CAST(SPLIT_PART(CR1."Call In GPS Coordinates", ',', 1) AS DOUBLE PRECISION)
            ELSE DPAD_P."Provider_Latitude"
        END AS "PLatitude",
        CR1."Payer Id" AS "PayerID",
        CR1."Application Payer Id" AS "AppPayerID",
        SPA."Payer State" AS "PayerState",
        CR1."Invoice Date"::timestamp AS "BilledDate",
        CR1."Billed Hours" AS "BilledHours",
        CR1."Billed" AS "Billed",
        DSC."Service Code Id" AS "ServiceCodeID",
        DSC."Application Service Code Id" AS "AppServiceCodeID",
        CR1."Bill Type" AS "RateType",
        DSC."Service Code" AS "ServiceCode",
        CR1."Visit Updated Timestamp"::timestamp AS "LastUpdatedDate",
        DUSR."User Fullname" AS "LastUpdatedBy",
        DPA_P."Admission Id" AS "P_PAdmissionID",
        DPA_P."Patient Name" AS "P_PName",
        DPA_P."Patient Firstname" AS "P_PFName",
        DPA_P."Patient Lastname" AS "P_PLName",
        DPA_P."Medicaid Number" AS "P_PMedicaidNumber",
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
        DPAD_PA."Patient Address Id" AS "PA_PAddressID",
        DPAD_PA."Application Patient Address Id" AS "PA_PAppAddressID",
        DPAD_PA."Address Line 1" AS "PA_PAddressL1",
        DPAD_PA."Address Line 2" AS "PA_PAddressL2",
        DPAD_PA."City" AS "PA_PCity",
        DPAD_PA."Address State" AS "PA_PAddressState",
        DPAD_PA."Zip Code" AS "PA_PZipCode",
        DPAD_PA."County" AS "PA_PCounty",
        DPA_PA."Patient Firstname" AS "PA_PFName",
        DPA_PA."Patient Lastname" AS "PA_PLName",
        DPA_PA."Medicaid Number" AS "PA_PMedicaidNumber"
    FROM {analytics_schema}.factvisitcallperformance_deleted_cr AS CR1
    INNER JOIN {analytics_schema}.dimcaregiver AS CAR 
        ON CAR."Caregiver Id" = CR1."Caregiver Id" 
        AND TRIM(CAR."SSN") IS NOT NULL 
        AND TRIM(CAR."SSN") != ''
    LEFT JOIN {analytics_schema}.dimoffice AS DOF 
        ON DOF."Office Id" = CR1."Office Id" 
        AND DOF."Is Active" = TRUE
    LEFT JOIN {analytics_schema}.dimpatient AS DPA_P 
        ON DPA_P."Patient Id" = CR1."Provider Patient Id"
    LEFT JOIN (
        SELECT 
            DDD."Patient Address Id",
            DDD."Application Patient Address Id",
            DDD."Address Line 1",
            DDD."Address Line 2",
            DDD."City",
            DDD."Address State",
            DDD."Zip Code",
            DDD."County",
            DDD."Patient Id",
            DDD."Application Patient Id",
            DDD."Longitude" AS "Provider_Longitude",
            DDD."Latitude" AS "Provider_Latitude",
            ROW_NUMBER() OVER (PARTITION BY DDD."Patient Id" ORDER BY DDD."Application Created UTC Timestamp" DESC) AS rn
        FROM {analytics_schema}.dimpatientaddress AS DDD
        WHERE DDD."Primary Address" = TRUE 
          AND DDD."Address Type" LIKE '%GPS%'
    ) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" AND DPAD_P.rn = 1
    LEFT JOIN {analytics_schema}.dimpatient AS DPA_PA 
        ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
    LEFT JOIN (
        SELECT 
            DDD."Patient Address Id",
            DDD."Application Patient Address Id",
            DDD."Address Line 1",
            DDD."Address Line 2",
            DDD."City",
            DDD."Address State",
            DDD."Zip Code",
            DDD."County",
            DDD."Patient Id",
            DDD."Application Patient Id",
            ROW_NUMBER() OVER (PARTITION BY DDD."Patient Id" ORDER BY DDD."Application Created UTC Timestamp" DESC) AS rn
        FROM {analytics_schema}.dimpatientaddress AS DDD
        WHERE DDD."Primary Address" = TRUE 
          AND DDD."Address Type" LIKE '%GPS%'
    ) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" AND DPAD_PA.rn = 1
    LEFT JOIN {analytics_schema}.dimpayer AS SPA 
        ON SPA."Payer Id" = CR1."Payer Id" 
        AND SPA."Is Active" = TRUE 
        AND SPA."Is Demo" = FALSE
    INNER JOIN {analytics_schema}.dimprovider AS DPR 
        ON DPR."Provider Id" = CR1."Provider Id" 
        AND DPR."Is Active" = TRUE 
        AND DPR."Is Demo" = FALSE
    LEFT JOIN {analytics_schema}.dimservicecode AS DSC 
        ON DSC."Service Code Id" = CR1."Service Code Id"
    LEFT JOIN {analytics_schema}.dimuser AS DUSR 
        ON DUSR."User Id" = CR1."Visit Updated User Id"
    INNER JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
        ON CVM2."VisitID" = CR1."Visit Id"
    INNER JOIN {conflict_schema}.conflicts AS C 
        ON C."CONFLICTID" = CVM2."CONFLICTID" 
        AND C."StatusFlag" = 'D'
    WHERE CR1."Visit Date" >= '{start_date}'::timestamp
      AND CR1."Visit Date" < ('{end_date}'::date + INTERVAL '1 day')
) AS ALLDATA
WHERE CVM."VisitID" = ALLDATA."VisitID";
