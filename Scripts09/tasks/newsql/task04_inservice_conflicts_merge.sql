-- ============================================================================
-- TASK_04: InService Conflict Detection & Sync (MERGE)
-- ============================================================================
-- Purpose:
--   Detect and sync conflicts between regular visits and caregiver in-service 
--   training periods. This script handles ONLY InService conflicts.
--
--   InService Conflict Definition:
--     A regular visit's actual time overlaps with a caregiver's in-service 
--     training period, indicating the caregiver cannot perform the visit.
--
--   This script mirrors Snowflake logic from:
--     - TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_1 (InService updates)
--     - TASK_03_INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_2 (InService inserts)
--
--   Key Differences from 7 Conflict Rules:
--     - InService records get SYNTHETIC VisitIDs (not from visits table)
--     - InService records have NULL patient/billing/address data
--     - All 7 conflict flags = 'N', only InServiceFlag = 'Y'
--     - Bidirectional: Visit↔InService treated as separate entities
--
-- Prerequisites:
--   - Run functions/distance_functions.sql ONCE before execution
--   - Run task02_03_conflict_detection_merge.sql FIRST (for 7 rules)
--
-- Execution: Run directly in DBeaver
--
-- Variables (set these before running):
--   :conflict_schema   - Conflict data schema (e.g., 'conflict_dev')
--   :analytics_schema  - Analytics data schema (e.g., 'analytics_dev')
--
-- Performance Notes:
--   - Two separate MERGE operations (bidirectional)
--   - Lighter than 7-rule script (no self-join on 58M rows)
--   - Expected runtime: 10-20 minutes
-- ============================================================================

-- ============================================================================
-- PART 1: Visit → InService Conflicts
-- ============================================================================
-- Detects when a regular visit overlaps with an InService training period
-- VisitID = Regular Visit ID
-- ConVisitID = Synthetic InService ID (MD5('I' || InService_ID))
-- ============================================================================

MERGE INTO :conflict_schema.conflictvisitmaps AS target
USING (
    WITH 
    -- Patient addresses CTE (reusable)
    patient_addresses AS (
        SELECT 
            "Patient Address Id",
            "Application Patient Address Id",
            "Address Line 1",
            "Address Line 2",
            "City",
            "Address State",
            "Zip Code",
            "County",
            "Patient Id",
            "Application Patient Id",
            "Longitude"::REAL AS "Provider_Longitude",
            "Latitude"::REAL AS "Provider_Latitude",
            ROW_NUMBER() OVER (
                PARTITION BY "Patient Id" 
                ORDER BY "Application Created UTC Timestamp" DESC
            ) AS rn
        FROM :analytics_schema.dimpatientaddress
        WHERE "Primary Address" = TRUE 
          AND "Address Type" LIKE '%GPS%'
    )
    
    SELECT DISTINCT
        V1."CONFLICTID",
        V1."SSN",
        V1."ProviderID",
        V1."AppProviderID",
        V1."ProviderName",
        V1."VisitID",
        V1."AppVisitID",
        V2."ProviderID" AS "ConProviderID",
        V2."AppProviderID" AS "ConAppProviderID",
        V2."ProviderName" AS "ConProviderName",
        V2."VisitID" AS "ConVisitID",
        V2."AppVisitID" AS "ConAppVisitID",
        V1."VisitDate",
        V1."SchStartTime",
        V1."SchEndTime",
        V2."SchStartTime" AS "ConSchStartTime",
        V2."SchEndTime" AS "ConSchEndTime",
        V1."VisitStartTime",
        V1."VisitEndTime",
        V2."VisitStartTime" AS "ConVisitStartTime",
        V2."VisitEndTime" AS "ConVisitEndTime",
        V1."EVVStartTime",
        V1."EVVEndTime",
        V2."EVVStartTime" AS "ConEVVStartTime",
        V2."EVVEndTime" AS "ConEVVEndTime",
        V1."CaregiverID",
        V1."AppCaregiverID",
        V1."AideCode",
        V1."AideName",
        V1."AideSSN",
        V2."CaregiverID" AS "ConCaregiverID",
        V2."AppCaregiverID" AS "ConAppCaregiverID",
        V2."AideCode" AS "ConAideCode",
        V2."AideName" AS "ConAideName",
        V2."AideSSN" AS "ConAideSSN",
        V1."OfficeID",
        V1."AppOfficeID",
        V1."Office",
        V2."OfficeID" AS "ConOfficeID",
        V2."AppOfficeID" AS "ConAppOfficeID",
        V2."Office" AS "ConOffice",
        V1."PatientID",
        V1."AppPatientID",
        V1."PAdmissionID",
        V1."PName",
        V1."PAddressID",
        V1."PAppAddressID",
        V1."PAddressL1",
        V1."PAddressL2",
        V1."PCity",
        V1."PAddressState",
        V1."PZipCode",
        V1."PCounty",
        V1."Longitude" AS "PLongitude",
        V1."Latitude" AS "PLatitude",
        -- V2 (InService) has NULL patient data
        V2."PatientID" AS "ConPatientID",
        V2."AppPatientID" AS "ConAppPatientID",
        V2."PAdmissionID" AS "ConPAdmissionID",
        V2."PName" AS "ConPName",
        V2."PAddressID" AS "ConPAddressID",
        V2."PAppAddressID" AS "ConPAppAddressID",
        V2."PAddressL1" AS "ConPAddressL1",
        V2."PAddressL2" AS "ConPAddressL2",
        V2."PCity" AS "ConPCity",
        V2."PAddressState" AS "ConPAddressState",
        V2."PZipCode" AS "ConPZipCode",
        V2."PCounty" AS "ConPCounty",
        V2."Longitude" AS "ConPLongitude",
        V2."Latitude" AS "ConPLatitude",
        V1."PayerID",
        V1."AppPayerID",
        V1."Contract",
        V2."PayerID" AS "ConPayerID",
        V2."AppPayerID" AS "ConAppPayerID",
        V2."Contract" AS "ConContract",
        V1."BilledDate",
        V2."BilledDate" AS "ConBilledDate",
        V1."BilledHours",
        V2."BilledHours" AS "ConBilledHours",
        V1."Billed",
        V2."Billed" AS "ConBilled",
        -- Distance/travel calculations are NULL for InService
        NULL::NUMERIC AS "MinuteDiffBetweenSch",
        NULL::REAL AS "DistanceMilesFromLatLng",
        NULL::REAL AS "AverageMilesPerHour",
        NULL::REAL AS "ETATravelMinutes",
        V1."ServiceCodeID",
        V1."AppServiceCodeID",
        V1."RateType",
        V1."ServiceCode",
        V2."ServiceCodeID" AS "ConServiceCodeID",
        V2."AppServiceCodeID" AS "ConAppServiceCodeID",
        V2."RateType" AS "ConRateType",
        V2."ServiceCode" AS "ConServiceCode",
        V1."InserviceStartDate",
        V1."InserviceEndDate",
        V2."InserviceStartDate" AS "ConInserviceStartDate",
        V2."InserviceEndDate" AS "ConInserviceEndDate",
        -- All 7 conflict flags = 'N' for InService conflicts
        'N' AS "SameSchTimeFlag",
        'N' AS "SameVisitTimeFlag",
        'N' AS "SchAndVisitTimeSameFlag",
        'N' AS "SchOverAnotherSchTimeFlag",
        'N' AS "VisitTimeOverAnotherVisitTimeFlag",
        'N' AS "SchTimeOverVisitTimeFlag",
        'N' AS "DistanceFlag",
        -- ONLY InServiceFlag = 'Y'
        'Y' AS "InServiceFlag",
        V1."AideFName",
        V1."AideLName",
        V2."AideFName" AS "ConAideFName",
        V2."AideLName" AS "ConAideLName",
        V1."PFName",
        V1."PLName",
        V2."PFName" AS "ConPFName",
        V2."PLName" AS "ConPLName",
        V1."PMedicaidNumber",
        V2."PMedicaidNumber" AS "ConPMedicaidNumber",
        V1."PayerState",
        V2."PayerState" AS "ConPayerState",
        V1."LastUpdatedBy",
        V2."LastUpdatedBy" AS "ConLastUpdatedBy",
        V1."LastUpdatedDate",
        V2."LastUpdatedDate" AS "ConLastUpdatedDate",
        V1."BilledRate",
        V1."TotalBilledAmount",
        V2."BilledRate" AS "ConBilledRate",
        V2."TotalBilledAmount" AS "ConTotalBilledAmount",
        V1."IsMissed",
        V1."MissedVisitReason",
        V1."EVVType",
        V2."IsMissed" AS "ConIsMissed",
        V2."MissedVisitReason" AS "ConMissedVisitReason",
        V2."EVVType" AS "ConEVVType",
        V1."PStatus",
        V2."PStatus" AS "ConPStatus",
        V1."AideStatus",
        V2."AideStatus" AS "ConAideStatus",
        V1."P_PatientID",
        V1."P_AppPatientID",
        V2."P_PatientID" AS "ConP_PatientID",
        V2."P_AppPatientID" AS "ConP_AppPatientID",
        V1."PA_PatientID",
        V1."PA_AppPatientID",
        V2."PA_PatientID" AS "ConPA_PatientID",
        V2."PA_AppPatientID" AS "ConPA_AppPatientID",
        V1."P_PAdmissionID",
        V1."P_PName",
        V1."P_PAddressID",
        V1."P_PAppAddressID",
        V1."P_PAddressL1",
        V1."P_PAddressL2",
        V1."P_PCity",
        V1."P_PAddressState",
        V1."P_PZipCode",
        V1."P_PCounty",
        V1."P_PFName",
        V1."P_PLName",
        V1."P_PMedicaidNumber",
        V2."P_PAdmissionID" AS "ConP_PAdmissionID",
        V2."P_PName" AS "ConP_PName",
        V2."P_PAddressID" AS "ConP_PAddressID",
        V2."P_PAppAddressID" AS "ConP_PAppAddressID",
        V2."P_PAddressL1" AS "ConP_PAddressL1",
        V2."P_PAddressL2" AS "ConP_PAddressL2",
        V2."P_PCity" AS "ConP_PCity",
        V2."P_PAddressState" AS "ConP_PAddressState",
        V2."P_PZipCode" AS "ConP_PZipCode",
        V2."P_PCounty" AS "ConP_PCounty",
        V2."P_PFName" AS "ConP_PFName",
        V2."P_PLName" AS "ConP_PLName",
        V2."P_PMedicaidNumber" AS "ConP_PMedicaidNumber",
        V1."PA_PAdmissionID",
        V1."PA_PName",
        V1."PA_PAddressID",
        V1."PA_PAppAddressID",
        V1."PA_PAddressL1",
        V1."PA_PAddressL2",
        V1."PA_PCity",
        V1."PA_PAddressState",
        V1."PA_PZipCode",
        V1."PA_PCounty",
        V1."PA_PFName",
        V1."PA_PLName",
        V1."PA_PMedicaidNumber",
        V2."PA_PAdmissionID" AS "ConPA_PAdmissionID",
        V2."PA_PName" AS "ConPA_PName",
        V2."PA_PAddressID" AS "ConPA_PAddressID",
        V2."PA_PAppAddressID" AS "ConPA_PAppAddressID",
        V2."PA_PAddressL1" AS "ConPA_PAddressL1",
        V2."PA_PAddressL2" AS "ConPA_PAddressL2",
        V2."PA_PCity" AS "ConPA_PCity",
        V2."PA_PAddressState" AS "ConPA_PAddressState",
        V2."PA_PZipCode" AS "ConPA_PZipCode",
        V2."PA_PCounty" AS "ConPA_PCounty",
        V2."PA_PFName" AS "ConPA_PFName",
        V2."PA_PLName" AS "ConPA_PLName",
        V2."PA_PMedicaidNumber" AS "ConPA_PMedicaidNumber",
        V1."ContractType",
        V2."ContractType" AS "ConContractType",
        V1."BillRateNonBilled",
        V2."BillRateNonBilled" AS "ConBillRateNonBilled",
        V1."BillRateBoth",
        V2."BillRateBoth" AS "ConBillRateBoth",
        V1."FederalTaxNumber",
        V2."FederalTaxNumber" AS "ConFederalTaxNumber"
        
    FROM (
        -- ====================================================================
        -- V1: Regular visits WITHOUT InService attached
        -- ====================================================================
        SELECT DISTINCT 
            CVM1."CONFLICTID",
            CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
            CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" 
                 ELSE CR1."Bill Rate Non-Billed" 
            END AS "BillRateBoth",
            TRIM(CAR."SSN") AS "SSN",
            NULL::TEXT AS "PStatus",
            CAR."Status" AS "AideStatus",
            CR1."Missed Visit Reason" AS "MissedVisitReason",
            CR1."Is Missed" AS "IsMissed",
            CR1."Call Out Device Type" AS "EVVType",
            CR1."Billed Rate" AS "BilledRate",
            CR1."Total Billed Amount" AS "TotalBilledAmount",
            CR1."Provider Id"::uuid AS "ProviderID",
            CR1."Application Provider Id" AS "AppProviderID",
            DPR."Provider Name" AS "ProviderName",
            DPR."Phone Number 1" AS "AgencyPhone",
            DPR."Federal Tax Number" AS "FederalTaxNumber",
            CR1."Visit Id"::uuid AS "VisitID",
            CR1."Application Visit Id" AS "AppVisitID",
            CR1."Visit Date"::date AS "VisitDate",
            CR1."Scheduled Start Time"::timestamp AS "SchStartTime",
            CR1."Scheduled End Time"::timestamp AS "SchEndTime",
            CR1."Visit Start Time"::timestamp AS "VisitStartTime",
            CR1."Visit End Time"::timestamp AS "VisitEndTime",
            CR1."Call In Time"::timestamp AS "EVVStartTime",
            CR1."Call Out Time"::timestamp AS "EVVEndTime",
            CR1."Caregiver Id"::uuid AS "CaregiverID",
            CR1."Application Caregiver Id" AS "AppCaregiverID",
            CAR."Caregiver Code" AS "AideCode",
            CAR."Caregiver Fullname" AS "AideName",
            CAR."Caregiver Firstname" AS "AideFName",
            CAR."Caregiver Lastname" AS "AideLName",
            TRIM(CAR."SSN") AS "AideSSN",
            CR1."Office Id"::uuid AS "OfficeID",
            CR1."Application Office Id" AS "AppOfficeID",
            DOF."Office Name" AS "Office",
            CR1."Payer Patient Id"::uuid AS "PA_PatientID",
            CR1."Application Payer Patient Id" AS "PA_AppPatientID",
            CR1."Provider Patient Id"::uuid AS "P_PatientID",
            CR1."Application Provider Patient Id" AS "P_AppPatientID",
            CR1."Patient Id"::uuid AS "PatientID",
            CR1."Application Patient Id" AS "AppPatientID",
            NULL::TEXT AS "PAdmissionID",
            NULL::TEXT AS "PName",
            NULL::TEXT AS "PFName",
            NULL::TEXT AS "PLName",
            NULL::TEXT AS "PMedicaidNumber",
            NULL::UUID AS "PAddressID",
            NULL::INT8 AS "PAppAddressID",
            NULL::TEXT AS "PAddressL1",
            NULL::TEXT AS "PAddressL2",
            NULL::TEXT AS "PCity",
            NULL::TEXT AS "PAddressState",
            NULL::TEXT AS "PZipCode",
            NULL::TEXT AS "PCounty",
            :conflict_schema.extract_gps_coordinate(
                CR1."Call Out GPS Coordinates",
                CR1."Call In GPS Coordinates",
                DPAD_P."Provider_Longitude",
                2
            ) AS "Longitude",
            :conflict_schema.extract_gps_coordinate(
                CR1."Call Out GPS Coordinates",
                CR1."Call In GPS Coordinates",
                DPAD_P."Provider_Latitude",
                1
            ) AS "Latitude",
            CR1."Payer Id"::uuid AS "PayerID",
            CR1."Application Payer Id" AS "AppPayerID",
            COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract",
            SPA."Payer State" AS "PayerState",
            CR1."Invoice Date"::timestamp AS "BilledDate",
            CR1."Billed Hours" AS "BilledHours",
            CR1."Billed" AS "Billed",
            NULL::TIMESTAMP AS "InserviceStartDate",
            NULL::TIMESTAMP AS "InserviceEndDate",
            DSC."Service Code Id"::uuid AS "ServiceCodeID",
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
                WHEN CR1."Application Payer Id" = 0 
                     AND CR1."Application Contract Id" != 0 THEN 'Internal'
                WHEN CR1."Application Payer Id" != 0 
                     AND CR1."Application Contract Id" != 0 THEN 'UPR'
                WHEN CR1."Application Payer Id" != 0 
                     AND CR1."Application Contract Id" = 0 THEN 'Payer'
            END AS "ContractType"
        FROM :analytics_schema.factvisitcallperformance_cr AS CR1
        INNER JOIN :analytics_schema.dimcaregiver AS CAR 
            ON CAR."Caregiver Id" = CR1."Caregiver Id" 
            AND TRIM(CAR."SSN") IS NOT NULL 
            AND TRIM(CAR."SSN") != ''
        LEFT JOIN :analytics_schema.dimoffice AS DOF 
            ON DOF."Office Id" = CR1."Office Id" 
            AND DOF."Is Active" = TRUE
        LEFT JOIN :analytics_schema.dimpatient AS DPA_P 
            ON DPA_P."Patient Id" = CR1."Provider Patient Id"
        LEFT JOIN patient_addresses AS DPAD_P 
            ON DPAD_P."Patient Id" = DPA_P."Patient Id" 
            AND DPAD_P.rn = 1
        LEFT JOIN :analytics_schema.dimpatient AS DPA_PA 
            ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
        LEFT JOIN patient_addresses AS DPAD_PA 
            ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" 
            AND DPAD_PA.rn = 1
        LEFT JOIN :analytics_schema.dimpayer AS SPA 
            ON SPA."Payer Id" = CR1."Payer Id" 
            AND SPA."Is Active" = TRUE 
            AND SPA."Is Demo" = FALSE
        LEFT JOIN :analytics_schema.dimcontract AS DCON 
            ON DCON."Contract Id" = CR1."Contract Id" 
            AND DCON."Is Active" = TRUE
        INNER JOIN :analytics_schema.dimprovider AS DPR 
            ON DPR."Provider Id" = CR1."Provider Id" 
            AND DPR."Is Active" = TRUE 
            AND DPR."Is Demo" = FALSE
        LEFT JOIN :conflict_schema.conflictvisitmaps AS CVM1 
            ON CVM1."VisitID" = CR1."Visit Id"::uuid
            AND CVM1."AppVisitID" = CR1."Application Visit Id"
            AND CVM1."CONFLICTID" IS NOT NULL
        LEFT JOIN :analytics_schema.dimservicecode AS DSC 
            ON DSC."Service Code Id" = CR1."Service Code Id"
        LEFT JOIN :analytics_schema.dimuser AS DUSR 
            ON DUSR."User Id" = CR1."Visit Updated User Id"
        -- LEFT JOIN to find if this visit has InService - we want visits WITHOUT InService
        LEFT JOIN :analytics_schema.factcaregiverinservice AS FCS_CHECK
            ON FCS_CHECK."Caregiver Id" = CR1."Caregiver Id"
            AND FCS_CHECK."Office Id" = CR1."Office Id"
            AND CR1."Visit Start Time" IS NOT NULL
            AND CR1."Visit End Time" IS NOT NULL
            AND CR1."Visit Start Time"::timestamp <= FCS_CHECK."Inservice end date"::timestamp
            AND CR1."Visit End Time"::timestamp >= FCS_CHECK."Inservice start date"::timestamp
            AND FCS_CHECK."Provider Id" = CR1."Provider Id"
        WHERE CR1."Is Missed" = FALSE
          AND CR1."Visit Start Time" IS NOT NULL
          AND CR1."Visit End Time" IS NOT NULL
          AND FCS_CHECK."Application Caregiver Inservice Id" IS NULL  -- Visit has NO InService
          AND CR1."Visit Date"::date BETWEEN 
              (CURRENT_DATE - INTERVAL '2 years') 
              AND (CURRENT_DATE + INTERVAL '45 days')
          AND CR1."Provider Id" NOT IN (
              SELECT "ProviderID" 
              FROM :conflict_schema.excluded_agency
          )
          AND NOT EXISTS (
              SELECT 1 
              FROM :conflict_schema.excluded_ssn AS SSN 
              WHERE TRIM(CAR."SSN") = SSN."SSN"
          )
    ) AS V1
    
    INNER JOIN (
        -- ====================================================================
        -- V2: Pure InService records (from factcaregiverinservice)
        -- ====================================================================
        SELECT DISTINCT
            CVM1."CONFLICTID",
            NULL::NUMERIC AS "BillRateNonBilled",
            NULL::NUMERIC AS "BillRateBoth",
            TRIM(CAR."SSN") AS "SSN",
            NULL::TEXT AS "PStatus",
            NULL::TEXT AS "AideStatus",
            NULL::TEXT AS "MissedVisitReason",
            NULL::BOOLEAN AS "IsMissed",
            NULL::TEXT AS "EVVType",
            NULL::NUMERIC AS "BilledRate",
            NULL::NUMERIC AS "TotalBilledAmount",
            DPR."Provider Id"::uuid AS "ProviderID",
            DPR."Application Provider Id" AS "AppProviderID",
            DPR."Provider Name" AS "ProviderName",
            DPR."Phone Number 1" AS "AgencyPhone",
            DPR."Federal Tax Number" AS "FederalTaxNumber",
            -- SYNTHETIC VisitID for InService records
            MD5('I' || FCS."Application Caregiver Inservice Id"::TEXT)::uuid AS "VisitID",
            FCS."Application Caregiver Inservice Id"::TEXT AS "AppVisitID",
            FCS."Inservice start date"::date AS "VisitDate",
            NULL::TIMESTAMP AS "SchStartTime",
            NULL::TIMESTAMP AS "SchEndTime",
            NULL::TIMESTAMP AS "VisitStartTime",
            NULL::TIMESTAMP AS "VisitEndTime",
            NULL::TIMESTAMP AS "EVVStartTime",
            NULL::TIMESTAMP AS "EVVEndTime",
            CAR."Caregiver Id"::uuid AS "CaregiverID",
            CAR."Application Caregiver Id" AS "AppCaregiverID",
            CAR."Caregiver Code" AS "AideCode",
            CAR."Caregiver Fullname" AS "AideName",
            CAR."Caregiver Firstname" AS "AideFName",
            CAR."Caregiver Lastname" AS "AideLName",
            TRIM(CAR."SSN") AS "AideSSN",
            DOF."Office Id"::uuid AS "OfficeID",
            DOF."Application Office Id" AS "AppOfficeID",
            DOF."Office Name" AS "Office",
            NULL::UUID AS "PA_PatientID",
            NULL::TEXT AS "PA_AppPatientID",
            NULL::UUID AS "P_PatientID",
            NULL::TEXT AS "P_AppPatientID",
            NULL::UUID AS "PatientID",
            NULL::TEXT AS "AppPatientID",
            NULL::TEXT AS "PAdmissionID",
            NULL::TEXT AS "PName",
            NULL::TEXT AS "PFName",
            NULL::TEXT AS "PLName",
            NULL::TEXT AS "PMedicaidNumber",
            NULL::UUID AS "PAddressID",
            NULL::INT8 AS "PAppAddressID",
            NULL::TEXT AS "PAddressL1",
            NULL::TEXT AS "PAddressL2",
            NULL::TEXT AS "PCity",
            NULL::TEXT AS "PAddressState",
            NULL::TEXT AS "PZipCode",
            NULL::TEXT AS "PCounty",
            NULL::REAL AS "Longitude",
            NULL::REAL AS "Latitude",
            NULL::UUID AS "PayerID",
            NULL::TEXT AS "AppPayerID",
            NULL::TEXT AS "Contract",
            NULL::TEXT AS "PayerState",
            NULL::TIMESTAMP AS "BilledDate",
            NULL::NUMERIC AS "BilledHours",
            NULL::TEXT AS "Billed",
            FCS."Inservice start date"::timestamp AS "InserviceStartDate",
            FCS."Inservice end date"::timestamp AS "InserviceEndDate",
            NULL::UUID AS "ServiceCodeID",
            NULL::TEXT AS "AppServiceCodeID",
            NULL::TEXT AS "RateType",
            NULL::TEXT AS "ServiceCode",
            NULL::TIMESTAMP AS "LastUpdatedDate",
            NULL::TEXT AS "LastUpdatedBy",
            NULL::TEXT AS "P_PAdmissionID",
            NULL::TEXT AS "P_PName",
            NULL::TEXT AS "P_PFName",
            NULL::TEXT AS "P_PLName",
            NULL::TEXT AS "P_PMedicaidNumber",
            NULL::TEXT AS "P_PStatus",
            NULL::UUID AS "P_PAddressID",
            NULL::INT8 AS "P_PAppAddressID",
            NULL::TEXT AS "P_PAddressL1",
            NULL::TEXT AS "P_PAddressL2",
            NULL::TEXT AS "P_PCity",
            NULL::TEXT AS "P_PAddressState",
            NULL::TEXT AS "P_PZipCode",
            NULL::TEXT AS "P_PCounty",
            NULL::TEXT AS "PA_PAdmissionID",
            NULL::TEXT AS "PA_PName",
            NULL::TEXT AS "PA_PFName",
            NULL::TEXT AS "PA_PLName",
            NULL::TEXT AS "PA_PMedicaidNumber",
            NULL::TEXT AS "PA_PStatus",
            NULL::UUID AS "PA_PAddressID",
            NULL::INT8 AS "PA_PAppAddressID",
            NULL::TEXT AS "PA_PAddressL1",
            NULL::TEXT AS "PA_PAddressL2",
            NULL::TEXT AS "PA_PCity",
            NULL::TEXT AS "PA_PAddressState",
            NULL::TEXT AS "PA_PZipCode",
            NULL::TEXT AS "PA_PCounty",
            NULL::TEXT AS "ContractType"
        FROM :analytics_schema.factcaregiverinservice AS FCS
        INNER JOIN :analytics_schema.dimcaregiver AS CAR 
            ON CAR."Caregiver Id" = FCS."Caregiver Id" 
            AND TRIM(CAR."SSN") IS NOT NULL 
            AND TRIM(CAR."SSN") != ''
        INNER JOIN :analytics_schema.dimprovider AS DPR 
            ON DPR."Provider Id" = FCS."Provider Id" 
            AND DPR."Is Active" = TRUE 
            AND DPR."Is Demo" = FALSE
        LEFT JOIN :analytics_schema.dimoffice AS DOF 
            ON DOF."Office Id" = FCS."Office Id" 
            AND DOF."Is Active" = TRUE
        LEFT JOIN :conflict_schema.conflictvisitmaps AS CVM1 
            ON CVM1."VisitID" = MD5('I' || FCS."Application Caregiver Inservice Id"::TEXT)::uuid
            AND CVM1."CONFLICTID" IS NOT NULL
        WHERE FCS."Inservice start date"::date BETWEEN 
            (CURRENT_DATE - INTERVAL '2 years') 
            AND (CURRENT_DATE + INTERVAL '45 days')
          AND FCS."Provider Id" NOT IN (
              SELECT "ProviderID" 
              FROM :conflict_schema.excluded_agency
          )
          AND NOT EXISTS (
              SELECT 1 
              FROM :conflict_schema.excluded_ssn AS SSN 
              WHERE TRIM(CAR."SSN") = SSN."SSN"
          )
    ) AS V2
        ON V1."VisitID" != V2."VisitID"
        AND V1."SSN" = V2."SSN"
        AND V1."VisitStartTime"::timestamp <= V2."InserviceEndDate"::timestamp
        AND V1."VisitEndTime"::timestamp >= V2."InserviceStartDate"::timestamp
        AND V1."ProviderID" IS NOT NULL
        AND V2."ProviderID" != V1."ProviderID"
        AND V2."AppVisitID" IS NOT NULL  -- InService has ID
        AND V1."InserviceStartDate" IS NULL  -- Visit has NO InService attached
) AS source
ON target."VisitID" = source."VisitID" 
   AND target."AppVisitID" = source."AppVisitID"
   AND target."ConVisitID" = source."ConVisitID"
   AND target."InserviceStartDate" IS NULL 
   AND target."ConInserviceStartDate" IS NOT NULL
WHEN MATCHED THEN
    UPDATE SET 
        "SSN" = source."SSN",
        "ProviderID" = source."ProviderID",
        "AppProviderID" = source."AppProviderID",
        "ProviderName" = source."ProviderName",
        "ConProviderID" = source."ConProviderID",
        "ConAppProviderID" = source."ConAppProviderID",
        "ConProviderName" = source."ConProviderName",
        "ConVisitID" = source."ConVisitID",
        "ConAppVisitID" = source."ConAppVisitID",
        "VisitDate" = source."VisitDate",
        "SchStartTime" = source."SchStartTime",
        "SchEndTime" = source."SchEndTime",
        "ConSchStartTime" = source."ConSchStartTime",
        "ConSchEndTime" = source."ConSchEndTime",
        "VisitStartTime" = source."VisitStartTime",
        "VisitEndTime" = source."VisitEndTime",
        "ConVisitStartTime" = source."ConVisitStartTime",
        "ConVisitEndTime" = source."ConVisitEndTime",
        "EVVStartTime" = source."EVVStartTime",
        "EVVEndTime" = source."EVVEndTime",
        "ConEVVStartTime" = source."ConEVVStartTime",
        "ConEVVEndTime" = source."ConEVVEndTime",
        "CaregiverID" = source."CaregiverID",
        "AppCaregiverID" = source."AppCaregiverID",
        "AideCode" = source."AideCode",
        "AideName" = source."AideName",
        "AideSSN" = source."AideSSN",
        "ConCaregiverID" = source."ConCaregiverID",
        "ConAppCaregiverID" = source."ConAppCaregiverID",
        "ConAideCode" = source."ConAideCode",
        "ConAideName" = source."ConAideName",
        "ConAideSSN" = source."ConAideSSN",
        "OfficeID" = source."OfficeID",
        "AppOfficeID" = source."AppOfficeID",
        "Office" = source."Office",
        "ConOfficeID" = source."ConOfficeID",
        "ConAppOfficeID" = source."ConAppOfficeID",
        "ConOffice" = source."ConOffice",
        "PatientID" = source."PatientID",
        "AppPatientID" = source."AppPatientID",
        "PAddressID" = source."PAddressID",
        "PAppAddressID" = source."PAppAddressID",
        "PAddressL1" = source."PAddressL1",
        "PAddressL2" = source."PAddressL2",
        "PCity" = source."PCity",
        "PAddressState" = source."PAddressState",
        "PZipCode" = source."PZipCode",
        "PCounty" = source."PCounty",
        "PLongitude" = source."PLongitude",
        "PLatitude" = source."PLatitude",
        "InServiceFlag" = CASE 
            WHEN target."InServiceFlag" = 'N' THEN source."InServiceFlag" 
            ELSE target."InServiceFlag" 
        END,
        "InserviceStartDate" = source."InserviceStartDate",
        "InserviceEndDate" = source."InserviceEndDate",
        "ConInserviceStartDate" = source."ConInserviceStartDate",
        "ConInserviceEndDate" = source."ConInserviceEndDate",
        "BillRateNonBilled" = source."BillRateNonBilled",
        "ConBillRateNonBilled" = source."ConBillRateNonBilled",
        "BillRateBoth" = source."BillRateBoth",
        "ConBillRateBoth" = source."ConBillRateBoth",
        "FederalTaxNumber" = source."FederalTaxNumber",
        "ConFederalTaxNumber" = source."ConFederalTaxNumber",
        "UpdatedDate" = CURRENT_TIMESTAMP,
        "StatusFlag" = CASE 
            WHEN target."StatusFlag" NOT IN ('W', 'I') THEN 'U' 
            ELSE target."StatusFlag" 
        END
WHEN NOT MATCHED THEN
    INSERT (
        "SSN", "ProviderID", "AppProviderID", "ProviderName",
        "VisitID", "AppVisitID",
        "ConProviderID", "ConAppProviderID", "ConProviderName",
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
        "PatientID", "AppPatientID",
        "PAddressID", "PAppAddressID",
        "PAddressL1", "PAddressL2", "PCity",
        "PAddressState", "PZipCode", "PCounty",
        "PLongitude", "PLatitude",
        "ConPatientID", "ConAppPatientID",
        "ConPAddressID", "ConPAppAddressID",
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
        "ETATravelMinutes",
        "ServiceCodeID", "AppServiceCodeID",
        "RateType", "ServiceCode",
        "ConServiceCodeID", "ConAppServiceCodeID",
        "ConRateType", "ConServiceCode",
        "InserviceStartDate", "InserviceEndDate",
        "ConInserviceStartDate", "ConInserviceEndDate",
        "SameSchTimeFlag", "SameVisitTimeFlag",
        "SchAndVisitTimeSameFlag",
        "SchOverAnotherSchTimeFlag",
        "VisitTimeOverAnotherVisitTimeFlag",
        "SchTimeOverVisitTimeFlag",
        "DistanceFlag",
        "InServiceFlag",
        "CreatedDate", "UpdatedDate",
        "AideFName", "AideLName",
        "ConAideFName", "ConAideLName",
        "PFName", "PLName",
        "ConPFName", "ConPLName",
        "PMedicaidNumber", "ConPMedicaidNumber",
        "PayerState", "ConPayerState",
        "LastUpdatedBy", "ConLastUpdatedBy",
        "LastUpdatedDate", "ConLastUpdatedDate",
        "BilledRate", "TotalBilledAmount",
        "ConBilledRate", "ConTotalBilledAmount",
        "IsMissed", "MissedVisitReason", "EVVType",
        "ConIsMissed", "ConMissedVisitReason", "ConEVVType",
        "PStatus", "ConPStatus",
        "AideStatus", "ConAideStatus",
        "P_PatientID", "P_AppPatientID",
        "ConP_PatientID", "ConP_AppPatientID",
        "PA_PatientID", "PA_AppPatientID",
        "ConPA_PatientID", "ConPA_AppPatientID",
        "P_PAdmissionID", "P_PName",
        "P_PAddressID", "P_PAppAddressID",
        "P_PAddressL1", "P_PAddressL2", "P_PCity",
        "P_PAddressState", "P_PZipCode", "P_PCounty",
        "P_PFName", "P_PLName", "P_PMedicaidNumber",
        "ConP_PAdmissionID", "ConP_PName",
        "ConP_PAddressID", "ConP_PAppAddressID",
        "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity",
        "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty",
        "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber",
        "PA_PAdmissionID", "PA_PName",
        "PA_PAddressID", "PA_PAppAddressID",
        "PA_PAddressL1", "PA_PAddressL2", "PA_PCity",
        "PA_PAddressState", "PA_PZipCode", "PA_PCounty",
        "PA_PFName", "PA_PLName", "PA_PMedicaidNumber",
        "ConPA_PAdmissionID", "ConPA_PName",
        "ConPA_PAddressID", "ConPA_PAppAddressID",
        "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity",
        "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty",
        "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber",
        "ContractType", "ConContractType",
        "BillRateNonBilled", "ConBillRateNonBilled",
        "BillRateBoth", "ConBillRateBoth",
        "FederalTaxNumber", "ConFederalTaxNumber"
    )
    VALUES (
        source."SSN", source."ProviderID", source."AppProviderID", source."ProviderName",
        source."VisitID", source."AppVisitID",
        source."ConProviderID", source."ConAppProviderID", source."ConProviderName",
        source."ConVisitID", source."ConAppVisitID",
        source."VisitDate",
        source."SchStartTime", source."SchEndTime",
        source."ConSchStartTime", source."ConSchEndTime",
        source."VisitStartTime", source."VisitEndTime",
        source."ConVisitStartTime", source."ConVisitEndTime",
        source."EVVStartTime", source."EVVEndTime",
        source."ConEVVStartTime", source."ConEVVEndTime",
        source."CaregiverID", source."AppCaregiverID",
        source."AideCode", source."AideName", source."AideSSN",
        source."ConCaregiverID", source."ConAppCaregiverID",
        source."ConAideCode", source."ConAideName", source."ConAideSSN",
        source."OfficeID", source."AppOfficeID", source."Office",
        source."ConOfficeID", source."ConAppOfficeID", source."ConOffice",
        source."PatientID", source."AppPatientID",
        source."PAddressID", source."PAppAddressID",
        source."PAddressL1", source."PAddressL2", source."PCity",
        source."PAddressState", source."PZipCode", source."PCounty",
        source."PLongitude", source."PLatitude",
        source."ConPatientID", source."ConAppPatientID",
        source."ConPAddressID", source."ConPAppAddressID",
        source."ConPAddressL1", source."ConPAddressL2", source."ConPCity",
        source."ConPAddressState", source."ConPZipCode", source."ConPCounty",
        source."ConPLongitude", source."ConPLatitude",
        source."PayerID", source."AppPayerID", source."Contract",
        source."ConPayerID", source."ConAppPayerID", source."ConContract",
        source."BilledDate", source."ConBilledDate",
        source."BilledHours", source."ConBilledHours",
        source."Billed", source."ConBilled",
        source."MinuteDiffBetweenSch",
        source."DistanceMilesFromLatLng",
        source."AverageMilesPerHour",
        source."ETATravelMinutes",
        source."ServiceCodeID", source."AppServiceCodeID",
        source."RateType", source."ServiceCode",
        source."ConServiceCodeID", source."ConAppServiceCodeID",
        source."ConRateType", source."ConServiceCode",
        source."InserviceStartDate", source."InserviceEndDate",
        source."ConInserviceStartDate", source."ConInserviceEndDate",
        source."SameSchTimeFlag", source."SameVisitTimeFlag",
        source."SchAndVisitTimeSameFlag",
        source."SchOverAnotherSchTimeFlag",
        source."VisitTimeOverAnotherVisitTimeFlag",
        source."SchTimeOverVisitTimeFlag",
        source."DistanceFlag",
        source."InServiceFlag",
        CURRENT_DATE, CURRENT_TIMESTAMP,
        source."AideFName", source."AideLName",
        source."ConAideFName", source."ConAideLName",
        source."PFName", source."PLName",
        source."ConPFName", source."ConPLName",
        source."PMedicaidNumber", source."ConPMedicaidNumber",
        source."PayerState", source."ConPayerState",
        source."LastUpdatedBy", source."ConLastUpdatedBy",
        source."LastUpdatedDate", source."ConLastUpdatedDate",
        source."BilledRate", source."TotalBilledAmount",
        source."ConBilledRate", source."ConTotalBilledAmount",
        source."IsMissed", source."MissedVisitReason", source."EVVType",
        source."ConIsMissed", source."ConMissedVisitReason", source."ConEVVType",
        source."PStatus", source."ConPStatus",
        source."AideStatus", source."ConAideStatus",
        source."P_PatientID", source."P_AppPatientID",
        source."ConP_PatientID", source."ConP_AppPatientID",
        source."PA_PatientID", source."PA_AppPatientID",
        source."ConPA_PatientID", source."ConPA_AppPatientID",
        source."P_PAdmissionID", source."P_PName",
        source."P_PAddressID", source."P_PAppAddressID",
        source."P_PAddressL1", source."P_PAddressL2", source."P_PCity",
        source."P_PAddressState", source."P_PZipCode", source."P_PCounty",
        source."P_PFName", source."P_PLName", source."P_PMedicaidNumber",
        source."ConP_PAdmissionID", source."ConP_PName",
        source."ConP_PAddressID", source."ConP_PAppAddressID",
        source."ConP_PAddressL1", source."ConP_PAddressL2", source."ConP_PCity",
        source."ConP_PAddressState", source."ConP_PZipCode", source."ConP_PCounty",
        source."ConP_PFName", source."ConP_PLName", source."ConP_PMedicaidNumber",
        source."PA_PAdmissionID", source."PA_PName",
        source."PA_PAddressID", source."PA_PAppAddressID",
        source."PA_PAddressL1", source."PA_PAddressL2", source."PA_PCity",
        source."PA_PAddressState", source."PA_PZipCode", source."PA_PCounty",
        source."PA_PFName", source."PA_PLName", source."PA_PMedicaidNumber",
        source."ConPA_PAdmissionID", source."ConPA_PName",
        source."ConPA_PAddressID", source."ConPA_PAppAddressID",
        source."ConPA_PAddressL1", source."ConPA_PAddressL2", source."ConPA_PCity",
        source."ConPA_PAddressState", source."ConPA_PZipCode", source."ConPA_PCounty",
        source."ConPA_PFName", source."ConPA_PLName", source."ConPA_PMedicaidNumber",
        source."ContractType", source."ConContractType",
        source."BillRateNonBilled", source."ConBillRateNonBilled",
        source."BillRateBoth", source."ConBillRateBoth",
        source."FederalTaxNumber", source."ConFederalTaxNumber"
WHEN NOT MATCHED THEN
    INSERT (
        "SSN", "ProviderID", "AppProviderID", "ProviderName",
        "VisitID", "AppVisitID",
        "ConProviderID", "ConAppProviderID", "ConProviderName",
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
        "PatientID", "AppPatientID",
        "PAddressID", "PAppAddressID",
        "PAddressL1", "PAddressL2", "PCity",
        "PAddressState", "PZipCode", "PCounty",
        "PLongitude", "PLatitude",
        "ConPatientID", "ConAppPatientID",
        "ConPAddressID", "ConPAppAddressID",
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
        "ETATravelMinutes",
        "ServiceCodeID", "AppServiceCodeID",
        "RateType", "ServiceCode",
        "ConServiceCodeID", "ConAppServiceCodeID",
        "ConRateType", "ConServiceCode",
        "InserviceStartDate", "InserviceEndDate",
        "ConInserviceStartDate", "ConInserviceEndDate",
        "SameSchTimeFlag", "SameVisitTimeFlag",
        "SchAndVisitTimeSameFlag",
        "SchOverAnotherSchTimeFlag",
        "VisitTimeOverAnotherVisitTimeFlag",
        "SchTimeOverVisitTimeFlag",
        "DistanceFlag",
        "InServiceFlag",
        "CreatedDate", "UpdatedDate",
        "AideFName", "AideLName",
        "ConAideFName", "ConAideLName",
        "PFName", "PLName",
        "ConPFName", "ConPLName",
        "PMedicaidNumber", "ConPMedicaidNumber",
        "PayerState", "ConPayerState",
        "LastUpdatedBy", "ConLastUpdatedBy",
        "LastUpdatedDate", "ConLastUpdatedDate",
        "BilledRate", "TotalBilledAmount",
        "ConBilledRate", "ConTotalBilledAmount",
        "IsMissed", "MissedVisitReason", "EVVType",
        "ConIsMissed", "ConMissedVisitReason", "ConEVVType",
        "PStatus", "ConPStatus",
        "AideStatus", "ConAideStatus",
        "P_PatientID", "P_AppPatientID",
        "ConP_PatientID", "ConP_AppPatientID",
        "PA_PatientID", "PA_AppPatientID",
        "ConPA_PatientID", "ConPA_AppPatientID",
        "P_PAdmissionID", "P_PName",
        "P_PAddressID", "P_PAppAddressID",
        "P_PAddressL1", "P_PAddressL2", "P_PCity",
        "P_PAddressState", "P_PZipCode", "P_PCounty",
        "P_PFName", "P_PLName", "P_PMedicaidNumber",
        "ConP_PAdmissionID", "ConP_PName",
        "ConP_PAddressID", "ConP_PAppAddressID",
        "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity",
        "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty",
        "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber",
        "PA_PAdmissionID", "PA_PName",
        "PA_PAddressID", "PA_PAppAddressID",
        "PA_PAddressL1", "PA_PAddressL2", "PA_PCity",
        "PA_PAddressState", "PA_PZipCode", "PA_PCounty",
        "PA_PFName", "PA_PLName", "PA_PMedicaidNumber",
        "ConPA_PAdmissionID", "ConPA_PName",
        "ConPA_PAddressID", "ConPA_PAppAddressID",
        "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity",
        "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty",
        "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber",
        "ContractType", "ConContractType",
        "BillRateNonBilled", "ConBillRateNonBilled",
        "BillRateBoth", "ConBillRateBoth",
        "FederalTaxNumber", "ConFederalTaxNumber"
    )
    VALUES (
        source."SSN", source."ProviderID", source."AppProviderID", source."ProviderName",
        source."VisitID", source."AppVisitID",
        source."ConProviderID", source."ConAppProviderID", source."ConProviderName",
        source."ConVisitID", source."ConAppVisitID",
        source."VisitDate",
        source."SchStartTime", source."SchEndTime",
        source."ConSchStartTime", source."ConSchEndTime",
        source."VisitStartTime", source."VisitEndTime",
        source."ConVisitStartTime", source."ConVisitEndTime",
        source."EVVStartTime", source."EVVEndTime",
        source."ConEVVStartTime", source."ConEVVEndTime",
        source."CaregiverID", source."AppCaregiverID",
        source."AideCode", source."AideName", source."AideSSN",
        source."ConCaregiverID", source."ConAppCaregiverID",
        source."ConAideCode", source."ConAideName", source."ConAideSSN",
        source."OfficeID", source."AppOfficeID", source."Office",
        source."ConOfficeID", source."ConAppOfficeID", source."ConOffice",
        source."PatientID", source."AppPatientID",
        source."PAddressID", source."PAppAddressID",
        source."PAddressL1", source."PAddressL2", source."PCity",
        source."PAddressState", source."PZipCode", source."PCounty",
        source."PLongitude", source."PLatitude",
        source."ConPatientID", source."ConAppPatientID",
        source."ConPAddressID", source."ConPAppAddressID",
        source."ConPAddressL1", source."ConPAddressL2", source."ConPCity",
        source."ConPAddressState", source."ConPZipCode", source."ConPCounty",
        source."ConPLongitude", source."ConPLatitude",
        source."PayerID", source."AppPayerID", source."Contract",
        source."ConPayerID", source."ConAppPayerID", source."ConContract",
        source."BilledDate", source."ConBilledDate",
        source."BilledHours", source."ConBilledHours",
        source."Billed", source."ConBilled",
        source."MinuteDiffBetweenSch",
        source."DistanceMilesFromLatLng",
        source."AverageMilesPerHour",
        source."ETATravelMinutes",
        source."ServiceCodeID", source."AppServiceCodeID",
        source."RateType", source."ServiceCode",
        source."ConServiceCodeID", source."ConAppServiceCodeID",
        source."ConRateType", source."ConServiceCode",
        source."InserviceStartDate", source."InserviceEndDate",
        source."ConInserviceStartDate", source."ConInserviceEndDate",
        source."SameSchTimeFlag", source."SameVisitTimeFlag",
        source."SchAndVisitTimeSameFlag",
        source."SchOverAnotherSchTimeFlag",
        source."VisitTimeOverAnotherVisitTimeFlag",
        source."SchTimeOverVisitTimeFlag",
        source."DistanceFlag",
        source."InServiceFlag",
        CURRENT_DATE, CURRENT_TIMESTAMP,
        source."AideFName", source."AideLName",
        source."ConAideFName", source."ConAideLName",
        source."PFName", source."PLName",
        source."ConPFName", source."ConPLName",
        source."PMedicaidNumber", source."ConPMedicaidNumber",
        source."PayerState", source."ConPayerState",
        source."LastUpdatedBy", source."ConLastUpdatedBy",
        source."LastUpdatedDate", source."ConLastUpdatedDate",
        source."BilledRate", source."TotalBilledAmount",
        source."ConBilledRate", source."ConTotalBilledAmount",
        source."IsMissed", source."MissedVisitReason", source."EVVType",
        source."ConIsMissed", source."ConMissedVisitReason", source."ConEVVType",
        source."PStatus", source."ConPStatus",
        source."AideStatus", source."ConAideStatus",
        source."P_PatientID", source."P_AppPatientID",
        source."ConP_PatientID", source."ConP_AppPatientID",
        source."PA_PatientID", source."PA_AppPatientID",
        source."ConPA_PatientID", source."ConPA_AppPatientID",
        source."P_PAdmissionID", source."P_PName",
        source."P_PAddressID", source."P_PAppAddressID",
        source."P_PAddressL1", source."P_PAddressL2", source."P_PCity",
        source."P_PAddressState", source."P_PZipCode", source."P_PCounty",
        source."P_PFName", source."P_PLName", source."P_PMedicaidNumber",
        source."ConP_PAdmissionID", source."ConP_PName",
        source."ConP_PAddressID", source."ConP_PAppAddressID",
        source."ConP_PAddressL1", source."ConP_PAddressL2", source."ConP_PCity",
        source."ConP_PAddressState", source."ConP_PZipCode", source."ConP_PCounty",
        source."ConP_PFName", source."ConP_PLName", source."ConP_PMedicaidNumber",
        source."PA_PAdmissionID", source."PA_PName",
        source."PA_PAddressID", source."PA_PAppAddressID",
        source."PA_PAddressL1", source."PA_PAddressL2", source."PA_PCity",
        source."PA_PAddressState", source."PA_PZipCode", source."PA_PCounty",
        source."PA_PFName", source."PA_PLName", source."PA_PMedicaidNumber",
        source."ConPA_PAdmissionID", source."ConPA_PName",
        source."ConPA_PAddressID", source."ConPA_PAppAddressID",
        source."ConPA_PAddressL1", source."ConPA_PAddressL2", source."ConPA_PCity",
        source."ConPA_PAddressState", source."ConPA_PZipCode", source."ConPA_PCounty",
        source."ConPA_PFName", source."ConPA_PLName", source."ConPA_PMedicaidNumber",
        source."ContractType", source."ConContractType",
        source."BillRateNonBilled", source."ConBillRateNonBilled",
        source."BillRateBoth", source."ConBillRateBoth",
        source."FederalTaxNumber", source."ConFederalTaxNumber"
    );

-- ============================================================================
-- PART 2: InService → Visit Conflicts  
-- ============================================================================
-- Detects when an InService training period overlaps with a regular visit
-- VisitID = Synthetic InService ID (MD5('I' || InService_ID))
-- ConVisitID = Regular Visit ID
-- This is the REVERSE pairing of Part 1
-- ============================================================================

MERGE INTO :conflict_schema.conflictvisitmaps AS target
USING (
    WITH 
    -- Patient addresses CTE (reusable)
    patient_addresses AS (
        SELECT 
            "Patient Address Id",
            "Application Patient Address Id",
            "Address Line 1",
            "Address Line 2",
            "City",
            "Address State",
            "Zip Code",
            "County",
            "Patient Id",
            "Application Patient Id",
            "Longitude"::REAL AS "Provider_Longitude",
            "Latitude"::REAL AS "Provider_Latitude",
            ROW_NUMBER() OVER (
                PARTITION BY "Patient Id" 
                ORDER BY "Application Created UTC Timestamp" DESC
            ) AS rn
        FROM :analytics_schema.dimpatientaddress
        WHERE "Primary Address" = TRUE 
          AND "Address Type" LIKE '%GPS%'
    )
    
    SELECT DISTINCT
        V1."CONFLICTID",
        V1."SSN",
        V1."ProviderID",
        V1."AppProviderID",
        V1."ProviderName",
        V1."VisitID",
        V1."AppVisitID",
        V2."ProviderID" AS "ConProviderID",
        V2."AppProviderID" AS "ConAppProviderID",
        V2."ProviderName" AS "ConProviderName",
        V2."VisitID" AS "ConVisitID",
        V2."AppVisitID" AS "ConAppVisitID",
        V2."VisitDate",  -- Use V2 VisitDate (regular visit date)
        V1."SchStartTime",
        V1."SchEndTime",
        V2."SchStartTime" AS "ConSchStartTime",
        V2."SchEndTime" AS "ConSchEndTime",
        V1."VisitStartTime",
        V1."VisitEndTime",
        V2."VisitStartTime" AS "ConVisitStartTime",
        V2."VisitEndTime" AS "ConVisitEndTime",
        V1."EVVStartTime",
        V1."EVVEndTime",
        V2."EVVStartTime" AS "ConEVVStartTime",
        V2."EVVEndTime" AS "ConEVVEndTime",
        V1."CaregiverID",
        V1."AppCaregiverID",
        V1."AideCode",
        V1."AideName",
        V1."AideSSN",
        V2."CaregiverID" AS "ConCaregiverID",
        V2."AppCaregiverID" AS "ConAppCaregiverID",
        V2."AideCode" AS "ConAideCode",
        V2."AideName" AS "ConAideName",
        V2."AideSSN" AS "ConAideSSN",
        V1."OfficeID",
        V1."AppOfficeID",
        V1."Office",
        V2."OfficeID" AS "ConOfficeID",
        V2."AppOfficeID" AS "ConAppOfficeID",
        V2."Office" AS "ConOffice",
        -- V1 (InService) has NULL patient data
        V1."PatientID",
        V1."AppPatientID",
        V1."PAdmissionID",
        V1."PName",
        V1."PAddressID",
        V1."PAppAddressID",
        V1."PAddressL1",
        V1."PAddressL2",
        V1."PCity",
        V1."PAddressState",
        V1."PZipCode",
        V1."PCounty",
        V1."Longitude" AS "PLongitude",
        V1."Latitude" AS "PLatitude",
        -- V2 (Regular Visit) has patient data
        V2."PatientID" AS "ConPatientID",
        V2."AppPatientID" AS "ConAppPatientID",
        V2."PAdmissionID" AS "ConPAdmissionID",
        V2."PName" AS "ConPName",
        V2."PAddressID" AS "ConPAddressID",
        V2."PAppAddressID" AS "ConPAppAddressID",
        V2."PAddressL1" AS "ConPAddressL1",
        V2."PAddressL2" AS "ConPAddressL2",
        V2."PCity" AS "ConPCity",
        V2."PAddressState" AS "ConPAddressState",
        V2."PZipCode" AS "ConPZipCode",
        V2."PCounty" AS "ConPCounty",
        V2."Longitude" AS "ConPLongitude",
        V2."Latitude" AS "ConPLatitude",
        V1."PayerID",
        V1."AppPayerID",
        V1."Contract",
        V2."PayerID" AS "ConPayerID",
        V2."AppPayerID" AS "ConAppPayerID",
        V2."Contract" AS "ConContract",
        V1."BilledDate",
        V2."BilledDate" AS "ConBilledDate",
        V1."BilledHours",
        V2."BilledHours" AS "ConBilledHours",
        V1."Billed",
        V2."Billed" AS "ConBilled",
        -- Distance/travel calculations are NULL for InService
        NULL::NUMERIC AS "MinuteDiffBetweenSch",
        NULL::REAL AS "DistanceMilesFromLatLng",
        NULL::REAL AS "AverageMilesPerHour",
        NULL::REAL AS "ETATravelMinutes",
        V1."ServiceCodeID",
        V1."AppServiceCodeID",
        V1."RateType",
        V1."ServiceCode",
        V2."ServiceCodeID" AS "ConServiceCodeID",
        V2."AppServiceCodeID" AS "ConAppServiceCodeID",
        V2."RateType" AS "ConRateType",
        V2."ServiceCode" AS "ConServiceCode",
        V1."InserviceStartDate",
        V1."InserviceEndDate",
        V2."InserviceStartDate" AS "ConInserviceStartDate",
        V2."InserviceEndDate" AS "ConInserviceEndDate",
        -- All 7 conflict flags = 'N' for InService conflicts
        'N' AS "SameSchTimeFlag",
        'N' AS "SameVisitTimeFlag",
        'N' AS "SchAndVisitTimeSameFlag",
        'N' AS "SchOverAnotherSchTimeFlag",
        'N' AS "VisitTimeOverAnotherVisitTimeFlag",
        'N' AS "SchTimeOverVisitTimeFlag",
        'N' AS "DistanceFlag",
        -- ONLY InServiceFlag = 'Y'
        'Y' AS "InServiceFlag",
        V1."AideFName",
        V1."AideLName",
        V2."AideFName" AS "ConAideFName",
        V2."AideLName" AS "ConAideLName",
        V1."PFName",
        V1."PLName",
        V2."PFName" AS "ConPFName",
        V2."PLName" AS "ConPLName",
        V1."PMedicaidNumber",
        V2."PMedicaidNumber" AS "ConPMedicaidNumber",
        V1."PayerState",
        V2."PayerState" AS "ConPayerState",
        V1."LastUpdatedBy",
        V2."LastUpdatedBy" AS "ConLastUpdatedBy",
        V1."LastUpdatedDate",
        V2."LastUpdatedDate" AS "ConLastUpdatedDate",
        V1."BilledRate",
        V1."TotalBilledAmount",
        V2."BilledRate" AS "ConBilledRate",
        V2."TotalBilledAmount" AS "ConTotalBilledAmount",
        V1."IsMissed",
        V1."MissedVisitReason",
        V1."EVVType",
        V2."IsMissed" AS "ConIsMissed",
        V2."MissedVisitReason" AS "ConMissedVisitReason",
        V2."EVVType" AS "ConEVVType",
        V1."PStatus",
        V2."PStatus" AS "ConPStatus",
        V1."AideStatus",
        V2."AideStatus" AS "ConAideStatus",
        V1."P_PatientID",
        V1."P_AppPatientID",
        V2."P_PatientID" AS "ConP_PatientID",
        V2."P_AppPatientID" AS "ConP_AppPatientID",
        V1."PA_PatientID",
        V1."PA_AppPatientID",
        V2."PA_PatientID" AS "ConPA_PatientID",
        V2."PA_AppPatientID" AS "ConPA_AppPatientID",
        V1."P_PAdmissionID",
        V1."P_PName",
        V1."P_PAddressID",
        V1."P_PAppAddressID",
        V1."P_PAddressL1",
        V1."P_PAddressL2",
        V1."P_PCity",
        V1."P_PAddressState",
        V1."P_PZipCode",
        V1."P_PCounty",
        V1."P_PFName",
        V1."P_PLName",
        V1."P_PMedicaidNumber",
        V2."P_PAdmissionID" AS "ConP_PAdmissionID",
        V2."P_PName" AS "ConP_PName",
        V2."P_PAddressID" AS "ConP_PAddressID",
        V2."P_PAppAddressID" AS "ConP_PAppAddressID",
        V2."P_PAddressL1" AS "ConP_PAddressL1",
        V2."P_PAddressL2" AS "ConP_PAddressL2",
        V2."P_PCity" AS "ConP_PCity",
        V2."P_PAddressState" AS "ConP_PAddressState",
        V2."P_PZipCode" AS "ConP_PZipCode",
        V2."P_PCounty" AS "ConP_PCounty",
        V2."P_PFName" AS "ConP_PFName",
        V2."P_PLName" AS "ConP_PLName",
        V2."P_PMedicaidNumber" AS "ConP_PMedicaidNumber",
        V1."PA_PAdmissionID",
        V1."PA_PName",
        V1."PA_PAddressID",
        V1."PA_PAppAddressID",
        V1."PA_PAddressL1",
        V1."PA_PAddressL2",
        V1."PA_PCity",
        V1."PA_PAddressState",
        V1."PA_PZipCode",
        V1."PA_PCounty",
        V1."PA_PFName",
        V1."PA_PLName",
        V1."PA_PMedicaidNumber",
        V2."PA_PAdmissionID" AS "ConPA_PAdmissionID",
        V2."PA_PName" AS "ConPA_PName",
        V2."PA_PAddressID" AS "ConPA_PAddressID",
        V2."PA_PAppAddressID" AS "ConPA_PAppAddressID",
        V2."PA_PAddressL1" AS "ConPA_PAddressL1",
        V2."PA_PAddressL2" AS "ConPA_PAddressL2",
        V2."PA_PCity" AS "ConPA_PCity",
        V2."PA_PAddressState" AS "ConPA_PAddressState",
        V2."PA_PZipCode" AS "ConPA_PZipCode",
        V2."PA_PCounty" AS "ConPA_PCounty",
        V2."PA_PFName" AS "ConPA_PFName",
        V2."PA_PLName" AS "ConPA_PLName",
        V2."PA_PMedicaidNumber" AS "ConPA_PMedicaidNumber",
        V1."ContractType",
        V2."ContractType" AS "ConContractType",
        V1."BillRateNonBilled",
        V2."BillRateNonBilled" AS "ConBillRateNonBilled",
        V1."BillRateBoth",
        V2."BillRateBoth" AS "ConBillRateBoth",
        V1."FederalTaxNumber",
        V2."FederalTaxNumber" AS "ConFederalTaxNumber"
        
    FROM (
        -- ====================================================================
        -- V1: Pure InService records (from factcaregiverinservice)
        -- ====================================================================
        SELECT DISTINCT
            CVM1."CONFLICTID",
            NULL::NUMERIC AS "BillRateNonBilled",
            NULL::NUMERIC AS "BillRateBoth",
            TRIM(CAR."SSN") AS "SSN",
            NULL::TEXT AS "PStatus",
            NULL::TEXT AS "AideStatus",
            NULL::TEXT AS "MissedVisitReason",
            NULL::BOOLEAN AS "IsMissed",
            NULL::TEXT AS "EVVType",
            NULL::NUMERIC AS "BilledRate",
            NULL::NUMERIC AS "TotalBilledAmount",
            DPR."Provider Id"::uuid AS "ProviderID",
            DPR."Application Provider Id" AS "AppProviderID",
            DPR."Provider Name" AS "ProviderName",
            DPR."Phone Number 1" AS "AgencyPhone",
            DPR."Federal Tax Number" AS "FederalTaxNumber",
            -- SYNTHETIC VisitID for InService records
            MD5('I' || FCS."Application Caregiver Inservice Id"::TEXT)::uuid AS "VisitID",
            FCS."Application Caregiver Inservice Id"::TEXT AS "AppVisitID",
            FCS."Inservice start date"::date AS "VisitDate",
            NULL::TIMESTAMP AS "SchStartTime",
            NULL::TIMESTAMP AS "SchEndTime",
            NULL::TIMESTAMP AS "VisitStartTime",
            NULL::TIMESTAMP AS "VisitEndTime",
            NULL::TIMESTAMP AS "EVVStartTime",
            NULL::TIMESTAMP AS "EVVEndTime",
            CAR."Caregiver Id"::uuid AS "CaregiverID",
            CAR."Application Caregiver Id" AS "AppCaregiverID",
            CAR."Caregiver Code" AS "AideCode",
            CAR."Caregiver Fullname" AS "AideName",
            CAR."Caregiver Firstname" AS "AideFName",
            CAR."Caregiver Lastname" AS "AideLName",
            TRIM(CAR."SSN") AS "AideSSN",
            DOF."Office Id"::uuid AS "OfficeID",
            DOF."Application Office Id" AS "AppOfficeID",
            DOF."Office Name" AS "Office",
            NULL::UUID AS "PA_PatientID",
            NULL::TEXT AS "PA_AppPatientID",
            NULL::UUID AS "P_PatientID",
            NULL::TEXT AS "P_AppPatientID",
            NULL::UUID AS "PatientID",
            NULL::TEXT AS "AppPatientID",
            NULL::TEXT AS "PAdmissionID",
            NULL::TEXT AS "PName",
            NULL::TEXT AS "PFName",
            NULL::TEXT AS "PLName",
            NULL::TEXT AS "PMedicaidNumber",
            NULL::UUID AS "PAddressID",
            NULL::INT8 AS "PAppAddressID",
            NULL::TEXT AS "PAddressL1",
            NULL::TEXT AS "PAddressL2",
            NULL::TEXT AS "PCity",
            NULL::TEXT AS "PAddressState",
            NULL::TEXT AS "PZipCode",
            NULL::TEXT AS "PCounty",
            NULL::REAL AS "Longitude",
            NULL::REAL AS "Latitude",
            NULL::UUID AS "PayerID",
            NULL::TEXT AS "AppPayerID",
            NULL::TEXT AS "Contract",
            NULL::TEXT AS "PayerState",
            NULL::TIMESTAMP AS "BilledDate",
            NULL::NUMERIC AS "BilledHours",
            NULL::TEXT AS "Billed",
            FCS."Inservice start date"::timestamp AS "InserviceStartDate",
            FCS."Inservice end date"::timestamp AS "InserviceEndDate",
            NULL::UUID AS "ServiceCodeID",
            NULL::TEXT AS "AppServiceCodeID",
            NULL::TEXT AS "RateType",
            NULL::TEXT AS "ServiceCode",
            NULL::TIMESTAMP AS "LastUpdatedDate",
            NULL::TEXT AS "LastUpdatedBy",
            NULL::TEXT AS "P_PAdmissionID",
            NULL::TEXT AS "P_PName",
            NULL::TEXT AS "P_PFName",
            NULL::TEXT AS "P_PLName",
            NULL::TEXT AS "P_PMedicaidNumber",
            NULL::TEXT AS "P_PStatus",
            NULL::UUID AS "P_PAddressID",
            NULL::INT8 AS "P_PAppAddressID",
            NULL::TEXT AS "P_PAddressL1",
            NULL::TEXT AS "P_PAddressL2",
            NULL::TEXT AS "P_PCity",
            NULL::TEXT AS "P_PAddressState",
            NULL::TEXT AS "P_PZipCode",
            NULL::TEXT AS "P_PCounty",
            NULL::TEXT AS "PA_PAdmissionID",
            NULL::TEXT AS "PA_PName",
            NULL::TEXT AS "PA_PFName",
            NULL::TEXT AS "PA_PLName",
            NULL::TEXT AS "PA_PMedicaidNumber",
            NULL::TEXT AS "PA_PStatus",
            NULL::UUID AS "PA_PAddressID",
            NULL::INT8 AS "PA_PAppAddressID",
            NULL::TEXT AS "PA_PAddressL1",
            NULL::TEXT AS "PA_PAddressL2",
            NULL::TEXT AS "PA_PCity",
            NULL::TEXT AS "PA_PAddressState",
            NULL::TEXT AS "PA_PZipCode",
            NULL::TEXT AS "PA_PCounty",
            NULL::TEXT AS "ContractType"
        FROM :analytics_schema.factcaregiverinservice AS FCS
        INNER JOIN :analytics_schema.dimcaregiver AS CAR 
            ON CAR."Caregiver Id" = FCS."Caregiver Id" 
            AND TRIM(CAR."SSN") IS NOT NULL 
            AND TRIM(CAR."SSN") != ''
        INNER JOIN :analytics_schema.dimprovider AS DPR 
            ON DPR."Provider Id" = FCS."Provider Id" 
            AND DPR."Is Active" = TRUE 
            AND DPR."Is Demo" = FALSE
        LEFT JOIN :analytics_schema.dimoffice AS DOF 
            ON DOF."Office Id" = FCS."Office Id" 
            AND DOF."Is Active" = TRUE
        LEFT JOIN :conflict_schema.conflictvisitmaps AS CVM1 
            ON CVM1."VisitID" = MD5('I' || FCS."Application Caregiver Inservice Id"::TEXT)::uuid
            AND CVM1."CONFLICTID" IS NOT NULL
        WHERE FCS."Inservice start date"::date BETWEEN 
            (CURRENT_DATE - INTERVAL '2 years') 
            AND (CURRENT_DATE + INTERVAL '45 days')
          AND FCS."Application Caregiver Inservice Id" IS NOT NULL
          AND FCS."Provider Id" NOT IN (
              SELECT "ProviderID" 
              FROM :conflict_schema.excluded_agency
          )
          AND NOT EXISTS (
              SELECT 1 
              FROM :conflict_schema.excluded_ssn AS SSN 
              WHERE TRIM(CAR."SSN") = SSN."SSN"
          )
    ) AS V1
    
    INNER JOIN (
        -- ====================================================================
        -- V2: Regular visits WITHOUT InService attached
        -- ====================================================================
        SELECT DISTINCT 
            CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
            CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" 
                 ELSE CR1."Bill Rate Non-Billed" 
            END AS "BillRateBoth",
            TRIM(CAR."SSN") AS "SSN",
            NULL::TEXT AS "PStatus",
            CAR."Status" AS "AideStatus",
            CR1."Missed Visit Reason" AS "MissedVisitReason",
            CR1."Is Missed" AS "IsMissed",
            CR1."Call Out Device Type" AS "EVVType",
            CR1."Billed Rate" AS "BilledRate",
            CR1."Total Billed Amount" AS "TotalBilledAmount",
            CR1."Provider Id"::uuid AS "ProviderID",
            CR1."Application Provider Id" AS "AppProviderID",
            DPR."Provider Name" AS "ProviderName",
            DPR."Phone Number 1" AS "AgencyPhone",
            DPR."Federal Tax Number" AS "FederalTaxNumber",
            CR1."Visit Id"::uuid AS "VisitID",
            CR1."Application Visit Id" AS "AppVisitID",
            CR1."Visit Date"::date AS "VisitDate",
            CR1."Scheduled Start Time"::timestamp AS "SchStartTime",
            CR1."Scheduled End Time"::timestamp AS "SchEndTime",
            CR1."Visit Start Time"::timestamp AS "VisitStartTime",
            CR1."Visit End Time"::timestamp AS "VisitEndTime",
            CR1."Call In Time"::timestamp AS "EVVStartTime",
            CR1."Call Out Time"::timestamp AS "EVVEndTime",
            CR1."Caregiver Id"::uuid AS "CaregiverID",
            CR1."Application Caregiver Id" AS "AppCaregiverID",
            CAR."Caregiver Code" AS "AideCode",
            CAR."Caregiver Fullname" AS "AideName",
            CAR."Caregiver Firstname" AS "AideFName",
            CAR."Caregiver Lastname" AS "AideLName",
            TRIM(CAR."SSN") AS "AideSSN",
            CR1."Office Id"::uuid AS "OfficeID",
            CR1."Application Office Id" AS "AppOfficeID",
            DOF."Office Name" AS "Office",
            CR1."Payer Patient Id"::uuid AS "PA_PatientID",
            CR1."Application Payer Patient Id" AS "PA_AppPatientID",
            CR1."Provider Patient Id"::uuid AS "P_PatientID",
            CR1."Application Provider Patient Id" AS "P_AppPatientID",
            CR1."Patient Id"::uuid AS "PatientID",
            CR1."Application Patient Id" AS "AppPatientID",
            NULL::TEXT AS "PAdmissionID",
            NULL::TEXT AS "PName",
            NULL::TEXT AS "PFName",
            NULL::TEXT AS "PLName",
            NULL::TEXT AS "PMedicaidNumber",
            NULL::UUID AS "PAddressID",
            NULL::INT8 AS "PAppAddressID",
            NULL::TEXT AS "PAddressL1",
            NULL::TEXT AS "PAddressL2",
            NULL::TEXT AS "PCity",
            NULL::TEXT AS "PAddressState",
            NULL::TEXT AS "PZipCode",
            NULL::TEXT AS "PCounty",
            :conflict_schema.extract_gps_coordinate(
                CR1."Call Out GPS Coordinates",
                CR1."Call In GPS Coordinates",
                DPAD_P."Provider_Longitude",
                2
            ) AS "Longitude",
            :conflict_schema.extract_gps_coordinate(
                CR1."Call Out GPS Coordinates",
                CR1."Call In GPS Coordinates",
                DPAD_P."Provider_Latitude",
                1
            ) AS "Latitude",
            CR1."Payer Id"::uuid AS "PayerID",
            CR1."Application Payer Id" AS "AppPayerID",
            COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract",
            SPA."Payer State" AS "PayerState",
            CR1."Invoice Date"::timestamp AS "BilledDate",
            CR1."Billed Hours" AS "BilledHours",
            CR1."Billed" AS "Billed",
            NULL::TIMESTAMP AS "InserviceStartDate",
            NULL::TIMESTAMP AS "InserviceEndDate",
            DSC."Service Code Id"::uuid AS "ServiceCodeID",
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
                WHEN CR1."Application Payer Id" = 0 
                     AND CR1."Application Contract Id" != 0 THEN 'Internal'
                WHEN CR1."Application Payer Id" != 0 
                     AND CR1."Application Contract Id" != 0 THEN 'UPR'
                WHEN CR1."Application Payer Id" != 0 
                     AND CR1."Application Contract Id" = 0 THEN 'Payer'
            END AS "ContractType"
        FROM :analytics_schema.factvisitcallperformance_cr AS CR1
        INNER JOIN :analytics_schema.dimcaregiver AS CAR 
            ON CAR."Caregiver Id" = CR1."Caregiver Id" 
            AND TRIM(CAR."SSN") IS NOT NULL 
            AND TRIM(CAR."SSN") != ''
        LEFT JOIN :analytics_schema.dimoffice AS DOF 
            ON DOF."Office Id" = CR1."Office Id" 
            AND DOF."Is Active" = TRUE
        LEFT JOIN :analytics_schema.dimpatient AS DPA_P 
            ON DPA_P."Patient Id" = CR1."Provider Patient Id"
        LEFT JOIN patient_addresses AS DPAD_P 
            ON DPAD_P."Patient Id" = DPA_P."Patient Id" 
            AND DPAD_P.rn = 1
        LEFT JOIN :analytics_schema.dimpatient AS DPA_PA 
            ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
        LEFT JOIN patient_addresses AS DPAD_PA 
            ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" 
            AND DPAD_PA.rn = 1
        LEFT JOIN :analytics_schema.dimpayer AS SPA 
            ON SPA."Payer Id" = CR1."Payer Id" 
            AND SPA."Is Active" = TRUE 
            AND SPA."Is Demo" = FALSE
        LEFT JOIN :analytics_schema.dimcontract AS DCON 
            ON DCON."Contract Id" = CR1."Contract Id" 
            AND DCON."Is Active" = TRUE
        INNER JOIN :analytics_schema.dimprovider AS DPR 
            ON DPR."Provider Id" = CR1."Provider Id" 
            AND DPR."Is Active" = TRUE 
            AND DPR."Is Demo" = FALSE
        LEFT JOIN :analytics_schema.dimservicecode AS DSC 
            ON DSC."Service Code Id" = CR1."Service Code Id"
        LEFT JOIN :analytics_schema.dimuser AS DUSR 
            ON DUSR."User Id" = CR1."Visit Updated User Id"
        -- LEFT JOIN to check for InService - we want visits WITHOUT InService
        LEFT JOIN :analytics_schema.factcaregiverinservice AS FCS_CHECK
            ON FCS_CHECK."Caregiver Id" = CR1."Caregiver Id"
            AND FCS_CHECK."Office Id" = CR1."Office Id"
            AND CR1."Visit Start Time" IS NOT NULL
            AND CR1."Visit End Time" IS NOT NULL
            AND CR1."Visit Start Time"::timestamp <= FCS_CHECK."Inservice end date"::timestamp
            AND CR1."Visit End Time"::timestamp >= FCS_CHECK."Inservice start date"::timestamp
            AND FCS_CHECK."Provider Id" = CR1."Provider Id"
        WHERE CR1."Is Missed" = FALSE
          AND CR1."Visit Start Time" IS NOT NULL
          AND CR1."Visit End Time" IS NOT NULL
          AND FCS_CHECK."Application Caregiver Inservice Id" IS NULL  -- Visit has NO InService
          AND CR1."Visit Date"::date BETWEEN 
              (CURRENT_DATE - INTERVAL '2 years') 
              AND (CURRENT_DATE + INTERVAL '45 days')
          AND CR1."Provider Id" NOT IN (
              SELECT "ProviderID" 
              FROM :conflict_schema.excluded_agency
          )
          AND NOT EXISTS (
              SELECT 1 
              FROM :conflict_schema.excluded_ssn AS SSN 
              WHERE TRIM(CAR."SSN") = SSN."SSN"
          )
    ) AS V2
        ON V1."VisitID" != V2."VisitID"
        AND V1."SSN" = V2."SSN"
        AND V2."VisitStartTime"::timestamp <= V1."InserviceEndDate"::timestamp
        AND V2."VisitEndTime"::timestamp >= V1."InserviceStartDate"::timestamp
        AND V1."ProviderID" IS NOT NULL
        AND V2."ProviderID" != V1."ProviderID"
) AS source
ON target."VisitID" = source."VisitID" 
   AND target."AppVisitID" = source."AppVisitID"
   AND target."ConVisitID" = source."ConVisitID"
   AND target."ConInserviceStartDate" IS NULL
   AND target."InserviceStartDate" IS NOT NULL
WHEN MATCHED THEN
    UPDATE SET 
        "SSN" = source."SSN",
        "ProviderID" = source."ProviderID",
        "AppProviderID" = source."AppProviderID",
        "ProviderName" = source."ProviderName",
        "ConProviderID" = source."ConProviderID",
        "ConAppProviderID" = source."ConAppProviderID",
        "ConProviderName" = source."ConProviderName",
        "ConVisitID" = source."ConVisitID",
        "ConAppVisitID" = source."ConAppVisitID",
        "VisitDate" = source."VisitDate",
        "SchStartTime" = source."SchStartTime",
        "SchEndTime" = source."SchEndTime",
        "ConSchStartTime" = source."ConSchStartTime",
        "ConSchEndTime" = source."ConSchEndTime",
        "VisitStartTime" = source."VisitStartTime",
        "VisitEndTime" = source."VisitEndTime",
        "ConVisitStartTime" = source."ConVisitStartTime",
        "ConVisitEndTime" = source."ConVisitEndTime",
        "EVVStartTime" = source."EVVStartTime",
        "EVVEndTime" = source."EVVEndTime",
        "ConEVVStartTime" = source."ConEVVStartTime",
        "ConEVVEndTime" = source."ConEVVEndTime",
        "CaregiverID" = source."CaregiverID",
        "AppCaregiverID" = source."AppCaregiverID",
        "AideCode" = source."AideCode",
        "AideName" = source."AideName",
        "AideSSN" = source."AideSSN",
        "ConCaregiverID" = source."ConCaregiverID",
        "ConAppCaregiverID" = source."ConAppCaregiverID",
        "ConAideCode" = source."ConAideCode",
        "ConAideName" = source."ConAideName",
        "ConAideSSN" = source."ConAideSSN",
        "OfficeID" = source."OfficeID",
        "AppOfficeID" = source."AppOfficeID",
        "Office" = source."Office",
        "ConOfficeID" = source."ConOfficeID",
        "ConAppOfficeID" = source."ConAppOfficeID",
        "ConOffice" = source."ConOffice",
        "PatientID" = source."PatientID",
        "AppPatientID" = source."AppPatientID",
        "PAddressID" = source."PAddressID",
        "PAppAddressID" = source."PAppAddressID",
        "PAddressL1" = source."PAddressL1",
        "PAddressL2" = source."PAddressL2",
        "PCity" = source."PCity",
        "PAddressState" = source."PAddressState",
        "PZipCode" = source."PZipCode",
        "PCounty" = source."PCounty",
        "PLongitude" = source."PLongitude",
        "PLatitude" = source."PLatitude",
        "InServiceFlag" = CASE 
            WHEN target."InServiceFlag" = 'N' THEN source."InServiceFlag" 
            ELSE target."InServiceFlag" 
        END,
        "InserviceStartDate" = source."InserviceStartDate",
        "InserviceEndDate" = source."InserviceEndDate",
        "ConInserviceStartDate" = source."ConInserviceStartDate",
        "ConInserviceEndDate" = source."ConInserviceEndDate",
        "BillRateNonBilled" = source."BillRateNonBilled",
        "ConBillRateNonBilled" = source."ConBillRateNonBilled",
        "BillRateBoth" = source."BillRateBoth",
        "ConBillRateBoth" = source."ConBillRateBoth",
        "FederalTaxNumber" = source."FederalTaxNumber",
        "ConFederalTaxNumber" = source."ConFederalTaxNumber",
        "UpdatedDate" = CURRENT_TIMESTAMP,
        "StatusFlag" = CASE 
            WHEN target."StatusFlag" NOT IN ('W', 'I') THEN 'U' 
            ELSE target."StatusFlag" 
        END
WHEN NOT MATCHED THEN
    INSERT (
        "SSN", "ProviderID", "AppProviderID", "ProviderName",
        "VisitID", "AppVisitID",
        "ConProviderID", "ConAppProviderID", "ConProviderName",
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
        "PatientID", "AppPatientID",
        "PAddressID", "PAppAddressID",
        "PAddressL1", "PAddressL2", "PCity",
        "PAddressState", "PZipCode", "PCounty",
        "PLongitude", "PLatitude",
        "ConPatientID", "ConAppPatientID",
        "ConPAddressID", "ConPAppAddressID",
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
        "ETATravelMinutes",
        "ServiceCodeID", "AppServiceCodeID",
        "RateType", "ServiceCode",
        "ConServiceCodeID", "ConAppServiceCodeID",
        "ConRateType", "ConServiceCode",
        "InserviceStartDate", "InserviceEndDate",
        "ConInserviceStartDate", "ConInserviceEndDate",
        "SameSchTimeFlag", "SameVisitTimeFlag",
        "SchAndVisitTimeSameFlag",
        "SchOverAnotherSchTimeFlag",
        "VisitTimeOverAnotherVisitTimeFlag",
        "SchTimeOverVisitTimeFlag",
        "DistanceFlag",
        "InServiceFlag",
        "CreatedDate", "UpdatedDate",
        "AideFName", "AideLName",
        "ConAideFName", "ConAideLName",
        "PFName", "PLName",
        "ConPFName", "ConPLName",
        "PMedicaidNumber", "ConPMedicaidNumber",
        "PayerState", "ConPayerState",
        "LastUpdatedBy", "ConLastUpdatedBy",
        "LastUpdatedDate", "ConLastUpdatedDate",
        "BilledRate", "TotalBilledAmount",
        "ConBilledRate", "ConTotalBilledAmount",
        "IsMissed", "MissedVisitReason", "EVVType",
        "ConIsMissed", "ConMissedVisitReason", "ConEVVType",
        "PStatus", "ConPStatus",
        "AideStatus", "ConAideStatus",
        "P_PatientID", "P_AppPatientID",
        "ConP_PatientID", "ConP_AppPatientID",
        "PA_PatientID", "PA_AppPatientID",
        "ConPA_PatientID", "ConPA_AppPatientID",
        "P_PAdmissionID", "P_PName",
        "P_PAddressID", "P_PAppAddressID",
        "P_PAddressL1", "P_PAddressL2", "P_PCity",
        "P_PAddressState", "P_PZipCode", "P_PCounty",
        "P_PFName", "P_PLName", "P_PMedicaidNumber",
        "ConP_PAdmissionID", "ConP_PName",
        "ConP_PAddressID", "ConP_PAppAddressID",
        "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity",
        "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty",
        "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber",
        "PA_PAdmissionID", "PA_PName",
        "PA_PAddressID", "PA_PAppAddressID",
        "PA_PAddressL1", "PA_PAddressL2", "PA_PCity",
        "PA_PAddressState", "PA_PZipCode", "PA_PCounty",
        "PA_PFName", "PA_PLName", "PA_PMedicaidNumber",
        "ConPA_PAdmissionID", "ConPA_PName",
        "ConPA_PAddressID", "ConPA_PAppAddressID",
        "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity",
        "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty",
        "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber",
        "ContractType", "ConContractType",
        "BillRateNonBilled", "ConBillRateNonBilled",
        "BillRateBoth", "ConBillRateBoth",
        "FederalTaxNumber", "ConFederalTaxNumber"
    )
    VALUES (
        source."SSN", source."ProviderID", source."AppProviderID", source."ProviderName",
        source."VisitID", source."AppVisitID",
        source."ConProviderID", source."ConAppProviderID", source."ConProviderName",
        source."ConVisitID", source."ConAppVisitID",
        source."VisitDate",
        source."SchStartTime", source."SchEndTime",
        source."ConSchStartTime", source."ConSchEndTime",
        source."VisitStartTime", source."VisitEndTime",
        source."ConVisitStartTime", source."ConVisitEndTime",
        source."EVVStartTime", source."EVVEndTime",
        source."ConEVVStartTime", source."ConEVVEndTime",
        source."CaregiverID", source."AppCaregiverID",
        source."AideCode", source."AideName", source."AideSSN",
        source."ConCaregiverID", source."ConAppCaregiverID",
        source."ConAideCode", source."ConAideName", source."ConAideSSN",
        source."OfficeID", source."AppOfficeID", source."Office",
        source."ConOfficeID", source."ConAppOfficeID", source."ConOffice",
        source."PatientID", source."AppPatientID",
        source."PAddressID", source."PAppAddressID",
        source."PAddressL1", source."PAddressL2", source."PCity",
        source."PAddressState", source."PZipCode", source."PCounty",
        source."PLongitude", source."PLatitude",
        source."ConPatientID", source."ConAppPatientID",
        source."ConPAddressID", source."ConPAppAddressID",
        source."ConPAddressL1", source."ConPAddressL2", source."ConPCity",
        source."ConPAddressState", source."ConPZipCode", source."ConPCounty",
        source."ConPLongitude", source."ConPLatitude",
        source."PayerID", source."AppPayerID", source."Contract",
        source."ConPayerID", source."ConAppPayerID", source."ConContract",
        source."BilledDate", source."ConBilledDate",
        source."BilledHours", source."ConBilledHours",
        source."Billed", source."ConBilled",
        source."MinuteDiffBetweenSch",
        source."DistanceMilesFromLatLng",
        source."AverageMilesPerHour",
        source."ETATravelMinutes",
        source."ServiceCodeID", source."AppServiceCodeID",
        source."RateType", source."ServiceCode",
        source."ConServiceCodeID", source."ConAppServiceCodeID",
        source."ConRateType", source."ConServiceCode",
        source."InserviceStartDate", source."InserviceEndDate",
        source."ConInserviceStartDate", source."ConInserviceEndDate",
        source."SameSchTimeFlag", source."SameVisitTimeFlag",
        source."SchAndVisitTimeSameFlag",
        source."SchOverAnotherSchTimeFlag",
        source."VisitTimeOverAnotherVisitTimeFlag",
        source."SchTimeOverVisitTimeFlag",
        source."DistanceFlag",
        source."InServiceFlag",
        CURRENT_DATE, CURRENT_TIMESTAMP,
        source."AideFName", source."AideLName",
        source."ConAideFName", source."ConAideLName",
        source."PFName", source."PLName",
        source."ConPFName", source."ConPLName",
        source."PMedicaidNumber", source."ConPMedicaidNumber",
        source."PayerState", source."ConPayerState",
        source."LastUpdatedBy", source."ConLastUpdatedBy",
        source."LastUpdatedDate", source."ConLastUpdatedDate",
        source."BilledRate", source."TotalBilledAmount",
        source."ConBilledRate", source."ConTotalBilledAmount",
        source."IsMissed", source."MissedVisitReason", source."EVVType",
        source."ConIsMissed", source."ConMissedVisitReason", source."ConEVVType",
        source."PStatus", source."ConPStatus",
        source."AideStatus", source."ConAideStatus",
        source."P_PatientID", source."P_AppPatientID",
        source."ConP_PatientID", source."ConP_AppPatientID",
        source."PA_PatientID", source."PA_AppPatientID",
        source."ConPA_PatientID", source."ConPA_AppPatientID",
        source."P_PAdmissionID", source."P_PName",
        "P_PAddressID", source."P_PAppAddressID",
        source."P_PAddressL1", source."P_PAddressL2", source."P_PCity",
        source."P_PAddressState", source."P_PZipCode", source."P_PCounty",
        source."P_PFName", source."P_PLName", source."P_PMedicaidNumber",
        source."ConP_PAdmissionID", source."ConP_PName",
        source."ConP_PAddressID", source."ConP_PAppAddressID",
        source."ConP_PAddressL1", source."ConP_PAddressL2", source."ConP_PCity",
        source."ConP_PAddressState", source."ConP_PZipCode", source."ConP_PCounty",
        source."ConP_PFName", source."ConP_PLName", source."ConP_PMedicaidNumber",
        source."PA_PAdmissionID", source."PA_PName",
        source."PA_PAddressID", source."PA_PAppAddressID",
        source."PA_PAddressL1", source."PA_PAddressL2", source."PA_PCity",
        source."PA_PAddressState", source."PA_PZipCode", source."PA_PCounty",
        source."PA_PFName", source."PA_PLName", source."PA_PMedicaidNumber",
        source."ConPA_PAdmissionID", source."ConPA_PName",
        source."ConPA_PAddressID", source."ConPA_PAppAddressID",
        source."ConPA_PAddressL1", source."ConPA_PAddressL2", source."ConPA_PCity",
        source."ConPA_PAddressState", source."ConPA_PZipCode", source."ConPA_PCounty",
        source."ConPA_PFName", source."ConPA_PLName", source."ConPA_PMedicaidNumber",
        source."ContractType", source."ConContractType",
        source."BillRateNonBilled", source."ConBillRateNonBilled",
        source."BillRateBoth", source."ConBillRateBoth",
        source."FederalTaxNumber", source."ConFederalTaxNumber"
WHEN NOT MATCHED THEN
    INSERT (
        "SSN", "ProviderID", "AppProviderID", "ProviderName",
        "VisitID", "AppVisitID",
        "ConProviderID", "ConAppProviderID", "ConProviderName",
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
        "PatientID", "AppPatientID",
        "PAddressID", "PAppAddressID",
        "PAddressL1", "PAddressL2", "PCity",
        "PAddressState", "PZipCode", "PCounty",
        "PLongitude", "PLatitude",
        "ConPatientID", "ConAppPatientID",
        "ConPAddressID", "ConPAppAddressID",
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
        "ETATravelMinutes",
        "ServiceCodeID", "AppServiceCodeID",
        "RateType", "ServiceCode",
        "ConServiceCodeID", "ConAppServiceCodeID",
        "ConRateType", "ConServiceCode",
        "InserviceStartDate", "InserviceEndDate",
        "ConInserviceStartDate", "ConInserviceEndDate",
        "SameSchTimeFlag", "SameVisitTimeFlag",
        "SchAndVisitTimeSameFlag",
        "SchOverAnotherSchTimeFlag",
        "VisitTimeOverAnotherVisitTimeFlag",
        "SchTimeOverVisitTimeFlag",
        "DistanceFlag",
        "InServiceFlag",
        "CreatedDate", "UpdatedDate",
        "AideFName", "AideLName",
        "ConAideFName", "ConAideLName",
        "PFName", "PLName",
        "ConPFName", "ConPLName",
        "PMedicaidNumber", "ConPMedicaidNumber",
        "PayerState", "ConPayerState",
        "LastUpdatedBy", "ConLastUpdatedBy",
        "LastUpdatedDate", "ConLastUpdatedDate",
        "BilledRate", "TotalBilledAmount",
        "ConBilledRate", "ConTotalBilledAmount",
        "IsMissed", "MissedVisitReason", "EVVType",
        "ConIsMissed", "ConMissedVisitReason", "ConEVVType",
        "PStatus", "ConPStatus",
        "AideStatus", "ConAideStatus",
        "P_PatientID", "P_AppPatientID",
        "ConP_PatientID", "ConP_AppPatientID",
        "PA_PatientID", "PA_AppPatientID",
        "ConPA_PatientID", "ConPA_AppPatientID",
        "P_PAdmissionID", "P_PName",
        "P_PAddressID", "P_PAppAddressID",
        "P_PAddressL1", "P_PAddressL2", "P_PCity",
        "P_PAddressState", "P_PZipCode", "P_PCounty",
        "P_PFName", "P_PLName", "P_PMedicaidNumber",
        "ConP_PAdmissionID", "ConP_PName",
        "ConP_PAddressID", "ConP_PAppAddressID",
        "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity",
        "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty",
        "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber",
        "PA_PAdmissionID", "PA_PName",
        "PA_PAddressID", "PA_PAppAddressID",
        "PA_PAddressL1", "PA_PAddressL2", "PA_PCity",
        "PA_PAddressState", "PA_PZipCode", "PA_PCounty",
        "PA_PFName", "PA_PLName", "PA_PMedicaidNumber",
        "ConPA_PAdmissionID", "ConPA_PName",
        "ConPA_PAddressID", "ConPA_PAppAddressID",
        "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity",
        "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty",
        "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber",
        "ContractType", "ConContractType",
        "BillRateNonBilled", "ConBillRateNonBilled",
        "BillRateBoth", "ConBillRateBoth",
        "FederalTaxNumber", "ConFederalTaxNumber"
    );

-- ============================================================================
-- TASK_04: InService Conflicts Complete
-- ============================================================================
-- Expected outcome:
--   - Part 1: Visit→InService conflicts detected and synced
--   - Part 2: InService→Visit conflicts detected and synced
--   - InServiceFlag = 'Y', all other flags = 'N'
--   - Synthetic VisitIDs created for InService records
--
-- Expected runtime: 10-20 minutes
--
-- Testing:
--   - Verify synthetic InService IDs: SELECT * WHERE "VisitID" LIKE 'I%'
--   - Check bidirectional pairs exist
--   - Verify InServiceFlag = 'Y' and other flags = 'N'
--
-- Execution Order:
--   1. Run functions/distance_functions.sql (ONCE)
--   2. Run task02_03_conflict_detection_merge_OPTIMIZED_V2.sql (7 rules)
--   3. Run THIS script (InService conflicts)
--   4. Run task05_pto_conflicts_merge.sql (PTO conflicts - if needed)
-- ============================================================================
