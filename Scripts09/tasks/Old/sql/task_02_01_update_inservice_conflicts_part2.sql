-- PART 2: Update conflicts where Visit HAS InService but ConVisit has NO InService
-- PostgreSQL UUID-based approach using CTE pattern for UPDATE...FROM

WITH source_data AS (
    SELECT 
        CVM."CONFLICTID",
        CVM."VisitID" AS cvm_visit_id,
        CVM."ConVisitID" AS cvm_con_visit_id,
        
        -- Visit side data (from CR1)
        TRIM(CAR1."SSN") AS "SSN",
        CR1."Provider Id" AS "ProviderID",
        CR1."Application Provider Id" AS "AppProviderID",
        DPR1."Provider Name" AS "ProviderName",
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
        CAR1."Caregiver Code" AS "AideCode",
        CAR1."Caregiver Fullname" AS "AideName",
        CAR1."Caregiver Firstname" AS "AideFName",
        CAR1."Caregiver Lastname" AS "AideLName",
        TRIM(CAR1."SSN") AS "AideSSN",
        CAR1."Status" AS "AideStatus",
        CR1."Office Id" AS "OfficeID",
        CR1."Application Office Id" AS "AppOfficeID",
        DOF1."Office Name" AS "Office",
        CR1."Patient Id" AS "PatientID",
        CR1."Application Patient Id" AS "AppPatientID",
        CR1."Provider Patient Id" AS "P_PatientID",
        CR1."Application Provider Patient Id" AS "P_AppPatientID",
        CR1."Payer Patient Id" AS "PA_PatientID",
        CR1."Application Payer Patient Id" AS "PA_AppPatientID",
        CR1."Payer Id" AS "PayerID",
        CR1."Application Payer Id" AS "AppPayerID",
        COALESCE(SPA1."Payer Name", DCON1."Contract Name") AS "Contract",
        SPA1."Payer State" AS "PayerState",
        CR1."Invoice Date"::timestamp AS "BilledDate",
        CR1."Billed Hours" AS "BilledHours",
        CR1."Billed" AS "Billed",
        CR1."Billed Rate" AS "BilledRate",
        CR1."Total Billed Amount" AS "TotalBilledAmount",
        CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
        CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth",
        DSC1."Service Code Id" AS "ServiceCodeID",
        DSC1."Application Service Code Id"::NUMERIC AS "AppServiceCodeID",
        CR1."Bill Type" AS "RateType",
        DSC1."Service Code" AS "ServiceCode",
        CR1."Is Missed" AS "IsMissed",
        CR1."Missed Visit Reason" AS "MissedVisitReason",
        CR1."Call Out Device Type" AS "EVVType",
        CR1."Visit Updated Timestamp"::timestamp AS "LastUpdatedDate",
        DUSR1."User Fullname" AS "LastUpdatedBy",
        DPR1."Phone Number 1" AS "AgencyPhone",
        DPR1."Federal Tax Number" AS "FederalTaxNumber",
        CASE 
            WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ',' 
            THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION)
            WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ',' 
            THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION)
            ELSE NULL 
        END AS "PLongitude",
        CASE 
            WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ',' 
            THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION)
            WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ',' 
            THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION)
            ELSE NULL 
        END AS "PLatitude",
        CASE 
            WHEN (CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" <> '0') THEN 'Internal'
            WHEN (CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" <> '0') THEN 'UPR'
            WHEN (CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" = '0') THEN 'Payer'
        END AS "ContractType",
        -- Visit side InService (should be populated for Part 2)
        FCS1."Inservice start date"::timestamp AS "InserviceStartDate",
        FCS1."Inservice end date"::timestamp AS "InserviceEndDate",
        
        -- ConVisit side data (from CR2)
        CR2."Provider Id" AS "ConProviderID",
        CR2."Application Provider Id" AS "ConAppProviderID",
        DPR2."Provider Name" AS "ConProviderName",
        CR2."Visit Id" AS "ConVisitID",
        CR2."Application Visit Id" AS "ConAppVisitID",
        CR2."Scheduled Start Time"::timestamp AS "ConSchStartTime",
        CR2."Scheduled End Time"::timestamp AS "ConSchEndTime",
        CR2."Visit Start Time"::timestamp AS "ConVisitStartTime",
        CR2."Visit End Time"::timestamp AS "ConVisitEndTime",
        CR2."Call In Time"::timestamp AS "ConEVVStartTime",
        CR2."Call Out Time"::timestamp AS "ConEVVEndTime",
        CR2."Caregiver Id" AS "ConCaregiverID",
        CR2."Application Caregiver Id" AS "ConAppCaregiverID",
        CAR2."Caregiver Code" AS "ConAideCode",
        CAR2."Caregiver Fullname" AS "ConAideName",
        CAR2."Caregiver Firstname" AS "ConAideFName",
        CAR2."Caregiver Lastname" AS "ConAideLName",
        TRIM(CAR2."SSN") AS "ConAideSSN",
        CAR2."Status" AS "ConAideStatus",
        CR2."Office Id" AS "ConOfficeID",
        CR2."Application Office Id" AS "ConAppOfficeID",
        DOF2."Office Name" AS "ConOffice",
        CR2."Patient Id" AS "ConPatientID",
        CR2."Application Patient Id" AS "ConAppPatientID",
        CR2."Provider Patient Id" AS "ConP_PatientID",
        CR2."Application Provider Patient Id" AS "ConP_AppPatientID",
        CR2."Payer Patient Id" AS "ConPA_PatientID",
        CR2."Application Payer Patient Id" AS "ConPA_AppPatientID",
        CR2."Payer Id" AS "ConPayerID",
        CR2."Application Payer Id" AS "ConAppPayerID",
        COALESCE(SPA2."Payer Name", DCON2."Contract Name") AS "ConContract",
        SPA2."Payer State" AS "ConPayerState",
        CR2."Invoice Date"::timestamp AS "ConBilledDate",
        CR2."Billed Hours" AS "ConBilledHours",
        CR2."Billed" AS "ConBilled",
        CR2."Billed Rate" AS "ConBilledRate",
        CR2."Total Billed Amount" AS "ConTotalBilledAmount",
        CR2."Bill Rate Non-Billed" AS "ConBillRateNonBilled",
        CASE WHEN CR2."Billed" = 'yes' THEN CR2."Billed Rate" ELSE CR2."Bill Rate Non-Billed" END AS "ConBillRateBoth",
        DSC2."Service Code Id" AS "ConServiceCodeID",
        DSC2."Application Service Code Id"::NUMERIC AS "ConAppServiceCodeID",
        CR2."Bill Type" AS "ConRateType",
        DSC2."Service Code" AS "ConServiceCode",
        CR2."Is Missed" AS "ConIsMissed",
        CR2."Missed Visit Reason" AS "ConMissedVisitReason",
        CR2."Call Out Device Type" AS "ConEVVType",
        CR2."Visit Updated Timestamp"::timestamp AS "ConLastUpdatedDate",
        DUSR2."User Fullname" AS "ConLastUpdatedBy",
        DPR2."Phone Number 1" AS "ConAgencyPhone",
        DPR2."Federal Tax Number" AS "ConFederalTaxNumber",
        CASE 
            WHEN CR2."Call Out GPS Coordinates" IS NOT NULL AND CR2."Call Out GPS Coordinates" <> ',' 
            THEN CAST(SPLIT_PART(REPLACE(CR2."Call Out GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION)
            WHEN CR2."Call In GPS Coordinates" IS NOT NULL AND CR2."Call In GPS Coordinates" <> ',' 
            THEN CAST(SPLIT_PART(REPLACE(CR2."Call In GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION)
            ELSE NULL 
        END AS "ConPLongitude",
        CASE 
            WHEN CR2."Call Out GPS Coordinates" IS NOT NULL AND CR2."Call Out GPS Coordinates" <> ',' 
            THEN CAST(SPLIT_PART(REPLACE(CR2."Call Out GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION)
            WHEN CR2."Call In GPS Coordinates" IS NOT NULL AND CR2."Call In GPS Coordinates" <> ',' 
            THEN CAST(SPLIT_PART(REPLACE(CR2."Call In GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION)
            ELSE NULL 
        END AS "ConPLatitude",
        CASE 
            WHEN (CR2."Application Payer Id" = '0' AND CR2."Application Contract Id" <> '0') THEN 'Internal'
            WHEN (CR2."Application Payer Id" <> '0' AND CR2."Application Contract Id" <> '0') THEN 'UPR'
            WHEN (CR2."Application Payer Id" <> '0' AND CR2."Application Contract Id" = '0') THEN 'Payer'
        END AS "ConContractType",
        -- ConVisit side InService (should be NULL for Part 2)
        FCS2."Inservice start date"::timestamp AS "ConInserviceStartDate",
        FCS2."Inservice end date"::timestamp AS "ConInserviceEndDate"
        
    FROM {conflict_schema}.conflictvisitmaps CVM
    -- Visit side joins
    INNER JOIN {analytics_schema}.FACTVISITCALLPERFORMANCE_CR CR1 ON CR1."Visit Id" = CVM."VisitID"
    INNER JOIN {analytics_schema}.DIMCAREGIVER CAR1 ON CAR1."Caregiver Id" = CR1."Caregiver Id"
    INNER JOIN {analytics_schema}.DIMPROVIDER DPR1 ON DPR1."Provider Id" = CR1."Provider Id" AND DPR1."Is Active" = TRUE AND DPR1."Is Demo" = FALSE
    LEFT JOIN {analytics_schema}.DIMOFFICE DOF1 ON DOF1."Office Id" = CR1."Office Id" AND DOF1."Is Active" = TRUE
    LEFT JOIN {analytics_schema}.DIMPAYER SPA1 ON SPA1."Payer Id" = CR1."Payer Id" AND SPA1."Is Active" = TRUE AND SPA1."Is Demo" = FALSE
    LEFT JOIN {analytics_schema}.DIMCONTRACT DCON1 ON DCON1."Contract Id" = CR1."Contract Id" AND DCON1."Is Active" = TRUE
    LEFT JOIN {analytics_schema}.DIMSERVICECODE DSC1 ON DSC1."Service Code Id" = CR1."Service Code Id"
    LEFT JOIN {analytics_schema}.DIMUSER DUSR1 ON DUSR1."User Id"::varchar = CR1."Visit Updated User Id"::varchar
    LEFT JOIN {analytics_schema}.FACTCAREGIVERINSERVICE FCS1 ON FCS1."Caregiver Id" = CR1."Caregiver Id" 
        AND FCS1."Provider Id" = CR1."Provider Id"
        AND CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL
        AND (CR1."Visit Start Time"::timestamp <= FCS1."Inservice end date"::timestamp 
             AND CR1."Visit End Time"::timestamp >= FCS1."Inservice start date"::timestamp)
    -- ConVisit side joins
    INNER JOIN {analytics_schema}.FACTVISITCALLPERFORMANCE_CR CR2 ON CR2."Visit Id" = CVM."ConVisitID"
    INNER JOIN {analytics_schema}.DIMCAREGIVER CAR2 ON CAR2."Caregiver Id" = CR2."Caregiver Id"
    INNER JOIN {analytics_schema}.DIMPROVIDER DPR2 ON DPR2."Provider Id" = CR2."Provider Id" AND DPR2."Is Active" = TRUE AND DPR2."Is Demo" = FALSE
    LEFT JOIN {analytics_schema}.DIMOFFICE DOF2 ON DOF2."Office Id" = CR2."Office Id" AND DOF2."Is Active" = TRUE
    LEFT JOIN {analytics_schema}.DIMPAYER SPA2 ON SPA2."Payer Id" = CR2."Payer Id" AND SPA2."Is Active" = TRUE AND SPA2."Is Demo" = FALSE
    LEFT JOIN {analytics_schema}.DIMCONTRACT DCON2 ON DCON2."Contract Id" = CR2."Contract Id" AND DCON2."Is Active" = TRUE
    LEFT JOIN {analytics_schema}.DIMSERVICECODE DSC2 ON DSC2."Service Code Id" = CR2."Service Code Id"
    LEFT JOIN {analytics_schema}.DIMUSER DUSR2 ON DUSR2."User Id"::varchar = CR2."Visit Updated User Id"::varchar
    LEFT JOIN {analytics_schema}.FACTCAREGIVERINSERVICE FCS2 ON FCS2."Caregiver Id" = CR2."Caregiver Id" 
        AND FCS2."Provider Id" = CR2."Provider Id"
        AND CR2."Visit Start Time" IS NOT NULL AND CR2."Visit End Time" IS NOT NULL
        AND (CR2."Visit Start Time"::timestamp <= FCS2."Inservice end date"::timestamp 
             AND CR2."Visit End Time"::timestamp >= FCS2."Inservice start date"::timestamp)
    WHERE CVM."UpdateFlag" = 1
      AND CVM."InserviceStartDate" IS NOT NULL 
      AND CVM."InserviceEndDate" IS NOT NULL 
      AND CVM."ConInserviceStartDate" IS NULL 
      AND CVM."ConInserviceEndDate" IS NULL
      AND {chunk_filter}
)
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "SSN" = SD."SSN",
    "ProviderID" = SD."ProviderID",
    "AppProviderID" = SD."AppProviderID",
    "ProviderName" = SD."ProviderName",
    "AppVisitID" = SD."AppVisitID",
    "VisitDate" = SD."VisitDate",
    "SchStartTime" = SD."SchStartTime",
    "SchEndTime" = SD."SchEndTime",
    "VisitStartTime" = SD."VisitStartTime",
    "VisitEndTime" = SD."VisitEndTime",
    "EVVStartTime" = SD."EVVStartTime",
    "EVVEndTime" = SD."EVVEndTime",
    "CaregiverID" = SD."CaregiverID",
    "AppCaregiverID" = SD."AppCaregiverID",
    "AideCode" = SD."AideCode",
    "AideName" = SD."AideName",
    "AideFName" = SD."AideFName",
    "AideLName" = SD."AideLName",
    "AideSSN" = SD."AideSSN",
    "AideStatus" = SD."AideStatus",
    "OfficeID" = SD."OfficeID",
    "AppOfficeID" = SD."AppOfficeID",
    "Office" = SD."Office",
    "PatientID" = SD."PatientID",
    "AppPatientID" = SD."AppPatientID",
    "P_PatientID" = SD."P_PatientID",
    "P_AppPatientID" = SD."P_AppPatientID",
    "PA_PatientID" = SD."PA_PatientID",
    "PA_AppPatientID" = SD."PA_AppPatientID",
    "PayerID" = SD."PayerID",
    "AppPayerID" = SD."AppPayerID",
    "Contract" = SD."Contract",
    "PayerState" = SD."PayerState",
    "BilledDate" = SD."BilledDate",
    "BilledHours" = SD."BilledHours",
    "Billed" = SD."Billed",
    "BilledRate" = SD."BilledRate",
    "TotalBilledAmount" = SD."TotalBilledAmount",
    "BillRateNonBilled" = SD."BillRateNonBilled",
    "BillRateBoth" = SD."BillRateBoth",
    "ServiceCodeID" = SD."ServiceCodeID",
    "AppServiceCodeID" = SD."AppServiceCodeID",
    "RateType" = SD."RateType",
    "ServiceCode" = SD."ServiceCode",
    "IsMissed" = SD."IsMissed",
    "MissedVisitReason" = SD."MissedVisitReason",
    "EVVType" = SD."EVVType",
    "LastUpdatedDate" = SD."LastUpdatedDate",
    "LastUpdatedBy" = SD."LastUpdatedBy",
    "AgencyPhone" = SD."AgencyPhone",
    "FederalTaxNumber" = SD."FederalTaxNumber",
    "PLongitude" = SD."PLongitude",
    "PLatitude" = SD."PLatitude",
    "ContractType" = SD."ContractType",
    "InserviceStartDate" = SD."InserviceStartDate",
    "InserviceEndDate" = SD."InserviceEndDate",
    -- ConVisit columns
    "ConProviderID" = SD."ConProviderID",
    "ConAppProviderID" = SD."ConAppProviderID",
    "ConProviderName" = SD."ConProviderName",
    "ConAppVisitID" = SD."ConAppVisitID",
    "ConSchStartTime" = SD."ConSchStartTime",
    "ConSchEndTime" = SD."ConSchEndTime",
    "ConVisitStartTime" = SD."ConVisitStartTime",
    "ConVisitEndTime" = SD."ConVisitEndTime",
    "ConEVVStartTime" = SD."ConEVVStartTime",
    "ConEVVEndTime" = SD."ConEVVEndTime",
    "ConCaregiverID" = SD."ConCaregiverID",
    "ConAppCaregiverID" = SD."ConAppCaregiverID",
    "ConAideCode" = SD."ConAideCode",
    "ConAideName" = SD."ConAideName",
    "ConAideFName" = SD."ConAideFName",
    "ConAideLName" = SD."ConAideLName",
    "ConAideSSN" = SD."ConAideSSN",
    "ConAideStatus" = SD."ConAideStatus",
    "ConOfficeID" = SD."ConOfficeID",
    "ConAppOfficeID" = SD."ConAppOfficeID",
    "ConOffice" = SD."ConOffice",
    "ConPatientID" = SD."ConPatientID",
    "ConAppPatientID" = SD."ConAppPatientID",
    "ConP_PatientID" = SD."ConP_PatientID",
    "ConP_AppPatientID" = SD."ConP_AppPatientID",
    "ConPA_PatientID" = SD."ConPA_PatientID",
    "ConPA_AppPatientID" = SD."ConPA_AppPatientID",
    "ConPayerID" = SD."ConPayerID",
    "ConAppPayerID" = SD."ConAppPayerID",
    "ConContract" = SD."ConContract",
    "ConPayerState" = SD."ConPayerState",
    "ConBilledDate" = SD."ConBilledDate",
    "ConBilledHours" = SD."ConBilledHours",
    "ConBilled" = SD."ConBilled",
    "ConBilledRate" = SD."ConBilledRate",
    "ConTotalBilledAmount" = SD."ConTotalBilledAmount",
    "ConBillRateNonBilled" = SD."ConBillRateNonBilled",
    "ConBillRateBoth" = SD."ConBillRateBoth",
    "ConServiceCodeID" = SD."ConServiceCodeID",
    "ConAppServiceCodeID" = SD."ConAppServiceCodeID",
    "ConRateType" = SD."ConRateType",
    "ConServiceCode" = SD."ConServiceCode",
    "ConIsMissed" = SD."ConIsMissed",
    "ConMissedVisitReason" = SD."ConMissedVisitReason",
    "ConEVVType" = SD."ConEVVType",
    "ConLastUpdatedDate" = SD."ConLastUpdatedDate",
    "ConLastUpdatedBy" = SD."ConLastUpdatedBy",
    "ConAgencyPhone" = SD."ConAgencyPhone",
    "ConFederalTaxNumber" = SD."ConFederalTaxNumber",
    "ConPLongitude" = SD."ConPLongitude",
    "ConPLatitude" = SD."ConPLatitude",
    "ConContractType" = SD."ConContractType",
    "ConInserviceStartDate" = SD."ConInserviceStartDate",
    "ConInserviceEndDate" = SD."ConInserviceEndDate",
    -- Flags
    "UpdateFlag" = NULL,
    "UpdatedDate" = NOW(),
    "StatusFlag" = CASE WHEN CVM."StatusFlag" NOT IN ('W', 'I') THEN 'U' ELSE CVM."StatusFlag" END,
    "ResolveDate" = NULL,
    "InServiceFlag" = CASE WHEN CVM."InServiceFlag" = 'N' THEN 'Y' ELSE CVM."InServiceFlag" END
FROM source_data SD
WHERE CVM."VisitID" = SD.cvm_visit_id 
  AND CVM."ConVisitID" = SD.cvm_con_visit_id;
