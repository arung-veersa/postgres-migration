UPDATE {conflict_schema}.conflictvisitmaps AS CVM
  	SET "CONFLICTID" = ALLDATA."CONFLICTID", "SSN" = ALLDATA."SSN", "ProviderID" = ALLDATA."ProviderID", "AppProviderID" = ALLDATA."AppProviderID", "ProviderName" = ALLDATA."ProviderName", "VisitID" = ALLDATA."VisitID", "AppVisitID" = ALLDATA."AppVisitID", "ConProviderID" = ALLDATA."ConProviderID", "ConAppProviderID" = ALLDATA."ConAppProviderID", "ConProviderName" = ALLDATA."ConProviderName", "ConVisitID" = ALLDATA."ConVisitID", "ConAppVisitID" = ALLDATA."ConAppVisitID", "VisitDate" = ALLDATA."VisitDate", "SchStartTime" = ALLDATA."SchStartTime", "SchEndTime" = ALLDATA."SchEndTime", "ConSchStartTime" = ALLDATA."ConSchStartTime", "ConSchEndTime" = ALLDATA."ConSchEndTime", "VisitStartTime" = ALLDATA."VisitStartTime", "VisitEndTime" = ALLDATA."VisitEndTime", "ConVisitStartTime" = ALLDATA."ConVisitStartTime", "ConVisitEndTime" = ALLDATA."ConVisitEndTime", "EVVStartTime" = ALLDATA."EVVStartTime", "EVVEndTime" = ALLDATA."EVVEndTime", "ConEVVStartTime" = ALLDATA."ConEVVStartTime", "ConEVVEndTime" = ALLDATA."ConEVVEndTime", "CaregiverID" = ALLDATA."CaregiverID", "AppCaregiverID" = ALLDATA."AppCaregiverID", "AideCode" = ALLDATA."AideCode", "AideName" = ALLDATA."AideName", "AideSSN" = ALLDATA."AideSSN", "ConCaregiverID" = ALLDATA."ConCaregiverID", "ConAppCaregiverID" = ALLDATA."ConAppCaregiverID", "ConAideCode" = ALLDATA."ConAideCode", "ConAideName" = ALLDATA."ConAideName", "ConAideSSN" = ALLDATA."ConAideSSN", "OfficeID" = ALLDATA."OfficeID", "AppOfficeID" = ALLDATA."AppOfficeID", "Office" = ALLDATA."Office", "ConOfficeID" = ALLDATA."ConOfficeID", "ConAppOfficeID" = ALLDATA."ConAppOfficeID", "ConOffice" = ALLDATA."ConOffice", "PatientID" = ALLDATA."PatientID", "AppPatientID" = ALLDATA."AppPatientID", "PAdmissionID" = ALLDATA."PAdmissionID", "PName" = ALLDATA."PName", "PAddressID" = ALLDATA."PAddressID", "PAppAddressID" = ALLDATA."PAppAddressID", "PAddressL1" = ALLDATA."PAddressL1", "PAddressL2" = ALLDATA."PAddressL2", "PCity" = ALLDATA."PCity", "PAddressState" = ALLDATA."PAddressState", "PZipCode" = ALLDATA."PZipCode", "PCounty" = ALLDATA."PCounty", "PLongitude" = ALLDATA."PLongitude", "PLatitude" = ALLDATA."PLatitude", "ConPatientID" = ALLDATA."ConPatientID", "ConAppPatientID" = ALLDATA."ConAppPatientID", "ConPAdmissionID" = ALLDATA."ConPAdmissionID", "ConPName" = ALLDATA."ConPName", "ConPAddressID" = ALLDATA."ConPAddressID", "ConPAppAddressID" = ALLDATA."ConPAppAddressID", "ConPAddressL1" = ALLDATA."ConPAddressL1", "ConPAddressL2" = ALLDATA."ConPAddressL2", "ConPCity" = ALLDATA."ConPCity", "ConPAddressState" = ALLDATA."ConPAddressState", "ConPZipCode" = ALLDATA."ConPZipCode", "ConPCounty" = ALLDATA."ConPCounty", "ConPLongitude" = ALLDATA."ConPLongitude", "ConPLatitude" = ALLDATA."ConPLatitude", "PayerID" = ALLDATA."PayerID", "AppPayerID" = ALLDATA."AppPayerID", "Contract" = ALLDATA."Contract", "ConPayerID" = ALLDATA."ConPayerID", "ConAppPayerID" = ALLDATA."ConAppPayerID", "ConContract" = ALLDATA."ConContract", "BilledDate" = ALLDATA."BilledDate", "ConBilledDate" = ALLDATA."ConBilledDate", "BilledHours" = ALLDATA."BilledHours", "ConBilledHours" = ALLDATA."ConBilledHours", "Billed" = ALLDATA."Billed", "ConBilled" = ALLDATA."ConBilled", "MinuteDiffBetweenSch" = ALLDATA."MinuteDiffBetweenSch", "DistanceMilesFromLatLng" = ALLDATA."DistanceMilesFromLatLng", "AverageMilesPerHour" = ALLDATA."AverageMilesPerHour", "ETATravelMinutes" = ALLDATA."ETATravelMinutes", "ServiceCodeID" = ALLDATA."ServiceCodeID", "AppServiceCodeID" = ALLDATA."AppServiceCodeID", "RateType" = ALLDATA."RateType", "ServiceCode" = ALLDATA."ServiceCode", "ConServiceCodeID" = ALLDATA."ConServiceCodeID", "ConAppServiceCodeID" = ALLDATA."ConAppServiceCodeID", "ConRateType" = ALLDATA."ConRateType", "ConServiceCode" = ALLDATA."ConServiceCode", "UpdateFlag" = NULL, "UpdatedDate" = NOW(), "StatusFlag" = CASE WHEN CVM."StatusFlag" NOT IN ('W', 'I') THEN 'U' ELSE CVM."StatusFlag" END, "ResolveDate" = NULL, "AideFName" = ALLDATA."AideFName", "AideLName" = ALLDATA."AideLName", "ConAideFName" = ALLDATA."ConAideFName", "ConAideLName" = ALLDATA."ConAideLName", "PFName" = ALLDATA."PFName", "PLName" = ALLDATA."PLName", "ConPFName" = ALLDATA."ConPFName", "ConPLName" = ALLDATA."ConPLName", "PMedicaidNumber" = ALLDATA."PMedicaidNumber", "ConPMedicaidNumber" = ALLDATA."ConPMedicaidNumber", "PayerState" = ALLDATA."PayerState", "ConPayerState" = ALLDATA."ConPayerState", "LastUpdatedBy" = ALLDATA."LastUpdatedBy", "ConLastUpdatedBy" = ALLDATA."ConLastUpdatedBy", "LastUpdatedDate" = ALLDATA."LastUpdatedDate", "ConLastUpdatedDate" = ALLDATA."ConLastUpdatedDate", "BilledRate" = ALLDATA."BilledRate", "TotalBilledAmount" = ALLDATA."TotalBilledAmount", "ConBilledRate" = ALLDATA."ConBilledRate", "ConTotalBilledAmount" = ALLDATA."ConTotalBilledAmount", "IsMissed" = ALLDATA."IsMissed", "MissedVisitReason" = ALLDATA."MissedVisitReason", "EVVType" = ALLDATA."EVVType", "ConIsMissed" = ALLDATA."ConIsMissed", "ConMissedVisitReason" = ALLDATA."ConMissedVisitReason", "ConEVVType" = ALLDATA."ConEVVType", "PStatus" = ALLDATA."PStatus", "ConPStatus" = ALLDATA."ConPStatus", "AideStatus" = ALLDATA."AideStatus", "ConAideStatus" = ALLDATA."ConAideStatus", "P_PatientID" = ALLDATA."P_PatientID", "P_AppPatientID" = ALLDATA."P_AppPatientID", "ConP_PatientID" = ALLDATA."ConP_PatientID", "ConP_AppPatientID" = ALLDATA."ConP_AppPatientID", "PA_PatientID" = ALLDATA."PA_PatientID", "PA_AppPatientID" = ALLDATA."PA_AppPatientID", "ConPA_PatientID" = ALLDATA."ConPA_PatientID", "ConPA_AppPatientID" = ALLDATA."ConPA_AppPatientID", "P_PAdmissionID" = ALLDATA."P_PAdmissionID", "P_PName" = ALLDATA."P_PName", "P_PAddressID" = ALLDATA."P_PAddressID", "P_PAppAddressID" = ALLDATA."P_PAppAddressID", "P_PAddressL1" = ALLDATA."P_PAddressL1", "P_PAddressL2" = ALLDATA."P_PAddressL2", "P_PCity" = ALLDATA."P_PCity", "P_PAddressState" = ALLDATA."P_PAddressState", "P_PZipCode" = ALLDATA."P_PZipCode", "P_PCounty" = ALLDATA."P_PCounty", "P_PFName" = ALLDATA."P_PFName", "P_PLName" = ALLDATA."P_PLName", "P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", "ConP_PAdmissionID" = ALLDATA."ConP_PAdmissionID", "ConP_PName" = ALLDATA."ConP_PName", "ConP_PAddressID" = ALLDATA."ConP_PAddressID", "ConP_PAppAddressID" = ALLDATA."ConP_PAppAddressID", "ConP_PAddressL1" = ALLDATA."ConP_PAddressL1", "ConP_PAddressL2" = ALLDATA."ConP_PAddressL2", "ConP_PCity" = ALLDATA."ConP_PCity", "ConP_PAddressState" = ALLDATA."ConP_PAddressState", "ConP_PZipCode" = ALLDATA."ConP_PZipCode", "ConP_PCounty" = ALLDATA."ConP_PCounty", "ConP_PFName" = ALLDATA."ConP_PFName", "ConP_PLName" = ALLDATA."ConP_PLName", "ConP_PMedicaidNumber" = ALLDATA."ConP_PMedicaidNumber", "PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", "PA_PName" = ALLDATA."PA_PName", "PA_PAddressID" = ALLDATA."PA_PAddressID", "PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", "PA_PAddressL1" = ALLDATA."PA_PAddressL1", "PA_PAddressL2" = ALLDATA."PA_PAddressL2", "PA_PCity" = ALLDATA."PA_PCity", "PA_PAddressState" = ALLDATA."PA_PAddressState", "PA_PZipCode" = ALLDATA."PA_PZipCode", "PA_PCounty" = ALLDATA."PA_PCounty", "PA_PFName" = ALLDATA."PA_PFName", "PA_PLName" = ALLDATA."PA_PLName", "PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", "ConPA_PAdmissionID" = ALLDATA."ConPA_PAdmissionID", "ConPA_PName" = ALLDATA."ConPA_PName", "ConPA_PAddressID" = ALLDATA."ConPA_PAddressID", "ConPA_PAppAddressID" = ALLDATA."ConPA_PAppAddressID", "ConPA_PAddressL1" = ALLDATA."ConPA_PAddressL1", "ConPA_PAddressL2" = ALLDATA."ConPA_PAddressL2", "ConPA_PCity" = ALLDATA."ConPA_PCity", "ConPA_PAddressState" = ALLDATA."ConPA_PAddressState", "ConPA_PZipCode" = ALLDATA."ConPA_PZipCode", "ConPA_PCounty" = ALLDATA."ConPA_PCounty", "ConPA_PFName" = ALLDATA."ConPA_PFName", "ConPA_PLName" = ALLDATA."ConPA_PLName", "ConPA_PMedicaidNumber" = ALLDATA."ConPA_PMedicaidNumber", "ContractType" = ALLDATA."ContractType", "ConContractType" = ALLDATA."ConContractType", "InServiceFlag" = CASE WHEN CVM."InServiceFlag" = 'N' THEN ALLDATA."InServiceFlag" ELSE CVM."InServiceFlag" END, "BillRateNonBilled" = ALLDATA."BillRateNonBilled", "ConBillRateNonBilled" = ALLDATA."ConBillRateNonBilled", "BillRateBoth" = ALLDATA."BillRateBoth", "ConBillRateBoth" = ALLDATA."ConBillRateBoth", "FederalTaxNumber" = ALLDATA."FederalTaxNumber", "ConFederalTaxNumber" = ALLDATA."ConFederalTaxNumber", "InserviceStartDate" = ALLDATA."InserviceStartDate", "InserviceEndDate" = ALLDATA."InserviceEndDate", "ConInserviceStartDate" = ALLDATA."ConInserviceStartDate", "ConInserviceEndDate" = ALLDATA."ConInserviceEndDate"
    FROM (
        WITH filtered_visits AS (
            SELECT CR1.*
            FROM {analytics_schema}.FACTVISITCALLPERFORMANCE_CR AS CR1
            WHERE CR1."Is Missed" = FALSE
              AND CR1."Visit Start Time" IS NOT NULL
              AND CR1."Visit End Time" IS NOT NULL
              AND CR1."Visit Date"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                              AND (NOW() + INTERVAL '45 days')::date
              AND {chunk_visit_date_filter}  -- ⭐ CHUNK FILTER: Only process visits in this chunk's date range
              AND NOT EXISTS (
                  SELECT 1 FROM {conflict_schema}.excluded_agency AS EA 
                  WHERE EA."ProviderID" = CR1."Provider Id"
              )
        ),
        filtered_caregivers AS (
            SELECT CAR.*
            FROM {analytics_schema}.DIMCAREGIVER AS CAR
            WHERE TRIM(CAR."SSN") IS NOT NULL 
              AND TRIM(CAR."SSN") <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM {conflict_schema}.excluded_ssn AS SSN 
                  WHERE TRIM(CAR."SSN") = SSN."SSN"
              )
              AND {chunk_ssn_filter}  -- ⭐ CHUNK FILTER: Only process caregivers with SSNs in this chunk
        )
	SELECT 
			DISTINCT
            V1."CONFLICTID",
            V1."SSN",
            V1."ProviderID" AS "ProviderID",
            V1."AppProviderID" AS "AppProviderID",
            V1."ProviderName" AS "ProviderName",
            V1."VisitID" AS "VisitID",
            V1."AppVisitID" AS "AppVisitID",
            V2."ProviderID" AS "ConProviderID",
            V2."AppProviderID" AS "ConAppProviderID",
            V2."ProviderName" AS "ConProviderName",
            V2."VisitID" AS "ConVisitID",
			V2."AppVisitID" AS "ConAppVisitID",
            V1."VisitDate" AS "VisitDate",
            V1."SchStartTime" AS "SchStartTime",
            V1."SchEndTime" AS "SchEndTime",
            V2."SchStartTime" AS "ConSchStartTime",
            V2."SchEndTime" AS "ConSchEndTime",
            V1."VisitStartTime" AS "VisitStartTime",
            V1."VisitEndTime" AS "VisitEndTime",
            V2."VisitStartTime" AS "ConVisitStartTime",
            V2."VisitEndTime" AS "ConVisitEndTime",
            V1."EVVStartTime" AS "EVVStartTime",
            V1."EVVEndTime" AS "EVVEndTime",
            V2."EVVStartTime" AS "ConEVVStartTime",
            V2."EVVEndTime" AS "ConEVVEndTime",
            V1."CaregiverID" AS "CaregiverID",
            V1."AppCaregiverID" AS "AppCaregiverID",
            V1."AideCode" AS "AideCode",
            V1."AideName" AS "AideName",
            V1."AideSSN" AS "AideSSN",
            V2."CaregiverID" AS "ConCaregiverID",
            V2."AppCaregiverID" AS "ConAppCaregiverID",
            V2."AideCode" AS "ConAideCode",
            V2."AideName" AS "ConAideName",
            V2."AideSSN" AS "ConAideSSN",
            V1."OfficeID" AS "OfficeID",
            V1."AppOfficeID" AS "AppOfficeID",
            V1."Office" AS "Office",
            V2."OfficeID" AS "ConOfficeID",
            V2."AppOfficeID" AS "ConAppOfficeID",
            V2."Office" AS "ConOffice",
            V1."PatientID" AS "PatientID",
            V1."AppPatientID" AS "AppPatientID",
            V1."PAdmissionID" AS "PAdmissionID",
            V1."PName" AS "PName",
            V1."PAddressID" AS "PAddressID",
            V1."PAppAddressID" AS "PAppAddressID",
            V1."PAddressL1" AS "PAddressL1",
            V1."PAddressL2" AS "PAddressL2",
            V1."PCity" AS "PCity",
            V1."PAddressState" AS "PAddressState",
            V1."PZipCode" AS "PZipCode",
            V1."PCounty" AS "PCounty",            
            V1."Longitude" AS "PLongitude",
            V1."Latitude" AS "PLatitude",
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
            V1."PayerID" AS "PayerID",
            V1."AppPayerID" AS "AppPayerID",
            V1."Contract" AS "Contract",
            V2."PayerID" AS "ConPayerID",
            V2."AppPayerID" AS "ConAppPayerID",
            V2."Contract" AS "ConContract",
            V1."BilledDate" AS "BilledDate",
            V2."BilledDate" AS "ConBilledDate",
            V1."BilledHours" AS "BilledHours",
            V2."BilledHours" AS "ConBilledHours",
            V1."Billed" AS "Billed",
            V2."Billed" AS "ConBilled",            
            CAST(NULL AS DOUBLE PRECISION) "MinuteDiffBetweenSch",
            CAST(NULL AS DOUBLE PRECISION) "DistanceMilesFromLatLng",
            CAST(NULL AS DOUBLE PRECISION) "AverageMilesPerHour",
           	CAST(NULL AS DOUBLE PRECISION) "ETATravelMinutes",
            V1."ServiceCodeID" AS "ServiceCodeID",
	        V1."AppServiceCodeID" AS "AppServiceCodeID",
	        V1."RateType" AS "RateType",
	        V1."ServiceCode" AS "ServiceCode",          
	        V2."ServiceCodeID" AS "ConServiceCodeID",
	        V2."AppServiceCodeID" AS "ConAppServiceCodeID",
	        V2."RateType" AS "ConRateType",
	        V2."ServiceCode" AS "ConServiceCode",
			'Y' AS "InServiceFlag",
            V1."AideFName" AS "AideFName",
            V1."AideLName" AS "AideLName",
            V2."AideFName" AS "ConAideFName",
            V2."AideLName" AS "ConAideLName",
            V1."PFName" AS "PFName",
            V1."PLName" AS "PLName",
            V2."PFName" AS "ConPFName",
            V2."PLName" AS "ConPLName",
			V1."PMedicaidNumber" AS "PMedicaidNumber",
			V2."PMedicaidNumber" AS "ConPMedicaidNumber",
			V1."PayerState" AS "PayerState",
			V2."PayerState" AS "ConPayerState",
			V1."AgencyContact" AS "AgencyContact",
			V2."AgencyContact" AS "ConAgencyContact",
			V1."AgencyPhone" AS "AgencyPhone",
			V2."AgencyPhone" AS "ConAgencyPhone",
			V1."LastUpdatedBy" AS "LastUpdatedBy",
			V2."LastUpdatedBy" AS "ConLastUpdatedBy",
			V1."LastUpdatedDate" AS "LastUpdatedDate",
			V2."LastUpdatedDate" AS "ConLastUpdatedDate",
			V1."BilledRate" AS "BilledRate",
			V1."TotalBilledAmount" AS "TotalBilledAmount",
			V2."BilledRate" AS "ConBilledRate",
			V2."TotalBilledAmount" AS "ConTotalBilledAmount",
			V1."IsMissed" AS "IsMissed",
			V1."MissedVisitReason" AS "MissedVisitReason",
			V1."EVVType" AS "EVVType",
			V2."IsMissed" AS "ConIsMissed",
			V2."MissedVisitReason" AS "ConMissedVisitReason",
			V2."EVVType" AS "ConEVVType",
			V1."PStatus" AS "PStatus",
			V2."PStatus" AS "ConPStatus",
			V1."AideStatus" AS "AideStatus",
			V2."AideStatus" AS "ConAideStatus",
			V1."P_PatientID" AS "P_PatientID",
			V1."P_AppPatientID" AS "P_AppPatientID",
			V2."P_PatientID" AS "ConP_PatientID",
			V2."P_AppPatientID" AS "ConP_AppPatientID",
			V1."PA_PatientID" AS "PA_PatientID",
			V1."PA_AppPatientID" AS "PA_AppPatientID",
			V2."PA_PatientID" AS "ConPA_PatientID",
			V2."PA_AppPatientID" AS "ConPA_AppPatientID",
            V1."P_PAdmissionID" AS "P_PAdmissionID",
            V1."P_PName" AS "P_PName",
            V1."P_PAddressID" AS "P_PAddressID",
            V1."P_PAppAddressID" AS "P_PAppAddressID",
            V1."P_PAddressL1" AS "P_PAddressL1",
            V1."P_PAddressL2" AS "P_PAddressL2",
            V1."P_PCity" AS "P_PCity",
            V1."P_PAddressState" AS "P_PAddressState",
            V1."P_PZipCode" AS "P_PZipCode",
            V1."P_PCounty" AS "P_PCounty",
			V1."P_PFName" AS "P_PFName",
            V1."P_PLName" AS "P_PLName",
			V1."P_PMedicaidNumber" AS "P_PMedicaidNumber",
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
            V1."PA_PAdmissionID" AS "PA_PAdmissionID",
            V1."PA_PName" AS "PA_PName",
            V1."PA_PAddressID" AS "PA_PAddressID",
            V1."PA_PAppAddressID" AS "PA_PAppAddressID",
            V1."PA_PAddressL1" AS "PA_PAddressL1",
            V1."PA_PAddressL2" AS "PA_PAddressL2",
            V1."PA_PCity" AS "PA_PCity",
            V1."PA_PAddressState" AS "PA_PAddressState",
            V1."PA_PZipCode" AS "PA_PZipCode",
            V1."PA_PCounty" AS "PA_PCounty",
			V1."PA_PFName" AS "PA_PFName",
            V1."PA_PLName" AS "PA_PLName",
			V1."PA_PMedicaidNumber" AS "PA_PMedicaidNumber",
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
			V1."ContractType" AS "ContractType",
			V2."ContractType" AS "ConContractType",
			V1."P_PStatus" AS "P_PStatus",
			V2."P_PStatus" AS "ConP_PStatus",
			V1."PA_PStatus" AS "PA_PStatus",
			V2."PA_PStatus" AS "ConPA_PStatus",
			V1."BillRateNonBilled" AS "BillRateNonBilled",
			V2."BillRateNonBilled" AS "ConBillRateNonBilled",
			V1."BillRateBoth" AS "BillRateBoth",
			V2."BillRateBoth" AS "ConBillRateBoth",
			V1."FederalTaxNumber" AS "FederalTaxNumber",
			V2."FederalTaxNumber" AS "ConFederalTaxNumber",
            V1."InserviceStartDate" AS "InserviceStartDate",
            V1."InserviceEndDate" AS "InserviceEndDate",
            V2."InserviceStartDate" AS "ConInserviceStartDate",
            V2."InserviceEndDate" AS "ConInserviceEndDate"
		FROM
       (SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS VARCHAR) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS VARCHAR) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", CR1."Visit Date"::date AS "VisitDate", CR1."Scheduled Start Time"::timestamp AS "SchStartTime", CR1."Scheduled End Time"::timestamp AS "SchEndTime", CR1."Visit Start Time"::timestamp AS "VisitStartTime", CR1."Visit End Time"::timestamp AS "VisitEndTime", CR1."Call In Time"::timestamp AS "EVVStartTime", CR1."Call Out Time"::timestamp AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS uuid) "PAdmissionID", CAST(NULL AS VARCHAR) "PName", CAST(NULL AS VARCHAR) "PFName", CAST(NULL AS VARCHAR) "PLName", CAST(NULL AS VARCHAR) "PMedicaidNumber", CAST(NULL AS uuid) "PAddressID", CAST(NULL AS NUMERIC) "PAppAddressID", CAST(NULL AS VARCHAR) "PAddressL1", CAST(NULL AS VARCHAR) "PAddressL2", CAST(NULL AS VARCHAR) "PCity", CAST(NULL AS VARCHAR) "PAddressState", CAST(NULL AS VARCHAR) "PZipCode", CAST(NULL AS VARCHAR) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ',' THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ',' THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ',' THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ',' THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CR1."Invoice Date"::timestamp AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", FCS."Inservice start date"::timestamp AS "InserviceStartDate", FCS."Inservice end date"::timestamp AS "InserviceEndDate", FCS."Application Caregiver Inservice Id"::varchar AS "AppCaregiverInserviceID", CAST(NULL AS TIMESTAMP) "PTOStartDate", CAST(NULL AS TIMESTAMP) "PTOEndDate", CAST(NULL AS uuid) "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id"::NUMERIC AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CR1."Visit Updated Timestamp"::timestamp as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" <> '0') THEN 'Internal' WHEN (CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" <> '0') THEN 'UPR' WHEN (CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" = '0') THEN 'Payer' END AS "ContractType" FROM filtered_visits AS CR1
	   INNER JOIN filtered_caregivers AS CAR ON CAR."Caregiver Id" = CR1."Caregiver Id"
	   LEFT JOIN {analytics_schema}.DIMOFFICE AS DOF ON DOF."Office Id" = CR1."Office Id" AND DOF."Is Active" = TRUE
	    LEFT JOIN {analytics_schema}.DIMPATIENT AS DPA_P ON DPA_P."Patient Id" = CR1."Provider Patient Id"		
		LEFT JOIN LATERAL (
			SELECT DDD."Patient Address Id", DDD."Application Patient Address Id", DDD."Address Line 1", DDD."Address Line 2", DDD."City", DDD."Address State", DDD."Zip Code", DDD."County", DDD."Patient Id", DDD."Application Patient Id", DDD."Longitude" AS "Provider_Longitude", DDD."Latitude" AS "Provider_Latitude"
			FROM {analytics_schema}.DIMPATIENTADDRESS AS DDD
			WHERE DDD."Patient Id" = DPA_P."Patient Id"
			  AND DDD."Primary Address" = TRUE 
			  AND DDD."Address Type" LIKE '%GPS%'
			ORDER BY DDD."Application Created UTC Timestamp" DESC
			LIMIT 1
		) AS DPAD_P ON TRUE		
		 LEFT JOIN {analytics_schema}.DIMPATIENT AS DPA_PA ON DPA_PA."Patient Id" = CR1."Payer Patient Id"		 
		 LEFT JOIN LATERAL (
			SELECT DDD."Patient Address Id", DDD."Application Patient Address Id", DDD."Address Line 1", DDD."Address Line 2", DDD."City", DDD."Address State", DDD."Zip Code", DDD."County", DDD."Patient Id", DDD."Application Patient Id"
			FROM {analytics_schema}.DIMPATIENTADDRESS AS DDD
			WHERE DDD."Patient Id" = DPA_PA."Patient Id"
			  AND DDD."Primary Address" = TRUE 
			  AND DDD."Address Type" LIKE '%GPS%'
			ORDER BY DDD."Application Created UTC Timestamp" DESC
			LIMIT 1
		) AS DPAD_PA ON TRUE		 
		 LEFT JOIN {analytics_schema}.DIMPAYER AS SPA ON SPA."Payer Id" = CR1."Payer Id" AND SPA."Is Active" = TRUE AND SPA."Is Demo" = FALSE 
		 LEFT JOIN {analytics_schema}.DIMCONTRACT AS DCON ON DCON."Contract Id" = CR1."Contract Id" AND DCON."Is Active" = TRUE
		 INNER JOIN {analytics_schema}.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
		 LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM1 ON CVM1."VisitID"::varchar = CR1."Visit Id"::varchar AND CVM1."CONFLICTID" IS NOT NULL 
		LEFT JOIN {analytics_schema}.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
		LEFT JOIN {analytics_schema}.DIMUSER AS DUSR ON DUSR."User Id"::varchar = CR1."Visit Updated User Id"::varchar
		LEFT JOIN {analytics_schema}.FACTCAREGIVERINSERVICE AS FCS ON FCS."Caregiver Id" = CR1."Caregiver Id" AND CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL AND (CR1."Visit Start Time"::timestamp <= FCS."Inservice end date"::timestamp AND CR1."Visit End Time"::timestamp >= FCS."Inservice start date"::timestamp) AND FCS."Provider Id" = CR1."Provider Id"

        WHERE FCS."Application Caregiver Inservice Id" IS NULL) AS V1
       INNER JOIN
       (
		SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", 
		CAST(NULL AS DOUBLE PRECISION) "BillRateNonBilled", 
		CAST(NULL AS DOUBLE PRECISION) "BillRateBoth", 
		TRIM(CAR."SSN") as "SSN", 
		CAST(NULL AS VARCHAR) "PStatus", 
		CAST(NULL AS VARCHAR) "AideStatus", 
		CAST(NULL AS VARCHAR) "MissedVisitReason", 
		CAST(NULL AS BOOLEAN) "IsMissed", 
		CAST(NULL AS VARCHAR) "EVVType", 
		CAST(NULL AS DOUBLE PRECISION) "BilledRate", 
		CAST(NULL AS DOUBLE PRECISION) "TotalBilledAmount",
		DPR."Provider Id" as "ProviderID", 
		DPR."Application Provider Id" as "AppProviderID", 
		DPR."Provider Name" AS "ProviderName", 
		CAST(NULL AS VARCHAR) "AgencyContact", 
		DPR."Phone Number 1" AS "AgencyPhone", 
		DPR."Federal Tax Number" AS "FederalTaxNumber",
		MD5(CONCAT('I', FCS."Application Caregiver Inservice Id"::varchar))::uuid AS "VisitID", 
		FCS."Application Caregiver Inservice Id"::bigint AS "AppVisitID",
		FCS."Inservice start date"::date AS "VisitDate", 
		CAST(NULL AS TIMESTAMP) "SchStartTime", 
		CAST(NULL AS TIMESTAMP) "SchEndTime", 
		CAST(NULL AS TIMESTAMP) "VisitStartTime", 
		CAST(NULL AS TIMESTAMP) "VisitEndTime", 
		CAST(NULL AS TIMESTAMP) "EVVStartTime", 
		CAST(NULL AS TIMESTAMP) "EVVEndTime",
		CAR."Caregiver Id" as "CaregiverID",
		CAR."Application Caregiver Id" as "AppCaregiverID",
		CAR."Caregiver Code" as "AideCode",
		CAR."Caregiver Fullname" as "AideName",
		CAR."Caregiver Firstname" as "AideFName",
		CAR."Caregiver Lastname" as "AideLName",
		TRIM(CAR."SSN") as "AideSSN",
		DOF."Office Id" as "OfficeID",
		DOF."Application Office Id" as "AppOfficeID",
		DOF."Office Name" as "Office",
		CAST(NULL AS uuid) "PA_PatientID",
		CAST(NULL AS NUMERIC) "PA_AppPatientID",
		CAST(NULL AS uuid) "P_PatientID",
		CAST(NULL AS NUMERIC) "P_AppPatientID",
		CAST(NULL AS uuid) "PatientID",
		CAST(NULL AS NUMERIC) "AppPatientID",
		CAST(NULL AS uuid) "PAdmissionID",
		CAST(NULL AS VARCHAR) "PName",
		CAST(NULL AS VARCHAR) "PFName",
		CAST(NULL AS VARCHAR) "PLName",
		CAST(NULL AS VARCHAR) "PMedicaidNumber",
		CAST(NULL AS uuid) "PAddressID",
		CAST(NULL AS NUMERIC) "PAppAddressID",
		CAST(NULL AS VARCHAR) "PAddressL1",
		CAST(NULL AS VARCHAR) "PAddressL2",
		CAST(NULL AS VARCHAR) "PCity",
		CAST(NULL AS VARCHAR) "PAddressState",
		CAST(NULL AS VARCHAR) "PZipCode",
		CAST(NULL AS VARCHAR) "PCounty",
		CAST(NULL AS DOUBLE PRECISION) "Longitude",
		CAST(NULL AS DOUBLE PRECISION) "Latitude",
		CAST(NULL AS uuid) "PayerID",
		CAST(NULL AS bigint) "AppPayerID",
		CAST(NULL AS VARCHAR) "Contract",
		CAST(NULL AS VARCHAR) "PayerState",
		CAST(NULL AS TIMESTAMP) "BilledDate",
		CAST(NULL AS DOUBLE PRECISION) "BilledHours",
		CAST(NULL AS VARCHAR) "Billed",
		FCS."Inservice start date"::timestamp AS "InserviceStartDate",
		FCS."Inservice end date"::timestamp AS "InserviceEndDate",
		FCS."Application Caregiver Inservice Id"::varchar AS "AppCaregiverInserviceID",
		CAST(NULL AS TIMESTAMP) "PTOStartDate",
		CAST(NULL AS TIMESTAMP) "PTOEndDate",
		CAST(NULL AS uuid) "PTOVacationID",
		CAST(NULL AS uuid) "ServiceCodeID",
		CAST(NULL AS NUMERIC) "AppServiceCodeID",
		CAST(NULL AS VARCHAR) "RateType",
		CAST(NULL AS VARCHAR) "ServiceCode",
		CAST(NULL AS TIMESTAMP) "LastUpdatedDate",
		CAST(NULL AS VARCHAR) "LastUpdatedBy",
		CAST(NULL AS uuid) "P_PAdmissionID",
		CAST(NULL AS VARCHAR) "P_PName",
		CAST(NULL AS VARCHAR) "P_PFName",
		CAST(NULL AS VARCHAR) "P_PLName",
		CAST(NULL AS VARCHAR) "P_PMedicaidNumber",
		CAST(NULL AS VARCHAR) "P_PStatus",
		CAST(NULL AS uuid) "P_PAddressID",
		CAST(NULL AS NUMERIC) "P_PAppAddressID",
		CAST(NULL AS VARCHAR) "P_PAddressL1",
		CAST(NULL AS VARCHAR) "P_PAddressL2",
		CAST(NULL AS VARCHAR) "P_PCity",
		CAST(NULL AS VARCHAR) "P_PAddressState",
		CAST(NULL AS VARCHAR) "P_PZipCode",
		CAST(NULL AS VARCHAR) "P_PCounty",
		CAST(NULL AS uuid) "PA_PAdmissionID",
		CAST(NULL AS VARCHAR) "PA_PName",
		CAST(NULL AS VARCHAR) "PA_PFName",
		CAST(NULL AS VARCHAR) "PA_PLName",
		CAST(NULL AS VARCHAR) "PA_PMedicaidNumber",
		CAST(NULL AS VARCHAR) "PA_PStatus",
		CAST(NULL AS uuid) "PA_PAddressID",
		CAST(NULL AS NUMERIC) "PA_PAppAddressID",
		CAST(NULL AS VARCHAR) "PA_PAddressL1",
		CAST(NULL AS VARCHAR) "PA_PAddressL2",
		CAST(NULL AS VARCHAR) "PA_PCity",
		CAST(NULL AS VARCHAR) "PA_PAddressState",
		CAST(NULL AS VARCHAR) "PA_PZipCode",
		CAST(NULL AS VARCHAR) "PA_PCounty",
		CAST(NULL AS VARCHAR) "ContractType"
		FROM 
	   {analytics_schema}.FACTCAREGIVERINSERVICE AS FCS
	   
	   INNER JOIN filtered_caregivers AS CAR ON CAR."Caregiver Id" = FCS."Caregiver Id"

	   INNER JOIN {analytics_schema}.DIMPROVIDER AS DPR ON DPR."Provider Id" = FCS."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
	   
	   LEFT JOIN {analytics_schema}.DIMOFFICE AS DOF ON DOF."Office Id" = FCS."Office Id" AND DOF."Is Active" = TRUE

	   LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM1 ON CVM1."VisitID" = MD5(CONCAT('I', FCS."Application Caregiver Inservice Id"::varchar))::uuid AND CVM1."CONFLICTID" IS NOT NULL
	   
   WHERE FCS."Inservice start date"::date BETWEEN (NOW() - INTERVAL '2 years')::date AND (NOW() + INTERVAL '45 days')::date
	     AND {chunk_date_filter}  -- ⭐ CHUNK FILTER: Only process inservice records in this chunk's date range
	   ) AS V2 ON
      	V1."VisitID" <> V2."VisitID"
	   	AND
	   	V1."SSN" = V2."SSN"
		AND 
		(V1."VisitStartTime"::timestamp <= V2."InserviceEndDate"::timestamp AND V1."VisitEndTime"::timestamp >= V2."InserviceStartDate"::timestamp)
		AND 
		V1."ProviderID" IS NOT NULL
		AND
		V2."ProviderID" <> V1."ProviderID"
		AND
		V2."AppCaregiverInserviceID" IS NOT NULL
		AND
		V1."AppCaregiverInserviceID" IS NULL
       ) AS ALLDATA WHERE CVM."VisitID" = ALLDATA."VisitID" AND CVM."ConVisitID" = ALLDATA."ConVisitID" AND CVM."InserviceStartDate" IS NULL AND CVM."InserviceEndDate" IS NULL AND CVM."ConInserviceStartDate" IS NOT NULL AND CVM."ConInserviceEndDate" IS NOT NULL AND CVM."UpdateFlag" = 1
    AND {chunk_filter};  -- ⭐ CHUNK FILTER: Only update rows in this chunk

-- Step 2: New InService Update
-- Updates rows where Visit HAS InService but ConVisit has NO InService

UPDATE {conflict_schema}.conflictvisitmaps AS CVM
  	SET "CONFLICTID" = ALLDATA."CONFLICTID", "SSN" = ALLDATA."SSN", "ProviderID" = ALLDATA."ProviderID", "AppProviderID" = ALLDATA."AppProviderID", "ProviderName" = ALLDATA."ProviderName", "VisitID" = ALLDATA."VisitID", "AppVisitID" = ALLDATA."AppVisitID", "ConProviderID" = ALLDATA."ConProviderID", "ConAppProviderID" = ALLDATA."ConAppProviderID", "ConProviderName" = ALLDATA."ConProviderName", "ConVisitID" = ALLDATA."ConVisitID", "ConAppVisitID" = ALLDATA."ConAppVisitID", "VisitDate" = ALLDATA."VisitDate", "SchStartTime" = ALLDATA."SchStartTime", "SchEndTime" = ALLDATA."SchEndTime", "ConSchStartTime" = ALLDATA."ConSchStartTime", "ConSchEndTime" = ALLDATA."ConSchEndTime", "VisitStartTime" = ALLDATA."VisitStartTime", "VisitEndTime" = ALLDATA."VisitEndTime", "ConVisitStartTime" = ALLDATA."ConVisitStartTime", "ConVisitEndTime" = ALLDATA."ConVisitEndTime", "EVVStartTime" = ALLDATA."EVVStartTime", "EVVEndTime" = ALLDATA."EVVEndTime", "ConEVVStartTime" = ALLDATA."ConEVVStartTime", "ConEVVEndTime" = ALLDATA."ConEVVEndTime", "CaregiverID" = ALLDATA."CaregiverID", "AppCaregiverID" = ALLDATA."AppCaregiverID", "AideCode" = ALLDATA."AideCode", "AideName" = ALLDATA."AideName", "AideSSN" = ALLDATA."AideSSN", "ConCaregiverID" = ALLDATA."ConCaregiverID", "ConAppCaregiverID" = ALLDATA."ConAppCaregiverID", "ConAideCode" = ALLDATA."ConAideCode", "ConAideName" = ALLDATA."ConAideName", "ConAideSSN" = ALLDATA."ConAideSSN", "OfficeID" = ALLDATA."OfficeID", "AppOfficeID" = ALLDATA."AppOfficeID", "Office" = ALLDATA."Office", "ConOfficeID" = ALLDATA."ConOfficeID", "ConAppOfficeID" = ALLDATA."ConAppOfficeID", "ConOffice" = ALLDATA."ConOffice", "PatientID" = ALLDATA."PatientID", "AppPatientID" = ALLDATA."AppPatientID", "PAdmissionID" = ALLDATA."PAdmissionID", "PName" = ALLDATA."PName", "PAddressID" = ALLDATA."PAddressID", "PAppAddressID" = ALLDATA."PAppAddressID", "PAddressL1" = ALLDATA."PAddressL1", "PAddressL2" = ALLDATA."PAddressL2", "PCity" = ALLDATA."PCity", "PAddressState" = ALLDATA."PAddressState", "PZipCode" = ALLDATA."PZipCode", "PCounty" = ALLDATA."PCounty", "PLongitude" = ALLDATA."PLongitude", "PLatitude" = ALLDATA."PLatitude", "ConPatientID" = ALLDATA."ConPatientID", "ConAppPatientID" = ALLDATA."ConAppPatientID", "ConPAdmissionID" = ALLDATA."ConPAdmissionID", "ConPName" = ALLDATA."ConPName", "ConPAddressID" = ALLDATA."ConPAddressID", "ConPAppAddressID" = ALLDATA."ConPAppAddressID", "ConPAddressL1" = ALLDATA."ConPAddressL1", "ConPAddressL2" = ALLDATA."ConPAddressL2", "ConPCity" = ALLDATA."ConPCity", "ConPAddressState" = ALLDATA."ConPAddressState", "ConPZipCode" = ALLDATA."ConPZipCode", "ConPCounty" = ALLDATA."ConPCounty", "ConPLongitude" = ALLDATA."ConPLongitude", "ConPLatitude" = ALLDATA."ConPLatitude", "PayerID" = ALLDATA."PayerID", "AppPayerID" = ALLDATA."AppPayerID", "Contract" = ALLDATA."Contract", "ConPayerID" = ALLDATA."ConPayerID", "ConAppPayerID" = ALLDATA."ConAppPayerID", "ConContract" = ALLDATA."ConContract", "BilledDate" = ALLDATA."BilledDate", "ConBilledDate" = ALLDATA."ConBilledDate", "BilledHours" = ALLDATA."BilledHours", "ConBilledHours" = ALLDATA."ConBilledHours", "Billed" = ALLDATA."Billed", "ConBilled" = ALLDATA."ConBilled", "MinuteDiffBetweenSch" = ALLDATA."MinuteDiffBetweenSch", "DistanceMilesFromLatLng" = ALLDATA."DistanceMilesFromLatLng", "AverageMilesPerHour" = ALLDATA."AverageMilesPerHour", "ETATravelMinutes" = ALLDATA."ETATravelMinutes", "ServiceCodeID" = ALLDATA."ServiceCodeID", "AppServiceCodeID" = ALLDATA."AppServiceCodeID", "RateType" = ALLDATA."RateType", "ServiceCode" = ALLDATA."ServiceCode", "ConServiceCodeID" = ALLDATA."ConServiceCodeID", "ConAppServiceCodeID" = ALLDATA."ConAppServiceCodeID", "ConRateType" = ALLDATA."ConRateType", "ConServiceCode" = ALLDATA."ConServiceCode", "UpdateFlag" = NULL, "UpdatedDate" = NOW(), "StatusFlag" = CASE WHEN CVM."StatusFlag" NOT IN ('W', 'I') THEN 'U' ELSE CVM."StatusFlag" END, "ResolveDate" = NULL, "AideFName" = ALLDATA."AideFName", "AideLName" = ALLDATA."AideLName", "ConAideFName" = ALLDATA."ConAideFName", "ConAideLName" = ALLDATA."ConAideLName", "PFName" = ALLDATA."PFName", "PLName" = ALLDATA."PLName", "ConPFName" = ALLDATA."ConPFName", "ConPLName" = ALLDATA."ConPLName", "PMedicaidNumber" = ALLDATA."PMedicaidNumber", "ConPMedicaidNumber" = ALLDATA."ConPMedicaidNumber", "PayerState" = ALLDATA."PayerState", "ConPayerState" = ALLDATA."ConPayerState", "LastUpdatedBy" = ALLDATA."LastUpdatedBy", "ConLastUpdatedBy" = ALLDATA."ConLastUpdatedBy", "LastUpdatedDate" = ALLDATA."LastUpdatedDate", "ConLastUpdatedDate" = ALLDATA."ConLastUpdatedDate", "BilledRate" = ALLDATA."BilledRate", "TotalBilledAmount" = ALLDATA."TotalBilledAmount", "ConBilledRate" = ALLDATA."ConBilledRate", "ConTotalBilledAmount" = ALLDATA."ConTotalBilledAmount", "IsMissed" = ALLDATA."IsMissed", "MissedVisitReason" = ALLDATA."MissedVisitReason", "EVVType" = ALLDATA."EVVType", "ConIsMissed" = ALLDATA."ConIsMissed", "ConMissedVisitReason" = ALLDATA."ConMissedVisitReason", "ConEVVType" = ALLDATA."ConEVVType", "PStatus" = ALLDATA."PStatus", "ConPStatus" = ALLDATA."ConPStatus", "AideStatus" = ALLDATA."AideStatus", "ConAideStatus" = ALLDATA."ConAideStatus", "P_PatientID" = ALLDATA."P_PatientID", "P_AppPatientID" = ALLDATA."P_AppPatientID", "ConP_PatientID" = ALLDATA."ConP_PatientID", "ConP_AppPatientID" = ALLDATA."ConP_AppPatientID", "PA_PatientID" = ALLDATA."PA_PatientID", "PA_AppPatientID" = ALLDATA."PA_AppPatientID", "ConPA_PatientID" = ALLDATA."ConPA_PatientID", "ConPA_AppPatientID" = ALLDATA."ConPA_AppPatientID", "P_PAdmissionID" = ALLDATA."P_PAdmissionID", "P_PName" = ALLDATA."P_PName", "P_PAddressID" = ALLDATA."P_PAddressID", "P_PAppAddressID" = ALLDATA."P_PAppAddressID", "P_PAddressL1" = ALLDATA."P_PAddressL1", "P_PAddressL2" = ALLDATA."P_PAddressL2", "P_PCity" = ALLDATA."P_PCity", "P_PAddressState" = ALLDATA."P_PAddressState", "P_PZipCode" = ALLDATA."P_PZipCode", "P_PCounty" = ALLDATA."P_PCounty", "P_PFName" = ALLDATA."P_PFName", "P_PLName" = ALLDATA."P_PLName", "P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", "ConP_PAdmissionID" = ALLDATA."ConP_PAdmissionID", "ConP_PName" = ALLDATA."ConP_PName", "ConP_PAddressID" = ALLDATA."ConP_PAddressID", "ConP_PAppAddressID" = ALLDATA."ConP_PAppAddressID", "ConP_PAddressL1" = ALLDATA."ConP_PAddressL1", "ConP_PAddressL2" = ALLDATA."ConP_PAddressL2", "ConP_PCity" = ALLDATA."ConP_PCity", "ConP_PAddressState" = ALLDATA."ConP_PAddressState", "ConP_PZipCode" = ALLDATA."ConP_PZipCode", "ConP_PCounty" = ALLDATA."ConP_PCounty", "ConP_PFName" = ALLDATA."ConP_PFName", "ConP_PLName" = ALLDATA."ConP_PLName", "ConP_PMedicaidNumber" = ALLDATA."ConP_PMedicaidNumber", "PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", "PA_PName" = ALLDATA."PA_PName", "PA_PAddressID" = ALLDATA."PA_PAddressID", "PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", "PA_PAddressL1" = ALLDATA."PA_PAddressL1", "PA_PAddressL2" = ALLDATA."PA_PAddressL2", "PA_PCity" = ALLDATA."PA_PCity", "PA_PAddressState" = ALLDATA."PA_PAddressState", "PA_PZipCode" = ALLDATA."PA_PZipCode", "PA_PCounty" = ALLDATA."PA_PCounty", "PA_PFName" = ALLDATA."PA_PFName", "PA_PLName" = ALLDATA."PA_PLName", "PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", "ConPA_PAdmissionID" = ALLDATA."ConPA_PAdmissionID", "ConPA_PName" = ALLDATA."ConPA_PName", "ConPA_PAddressID" = ALLDATA."ConPA_PAddressID", "ConPA_PAppAddressID" = ALLDATA."ConPA_PAppAddressID", "ConPA_PAddressL1" = ALLDATA."ConPA_PAddressL1", "ConPA_PAddressL2" = ALLDATA."ConPA_PAddressL2", "ConPA_PCity" = ALLDATA."ConPA_PCity", "ConPA_PAddressState" = ALLDATA."ConPA_PAddressState", "ConPA_PZipCode" = ALLDATA."ConPA_PZipCode", "ConPA_PCounty" = ALLDATA."ConPA_PCounty", "ConPA_PFName" = ALLDATA."ConPA_PFName", "ConPA_PLName" = ALLDATA."ConPA_PLName", "ConPA_PMedicaidNumber" = ALLDATA."ConPA_PMedicaidNumber", "ContractType" = ALLDATA."ContractType", "ConContractType" = ALLDATA."ConContractType", "InServiceFlag" = CASE WHEN CVM."InServiceFlag" = 'N' THEN ALLDATA."InServiceFlag" ELSE CVM."InServiceFlag" END, "BillRateNonBilled" = ALLDATA."BillRateNonBilled", "ConBillRateNonBilled" = ALLDATA."ConBillRateNonBilled", "BillRateBoth" = ALLDATA."BillRateBoth", "ConBillRateBoth" = ALLDATA."ConBillRateBoth", "FederalTaxNumber" = ALLDATA."FederalTaxNumber", "ConFederalTaxNumber" = ALLDATA."ConFederalTaxNumber", "InserviceStartDate" = ALLDATA."InserviceStartDate", "InserviceEndDate" = ALLDATA."InserviceEndDate", "ConInserviceStartDate" = ALLDATA."ConInserviceStartDate", "ConInserviceEndDate" = ALLDATA."ConInserviceEndDate"
    FROM (
       WITH filtered_visits AS (
           SELECT CR1.*
           FROM {analytics_schema}.FACTVISITCALLPERFORMANCE_CR AS CR1
           WHERE CR1."Is Missed" = FALSE
             AND CR1."Visit Start Time" IS NOT NULL
             AND CR1."Visit End Time" IS NOT NULL
             AND CR1."Visit Date"::date BETWEEN (NOW() - INTERVAL '2 years')::date
                                             AND (NOW() + INTERVAL '45 days')::date
             AND {chunk_visit_date_filter}  -- ⭐ CHUNK FILTER: Only process visits in this chunk's date range
       ),
       filtered_caregivers AS (
           SELECT CAR.*
           FROM {analytics_schema}.DIMCAREGIVER AS CAR
           WHERE TRIM(CAR."SSN") IS NOT NULL
             AND TRIM(CAR."SSN") <> ''
             AND NOT EXISTS (
                 SELECT 1 FROM {conflict_schema}.excluded_ssn AS SSN
                 WHERE TRIM(CAR."SSN") = SSN."SSN"
             )
             AND {chunk_ssn_filter}  -- ⭐ CHUNK FILTER: Only process caregivers with SSNs in this chunk
       )
            SELECT
			DISTINCT
            V1."CONFLICTID",
            V1."SSN",
            V1."ProviderID" AS "ProviderID",
            V1."AppProviderID" AS "AppProviderID",
            V1."ProviderName" AS "ProviderName",
            V1."VisitID" AS "VisitID",
            V1."AppVisitID" AS "AppVisitID",
            V2."ProviderID" AS "ConProviderID",
            V2."AppProviderID" AS "ConAppProviderID",
            V2."ProviderName" AS "ConProviderName",
            V2."VisitID" AS "ConVisitID",
            V2."AppVisitID" AS "ConAppVisitID",
            V2."VisitDate" AS "VisitDate",
            V1."SchStartTime" AS "SchStartTime",
            V1."SchEndTime" AS "SchEndTime",
            V2."SchStartTime" AS "ConSchStartTime",
            V2."SchEndTime" AS "ConSchEndTime",
            V1."VisitStartTime" AS "VisitStartTime",
            V1."VisitEndTime" AS "VisitEndTime",
            V2."VisitStartTime" AS "ConVisitStartTime",
            V2."VisitEndTime" AS "ConVisitEndTime",
            V1."EVVStartTime" AS "EVVStartTime",
            V1."EVVEndTime" AS "EVVEndTime",
            V2."EVVStartTime" AS "ConEVVStartTime",
            V2."EVVEndTime" AS "ConEVVEndTime",
            V1."CaregiverID" AS "CaregiverID",
            V1."AppCaregiverID" AS "AppCaregiverID",
            V1."AideCode" AS "AideCode",
            V1."AideName" AS "AideName",
            V1."AideSSN" AS "AideSSN",
            V2."CaregiverID" AS "ConCaregiverID",
            V2."AppCaregiverID" AS "ConAppCaregiverID",
            V2."AideCode" AS "ConAideCode",
            V2."AideName" AS "ConAideName",
            V2."AideSSN" AS "ConAideSSN",
            V1."OfficeID" AS "OfficeID",
            V1."AppOfficeID" AS "AppOfficeID",
            V1."Office" AS "Office",
            V2."OfficeID" AS "ConOfficeID",
            V2."AppOfficeID" AS "ConAppOfficeID",
            V2."Office" AS "ConOffice",
            V1."PatientID" AS "PatientID",
            V1."AppPatientID" AS "AppPatientID",
            V1."PAdmissionID" AS "PAdmissionID",
            V1."PName" AS "PName",
            V1."PAddressID" AS "PAddressID",
            V1."PAppAddressID" AS "PAppAddressID",
            V1."PAddressL1" AS "PAddressL1",
            V1."PAddressL2" AS "PAddressL2",
            V1."PCity" AS "PCity",
            V1."PAddressState" AS "PAddressState",
            V1."PZipCode" AS "PZipCode",
            V1."PCounty" AS "PCounty",            
            V1."Longitude" AS "PLongitude",
            V1."Latitude" AS "PLatitude",
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
            V1."PayerID" AS "PayerID",
            V1."AppPayerID" AS "AppPayerID",
            V1."Contract" AS "Contract",
            V2."PayerID" AS "ConPayerID",
            V2."AppPayerID" AS "ConAppPayerID",
            V2."Contract" AS "ConContract",
            V1."BilledDate" AS "BilledDate",
            V2."BilledDate" AS "ConBilledDate",
            V1."BilledHours" AS "BilledHours",
            V2."BilledHours" AS "ConBilledHours",
            V1."Billed" AS "Billed",
            V2."Billed" AS "ConBilled",            
            CAST(NULL AS DOUBLE PRECISION) "MinuteDiffBetweenSch",
            CAST(NULL AS DOUBLE PRECISION) "DistanceMilesFromLatLng",
            CAST(NULL AS DOUBLE PRECISION) "AverageMilesPerHour",
            CAST(NULL AS DOUBLE PRECISION) "ETATravelMinutes",
            V1."InserviceStartDate" AS "InserviceStartDate",
			V1."InserviceEndDate" AS "InserviceEndDate",
            V2."InserviceStartDate" AS "ConInserviceStartDate",
			V2."InserviceEndDate" AS "ConInserviceEndDate",
            V1."PTOStartDate" AS "PTOStartDate",
			V1."PTOEndDate" AS "PTOEndDate",
			V1."ServiceCodeID" AS "ServiceCodeID",
	        V1."AppServiceCodeID" AS "AppServiceCodeID",
	        V1."RateType" AS "RateType",
	        V1."ServiceCode" AS "ServiceCode",          
	        V2."ServiceCodeID" AS "ConServiceCodeID",
	        V2."AppServiceCodeID" AS "ConAppServiceCodeID",
	        V2."RateType" AS "ConRateType",
	        V2."ServiceCode" AS "ConServiceCode",
            'N' AS "SameSchTimeFlag",
            'N' AS "SameVisitTimeFlag",
            'N' AS "SchAndVisitTimeSameFlag",
            'N' AS "SchOverAnotherSchTimeFlag",
            'N' AS "VisitTimeOverAnotherVisitTimeFlag",
            'N' AS "SchTimeOverVisitTimeFlag",
            'N' AS "DistanceFlag",
            'Y' AS "InServiceFlag",
            'N' AS "PTOFlag",
            V1."AideFName" AS "AideFName",
            V1."AideLName" AS "AideLName",
            V2."AideFName" AS "ConAideFName",
            V2."AideLName" AS "ConAideLName",
            V1."PFName" AS "PFName",
            V1."PLName" AS "PLName",
            V2."PFName" AS "ConPFName",
            V2."PLName" AS "ConPLName",
			V1."PMedicaidNumber" AS "PMedicaidNumber",
			V2."PMedicaidNumber" AS "ConPMedicaidNumber",
			V1."PayerState" AS "PayerState",
			V2."PayerState" AS "ConPayerState",
			V1."AgencyContact" AS "AgencyContact",
			V2."AgencyContact" AS "ConAgencyContact",
			V1."AgencyPhone" AS "AgencyPhone",
			V2."AgencyPhone" AS "ConAgencyPhone",
			V1."LastUpdatedBy" AS "LastUpdatedBy",
			V2."LastUpdatedBy" AS "ConLastUpdatedBy",
			V1."LastUpdatedDate" AS "LastUpdatedDate",
			V2."LastUpdatedDate" AS "ConLastUpdatedDate",
			V1."BilledRate" AS "BilledRate",
			V1."TotalBilledAmount" AS "TotalBilledAmount",
			V2."BilledRate" AS "ConBilledRate",
			V2."TotalBilledAmount" AS "ConTotalBilledAmount",
			V1."IsMissed" AS "IsMissed",
			V1."MissedVisitReason" AS "MissedVisitReason",
			V1."EVVType" AS "EVVType",
			V2."IsMissed" AS "ConIsMissed",
			V2."MissedVisitReason" AS "ConMissedVisitReason",
			V2."EVVType" AS "ConEVVType",
			V1."PStatus" AS "PStatus",
			V2."PStatus" AS "ConPStatus",
			V1."AideStatus" AS "AideStatus",
			V2."AideStatus" AS "ConAideStatus",
			V1."P_PatientID" AS "P_PatientID",
			V1."P_AppPatientID" AS "P_AppPatientID",
			V2."P_PatientID" AS "ConP_PatientID",
			V2."P_AppPatientID" AS "ConP_AppPatientID",
			V1."PA_PatientID" AS "PA_PatientID",
			V1."PA_AppPatientID" AS "PA_AppPatientID",
			V2."PA_PatientID" AS "ConPA_PatientID",
			V2."PA_AppPatientID" AS "ConPA_AppPatientID",
            V1."P_PAdmissionID" AS "P_PAdmissionID",
            V1."P_PName" AS "P_PName",
            V1."P_PAddressID" AS "P_PAddressID",
            V1."P_PAppAddressID" AS "P_PAppAddressID",
            V1."P_PAddressL1" AS "P_PAddressL1",
            V1."P_PAddressL2" AS "P_PAddressL2",
            V1."P_PCity" AS "P_PCity",
            V1."P_PAddressState" AS "P_PAddressState",
            V1."P_PZipCode" AS "P_PZipCode",
            V1."P_PCounty" AS "P_PCounty",
			V1."P_PFName" AS "P_PFName",
            V1."P_PLName" AS "P_PLName",
			V1."P_PMedicaidNumber" AS "P_PMedicaidNumber",
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
            V1."PA_PAdmissionID" AS "PA_PAdmissionID",
            V1."PA_PName" AS "PA_PName",
            V1."PA_PAddressID" AS "PA_PAddressID",
            V1."PA_PAppAddressID" AS "PA_PAppAddressID",
            V1."PA_PAddressL1" AS "PA_PAddressL1",
            V1."PA_PAddressL2" AS "PA_PAddressL2",
            V1."PA_PCity" AS "PA_PCity",
            V1."PA_PAddressState" AS "PA_PAddressState",
            V1."PA_PZipCode" AS "PA_PZipCode",
            V1."PA_PCounty" AS "PA_PCounty",
			V1."PA_PFName" AS "PA_PFName",
            V1."PA_PLName" AS "PA_PLName",
			V1."PA_PMedicaidNumber" AS "PA_PMedicaidNumber",
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
			V1."ContractType" AS "ContractType",
			V2."ContractType" AS "ConContractType",
			V1."P_PStatus" AS "P_PStatus",
			V2."P_PStatus" AS "ConP_PStatus",
			V1."PA_PStatus" AS "PA_PStatus",
			V2."PA_PStatus" AS "ConPA_PStatus",
			V1."BillRateNonBilled" AS "BillRateNonBilled",
			V2."BillRateNonBilled" AS "ConBillRateNonBilled",
			V1."BillRateBoth" AS "BillRateBoth",
			V2."BillRateBoth" AS "ConBillRateBoth",
			V1."FederalTaxNumber" AS "FederalTaxNumber",
			V2."FederalTaxNumber" AS "ConFederalTaxNumber"
		FROM
       (
        WITH filtered_visits AS (
            SELECT CR1.*
            FROM {analytics_schema}.FACTVISITCALLPERFORMANCE_CR AS CR1
            WHERE CR1."Is Missed" = FALSE
              AND CR1."Visit Start Time" IS NOT NULL
              AND CR1."Visit End Time" IS NOT NULL
              AND CR1."Visit Date"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                              AND (NOW() + INTERVAL '45 days')::date
              AND {chunk_visit_date_filter}  -- ⭐ CHUNK FILTER: Only process visits in this chunk's date range
              AND NOT EXISTS (
                  SELECT 1 FROM {conflict_schema}.excluded_agency AS EA 
                  WHERE EA."ProviderID" = CR1."Provider Id"
              )
        ),
        filtered_caregivers AS (
            SELECT CAR.*
            FROM {analytics_schema}.DIMCAREGIVER AS CAR
            WHERE TRIM(CAR."SSN") IS NOT NULL 
              AND TRIM(CAR."SSN") <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM {conflict_schema}.excluded_ssn AS SSN 
                  WHERE TRIM(CAR."SSN") = SSN."SSN"
              )
              AND {chunk_ssn_filter}  -- ⭐ CHUNK FILTER: Only process caregivers with SSNs in this chunk
        )
		SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", 
		CAST(NULL AS DOUBLE PRECISION) "BillRateNonBilled", 
		CAST(NULL AS DOUBLE PRECISION) "BillRateBoth", 
		TRIM(CAR."SSN") as "SSN", 
		CAST(NULL AS VARCHAR) "PStatus", 
		CAST(NULL AS VARCHAR) "AideStatus", 
		CAST(NULL AS VARCHAR) "MissedVisitReason", 
		CAST(NULL AS BOOLEAN) "IsMissed", 
		CAST(NULL AS VARCHAR) "EVVType", 
		CAST(NULL AS DOUBLE PRECISION) "BilledRate", 
		CAST(NULL AS DOUBLE PRECISION) "TotalBilledAmount",
		DPR."Provider Id" as "ProviderID", 
		DPR."Application Provider Id" as "AppProviderID", 
		DPR."Provider Name" AS "ProviderName", 
		CAST(NULL AS VARCHAR) "AgencyContact", 
		DPR."Phone Number 1" AS "AgencyPhone", 
		DPR."Federal Tax Number" AS "FederalTaxNumber",
		MD5(CONCAT('I', FCS."Application Caregiver Inservice Id"::varchar))::uuid AS "VisitID", 
		FCS."Application Caregiver Inservice Id"::bigint AS "AppVisitID",
		FCS."Inservice start date"::date AS "VisitDate", 
		CAST(NULL AS TIMESTAMP) "SchStartTime", 
		CAST(NULL AS TIMESTAMP) "SchEndTime", 
		CAST(NULL AS TIMESTAMP) "VisitStartTime", 
		CAST(NULL AS TIMESTAMP) "VisitEndTime", 
		CAST(NULL AS TIMESTAMP) "EVVStartTime", 
		CAST(NULL AS TIMESTAMP) "EVVEndTime",
		CAR."Caregiver Id" as "CaregiverID",
		CAR."Application Caregiver Id" as "AppCaregiverID",
		CAR."Caregiver Code" as "AideCode",
		CAR."Caregiver Fullname" as "AideName",
		CAR."Caregiver Firstname" as "AideFName",
		CAR."Caregiver Lastname" as "AideLName",
		TRIM(CAR."SSN") as "AideSSN",
		DOF."Office Id" as "OfficeID",
		DOF."Application Office Id" as "AppOfficeID",
		DOF."Office Name" as "Office",
		CAST(NULL AS uuid) "PA_PatientID",
		CAST(NULL AS NUMERIC) "PA_AppPatientID",
		CAST(NULL AS uuid) "P_PatientID",
		CAST(NULL AS NUMERIC) "P_AppPatientID",
		CAST(NULL AS uuid) "PatientID",
		CAST(NULL AS NUMERIC) "AppPatientID",
		CAST(NULL AS uuid) "PAdmissionID",
		CAST(NULL AS VARCHAR) "PName",
		CAST(NULL AS VARCHAR) "PFName",
		CAST(NULL AS VARCHAR) "PLName",
		CAST(NULL AS VARCHAR) "PMedicaidNumber",
		CAST(NULL AS uuid) "PAddressID",
		CAST(NULL AS NUMERIC) "PAppAddressID",
		CAST(NULL AS VARCHAR) "PAddressL1",
		CAST(NULL AS VARCHAR) "PAddressL2",
		CAST(NULL AS VARCHAR) "PCity",
		CAST(NULL AS VARCHAR) "PAddressState",
		CAST(NULL AS VARCHAR) "PZipCode",
		CAST(NULL AS VARCHAR) "PCounty",
		CAST(NULL AS DOUBLE PRECISION) "Longitude",
		CAST(NULL AS DOUBLE PRECISION) "Latitude",
		CAST(NULL AS uuid) "PayerID",
		CAST(NULL AS bigint) "AppPayerID",
		CAST(NULL AS VARCHAR) "Contract",
		CAST(NULL AS VARCHAR) "PayerState",
		CAST(NULL AS TIMESTAMP) "BilledDate",
		CAST(NULL AS DOUBLE PRECISION) "BilledHours",
		CAST(NULL AS VARCHAR) "Billed",
		FCS."Inservice start date"::timestamp AS "InserviceStartDate",
		FCS."Inservice end date"::timestamp AS "InserviceEndDate",
		FCS."Application Caregiver Inservice Id"::varchar AS "AppCaregiverInserviceID",
		CAST(NULL AS TIMESTAMP) "PTOStartDate",
		CAST(NULL AS TIMESTAMP) "PTOEndDate",
		CAST(NULL AS uuid) "PTOVacationID",
		CAST(NULL AS uuid) "ServiceCodeID",
		CAST(NULL AS NUMERIC) "AppServiceCodeID",
		CAST(NULL AS VARCHAR) "RateType",
		CAST(NULL AS VARCHAR) "ServiceCode",
		CAST(NULL AS TIMESTAMP) "LastUpdatedDate",
		CAST(NULL AS VARCHAR) "LastUpdatedBy",
		CAST(NULL AS uuid) "P_PAdmissionID",
		CAST(NULL AS VARCHAR) "P_PName",
		CAST(NULL AS VARCHAR) "P_PFName",
		CAST(NULL AS VARCHAR) "P_PLName",
		CAST(NULL AS VARCHAR) "P_PMedicaidNumber",
		CAST(NULL AS VARCHAR) "P_PStatus",
		CAST(NULL AS uuid) "P_PAddressID",
		CAST(NULL AS NUMERIC) "P_PAppAddressID",
		CAST(NULL AS VARCHAR) "P_PAddressL1",
		CAST(NULL AS VARCHAR) "P_PAddressL2",
		CAST(NULL AS VARCHAR) "P_PCity",
		CAST(NULL AS VARCHAR) "P_PAddressState",
		CAST(NULL AS VARCHAR) "P_PZipCode",
		CAST(NULL AS VARCHAR) "P_PCounty",
		CAST(NULL AS uuid) "PA_PAdmissionID",
		CAST(NULL AS VARCHAR) "PA_PName",
		CAST(NULL AS VARCHAR) "PA_PFName",
		CAST(NULL AS VARCHAR) "PA_PLName",
		CAST(NULL AS VARCHAR) "PA_PMedicaidNumber",
		CAST(NULL AS VARCHAR) "PA_PStatus",
		CAST(NULL AS uuid) "PA_PAddressID",
		CAST(NULL AS NUMERIC) "PA_PAppAddressID",
		CAST(NULL AS VARCHAR) "PA_PAddressL1",
		CAST(NULL AS VARCHAR) "PA_PAddressL2",
		CAST(NULL AS VARCHAR) "PA_PCity",
		CAST(NULL AS VARCHAR) "PA_PAddressState",
		CAST(NULL AS VARCHAR) "PA_PZipCode",
		CAST(NULL AS VARCHAR) "PA_PCounty",
		CAST(NULL AS VARCHAR) "ContractType"
		FROM 
	   {analytics_schema}.FACTCAREGIVERINSERVICE AS FCS
	   
	   INNER JOIN filtered_caregivers AS CAR ON CAR."Caregiver Id" = FCS."Caregiver Id"

	   INNER JOIN {analytics_schema}.DIMPROVIDER AS DPR ON DPR."Provider Id" = FCS."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
	   
	   LEFT JOIN {analytics_schema}.DIMOFFICE AS DOF ON DOF."Office Id" = FCS."Office Id" AND DOF."Is Active" = TRUE

	   LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM1 ON CVM1."VisitID" = MD5(CONCAT('I', FCS."Application Caregiver Inservice Id"::varchar))::uuid AND CVM1."CONFLICTID" IS NOT NULL
	   
	   WHERE FCS."Inservice start date"::date BETWEEN (NOW() - INTERVAL '2 years')::date AND (NOW() + INTERVAL '45 days')::date
	     AND {chunk_date_filter}  -- ⭐ CHUNK FILTER: Only process inservice records in this chunk's date range
		) AS V1
       INNER JOIN
       (
			SELECT DISTINCT CAST(NULL AS DOUBLE PRECISION) "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = 'yes' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") AS "SSN", CAST(NULL AS VARCHAR) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS VARCHAR) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id"::bigint as "AppVisitID", CR1."Visit Date"::date AS "VisitDate", CR1."Scheduled Start Time"::timestamp AS "SchStartTime", CR1."Scheduled End Time"::timestamp AS "SchEndTime", CR1."Visit Start Time"::timestamp AS "VisitStartTime", CR1."Visit End Time"::timestamp AS "VisitEndTime", CR1."Call In Time"::timestamp AS "EVVStartTime", CR1."Call Out Time"::timestamp AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS uuid) "PAdmissionID", CAST(NULL AS VARCHAR) "PName", CAST(NULL AS VARCHAR) "PFName", CAST(NULL AS VARCHAR) "PLName", CAST(NULL AS VARCHAR) "PMedicaidNumber", CAST(NULL AS uuid) "PAddressID", CAST(NULL AS NUMERIC) "PAppAddressID", CAST(NULL AS VARCHAR) "PAddressL1", CAST(NULL AS VARCHAR) "PAddressL2", CAST(NULL AS VARCHAR) "PCity", CAST(NULL AS VARCHAR) "PAddressState", CAST(NULL AS VARCHAR) "PZipCode", CAST(NULL AS VARCHAR) "PCounty", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ',' THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ',' THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 2) AS DOUBLE PRECISION) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" <> ',' THEN CAST(SPLIT_PART(REPLACE(CR1."Call In GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" <> ',' THEN CAST(SPLIT_PART(REPLACE(CR1."Call Out GPS Coordinates", '"', ''), ',', 1) AS DOUBLE PRECISION) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id"::bigint as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CR1."Invoice Date"::timestamp AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", FCS."Inservice start date"::timestamp AS "InserviceStartDate", FCS."Inservice end date"::timestamp AS "InserviceEndDate", FCS."Application Caregiver Inservice Id"::varchar AS "AppCaregiverInserviceID", CAST(NULL AS VARCHAR) "PTOStartDate", CAST(NULL AS VARCHAR) "PTOEndDate", CAST(NULL AS uuid) "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id"::NUMERIC AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CR1."Visit Updated Timestamp"::timestamp as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" <> '0') THEN 'Internal' WHEN (CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" <> '0') THEN 'UPR' WHEN (CR1."Application Payer Id" <> '0' AND CR1."Application Contract Id" = '0') THEN 'Payer' END AS "ContractType" FROM filtered_visits AS CR1
			INNER JOIN filtered_caregivers AS CAR ON CAR."Caregiver Id" = CR1."Caregiver Id"
			LEFT JOIN {analytics_schema}.DIMOFFICE AS DOF ON DOF."Office Id" = CR1."Office Id" AND DOF."Is Active" = TRUE	   
			LEFT JOIN {analytics_schema}.DIMPATIENT AS DPA_P ON DPA_P."Patient Id" = CR1."Provider Patient Id"
			LEFT JOIN LATERAL (
				SELECT DDD."Patient Address Id", DDD."Application Patient Address Id", DDD."Address Line 1", DDD."Address Line 2", DDD."City", DDD."Address State", DDD."Zip Code", DDD."County", DDD."Patient Id", DDD."Application Patient Id", DDD."Longitude" AS "Provider_Longitude", DDD."Latitude" AS "Provider_Latitude"
				FROM {analytics_schema}.DIMPATIENTADDRESS AS DDD
				WHERE DDD."Patient Id" = DPA_P."Patient Id"
				  AND DDD."Primary Address" = TRUE 
				  AND DDD."Address Type" LIKE '%GPS%'
				ORDER BY DDD."Application Created UTC Timestamp" DESC
				LIMIT 1
			) AS DPAD_P ON TRUE		
			LEFT JOIN {analytics_schema}.DIMPATIENT AS DPA_PA ON DPA_PA."Patient Id" = CR1."Payer Patient Id"		 
			LEFT JOIN LATERAL (
				SELECT DDD."Patient Address Id", DDD."Application Patient Address Id", DDD."Address Line 1", DDD."Address Line 2", DDD."City", DDD."Address State", DDD."Zip Code", DDD."County", DDD."Patient Id", DDD."Application Patient Id"
				FROM {analytics_schema}.DIMPATIENTADDRESS AS DDD
				WHERE DDD."Patient Id" = DPA_PA."Patient Id"
				  AND DDD."Primary Address" = TRUE 
				  AND DDD."Address Type" LIKE '%GPS%'
				ORDER BY DDD."Application Created UTC Timestamp" DESC
				LIMIT 1
			) AS DPAD_PA ON TRUE	   
			LEFT JOIN {analytics_schema}.DIMPAYER AS SPA ON SPA."Payer Id" = CR1."Payer Id" AND SPA."Is Active" = TRUE AND SPA."Is Demo" = FALSE
			LEFT JOIN {analytics_schema}.DIMCONTRACT AS DCON ON DCON."Contract Id" = CR1."Contract Id" AND DCON."Is Active" = TRUE
			INNER JOIN {analytics_schema}.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
			
			LEFT JOIN {analytics_schema}.FACTCAREGIVERINSERVICE AS FCS ON FCS."Caregiver Id" = CR1."Caregiver Id" AND CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL AND (CR1."Visit Start Time"::timestamp <= FCS."Inservice end date"::timestamp AND CR1."Visit End Time"::timestamp >= FCS."Inservice start date"::timestamp)

			LEFT JOIN {analytics_schema}.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
			LEFT JOIN {analytics_schema}.DIMUSER AS DUSR ON DUSR."User Id"::varchar = CR1."Visit Updated User Id"::varchar
			WHERE FCS."Application Caregiver Inservice Id"::varchar IS NULL
		) AS V2 ON
       	V1."VisitID" <> V2."VisitID"
	   	AND
	   	V1."SSN" = V2."SSN"
		AND		
		(V2."VisitStartTime"::timestamp <= V1."InserviceEndDate"::timestamp AND V2."VisitEndTime"::timestamp >= V1."InserviceStartDate"::timestamp)
		 AND V2."ProviderID" IS NOT NULL
       			AND
       			V2."AppProviderID" IS NOT NULL
				AND V1."ProviderID" <> V2."ProviderID"
				AND
				V1."AppCaregiverInserviceID" IS NOT NULL
				AND
				V2."AppCaregiverInserviceID" IS NULL
       ) AS ALLDATA WHERE CVM."VisitID" = ALLDATA."VisitID" AND CVM."ConVisitID" = ALLDATA."ConVisitID" AND CVM."InserviceStartDate" IS NOT NULL AND CVM."InserviceEndDate" IS NOT NULL AND CVM."ConInserviceStartDate" IS NULL AND CVM."ConInserviceEndDate" IS NULL AND CVM."UpdateFlag" = 1
    AND {chunk_filter};  -- ⭐ CHUNK FILTER: Only update rows in this chunk