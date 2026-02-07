CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_2()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	
	try {
		var sql_query_reverse_inservice = `INSERT INTO CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS ("CONFLICTID", "SSN", "ProviderID", "AppProviderID", "ProviderName", "VisitID", "AppVisitID", "ConProviderID", "ConAppProviderID", "ConProviderName", "ConVisitID", "ConAppVisitID", "VisitDate", "SchStartTime", "SchEndTime", "ConSchStartTime", "ConSchEndTime", "VisitStartTime", "VisitEndTime", "ConVisitStartTime", "ConVisitEndTime", "EVVStartTime", "EVVEndTime", "ConEVVStartTime", "ConEVVEndTime", "CaregiverID", "AppCaregiverID", "AideCode", "AideName", "AideSSN", "ConCaregiverID", "ConAppCaregiverID", "ConAideCode", "ConAideName", "ConAideSSN", "OfficeID", "AppOfficeID", "Office", "ConOfficeID", "ConAppOfficeID", "ConOffice", "PatientID", "AppPatientID", "PAdmissionID", "PName", "PAddressID", "PAppAddressID", "PAddressL1", "PAddressL2", "PCity", "PAddressState", "PZipCode", "PCounty", "PLongitude", "PLatitude", "ConPatientID", "ConAppPatientID", "ConPAdmissionID", "ConPName", "ConPAddressID", "ConPAppAddressID", "ConPAddressL1", "ConPAddressL2", "ConPCity", "ConPAddressState", "ConPZipCode", "ConPCounty", "ConPLongitude", "ConPLatitude", "PayerID", "AppPayerID", "Contract", "ConPayerID", "ConAppPayerID", "ConContract", "BilledDate", "ConBilledDate", "BilledHours", "ConBilledHours", "Billed", "ConBilled", "MinuteDiffBetweenSch", "DistanceMilesFromLatLng", "AverageMilesPerHour", "ETATravleMinutes", "ServiceCodeID", "AppServiceCodeID", "RateType", "ServiceCode", "ConServiceCodeID", "ConAppServiceCodeID", "ConRateType", "ConServiceCode", "SameSchTimeFlag", "SameVisitTimeFlag", "SchAndVisitTimeSameFlag", "SchOverAnotherSchTimeFlag", "VisitTimeOverAnotherVisitTimeFlag", "SchTimeOverVisitTimeFlag", "DistanceFlag", "InServiceFlag", "PTOFlag", "AideFName", "AideLName", "ConAideFName", "ConAideLName", "PFName", "PLName", "ConPFName", "ConPLName", "PMedicaidNumber", "ConPMedicaidNumber", "PayerState", "ConPayerState", "AgencyContact", "ConAgencyContact", "AgencyPhone", "ConAgencyPhone", "LastUpdatedBy", "ConLastUpdatedBy", "LastUpdatedDate", "ConLastUpdatedDate", "BilledRate", "TotalBilledAmount", "ConBilledRate", "ConTotalBilledAmount", "IsMissed", "MissedVisitReason", "EVVType", "ConIsMissed", "ConMissedVisitReason", "ConEVVType", "PStatus", "ConPStatus", "AideStatus", "ConAideStatus", "P_PatientID", "P_AppPatientID", "ConP_PatientID", "ConP_AppPatientID", "PA_PatientID", "PA_AppPatientID", "ConPA_PatientID", "ConPA_AppPatientID", "P_PAdmissionID", "P_PName", "P_PAddressID", "P_PAppAddressID", "P_PAddressL1", "P_PAddressL2", "P_PCity", "P_PAddressState", "P_PZipCode", "P_PCounty", "P_PFName", "P_PLName", "P_PMedicaidNumber", "ConP_PAdmissionID", "ConP_PName", "ConP_PAddressID", "ConP_PAppAddressID", "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity", "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty", "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber", "PA_PAdmissionID", "PA_PName", "PA_PAddressID", "PA_PAppAddressID", "PA_PAddressL1", "PA_PAddressL2", "PA_PCity", "PA_PAddressState", "PA_PZipCode", "PA_PCounty", "PA_PFName", "PA_PLName", "PA_PMedicaidNumber", "ConPA_PAdmissionID", "ConPA_PName", "ConPA_PAddressID", "ConPA_PAppAddressID", "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity", "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty", "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber", "ContractType", "ConContractType", "P_PStatus", "ConP_PStatus", "PA_PStatus", "ConPA_PStatus", "BillRateNonBilled", "ConBillRateNonBilled", "BillRateBoth", "ConBillRateBoth", "FederalTaxNumber", "ConFederalTaxNumber", "InserviceStartDate", "InserviceEndDate", "ConInserviceStartDate", "ConInserviceEndDate")
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
            CAST(NULL AS NUMBER) "MinuteDiffBetweenSch",
            CAST(NULL AS NUMBER) "DistanceMilesFromLatLng",
            CAST(NULL AS NUMBER) "AverageMilesPerHour",
           	CAST(NULL AS NUMBER) "ETATravleMinutes",
            V1."ServiceCodeID" AS "ServiceCodeID",
	        V1."AppServiceCodeID" AS "AppServiceCodeID",
	        V1."RateType" AS "RateType",
	        V1."ServiceCode" AS "ServiceCode",          
	        V2."ServiceCodeID" AS "ConServiceCodeID",
	        V2."AppServiceCodeID" AS "ConAppServiceCodeID",
	        V2."RateType" AS "ConRateType",
	        V2."ServiceCode" AS "ConServiceCode",
            ''N'' AS "SameSchTimeFlag",
            ''N'' AS "SameVisitTimeFlag",
            ''N'' AS "SchVisitTimeSame",
            ''N'' AS "SchOverAnotherSchTimeFlag",
            ''N'' AS "VisitTimeOverAnotherVisitTimeFlag",
            ''N'' AS "SchTimeOverVisitTimeFlag",
            ''N'' AS "DistanceFlag",
			''Y'' AS "InServiceFlag",
			''N'' AS "PTOFlag",
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
       (SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(FCS."Inservice start date" AS timestamp) AS "InserviceStartDate", CAST(FCS."Inservice end date" AS timestamp) AS "InserviceEndDate", CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) AS "AppCaregiverInserviceID", CAST(NULL AS TIMESTAMP) "PTOStartDate", CAST(NULL AS TIMESTAMP) "PTOEndDate", CAST(NULL AS STRING) "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
       INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = CR1."Caregiver Id" AND TRIM(CAR."SSN") IS NOT NULL AND TRIM(CAR."SSN")!=''''
       LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = CR1."Office Id" AND DOF."Is Active" = TRUE
	    LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_P ON DPA_P."Patient Id" = CR1."Provider Patient Id"		
		LEFT JOIN (
			SELECT DDD."Patient Address Id", DDD."Application Patient Address Id", DDD."Address Line 1", DDD."Address Line 2", DDD."City", DDD."Address State", DDD."Zip Code", DDD."County", DDD."Patient Id", DDD."Application Patient Id", DDD."Longitude" AS "Provider_Longitude", DDD."Latitude" AS "Provider_Latitude", ROW_NUMBER() OVER (PARTITION BY DDD."Patient Id" ORDER BY DDD."Application Created UTC Timestamp" DESC) AS rn
			FROM ANALYTICS.BI.DIMPATIENTADDRESS AS DDD
			WHERE DDD."Primary Address" = TRUE AND DDD."Address Type" LIKE ''%GPS%''
		) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" AND DPAD_P."RN" = 1		
		 LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_PA ON DPA_PA."Patient Id" = CR1."Payer Patient Id"		 
		 LEFT JOIN (
			SELECT DDD."Patient Address Id", DDD."Application Patient Address Id", DDD."Address Line 1", DDD."Address Line 2", DDD."City", DDD."Address State", DDD."Zip Code", DDD."County", DDD."Patient Id", DDD."Application Patient Id", ROW_NUMBER() OVER (PARTITION BY DDD."Patient Id" ORDER BY DDD."Application Created UTC Timestamp" DESC) AS rn
			FROM ANALYTICS.BI.DIMPATIENTADDRESS AS DDD
			WHERE DDD."Primary Address" = TRUE AND DDD."Address Type" LIKE ''%GPS%''
		) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" AND DPAD_PA."RN" = 1		 
		 LEFT JOIN ANALYTICS.BI.DIMPAYER AS SPA ON SPA."Payer Id" = CR1."Payer Id" AND SPA."Is Active" = TRUE AND SPA."Is Demo" = FALSE 
		 LEFT JOIN ANALYTICS.BI.DIMCONTRACT AS DCON ON DCON."Contract Id" = CR1."Contract Id" AND DCON."Is Active" = TRUE
		 INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
         LEFT JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM1 ON CVM1."VisitID" = CR1."Visit Id" AND CVM1."CONFLICTID" IS NOT NULL 
		LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
        LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"

		LEFT JOIN ANALYTICS.BI.FACTCAREGIVERINSERVICE AS FCS ON FCS."Caregiver Id" = CR1."Caregiver Id" AND CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL AND (CAST(CR1."Visit Start Time" AS timestamp) <= CAST(FCS."Inservice end date" AS timestamp) AND CAST(CR1."Visit End Time" AS timestamp) >= CAST(FCS."Inservice start date" AS timestamp)) AND FCS."Provider Id" = CR1."Provider Id"

        WHERE CR1."Is Missed" = FALSE
        AND
        CR1."Visit Start Time" IS NOT NULL
        AND
        CR1."Visit End Time" IS NOT NULL
        AND
        FCS."Application Caregiver Inservice Id" IS NULL
        AND
        DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND
        CR1."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
        ) AS V1
       INNER JOIN
       (
		SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", 
		CAST(NULL AS NUMBER) "BillRateNonBilled", 
		CAST(NULL AS NUMBER) "BillRateBoth", 
		TRIM(CAR."SSN") as "SSN", 
		CAST(NULL AS STRING) "PStatus", 
		CAST(NULL AS STRING) "AideStatus", 
		CAST(NULL AS STRING) "MissedVisitReason", 
		CAST(NULL AS BOOLEAN) "IsMissed", 
		CAST(NULL AS STRING) "EVVType", 
		CAST(NULL AS NUMBER) "BilledRate", 
		CAST(NULL AS NUMBER) "TotalBilledAmount",
		DPR."Provider Id" as "ProviderID", 
		DPR."Application Provider Id" as "AppProviderID", 
		DPR."Provider Name" AS "ProviderName", 
		CAST(NULL AS STRING) "AgencyContact", 
		DPR."Phone Number 1" AS "AgencyPhone", 
		DPR."Federal Tax Number" AS "FederalTaxNumber",
		MD5(CONCAT(''I'', CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR))) AS "VisitID", 
		CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) AS "AppVisitID",
		CAST(FCS."Inservice start date" AS DATE) AS "VisitDate", 
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
		CAST(NULL AS STRING) "PA_PatientID",
		CAST(NULL AS STRING) "PA_AppPatientID",
		CAST(NULL AS STRING) "P_PatientID",
		CAST(NULL AS STRING) "P_AppPatientID",
		CAST(NULL AS STRING) "PatientID",
		CAST(NULL AS STRING) "AppPatientID",
		CAST(NULL AS STRING) "PAdmissionID",
		CAST(NULL AS STRING) "PName",
		CAST(NULL AS STRING) "PFName",
		CAST(NULL AS STRING) "PLName",
		CAST(NULL AS STRING) "PMedicaidNumber",
		CAST(NULL AS STRING) "PAddressID",
		CAST(NULL AS STRING) "PAppAddressID",
		CAST(NULL AS STRING) "PAddressL1",
		CAST(NULL AS STRING) "PAddressL2",
		CAST(NULL AS STRING) "PCity",
		CAST(NULL AS STRING) "PAddressState",
		CAST(NULL AS STRING) "PZipCode",
		CAST(NULL AS STRING) "PCounty",
		CAST(NULL AS NUMBER) "Longitude",
		CAST(NULL AS NUMBER) "Latitude",
		CAST(NULL AS STRING) "PayerID",
		CAST(NULL AS STRING) "AppPayerID",
		CAST(NULL AS STRING) "Contract",
		CAST(NULL AS STRING) "PayerState",
		CAST(NULL AS TIMESTAMP) "BilledDate",
		CAST(NULL AS NUMBER) "BilledHours",
		CAST(NULL AS STRING) "Billed",
		CAST(FCS."Inservice start date" AS timestamp) AS "InserviceStartDate",
		CAST(FCS."Inservice end date" AS timestamp) AS "InserviceEndDate",
		CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) AS "AppCaregiverInserviceID",
		CAST(NULL AS TIMESTAMP) "PTOStartDate",
		CAST(NULL AS TIMESTAMP) "PTOEndDate",
		CAST(NULL AS STRING) "PTOVacationID",
		CAST(NULL AS STRING) "ServiceCodeID",
		CAST(NULL AS STRING) "AppServiceCodeID",
		CAST(NULL AS STRING) "RateType",
		CAST(NULL AS STRING) "ServiceCode",
		CAST(NULL AS TIMESTAMP) "LastUpdatedDate",
		CAST(NULL AS STRING) "LastUpdatedBy",
		CAST(NULL AS STRING) "P_PAdmissionID",
		CAST(NULL AS STRING) "P_PName",
		CAST(NULL AS STRING) "P_PFName",
		CAST(NULL AS STRING) "P_PLName",
		CAST(NULL AS STRING) "P_PMedicaidNumber",
		CAST(NULL AS STRING) "P_PStatus",
		CAST(NULL AS STRING) "P_PAddressID",
		CAST(NULL AS STRING) "P_PAppAddressID",
		CAST(NULL AS STRING) "P_PAddressL1",
		CAST(NULL AS STRING) "P_PAddressL2",
		CAST(NULL AS STRING) "P_PCity",
		CAST(NULL AS STRING) "P_PAddressState",
		CAST(NULL AS STRING) "P_PZipCode",
		CAST(NULL AS STRING) "P_PCounty",
		CAST(NULL AS STRING) "PA_PAdmissionID",
		CAST(NULL AS STRING) "PA_PName",
		CAST(NULL AS STRING) "PA_PFName",
		CAST(NULL AS STRING) "PA_PLName",
		CAST(NULL AS STRING) "PA_PMedicaidNumber",
		CAST(NULL AS STRING) "PA_PStatus",
		CAST(NULL AS STRING) "PA_PAddressID",
		CAST(NULL AS STRING) "PA_PAppAddressID",
		CAST(NULL AS STRING) "PA_PAddressL1",
		CAST(NULL AS STRING) "PA_PAddressL2",
		CAST(NULL AS STRING) "PA_PCity",
		CAST(NULL AS STRING) "PA_PAddressState",
		CAST(NULL AS STRING) "PA_PZipCode",
		CAST(NULL AS STRING) "PA_PCounty",
		CAST(NULL AS STRING) "ContractType"
		FROM 
	   ANALYTICS.BI.FACTCAREGIVERINSERVICE AS FCS
	   
	   INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = FCS."Caregiver Id" AND TRIM(CAR."SSN") IS NOT NULL AND TRIM(CAR."SSN")!=''''

	   INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = FCS."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
	   
	   LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = FCS."Office Id" AND DOF."Is Active" = TRUE

	   LEFT JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM1 ON CVM1."VisitID" = MD5(CONCAT(''I'', CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR))) AND CVM1."CONFLICTID" IS NOT NULL
	   
	   WHERE CAST(FCS."Inservice start date" AS DATE) BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND
	   
        DPR."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
	   ) AS V2 ON
       V1."VisitID" != V2."VisitID"
	   	AND
	   	V1.SSN = V2.SSN
		AND 
		(CAST(V1."VisitStartTime" AS timestamp) <= CAST(V2."InserviceEndDate" AS timestamp) AND CAST(V1."VisitEndTime" AS timestamp) >= CAST(V2."InserviceStartDate" AS timestamp))
		AND V1."ProviderID" IS NOT NULL
		AND
		V1."AppProviderID" IS NOT NULL
		AND V2."ProviderID" != V1."ProviderID"
		AND
		V2."AppCaregiverInserviceID" IS NOT NULL
		AND
		V1."AppCaregiverInserviceID" IS NULL
       WHERE 
       NOT EXISTS (
	        SELECT 1
	        FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
	        WHERE 
	        NVL(NULLIF(CVM."VisitID", ''''), ''9999999'') = NVL(NULLIF(V1."VisitID", ''''), ''9999999'')
	        AND
	        NVL(NULLIF(CVM."ConVisitID", ''''), ''9999999'') = NVL(NULLIF(V2."VisitID", ''''), ''9999999'')
			AND
			DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
	        
	    )`;

        var insertinservice = `
        INSERT INTO CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS ("CONFLICTID", "SSN", "ProviderID", "AppProviderID", "ProviderName", "VisitID", "AppVisitID", "ConProviderID", "ConAppProviderID", "ConProviderName", "ConVisitID", "ConAppVisitID", "VisitDate", "SchStartTime", "SchEndTime", "ConSchStartTime", "ConSchEndTime", "VisitStartTime", "VisitEndTime", "ConVisitStartTime", "ConVisitEndTime", "EVVStartTime", "EVVEndTime", "ConEVVStartTime", "ConEVVEndTime", "CaregiverID", "AppCaregiverID", "AideCode", "AideName", "AideSSN", "ConCaregiverID", "ConAppCaregiverID", "ConAideCode", "ConAideName", "ConAideSSN", "OfficeID", "AppOfficeID", "Office", "ConOfficeID", "ConAppOfficeID", "ConOffice", "PatientID", "AppPatientID", "PAdmissionID", "PName", "PAddressID", "PAppAddressID", "PAddressL1", "PAddressL2", "PCity", "PAddressState", "PZipCode", "PCounty", "PLongitude", "PLatitude", "ConPatientID", "ConAppPatientID", "ConPAdmissionID", "ConPName", "ConPAddressID", "ConPAppAddressID", "ConPAddressL1", "ConPAddressL2", "ConPCity", "ConPAddressState", "ConPZipCode", "ConPCounty", "ConPLongitude", "ConPLatitude", "PayerID", "AppPayerID", "Contract", "ConPayerID", "ConAppPayerID", "ConContract", "BilledDate", "ConBilledDate", "BilledHours", "ConBilledHours", "Billed", "ConBilled", "MinuteDiffBetweenSch", "DistanceMilesFromLatLng", "AverageMilesPerHour", "ETATravleMinutes", "InserviceStartDate", "InserviceEndDate", "ConInserviceStartDate", "ConInserviceEndDate", "PTOStartDate", "PTOEndDate", "ServiceCodeID", "AppServiceCodeID", "RateType", "ServiceCode", "ConServiceCodeID", "ConAppServiceCodeID", "ConRateType", "ConServiceCode", "SameSchTimeFlag", "SameVisitTimeFlag", "SchAndVisitTimeSameFlag", "SchOverAnotherSchTimeFlag", "VisitTimeOverAnotherVisitTimeFlag", "SchTimeOverVisitTimeFlag", "DistanceFlag", "InServiceFlag", "PTOFlag", "AideFName", "AideLName", "ConAideFName", "ConAideLName", "PFName", "PLName", "ConPFName", "ConPLName", "PMedicaidNumber", "ConPMedicaidNumber", "PayerState", "ConPayerState", "AgencyContact", "ConAgencyContact", "AgencyPhone", "ConAgencyPhone", "LastUpdatedBy", "ConLastUpdatedBy", "LastUpdatedDate", "ConLastUpdatedDate", "BilledRate", "TotalBilledAmount", "ConBilledRate", "ConTotalBilledAmount", "IsMissed", "MissedVisitReason", "EVVType", "ConIsMissed", "ConMissedVisitReason", "ConEVVType", "PStatus", "ConPStatus", "AideStatus", "ConAideStatus", "P_PatientID", "P_AppPatientID", "ConP_PatientID", "ConP_AppPatientID", "PA_PatientID", "PA_AppPatientID", "ConPA_PatientID", "ConPA_AppPatientID", "P_PAdmissionID", "P_PName", "P_PAddressID", "P_PAppAddressID", "P_PAddressL1", "P_PAddressL2", "P_PCity", "P_PAddressState", "P_PZipCode", "P_PCounty", "P_PFName", "P_PLName", "P_PMedicaidNumber", "ConP_PAdmissionID", "ConP_PName", "ConP_PAddressID", "ConP_PAppAddressID", "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity", "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty", "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber", "PA_PAdmissionID", "PA_PName", "PA_PAddressID", "PA_PAppAddressID", "PA_PAddressL1", "PA_PAddressL2", "PA_PCity", "PA_PAddressState", "PA_PZipCode", "PA_PCounty", "PA_PFName", "PA_PLName", "PA_PMedicaidNumber", "ConPA_PAdmissionID", "ConPA_PName", "ConPA_PAddressID", "ConPA_PAppAddressID", "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity", "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty", "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber", "ContractType", "ConContractType", "P_PStatus", "ConP_PStatus", "PA_PStatus", "ConPA_PStatus", "BillRateNonBilled", "ConBillRateNonBilled", "BillRateBoth", "ConBillRateBoth", "FederalTaxNumber", "ConFederalTaxNumber")
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
            CAST(NULL AS NUMBER) "MinuteDiffBetweenSch",
            CAST(NULL AS NUMBER) "DistanceMilesFromLatLng",
            CAST(NULL AS NUMBER) "AverageMilesPerHour",
            CAST(NULL AS NUMBER) "ETATravleMinutes",
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
            ''N'' AS "SameSchTimeFlag",
            ''N'' AS "SameVisitTimeFlag",
            ''N'' AS "SchAndVisitTimeSameFlag",
            ''N'' AS "SchOverAnotherSchTimeFlag",
            ''N'' AS "VisitTimeOverAnotherVisitTimeFlag",
            ''N'' AS "SchTimeOverVisitTimeFlag",
            ''N'' AS "DistanceFlag",
            ''Y'' AS "InServiceFlag",
            ''N'' AS "PTOFlag",
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
		
		SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", 
		CAST(NULL AS NUMBER) "BillRateNonBilled", 
		CAST(NULL AS NUMBER) "BillRateBoth", 
		TRIM(CAR."SSN") as "SSN", 
		CAST(NULL AS STRING) "PStatus", 
		CAST(NULL AS STRING) "AideStatus", 
		CAST(NULL AS STRING) "MissedVisitReason", 
		CAST(NULL AS BOOLEAN) "IsMissed", 
		CAST(NULL AS STRING) "EVVType", 
		CAST(NULL AS NUMBER) "BilledRate", 
		CAST(NULL AS NUMBER) "TotalBilledAmount",
		DPR."Provider Id" as "ProviderID", 
		DPR."Application Provider Id" as "AppProviderID", 
		DPR."Provider Name" AS "ProviderName", 
		CAST(NULL AS STRING) "AgencyContact", 
		DPR."Phone Number 1" AS "AgencyPhone", 
		DPR."Federal Tax Number" AS "FederalTaxNumber",
		MD5(CONCAT(''I'', CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR))) AS "VisitID", 
		CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) AS "AppVisitID",
		CAST(FCS."Inservice start date" AS DATE) AS "VisitDate", 
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
		CAST(NULL AS STRING) "PA_PatientID",
		CAST(NULL AS STRING) "PA_AppPatientID",
		CAST(NULL AS STRING) "P_PatientID",
		CAST(NULL AS STRING) "P_AppPatientID",
		CAST(NULL AS STRING) "PatientID",
		CAST(NULL AS STRING) "AppPatientID",
		CAST(NULL AS STRING) "PAdmissionID",
		CAST(NULL AS STRING) "PName",
		CAST(NULL AS STRING) "PFName",
		CAST(NULL AS STRING) "PLName",
		CAST(NULL AS STRING) "PMedicaidNumber",
		CAST(NULL AS STRING) "PAddressID",
		CAST(NULL AS STRING) "PAppAddressID",
		CAST(NULL AS STRING) "PAddressL1",
		CAST(NULL AS STRING) "PAddressL2",
		CAST(NULL AS STRING) "PCity",
		CAST(NULL AS STRING) "PAddressState",
		CAST(NULL AS STRING) "PZipCode",
		CAST(NULL AS STRING) "PCounty",
		CAST(NULL AS NUMBER) "Longitude",
		CAST(NULL AS NUMBER) "Latitude",
		CAST(NULL AS STRING) "PayerID",
		CAST(NULL AS STRING) "AppPayerID",
		CAST(NULL AS STRING) "Contract",
		CAST(NULL AS STRING) "PayerState",
		CAST(NULL AS TIMESTAMP) "BilledDate",
		CAST(NULL AS NUMBER) "BilledHours",
		CAST(NULL AS STRING) "Billed",
		CAST(FCS."Inservice start date" AS timestamp) AS "InserviceStartDate",
		CAST(FCS."Inservice end date" AS timestamp) AS "InserviceEndDate",
		CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) AS "AppCaregiverInserviceID",
		CAST(NULL AS TIMESTAMP) "PTOStartDate",
		CAST(NULL AS TIMESTAMP) "PTOEndDate",
		CAST(NULL AS STRING) "PTOVacationID",
		CAST(NULL AS STRING) "ServiceCodeID",
		CAST(NULL AS STRING) "AppServiceCodeID",
		CAST(NULL AS STRING) "RateType",
		CAST(NULL AS STRING) "ServiceCode",
		CAST(NULL AS TIMESTAMP) "LastUpdatedDate",
		CAST(NULL AS STRING) "LastUpdatedBy",
		CAST(NULL AS STRING) "P_PAdmissionID",
		CAST(NULL AS STRING) "P_PName",
		CAST(NULL AS STRING) "P_PFName",
		CAST(NULL AS STRING) "P_PLName",
		CAST(NULL AS STRING) "P_PMedicaidNumber",
		CAST(NULL AS STRING) "P_PStatus",
		CAST(NULL AS STRING) "P_PAddressID",
		CAST(NULL AS STRING) "P_PAppAddressID",
		CAST(NULL AS STRING) "P_PAddressL1",
		CAST(NULL AS STRING) "P_PAddressL2",
		CAST(NULL AS STRING) "P_PCity",
		CAST(NULL AS STRING) "P_PAddressState",
		CAST(NULL AS STRING) "P_PZipCode",
		CAST(NULL AS STRING) "P_PCounty",
		CAST(NULL AS STRING) "PA_PAdmissionID",
		CAST(NULL AS STRING) "PA_PName",
		CAST(NULL AS STRING) "PA_PFName",
		CAST(NULL AS STRING) "PA_PLName",
		CAST(NULL AS STRING) "PA_PMedicaidNumber",
		CAST(NULL AS STRING) "PA_PStatus",
		CAST(NULL AS STRING) "PA_PAddressID",
		CAST(NULL AS STRING) "PA_PAppAddressID",
		CAST(NULL AS STRING) "PA_PAddressL1",
		CAST(NULL AS STRING) "PA_PAddressL2",
		CAST(NULL AS STRING) "PA_PCity",
		CAST(NULL AS STRING) "PA_PAddressState",
		CAST(NULL AS STRING) "PA_PZipCode",
		CAST(NULL AS STRING) "PA_PCounty",
		CAST(NULL AS STRING) "ContractType"
		FROM 
	   ANALYTICS.BI.FACTCAREGIVERINSERVICE AS FCS
	   
	   INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = FCS."Caregiver Id" AND TRIM(CAR."SSN") IS NOT NULL AND TRIM(CAR."SSN")!=''''

	   INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = FCS."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
	   
	   LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = FCS."Office Id" AND DOF."Is Active" = TRUE

	   LEFT JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM1 ON CVM1."VisitID" = MD5(CONCAT(''I'', CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR))) AND CVM1."CONFLICTID" IS NOT NULL
	   
	   WHERE CAST(FCS."Inservice start date" AS DATE) BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND
	   
        DPR."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)) AS V1
       INNER JOIN
       (
			SELECT DISTINCT CAST(NULL AS NUMBER) "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") AS "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(FCS."Inservice start date" AS timestamp) AS "InserviceStartDate", CAST(FCS."Inservice end date" AS timestamp) AS "InserviceEndDate", CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) AS "AppCaregiverInserviceID", CAST(NULL AS TIMESTAMP) "PTOStartDate", CAST(NULL AS TIMESTAMP) "PTOEndDate", CAST(NULL AS STRING) "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1 INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = CR1."Caregiver Id" AND TRIM(CAR."SSN") IS NOT NULL AND TRIM(CAR."SSN")!=''''
            LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = CR1."Office Id" AND DOF."Is Active" = TRUE
			LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_P ON DPA_P."Patient Id" = CR1."Provider Patient Id"
			LEFT JOIN (
				SELECT DDD."Patient Address Id", DDD."Application Patient Address Id", DDD."Address Line 1", DDD."Address Line 2", DDD."City", DDD."Address State", DDD."Zip Code", DDD."County", DDD."Patient Id", DDD."Application Patient Id", DDD."Longitude" AS "Provider_Longitude", DDD."Latitude" AS "Provider_Latitude", ROW_NUMBER() OVER (PARTITION BY DDD."Patient Id" ORDER BY DDD."Application Created UTC Timestamp" DESC) AS rn
				FROM ANALYTICS.BI.DIMPATIENTADDRESS AS DDD
				WHERE DDD."Primary Address" = TRUE AND DDD."Address Type" LIKE ''%GPS%''
			) AS DPAD_P ON DPAD_P."Patient Id" = DPA_P."Patient Id" AND DPAD_P."RN" = 1		
			LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_PA ON DPA_PA."Patient Id" = CR1."Payer Patient Id"		 
			LEFT JOIN (
				SELECT DDD."Patient Address Id", DDD."Application Patient Address Id", DDD."Address Line 1", DDD."Address Line 2", DDD."City", DDD."Address State", DDD."Zip Code", DDD."County", DDD."Patient Id", DDD."Application Patient Id", ROW_NUMBER() OVER (PARTITION BY DDD."Patient Id" ORDER BY DDD."Application Created UTC Timestamp" DESC) AS rn
				FROM ANALYTICS.BI.DIMPATIENTADDRESS AS DDD
				WHERE DDD."Primary Address" = TRUE AND DDD."Address Type" LIKE ''%GPS%''
			) AS DPAD_PA ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" AND DPAD_PA."RN" = 1	   
			LEFT JOIN ANALYTICS.BI.DIMPAYER AS SPA ON SPA."Payer Id" = CR1."Payer Id" AND SPA."Is Active" = TRUE AND SPA."Is Demo" = FALSE
			LEFT JOIN ANALYTICS.BI.DIMCONTRACT AS DCON ON DCON."Contract Id" = CR1."Contract Id" AND DCON."Is Active" = TRUE
			INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
			
			LEFT JOIN ANALYTICS.BI.FACTCAREGIVERINSERVICE AS FCS ON FCS."Caregiver Id" = CR1."Caregiver Id" AND CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL AND (CAST(CR1."Visit Start Time" AS timestamp) <= CAST(FCS."Inservice end date" AS timestamp) AND CAST(CR1."Visit End Time" AS timestamp) >= CAST(FCS."Inservice start date" AS timestamp))

			LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
			LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
			WHERE CR1."Is Missed" = FALSE AND CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) IS NULL AND DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
            AND
			CR1."Visit Start Time" IS NOT NULL
			AND
			CR1."Visit End Time" IS NOT NULL
            AND
            CR1."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
		) AS V2 ON
       	V1."VisitID" != V2."VisitID"
	   	AND
	   	V1.SSN = V2.SSN
		AND 
		
		(CAST(V2."VisitStartTime" AS timestamp) <= CAST(V1."InserviceEndDate" AS timestamp) AND CAST(V2."VisitEndTime" AS timestamp) >= CAST(V1."InserviceStartDate" AS timestamp))
		 AND V2."ProviderID" IS NOT NULL
				AND V1."ProviderID" != V2."ProviderID"
				AND
				V1."AppCaregiverInserviceID" IS NOT NULL
				AND
				V2."AppCaregiverInserviceID" IS NULL
       WHERE  
       NOT EXISTS (
	        SELECT 1
	        FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
	        WHERE 
	        NVL(NULLIF(CVM."VisitID", ''''), ''9999999'') = NVL(NULLIF(V1."VisitID", ''''), ''9999999'')
	        AND
	        NVL(NULLIF(CVM."ConVisitID", ''''), ''9999999'') = NVL(NULLIF(V2."VisitID", ''''), ''9999999'')
			AND
			DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
	        
	    )
        `;
	   
	   var updatequery = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
		SET 
		    "ShVTSTTime" = COALESCE("VisitStartTime", "SchStartTime", "InserviceStartDate"),
		    "ShVTENTime" = COALESCE("VisitEndTime", "SchEndTime", "InserviceEndDate"),
		    "CShVTSTTime" = COALESCE("ConVisitStartTime", "ConSchStartTime", "ConInserviceStartDate"),
		    "CShVTENTime" = COALESCE("ConVisitEndTime", "ConSchEndTime", "ConInserviceEndDate") WHERE DATE("VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;

		   
		var updatequerya = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS SET "BilledRateMinute" = (CASE 
				WHEN "Billed" = ''yes'' AND "RateType" = ''Hourly'' AND "BillRateBoth" > 0 THEN "BillRateBoth"/60
				WHEN "Billed" = ''yes'' AND "RateType" = ''Daily'' AND "BillRateBoth" > 0 AND "BilledHours" > 0 THEN ("BillRateBoth"/"BilledHours")/60
				WHEN "Billed" = ''yes'' AND "RateType" = ''Visit'' AND "BillRateBoth" > 0 AND "BilledHours" > 0 THEN ("BillRateBoth"/"BilledHours")/60
				WHEN "Billed" != ''yes'' AND "RateType" = ''Hourly'' AND "BillRateBoth" > 0 THEN "BillRateBoth"/60
				WHEN "Billed" != ''yes'' AND "RateType" = ''Daily'' AND "BillRateBoth" > 0 AND "SchStartTime" IS NOT NULL AND "SchEndTime" IS NOT NULL AND "SchStartTime"!="SchEndTime" THEN ("BillRateBoth"/(TIMESTAMPDIFF(MINUTE, "SchStartTime", "SchEndTime")/60))/60
				WHEN "Billed" != ''yes'' AND "RateType" = ''Visit'' AND "BillRateBoth" > 0 AND "SchStartTime" IS NOT NULL AND "SchEndTime" IS NOT NULL AND "SchStartTime"!="SchEndTime" THEN ("BillRateBoth"/(TIMESTAMPDIFF(MINUTE, "SchStartTime", "SchEndTime")/60))/60
				ELSE 
				0
			END),
			"ConBilledRateMinute" = (CASE 
				WHEN "ConBilled" = ''yes'' AND "ConRateType" = ''Hourly'' AND "ConBillRateBoth" > 0 THEN "ConBillRateBoth"/60
				WHEN "ConBilled" = ''yes'' AND "ConRateType" = ''Daily'' AND "ConBillRateBoth" > 0 AND "ConBilledHours" > 0 THEN ("ConBillRateBoth"/"ConBilledHours")/60
				WHEN "ConBilled" = ''yes'' AND "ConRateType" = ''Visit'' AND "ConBillRateBoth" > 0 AND "ConBilledHours" > 0 THEN ("ConBillRateBoth"/"ConBilledHours")/60
				WHEN "ConBilled" != ''yes'' AND "ConRateType" = ''Hourly'' AND "ConBillRateBoth" > 0 THEN "ConBillRateBoth"/60
				WHEN "ConBilled" != ''yes'' AND "ConRateType" = ''Daily'' AND "ConBillRateBoth" > 0 AND "ConSchStartTime" IS NOT NULL AND "ConSchEndTime" IS NOT NULL AND "ConSchStartTime"!="ConSchEndTime" THEN ("ConBillRateBoth"/(TIMESTAMPDIFF(MINUTE, "ConSchStartTime", "ConSchEndTime")/60))/60
				WHEN "ConBilled" != ''yes'' AND "ConRateType" = ''Visit'' AND "ConBillRateBoth" > 0 AND "ConSchStartTime" IS NOT NULL AND "ConSchEndTime" IS NOT NULL AND "ConSchStartTime"!="ConSchEndTime" THEN ("ConBillRateBoth"/(TIMESTAMPDIFF(MINUTE, "ConSchStartTime", "ConSchEndTime")/60))/60
				ELSE 
				0
			END) WHERE DATE("VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
		//var updateflag = `UPDATE CONFLICTREPORT.PUBLIC.SETTINGS SET "InsertCronFlag" = 1`;
		
		var UUIDSASSI = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS SET "ReverseUUID" = CONCAT(LEAST(CONCAT("VisitID", ''~'', "AppVisitID"), CONCAT("ConVisitID", ''~'', "ConAppVisitID")), ''_'', GREATEST(CONCAT("VisitID", ''~'', "AppVisitID"), CONCAT("ConVisitID", ''~'', "ConAppVisitID"))) WHERE "ReverseUUID" IS NULL AND "VisitID" IS NOT NULL AND "AppVisitID" IS NOT NULL AND "ConVisitID" IS NOT NULL AND "ConAppVisitID" IS NOT NULL AND DATE("VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
		snowflake.execute({sqlText: insertinservice});
		snowflake.execute({sqlText: sql_query_reverse_inservice});
		snowflake.execute({sqlText: updatequery});
		snowflake.execute({sqlText: updatequerya});

		//snowflake.execute({sqlText: updateflag});
		snowflake.execute({sqlText: UUIDSASSI});
	
		return "Procedure executed successfully.";
  	} catch (err) {
		var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
	  // If an error occurs, capture it and raise it with a custom message
	  throw "ERROR: " + err.message;  // Returns the error message to the caller
  	}
';