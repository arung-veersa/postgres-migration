CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.COPY_DATA_FROM_CONFLICTVISITMAPS_TO_TEMP()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '	

	var table_command2 = `INSERT INTO CONFLICTREPORT."PUBLIC".PAYER_PROVIDER_REMINDERS
	("PayerID", "AppPayerID", "Contract", "ProviderID", "AppProviderID", "ProviderName", "CreatedDateTime", "NumberOfDays")
	SELECT DISTINCT DPP."Payer Id" AS "PayerID", DPP."Application Payer Id" AS "AppPayerID", DPA."Payer Name" AS "Contract", DPP."Provider Id" AS "ProviderID", DPP."Application Provider Id" AS "AppProviderID", DP."Provider Name" AS "ProviderName", CURRENT_TIMESTAMP AS "CreatedDateTime", CAST(NULL AS NUMBER) "NumberOfDays"
	FROM ANALYTICS.BI.DIMPROVIDER AS DP
	INNER JOIN ANALYTICS.BI.DIMPAYERPROVIDER AS DPP ON DPP."Provider Id" = DP."Provider Id"
	INNER JOIN ANALYTICS.BI.DIMPAYER AS DPA ON DPA."Payer Id" = DPP."Payer Id"
	WHERE NOT EXISTS (
	    SELECT 1 
	    FROM CONFLICTREPORT."PUBLIC".PAYER_PROVIDER_REMINDERS AS PPR_N 
	    WHERE PPR_N."PayerID" = DPP."Payer Id"
	    AND PPR_N."ProviderID" = DPP."Provider Id"
	)`;
	
	var table_command3 = `UPDATE CONFLICTREPORT."PUBLIC".PAYER_PROVIDER_REMINDERS AS PPR
	SET 
	    PPR."Contract" = DPA."Payer Name",
	    PPR."ProviderName" = DP."Provider Name"
	FROM ANALYTICS.BI.DIMPROVIDER AS DP
	INNER JOIN ANALYTICS.BI.DIMPAYERPROVIDER AS DPP 
	    ON DPP."Provider Id" = DP."Provider Id"
	INNER JOIN ANALYTICS.BI.DIMPAYER AS DPA 
	    ON DPA."Payer Id" = DPP."Payer Id"
	WHERE 
	    PPR."PayerID" = DPP."Payer Id"
	    AND PPR."ProviderID" = DPP."Provider Id"`;
	var truncate_query = `
    TRUNCATE TABLE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS_TEMP`;
   
  var sql_query = `INSERT INTO CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS_TEMP (ID, CONFLICTID, SSN, "ProviderID", "AppProviderID", "ProviderName", "VisitID", "AppVisitID", "ConProviderID", "ConAppProviderID", "ConProviderName", "ConVisitID", "ConAppVisitID", "VisitDate", "SchStartTime", "SchEndTime", "ConSchStartTime", "ConSchEndTime", "VisitStartTime", "VisitEndTime", "ConVisitStartTime", "ConVisitEndTime", "EVVStartTime", "EVVEndTime", "ConEVVStartTime", "ConEVVEndTime", "CaregiverID", "AppCaregiverID", "AideCode", "AideName", "AideSSN", "ConCaregiverID", "ConAppCaregiverID", "ConAideCode", "ConAideName", "ConAideSSN", "OfficeID", "AppOfficeID", "Office", "ConOfficeID", "ConAppOfficeID", "ConOffice", "PatientID", "AppPatientID", "PAdmissionID", "PName", "PAddressID", "PAppAddressID", "PAddressL1", "PAddressL2", "PCity", "PAddressState", "PZipCode", "PCounty", "PLongitude", "PLatitude", "ConPatientID", "ConAppPatientID", "ConPAdmissionID", "ConPName", "ConPAddressID", "ConPAppAddressID", "ConPAddressL1", "ConPAddressL2", "ConPCity", "ConPAddressState", "ConPZipCode", "ConPCounty", "ConPLongitude", "ConPLatitude", "PayerID", "AppPayerID", "Contract", "ConPayerID", "ConAppPayerID", "ConContract", "BilledDate", "ConBilledDate", "BilledHours", "ConBilledHours", "Billed", "ConBilled", "MinuteDiffBetweenSch", "DistanceMilesFromLatLng", "AverageMilesPerHour", "ETATravleMinutes", "InserviceStartDate", "InserviceEndDate", "PTOStartDate", "PTOEndDate", "ConInserviceStartDate", "ConInserviceEndDate", "ConPTOStartDate", "ConPTOEndDate", "ServiceCodeID", "AppServiceCodeID", "RateType", "ServiceCode", "ConServiceCodeID", "ConAppServiceCodeID", "ConRateType", "ConServiceCode", "SameSchTimeFlag", "SameVisitTimeFlag", "SchAndVisitTimeSameFlag", "SchOverAnotherSchTimeFlag", "VisitTimeOverAnotherVisitTimeFlag", "SchTimeOverVisitTimeFlag", "DistanceFlag", "InServiceFlag", "PTOFlag", "StatusFlag", "ConStatusFlag", "AideFName", "AideLName", "ConAideFName", "ConAideLName", "PFName", "PLName", "ConPFName", "ConPLName", "PMedicaidNumber", "ConPMedicaidNumber", "PayerState", "ConPayerState", "AgencyContact", "ConAgencyContact", "AgencyPhone", "ConAgencyPhone", "LastUpdatedBy", "ConLastUpdatedBy", "LastUpdatedDate", "ConLastUpdatedDate", "BilledRate", "TotalBilledAmount", "ConBilledRate", "ConTotalBilledAmount", "IsMissed", "MissedVisitReason", "EVVType", "ConIsMissed", "ConMissedVisitReason", "ConEVVType", "PStatus", "ConPStatus", "AideStatus", "ConAideStatus", "P_PatientID", "P_AppPatientID", "ConP_PatientID", "ConP_AppPatientID", "PA_PatientID", "PA_AppPatientID", "ConPA_PatientID", "ConPA_AppPatientID", "P_PAdmissionID", "P_PName", "P_PAddressID", "P_PAppAddressID", "P_PAddressL1", "P_PAddressL2", "P_PCity", "P_PAddressState", "P_PZipCode", "P_PCounty", "P_PFName", "P_PLName", "P_PMedicaidNumber", "ConP_PAdmissionID", "ConP_PName", "ConP_PAddressID", "ConP_PAppAddressID", "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity", "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty", "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber", "PA_PAdmissionID", "PA_PName", "PA_PAddressID", "PA_PAppAddressID", "PA_PAddressL1", "PA_PAddressL2", "PA_PCity", "PA_PAddressState", "PA_PZipCode", "PA_PCounty", "PA_PFName", "PA_PLName", "PA_PMedicaidNumber", "ConPA_PAdmissionID", "ConPA_PName", "ConPA_PAddressID", "ConPA_PAppAddressID", "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity", "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty", "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber", "ContractType", "ConContractType", "CreatedDate", "ConNoResponseFlag", "ConNoResponseReasonID", "ConNoResponseTitle", "ConNoResponseNotes", "P_PStatus", "ConP_PStatus", "PA_PStatus", "ConPA_PStatus", "BillRateNonBilled", "ConBillRateNonBilled", "BillRateBoth", "ConBillRateBoth", "FederalTaxNumber", "ConFederalTaxNumber", "FlagForReview", "FlagForReviewDate", "ConFlagForReview", "ConFlagForReviewDate")
	SELECT CVM.ID, CVM.CONFLICTID, CVM.SSN, CVM."ProviderID", CVM."AppProviderID", CVM."ProviderName", CVM."VisitID", CVM."AppVisitID", CVM."ConProviderID", CVM."ConAppProviderID", CVM."ConProviderName", CVM."ConVisitID", CVM."ConAppVisitID", CVM."VisitDate", CVM."SchStartTime", CVM."SchEndTime", CVM."ConSchStartTime", CVM."ConSchEndTime", CVM."VisitStartTime", CVM."VisitEndTime", CVM."ConVisitStartTime", CVM."ConVisitEndTime", CVM."EVVStartTime", CVM."EVVEndTime", CVM."ConEVVStartTime", CVM."ConEVVEndTime", CVM."CaregiverID", CVM."AppCaregiverID", CVM."AideCode", CVM."AideName", CVM."AideSSN", CVM."ConCaregiverID", CVM."ConAppCaregiverID", CVM."ConAideCode", CVM."ConAideName", CVM."ConAideSSN", CVM."OfficeID", CVM."AppOfficeID", CVM."Office", CVM."ConOfficeID", CVM."ConAppOfficeID", CVM."ConOffice", CVM."PatientID", CVM."AppPatientID", CVM."PAdmissionID", CVM."PName", CVM."PAddressID", CVM."PAppAddressID", CVM."PAddressL1", CVM."PAddressL2", CVM."PCity", CVM."PAddressState", CVM."PZipCode", CVM."PCounty", CVM."PLongitude", CVM."PLatitude", CVM."ConPatientID", CVM."ConAppPatientID", CVM."ConPAdmissionID", CVM."ConPName", CVM."ConPAddressID", CVM."ConPAppAddressID", CVM."ConPAddressL1", CVM."ConPAddressL2", CVM."ConPCity", CVM."ConPAddressState", CVM."ConPZipCode", CVM."ConPCounty", CVM."ConPLongitude", CVM."ConPLatitude", CVM."PayerID", CVM."AppPayerID", CVM."Contract", CVM."ConPayerID", CVM."ConAppPayerID", CVM."ConContract", CVM."BilledDate", CVM."ConBilledDate", CVM."BilledHours", CVM."ConBilledHours", CVM."Billed", CVM."ConBilled", CVM."MinuteDiffBetweenSch", CVM."DistanceMilesFromLatLng", CVM."AverageMilesPerHour", CVM."ETATravleMinutes", CVM."InserviceStartDate", CVM."InserviceEndDate", CVM."PTOStartDate", CVM."PTOEndDate", CVM."ConInserviceStartDate", CVM."ConInserviceEndDate", CVM."ConPTOStartDate", CVM."ConPTOEndDate", CVM."ServiceCodeID", CVM."AppServiceCodeID", CVM."RateType", CVM."ServiceCode", CVM."ConServiceCodeID", CVM."ConAppServiceCodeID", CVM."ConRateType", CVM."ConServiceCode", CVM."SameSchTimeFlag", CVM."SameVisitTimeFlag", CVM."SchAndVisitTimeSameFlag", CVM."SchOverAnotherSchTimeFlag", CVM."VisitTimeOverAnotherVisitTimeFlag", CVM."SchTimeOverVisitTimeFlag", CVM."DistanceFlag", CVM."InServiceFlag", CVM."PTOFlag", C."StatusFlag", CVM."StatusFlag" AS "ConStatusFlag", CVM."AideFName", CVM."AideLName", CVM."ConAideFName", CVM."ConAideLName", CVM."PFName", CVM."PLName", CVM."ConPFName", CVM."ConPLName", CVM."PMedicaidNumber", CVM."ConPMedicaidNumber", CVM."PayerState", CVM."ConPayerState", CVM."AgencyContact", CVM."ConAgencyContact", CVM."AgencyPhone", CVM."ConAgencyPhone", CVM."LastUpdatedBy", CVM."ConLastUpdatedBy", CVM."LastUpdatedDate", CVM."ConLastUpdatedDate", CVM."BilledRate", CVM."TotalBilledAmount", CVM."ConBilledRate", CVM."ConTotalBilledAmount", CVM."IsMissed", CVM."MissedVisitReason", CVM."EVVType", CVM."ConIsMissed", CVM."ConMissedVisitReason", CVM."ConEVVType", CVM."PStatus", CVM."ConPStatus", CVM."AideStatus", CVM."ConAideStatus", CVM."P_PatientID", CVM."P_AppPatientID", CVM."ConP_PatientID", CVM."ConP_AppPatientID", CVM."PA_PatientID", CVM."PA_AppPatientID", CVM."ConPA_PatientID", CVM."ConPA_AppPatientID", CVM."P_PAdmissionID", CVM."P_PName", CVM."P_PAddressID", CVM."P_PAppAddressID", CVM."P_PAddressL1", CVM."P_PAddressL2", CVM."P_PCity", CVM."P_PAddressState", CVM."P_PZipCode", CVM."P_PCounty", CVM."P_PFName", CVM."P_PLName", CVM."P_PMedicaidNumber", CVM."ConP_PAdmissionID", CVM."ConP_PName", CVM."ConP_PAddressID", CVM."ConP_PAppAddressID", CVM."ConP_PAddressL1", CVM."ConP_PAddressL2", CVM."ConP_PCity", CVM."ConP_PAddressState", CVM."ConP_PZipCode", CVM."ConP_PCounty", CVM."ConP_PFName", CVM."ConP_PLName", CVM."ConP_PMedicaidNumber", CVM."PA_PAdmissionID", CVM."PA_PName", CVM."PA_PAddressID", CVM."PA_PAppAddressID", CVM."PA_PAddressL1", CVM."PA_PAddressL2", CVM."PA_PCity", CVM."PA_PAddressState", CVM."PA_PZipCode", CVM."PA_PCounty", CVM."PA_PFName", CVM."PA_PLName", CVM."PA_PMedicaidNumber", CVM."ConPA_PAdmissionID", CVM."ConPA_PName", CVM."ConPA_PAddressID", CVM."ConPA_PAppAddressID", CVM."ConPA_PAddressL1", CVM."ConPA_PAddressL2", CVM."ConPA_PCity", CVM."ConPA_PAddressState", CVM."ConPA_PZipCode", CVM."ConPA_PCounty", CVM."ConPA_PFName", CVM."ConPA_PLName", CVM."ConPA_PMedicaidNumber", CVM."ContractType", CVM."ConContractType", CURRENT_TIMESTAMP(), CVM."ConNoResponseFlag", CVM."ConNoResponseReasonID", CVM."ConNoResponseTitle", CVM."ConNoResponseNotes", CVM."P_PStatus", CVM."ConP_PStatus", CVM."PA_PStatus", CVM."ConPA_PStatus", CVM."BillRateNonBilled", CVM."ConBillRateNonBilled", CVM."BillRateBoth", CVM."ConBillRateBoth", CVM."FederalTaxNumber", CVM."ConFederalTaxNumber", C."FlagForReview", C."FlagForReviewDate", CVM."FlagForReview" AS "ConFlagForReview", CVM."FlagForReviewDate" AS "ConFlagForReviewDate" FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTS AS C ON C.CONFLICTID = CVM.CONFLICTID
WHERE DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;

  try {
  	
  	var truncate_stmt = snowflake.createStatement({sqlText: truncate_query});
    var truncate_res = truncate_stmt.execute();
  
    var stmt = snowflake.createStatement({sqlText: sql_query});
    var res = stmt.execute();
	
	
	snowflake.execute({sqlText: table_command2});
	snowflake.execute({sqlText: table_command3});

	var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 1`;
		snowflake.execute({ sqlText: updatesetting });

    return "CONFLICTVISITMAPS_TEMP table truncated and data copied from CONFLICTVISITMAPS to TEMP table successfully.";
  } catch (err) {
	var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
    throw "ERROR: " + err;
  }
';