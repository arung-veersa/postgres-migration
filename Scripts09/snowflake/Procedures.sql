CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.ASSIGN_GROUP_IDS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {

	var update_query = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM SET CVM."TempGroupID" = NULL WHERE CVM."TempGroupID" IS NOT NULL`;
	var assign_temp_ids = `MERGE INTO CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS c
		USING (
		    WITH GroupedLinks AS (
		        SELECT
		            "VisitID",
		            "ConVisitID",
		            MIN("TempGroupID") OVER (PARTITION BY "VisitID") AS "MinTempGroupID"
		        FROM (
		            SELECT
		                CAST("VisitID" AS VARCHAR) AS "VisitID",
		                CAST("ConVisitID" AS VARCHAR) AS "ConVisitID",
		                CAST("VisitID" AS VARCHAR) AS "TempGroupID"
		            FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS            
		            UNION
		            SELECT
		                CAST(v."VisitID" AS VARCHAR) AS "VisitID",
		                CAST(v."ConVisitID" AS VARCHAR) AS "ConVisitID",
		                CASE
		                    WHEN lg."TempGroupID" IS NULL THEN CAST(v."VisitID" AS VARCHAR)
		                    WHEN CAST(lg."TempGroupID" AS VARCHAR) < CAST(v."VisitID" AS VARCHAR) THEN CAST(lg."TempGroupID" AS VARCHAR)
		                    ELSE CAST(v."VisitID" AS VARCHAR)
		                END AS "TempGroupID"
		            FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS v
		            JOIN (
		                SELECT
		                    CAST("VisitID" AS VARCHAR) AS "VisitID",
		                    CAST("ConVisitID" AS VARCHAR) AS "ConVisitID",
		                    CAST("VisitID" AS VARCHAR) AS "TempGroupID"
		                FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS
		                
		                UNION
		                SELECT
		                    CAST(v."VisitID" AS VARCHAR) AS "VisitID",
		                    CAST(v."ConVisitID" AS VARCHAR) AS "ConVisitID",
		                    CASE
		                        WHEN lg."TempGroupID" IS NULL THEN CAST(v."VisitID" AS VARCHAR)
		                        WHEN CAST(lg."TempGroupID" AS VARCHAR) < CAST(v."VisitID" AS VARCHAR) THEN CAST(lg."TempGroupID" AS VARCHAR)
		                        ELSE CAST(v."VisitID" AS VARCHAR)
		                    END AS "TempGroupID"
		                FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS v
		                JOIN (
		                    SELECT
		                        CAST("VisitID" AS VARCHAR) AS "VisitID",
		                        CAST("ConVisitID" AS VARCHAR) AS "ConVisitID",
		                        CAST("VisitID" AS VARCHAR) AS "TempGroupID"
		                    FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS
		                    
		                    UNION
		                    SELECT
		                        CAST(v."VisitID" AS VARCHAR) AS "VisitID",
		                        CAST(v."ConVisitID" AS VARCHAR) AS "ConVisitID",
		                        CASE
		                            WHEN lg."TempGroupID" IS NULL THEN CAST(v."VisitID" AS VARCHAR)
		                            WHEN CAST(lg."TempGroupID" AS VARCHAR) < CAST(v."VisitID" AS VARCHAR) THEN CAST(lg."TempGroupID" AS VARCHAR)
		                            ELSE CAST(v."VisitID" AS VARCHAR)
		                        END AS "TempGroupID"
		                    FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS v
		                    JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS lg 
		                    ON CAST(v."VisitID" AS VARCHAR) = CAST(lg."ConVisitID" AS VARCHAR) 
		                    OR CAST(v."ConVisitID" AS VARCHAR) = CAST(lg."VisitID" AS VARCHAR)
		                ) lg 
		                ON CAST(v."VisitID" AS VARCHAR) = CAST(lg."ConVisitID" AS VARCHAR) 
		                OR CAST(v."ConVisitID" AS VARCHAR) = CAST(lg."VisitID" AS VARCHAR)
		            ) lg 
		            ON CAST(v."VisitID" AS VARCHAR) = CAST(lg."ConVisitID" AS VARCHAR) 
		            OR CAST(v."ConVisitID" AS VARCHAR) = CAST(lg."VisitID" AS VARCHAR)
		        ) LinkGroups
		    ),
		    RankedLinks AS (
		        SELECT
		            "VisitID",
		            "ConVisitID",
		            DENSE_RANK() OVER (ORDER BY "MinTempGroupID") AS "NewTempGroupID"
		        FROM GroupedLinks
		    )
		    SELECT DISTINCT
		        "VisitID",
		        "ConVisitID",
		        "NewTempGroupID"
		    FROM RankedLinks
		) AS src
		ON c."VisitID" = src."VisitID" AND c."ConVisitID" = src."ConVisitID"
		WHEN MATCHED AND c."TempGroupID" IS NULL THEN
    UPDATE SET c."TempGroupID" = src."NewTempGroupID"`;
   
    var assign_old_groupids = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS A
		SET 
		    A.G_CRDATEUNIQUE = T."NewG_CRDATEUNIQUE", 
		    A."GroupID" = T."NewGroupID"
		FROM (
		    SELECT 
		        "TempGroupID" AS "NewTempGroupID", 
		        "GroupID" AS "NewGroupID",
		        "G_CRDATEUNIQUE" AS "NewG_CRDATEUNIQUE"
		    FROM 
		        CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS 
		    WHERE 
		        "TempGroupID" IN (
		            SELECT "TempGroupID" 
		            FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS 
		            WHERE "GroupID" IS NOT NULL 
		            GROUP BY "TempGroupID"
		        ) 
		        AND "GroupID" IS NOT NULL
		) AS T
		WHERE T."NewTempGroupID" = A."TempGroupID"`;
	
	var assigngroupnew = `MERGE INTO CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS c
		USING (
		    WITH GroupedLinks AS (
		        SELECT
		            "VisitID",
		            "ConVisitID",
		            MIN("GroupID") OVER (PARTITION BY "VisitID") AS "MinGroupID"
		        FROM (
		            SELECT
		                CAST("VisitID" AS VARCHAR) AS "VisitID",
		                CAST("ConVisitID" AS VARCHAR) AS "ConVisitID",
		                CAST("VisitID" AS VARCHAR) AS "GroupID"
		            FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS            
		            UNION
		            SELECT
		                CAST(v."VisitID" AS VARCHAR) AS "VisitID",
		                CAST(v."ConVisitID" AS VARCHAR) AS "ConVisitID",
		                CASE
		                    WHEN lg."GroupID" IS NULL THEN CAST(v."VisitID" AS VARCHAR)
		                    WHEN CAST(lg."GroupID" AS VARCHAR) < CAST(v."VisitID" AS VARCHAR) THEN CAST(lg."GroupID" AS VARCHAR)
		                    ELSE CAST(v."VisitID" AS VARCHAR)
		                END AS "GroupID"
		            FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS v
		            JOIN (
		                SELECT
		                    CAST("VisitID" AS VARCHAR) AS "VisitID",
		                    CAST("ConVisitID" AS VARCHAR) AS "ConVisitID",
		                    CAST("VisitID" AS VARCHAR) AS "GroupID"
		                FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS
		                
		                UNION
		                SELECT
		                    CAST(v."VisitID" AS VARCHAR) AS "VisitID",
		                    CAST(v."ConVisitID" AS VARCHAR) AS "ConVisitID",
		                    CASE
		                        WHEN lg."GroupID" IS NULL THEN CAST(v."VisitID" AS VARCHAR)
		                        WHEN CAST(lg."GroupID" AS VARCHAR) < CAST(v."VisitID" AS VARCHAR) THEN CAST(lg."GroupID" AS VARCHAR)
		                        ELSE CAST(v."VisitID" AS VARCHAR)
		                    END AS "GroupID"
		                FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS v
		                JOIN (
		                    SELECT
		                        CAST("VisitID" AS VARCHAR) AS "VisitID",
		                        CAST("ConVisitID" AS VARCHAR) AS "ConVisitID",
		                        CAST("VisitID" AS VARCHAR) AS "GroupID"
		                    FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS
		                    
		                    UNION
		                    SELECT
		                        CAST(v."VisitID" AS VARCHAR) AS "VisitID",
		                        CAST(v."ConVisitID" AS VARCHAR) AS "ConVisitID",
		                        CASE
		                            WHEN lg."GroupID" IS NULL THEN CAST(v."VisitID" AS VARCHAR)
		                            WHEN CAST(lg."GroupID" AS VARCHAR) < CAST(v."VisitID" AS VARCHAR) THEN CAST(lg."GroupID" AS VARCHAR)
		                            ELSE CAST(v."VisitID" AS VARCHAR)
		                        END AS "GroupID"
		                    FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS v
		                    JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS lg 
		                    ON CAST(v."VisitID" AS VARCHAR) = CAST(lg."ConVisitID" AS VARCHAR) 
		                    OR CAST(v."ConVisitID" AS VARCHAR) = CAST(lg."VisitID" AS VARCHAR)
		                ) lg 
		                ON CAST(v."VisitID" AS VARCHAR) = CAST(lg."ConVisitID" AS VARCHAR) 
		                OR CAST(v."ConVisitID" AS VARCHAR) = CAST(lg."VisitID" AS VARCHAR)
		            ) lg 
		            ON CAST(v."VisitID" AS VARCHAR) = CAST(lg."ConVisitID" AS VARCHAR) 
		            OR CAST(v."ConVisitID" AS VARCHAR) = CAST(lg."VisitID" AS VARCHAR)
		        ) LinkGroups
		    ),
		    RankedLinks AS (
		        SELECT
		            "VisitID",
		            "ConVisitID",
		            DENSE_RANK() OVER (ORDER BY "MinGroupID") + COALESCE((SELECT MAX("GroupID") FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS), 0) AS "NewGroupID"
		        FROM GroupedLinks
		    )
		    SELECT DISTINCT
		        "VisitID",
		        "ConVisitID",
		        "NewGroupID"
		    FROM RankedLinks
		) AS src
		ON c."VisitID" = src."VisitID" AND c."ConVisitID" = src."ConVisitID"
		WHEN MATCHED AND c."GroupID" IS NULL THEN
		    UPDATE SET c."GroupID" = src."NewGroupID", c."G_CRDATEUNIQUE" = CURRENT_TIMESTAMP`;

	

		var table_command4 = `INSERT INTO CONFLICTREPORT."PUBLIC".NOTIFICATIONS (CONFLICTID, "ProviderID", "AppProviderID", "NotificationType", "CreatedDate", "CreatedDateTime", "Contract")
		   SELECT DISTINCT C.CONFLICTID, CVM."ProviderID", CVM."AppProviderID", ''From Payer'' AS "NotificationType", CURRENT_DATE AS "CreatedDate", CURRENT_TIMESTAMP AS "CreatedDateTime", PPR."Contract" FROM CONFLICTREPORT."PUBLIC".PAYER_PROVIDER_REMINDERS AS PPR
		   INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CONCAT(CVM."ProviderID", ''~'', CVM."AppProviderID") = CONCAT(PPR."ProviderID", ''~'', PPR."AppProviderID") AND CONCAT(CVM."PayerID" , ''~'', CVM."AppPayerID") = CONCAT(PPR."PayerID", ''~'', PPR."AppPayerID")
		   INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTS AS C ON CVM.CONFLICTID = C.CONFLICTID AND C."StatusFlag" = ''U'' AND DATE(DATEADD(day, PPR."NumberOfDays", C.RECORDEDDATETIME)) = CURRENT_DATE
		   WHERE PPR."NumberOfDays" IS NOT NULL AND PPR."NumberOfDays" > 0 AND NOT EXISTS (
			    SELECT 1 
			    FROM CONFLICTREPORT.PUBLIC.NOTIFICATIONS AS N 
			    WHERE N.CONFLICTID = C.CONFLICTID
			    AND N."ProviderID" = CVM."ProviderID"
			    AND N."AppProviderID" = CVM."AppProviderID" AND N."NotificationType" = ''From Payer''
			)`;

		var table_command5 = `INSERT INTO CONFLICTREPORT."PUBLIC".CONFLICT_COMMU_INTERS ("ReverseUUID", "Description", "CommentType", "communications_type", "created_at", "updated_at", "created_by", "updated_by", "created_by_name", "updated_by_name")
			SELECT DISTINCT C.CONFLICTID AS "ReverseUUID", CONCAT(TO_CHAR(CURRENT_DATE, ''MM/DD/YYYY''), '' – Payer '', PPR."Contract", '' has sent you a notice that ConflictID: '', C.CONFLICTID, '' is still unresolved. Please work on resolving as soon as possible.'') AS "Description", ''1'' AS "CommentType", ''2'' AS "communications_type", CURRENT_TIMESTAMP AS "created_at", CURRENT_TIMESTAMP AS "updated_at", ''99999'' AS "created_by", ''99999'' AS "updated_by", PPR."Contract" AS "created_by_name", PPR."Contract" AS "updated_by_name" FROM CONFLICTREPORT."PUBLIC".PAYER_PROVIDER_REMINDERS AS PPR
			INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CONCAT(CVM."ProviderID", ''~'', CVM."AppProviderID") = CONCAT(PPR."ProviderID", ''~'', PPR."AppProviderID") AND CONCAT(CVM."PayerID" , ''~'', CVM."AppPayerID") = CONCAT(PPR."PayerID", ''~'', PPR."AppPayerID")
			INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTS AS C ON CVM.CONFLICTID = C.CONFLICTID AND C."StatusFlag" = ''U'' AND DATE(DATEADD(day, PPR."NumberOfDays", C.RECORDEDDATETIME)) = CURRENT_DATE
			WHERE PPR."NumberOfDays" IS NOT NULL AND PPR."NumberOfDays" > 0 AND NOT EXISTS (
			    SELECT 1 
			    FROM CONFLICTREPORT."PUBLIC".CONFLICT_COMMU_INTERS AS N 
			    WHERE N."ReverseUUID" = C.CONFLICTID
			    AND DATE(N."created_at") = CURRENT_DATE
			    AND N."created_by" = ''99999''
			)`;
		//var updateflag = `UPDATE CONFLICTREPORT.PUBLIC.SETTINGS SET "GroupIDFlag" = 1`;
		//var finalupdate = `UPDATE CONFLICTREPORT.PUBLIC.SETTINGS SET "LastLoadDate" = CURRENT_TIMESTAMP WHERE "UpdateCronFlag" IS NOT NULL AND "InsertCronFlag" IS NOT NULL AND "ConflictIDFlag" IS NOT NULL AND "GroupIDFlag" IS NOT NULL AND "VisitHistoryFlag" IS NOT NULL`;
   
		snowflake.execute({sqlText: update_query});
		snowflake.execute({sqlText: assign_temp_ids});
		snowflake.execute({sqlText: assign_old_groupids});
		snowflake.execute({sqlText: assigngroupnew});
		snowflake.execute({sqlText: table_command4});
		snowflake.execute({sqlText: table_command5});
		//snowflake.execute({sqlText: updateflag});
		//snowflake.execute({sqlText: finalupdate});
	    
	    return "Group id assigned successfully.";
	} catch (err) {
		var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
  		// If an error occurs, capture it and raise it with a custom message
  		throw "ERROR: " + err.message;  // Returns the error message to the caller
	}
';

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

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_CONFLICTS_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {
	    // Create table if not exists
	    var tableName = "CONFLICTS";
	    var table_command = `
	   CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.CONFLICTS (
	  	CONFLICTID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
		RECORDEDDATETIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		"StatusFlag" VARCHAR(5) DEFAULT ''U'',
		"NoResponseFlag" VARCHAR(10) DEFAULT NULL, 
		"NoResponseReasonID" NUMBER(38) DEFAULT NULL,
		"NoResponseTitle" VARCHAR(500) DEFAULT NULL,
		"NoResponseNotes" VARCHAR(500) DEFAULT NULL,
		"NoResponseDate" TIMESTAMP DEFAULT NULL,
		"ResolveDate" TIMESTAMP DEFAULT NULL,
		"ResolvedBy" VARCHAR(200) DEFAULT NULL,
		"CreatedDate" TIMESTAMP DEFAULT NULL,
		"FlagForReview" VARCHAR(5) DEFAULT NULL,
		"FlagForReviewDate" TIMESTAMP DEFAULT NULL,
		PRIMARY KEY (CONFLICTID))`;
	    snowflake.execute({sqlText: table_command});
	    
	    return "Table " + tableName + " created or already exists.";
	} catch (err) {
	    return "Error: " + err.message;
	}
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_CONFLICTVISITMAPS_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var tableName = "CONFLICTVISITMAPS";
  var sql_command = `
      CREATE TABLE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS (
        ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        CONFLICTID NUMBER(38,0) DEFAULT NULL,
        "GroupID" NUMBER(38,0) DEFAULT NULL,
        "SSN" VARCHAR(50) DEFAULT NULL,
        "ProviderID" VARCHAR(50) DEFAULT NULL,
        "AppProviderID" VARCHAR(50) DEFAULT NULL,
        "ProviderName" VARCHAR(100) DEFAULT NULL,
        "FederalTaxNumber" VARCHAR(100) DEFAULT NULL,
        "ConProviderID" VARCHAR(50) DEFAULT NULL,
        "ConAppProviderID" VARCHAR(50) DEFAULT NULL,
        "ConProviderName" VARCHAR(100) DEFAULT NULL,
		"ConFederalTaxNumber" VARCHAR(100) DEFAULT NULL,
        "VisitID" VARCHAR(50) DEFAULT NULL,
        "AppVisitID" VARCHAR(50) DEFAULT NULL,
        "ConVisitID" VARCHAR(50) DEFAULT NULL,
        "ConAppVisitID" VARCHAR(50) DEFAULT NULL,
        "VisitDate" DATE DEFAULT NULL,
        "SchStartTime" DATETIME DEFAULT NULL,
        "SchEndTime" DATETIME DEFAULT NULL,
        "ConSchStartTime" DATETIME DEFAULT NULL,
        "ConSchEndTime" DATETIME DEFAULT NULL,
        "VisitStartTime" DATETIME DEFAULT NULL,
        "VisitEndTime" DATETIME DEFAULT NULL,
        "ConVisitStartTime" DATETIME DEFAULT NULL,
        "ConVisitEndTime" DATETIME DEFAULT NULL,
        "EVVStartTime" DATETIME DEFAULT NULL,
        "EVVEndTime" DATETIME DEFAULT NULL,
        "ConEVVStartTime" DATETIME DEFAULT NULL,
        "ConEVVEndTime" DATETIME DEFAULT NULL,
        "ShVTSTTime" DATETIME DEFAULT NULL,
        "ShVTENTime" DATETIME DEFAULT NULL,
        "CShVTSTTime" DATETIME DEFAULT NULL,
        "CShVTENTime" DATETIME DEFAULT NULL,
        "CaregiverID" VARCHAR(50) DEFAULT NULL,
        "AppCaregiverID" NUMBER(38) DEFAULT NULL,
        "AideCode" VARCHAR(50) DEFAULT NULL,
        "AideName" VARCHAR(101) DEFAULT NULL,
        "AideFName" VARCHAR(50),
        "AideLName" VARCHAR(50),
        "AideSSN" VARCHAR(50) DEFAULT NULL,          
        "AideStatus" VARCHAR(50) DEFAULT NULL,
        "ConCaregiverID" VARCHAR(50) DEFAULT NULL,
        "ConAppCaregiverID" NUMBER(38) DEFAULT NULL,
        "ConAideCode" VARCHAR(50) DEFAULT NULL,
        "ConAideName" VARCHAR(101) DEFAULT NULL,
        "ConAideFName" VARCHAR(50),
        "ConAideLName" VARCHAR(50),
        "ConAideSSN" VARCHAR(50) DEFAULT NULL,
        "ConAideStatus" VARCHAR(20) DEFAULT NULL,
        "OfficeID" VARCHAR(50) DEFAULT NULL,
        "AppOfficeID" VARCHAR(50) DEFAULT NULL,
        "Office" VARCHAR(100) DEFAULT NULL,
        "ConOfficeID" VARCHAR(50) DEFAULT NULL,
        "ConAppOfficeID" VARCHAR(50) DEFAULT NULL,
        "ConOffice" VARCHAR(100) DEFAULT NULL,
        "PatientID" VARCHAR(50) DEFAULT NULL,
        "AppPatientID" NUMBER(38,5) DEFAULT NULL,
        "PAdmissionID" VARCHAR(500) DEFAULT NULL,
        "PName" VARCHAR(201) DEFAULT NULL,
        "PFName" VARCHAR(100),
        "PLName" VARCHAR(100),
        "PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
        "PStatus" VARCHAR(50) DEFAULT NULL,
        "PAddressID" VARCHAR(50) DEFAULT NULL,
        "PAppAddressID" NUMBER(38,5) DEFAULT NULL,
        "PAddressL1" VARCHAR(500) DEFAULT NULL,
        "PAddressL2" VARCHAR(100) DEFAULT NULL,
        "PCity" VARCHAR(255) DEFAULT NULL,
        "PAddressState" VARCHAR(100) DEFAULT NULL,
        "PZipCode" VARCHAR(100) DEFAULT NULL,
        "PCounty" VARCHAR(100) DEFAULT NULL,          
        "PLongitude" VARCHAR(50) DEFAULT NULL,          
        "PLatitude" VARCHAR(50) DEFAULT NULL,          
        "ConPatientID" VARCHAR(50) DEFAULT NULL,
        "ConAppPatientID" NUMBER(38,5) DEFAULT NULL,
        "ConPAdmissionID" VARCHAR(500) DEFAULT NULL,
        "ConPName" VARCHAR(201) DEFAULT NULL,
        "ConPFName" VARCHAR(100),
        "ConPLName" VARCHAR(100),
        "ConPMedicaidNumber" VARCHAR(100) DEFAULT NULL,
        "ConPStatus" VARCHAR(20) DEFAULT NULL,
        "ConPAddressID" VARCHAR(50) DEFAULT NULL,
        "ConPAppAddressID" NUMBER(38,5) DEFAULT NULL,
        "ConPAddressL1" VARCHAR(500) DEFAULT NULL,
        "ConPAddressL2" VARCHAR(100) DEFAULT NULL,
        "ConPCity" VARCHAR(255) DEFAULT NULL,
        "ConPAddressState" VARCHAR(100) DEFAULT NULL,
        "ConPZipCode" VARCHAR(100) DEFAULT NULL,
        "ConPCounty" VARCHAR(100) DEFAULT NULL,
        "ConPLongitude" VARCHAR(50) DEFAULT NULL,
        "ConPLatitude" VARCHAR(50) DEFAULT NULL,
        "PayerID" VARCHAR(50) DEFAULT NULL,
        "AppPayerID" VARCHAR(50) DEFAULT NULL,
        "Contract" VARCHAR(50) DEFAULT NULL,
        "PayerState" VARCHAR(100) DEFAULT NULL,
        "ConPayerID" VARCHAR(50) DEFAULT NULL,
        "ConAppPayerID" VARCHAR(50) DEFAULT NULL,
        "ConContract" VARCHAR(50) DEFAULT NULL,
        "ConPayerState" VARCHAR(100) DEFAULT NULL,
        "Billed" VARCHAR(3) DEFAULT NULL,
        "BilledDate" DATETIME DEFAULT NULL,
        "BilledHours" NUMBER(38,3) DEFAULT NULL,
        "BilledRate" NUMBER(19,3) DEFAULT NULL,
        "TotalBilledAmount" NUMBER(19,3) DEFAULT NULL,       
        "BillRateNonBilled" NUMBER(22,6) DEFAULT NULL,
        "BillRateBoth" NUMBER(22,6) DEFAULT NULL,
        "BilledRateMinute" NUMBER(38,15) DEFAULT NULL,
        "RateType" VARCHAR(50) DEFAULT NULL,
        "ConBilled" VARCHAR(3) DEFAULT NULL,
        "ConBilledDate" DATETIME DEFAULT NULL,
        "ConBilledHours" NUMBER(38,3) DEFAULT NULL,
        "ConBilledRate" NUMBER(19,3) DEFAULT NULL,
        "ConTotalBilledAmount" NUMBER(19,3) DEFAULT NULL,          
        "ConBillRateNonBilled" NUMBER(22,6) DEFAULT NULL,
        "ConBillRateBoth" NUMBER(22,6) DEFAULT NULL,
        "ConBilledRateMinute" NUMBER(38,15) DEFAULT NULL,
        "ConRateType" VARCHAR(50) DEFAULT NULL,
        "MinuteDiffBetweenSch" NUMBER(38,0) DEFAULT NULL,
        "DistanceMilesFromLatLng" NUMBER(38,2) DEFAULT NULL,
        "AverageMilesPerHour" NUMBER(38,2) DEFAULT NULL,
        "ETATravleMinutes" NUMBER(38,0) DEFAULT NULL,
        "InserviceStartDate" DATETIME DEFAULT NULL,
        "InserviceEndDate" DATETIME DEFAULT NULL,
        "PTOStartDate" DATETIME DEFAULT NULL,
        "PTOEndDate" DATETIME DEFAULT NULL,
		"ConInserviceStartDate" DATETIME DEFAULT NULL,
        "ConInserviceEndDate" DATETIME DEFAULT NULL,
        "ConPTOStartDate" DATETIME DEFAULT NULL,
        "ConPTOEndDate" DATETIME DEFAULT NULL,
        "ServiceCodeID" VARCHAR(50) DEFAULT NULL,
        "AppServiceCodeID" NUMBER(38) DEFAULT NULL,
        "ServiceCode" VARCHAR(50) DEFAULT NULL,          
        "ConServiceCodeID" VARCHAR(50) DEFAULT NULL,
        "ConAppServiceCodeID" NUMBER(38) DEFAULT NULL,
        "ConServiceCode" VARCHAR(50) DEFAULT NULL,
        "SameSchTimeFlag" VARCHAR(5),
        "SameVisitTimeFlag" VARCHAR(5),
        "SchAndVisitTimeSameFlag" VARCHAR(5),
        "SchOverAnotherSchTimeFlag" VARCHAR(5),
        "VisitTimeOverAnotherVisitTimeFlag" VARCHAR(5),
        "SchTimeOverVisitTimeFlag" VARCHAR(5),
        "DistanceFlag" VARCHAR(5),
        "InServiceFlag" VARCHAR(5),
        "PTOFlag" VARCHAR(5),
        "AgencyContact" VARCHAR(100) DEFAULT NULL,
        "AgencyPhone" VARCHAR(30) DEFAULT NULL,
        "ConAgencyContact" VARCHAR(100) DEFAULT NULL,
        "ConAgencyPhone" VARCHAR(30) DEFAULT NULL,
        "IsMissed" BOOLEAN,
        "MissedVisitReason" VARCHAR(500) DEFAULT NULL,
        "EVVType" VARCHAR(20) DEFAULT NULL,          
        "ConIsMissed" BOOLEAN,
        "ConMissedVisitReason" VARCHAR(500) DEFAULT NULL,
        "ConEVVType" VARCHAR(20) DEFAULT NULL,
        "ConNoResponseFlag" VARCHAR(10) DEFAULT NULL,
        "ConNoResponseReasonID" NUMBER(38) DEFAULT NULL,
        "ConNoResponseTitle" VARCHAR(500) DEFAULT NULL,
        "ConNoResponseNotes" VARCHAR(500) DEFAULT NULL,
		"ConNoResponseDate" TIMESTAMP DEFAULT NULL,         
        "P_PatientID" VARCHAR(50) DEFAULT NULL,
        "P_AppPatientID" NUMBER(38,5) DEFAULT NULL,
        "P_PAdmissionID" VARCHAR(500) DEFAULT NULL,
        "P_PName" VARCHAR(201) DEFAULT NULL,
        "P_PAddressID" VARCHAR(50) DEFAULT NULL,
        "P_PAppAddressID" NUMBER(38,5) DEFAULT NULL,
        "P_PAddressL1" VARCHAR(500) DEFAULT NULL,
        "P_PAddressL2" VARCHAR(100) DEFAULT NULL,
        "P_PCity" VARCHAR(255) DEFAULT NULL,
        "P_PAddressState" VARCHAR(100) DEFAULT NULL,
        "P_PZipCode" VARCHAR(100) DEFAULT NULL,
        "P_PCounty" VARCHAR(100) DEFAULT NULL, 
        "P_PFName" VARCHAR(100) DEFAULT NULL,
        "P_PLName" VARCHAR(100) DEFAULT NULL,
        "P_PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
        "P_PStatus" VARCHAR(50) DEFAULT NULL,          
        "ConP_PatientID" VARCHAR(50) DEFAULT NULL,
        "ConP_AppPatientID" NUMBER(38,5) DEFAULT NULL, 
        "ConP_PAdmissionID" VARCHAR(500) DEFAULT NULL,
        "ConP_PName" VARCHAR(201) DEFAULT NULL,
        "ConP_PAddressID" VARCHAR(50) DEFAULT NULL,
        "ConP_PAppAddressID" NUMBER(38,5) DEFAULT NULL,
        "ConP_PAddressL1" VARCHAR(500) DEFAULT NULL,
        "ConP_PAddressL2" VARCHAR(100) DEFAULT NULL,
        "ConP_PCity" VARCHAR(255) DEFAULT NULL,
        "ConP_PAddressState" VARCHAR(100) DEFAULT NULL,
        "ConP_PZipCode" VARCHAR(100) DEFAULT NULL,
        "ConP_PCounty" VARCHAR(100) DEFAULT NULL, 
        "ConP_PFName" VARCHAR(100) DEFAULT NULL,
        "ConP_PLName" VARCHAR(100) DEFAULT NULL,
        "ConP_PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
        "ConP_PStatus" VARCHAR(50) DEFAULT NULL,         
        "PA_PatientID" VARCHAR(50) DEFAULT NULL,
        "PA_AppPatientID" NUMBER(38,5) DEFAULT NULL,   
        "PA_PAdmissionID" VARCHAR(500) DEFAULT NULL,
        "PA_PName" VARCHAR(201) DEFAULT NULL,
        "PA_PAddressID" VARCHAR(50) DEFAULT NULL,
        "PA_PAppAddressID" NUMBER(38,5) DEFAULT NULL,
        "PA_PAddressL1" VARCHAR(500) DEFAULT NULL,
        "PA_PAddressL2" VARCHAR(100) DEFAULT NULL,
        "PA_PCity" VARCHAR(255) DEFAULT NULL,
        "PA_PAddressState" VARCHAR(100) DEFAULT NULL,
        "PA_PZipCode" VARCHAR(100) DEFAULT NULL,
        "PA_PCounty" VARCHAR(100) DEFAULT NULL, 
        "PA_PFName" VARCHAR(100) DEFAULT NULL,
        "PA_PLName" VARCHAR(100) DEFAULT NULL,
        "PA_PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
        "PA_PStatus" VARCHAR(20) DEFAULT NULL,       
        "ConPA_PatientID" VARCHAR(50) DEFAULT NULL,
        "ConPA_AppPatientID" NUMBER(38,5) DEFAULT NULL,
        "ConPA_PAdmissionID" VARCHAR(500) DEFAULT NULL,
        "ConPA_PName" VARCHAR(201) DEFAULT NULL,
        "ConPA_PAddressID" VARCHAR(50) DEFAULT NULL,
        "ConPA_PAppAddressID" NUMBER(38,5) DEFAULT NULL,
        "ConPA_PAddressL1" VARCHAR(500) DEFAULT NULL,
        "ConPA_PAddressL2" VARCHAR(100) DEFAULT NULL,
        "ConPA_PCity" VARCHAR(255) DEFAULT NULL,
        "ConPA_PAddressState" VARCHAR(100) DEFAULT NULL,
        "ConPA_PZipCode" VARCHAR(100) DEFAULT NULL,
        "ConPA_PCounty" VARCHAR(100) DEFAULT NULL, 
        "ConPA_PFName" VARCHAR(100) DEFAULT NULL,
        "ConPA_PLName" VARCHAR(100) DEFAULT NULL,
        "ConPA_PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
        "ConPA_PStatus" VARCHAR(20) DEFAULT NULL,
        "ContractType" VARCHAR(30) DEFAULT NULL,
        "ConContractType" VARCHAR(30) DEFAULT NULL,
        "LastUpdatedBy" VARCHAR(100) DEFAULT NULL,
        "LastUpdatedDate" DATETIME DEFAULT NULL,
        "ConLastUpdatedBy" VARCHAR(100) DEFAULT NULL,
        "ConLastUpdatedDate" DATETIME DEFAULT NULL,
        "CreatedDate" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "ResolveDate" TIMESTAMP DEFAULT NULL,
        "ResolvedBy" VARCHAR(200) DEFAULT NULL,
        "CRDATEUNIQUE" TIMESTAMP DEFAULT NULL,
        "G_CRDATEUNIQUE" TIMESTAMP DEFAULT NULL,
        "UpdateFlag" NUMBER DEFAULT NULL,
        "UpdatedDate" TIMESTAMP DEFAULT NULL,
        "StatusFlag" VARCHAR(5) DEFAULT ''U'',
        "ReverseUUID" VARCHAR(100) DEFAULT NULL,
		"FlagForReview" VARCHAR(5) DEFAULT NULL,
		"FlagForReviewDate" TIMESTAMP DEFAULT NULL,
        PRIMARY KEY (ID)
      )
  `;
   try {
     var stmt = snowflake.createStatement({sqlText: "SHOW TABLES LIKE ''" + tableName + "''"});
     var resultSet = stmt.execute();
     if (!resultSet.next()) {
         snowflake.execute({sqlText: sql_command});
         return "Table " + tableName + " created successfully.";
     } else {
         return "Table " + tableName + " already exists.";
     }
 } catch (err) {
     return "Error: " + err.message;
 }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_CONFLICTVISITMAPS_TEMP_TABLE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var tableName = "CONFLICTVISITMAPS_TEMP";
  var sql_command = `
      CREATE TABLE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS_TEMP (
          ID NUMBER(38,0) DEFAULT NULL,
          CONFLICTID NUMBER(38,0) DEFAULT NULL,
          "SSN" VARCHAR(50) DEFAULT NULL,
          "ProviderID" VARCHAR(50) DEFAULT NULL,
          "AppProviderID" VARCHAR(50) DEFAULT NULL,
          "ProviderName" VARCHAR(100) DEFAULT NULL,
		  "FederalTaxNumber" VARCHAR(100) DEFAULT NULL,
          "VisitID" VARCHAR(50) DEFAULT NULL,
          "AppVisitID" VARCHAR(50) DEFAULT NULL,
          "ConProviderID" VARCHAR(50) DEFAULT NULL,
          "ConAppProviderID" VARCHAR(50) DEFAULT NULL,
          "ConProviderName" VARCHAR(100) DEFAULT NULL,
		  "ConFederalTaxNumber" VARCHAR(100) DEFAULT NULL,
          "ConVisitID" VARCHAR(50) DEFAULT NULL,
          "ConAppVisitID" VARCHAR(50) DEFAULT NULL,
          "VisitDate" DATE DEFAULT NULL,
          "SchStartTime" DATETIME DEFAULT NULL,
          "SchEndTime" DATETIME DEFAULT NULL,
          "ConSchStartTime" DATETIME DEFAULT NULL,
          "ConSchEndTime" DATETIME DEFAULT NULL,
          "VisitStartTime" DATETIME DEFAULT NULL,
          "VisitEndTime" DATETIME DEFAULT NULL,
          "ConVisitStartTime" DATETIME DEFAULT NULL,
          "ConVisitEndTime" DATETIME DEFAULT NULL,
          "EVVStartTime" DATETIME DEFAULT NULL,
          "EVVEndTime" DATETIME DEFAULT NULL,
          "ConEVVStartTime" DATETIME DEFAULT NULL,
          "ConEVVEndTime" DATETIME DEFAULT NULL,
          "CaregiverID" VARCHAR(50) DEFAULT NULL,
          "AppCaregiverID" NUMBER(38) DEFAULT NULL,
          "AideCode" VARCHAR(50) DEFAULT NULL,
          "AideName" VARCHAR(101) DEFAULT NULL,
          "AideSSN" VARCHAR(50) DEFAULT NULL,
          "ConCaregiverID" VARCHAR(50) DEFAULT NULL,
          "ConAppCaregiverID" NUMBER(38) DEFAULT NULL,
          "ConAideCode" VARCHAR(50) DEFAULT NULL,
          "ConAideName" VARCHAR(101) DEFAULT NULL,
          "ConAideSSN" VARCHAR(50) DEFAULT NULL,
          "OfficeID" VARCHAR(50) DEFAULT NULL,
          "AppOfficeID" VARCHAR(50) DEFAULT NULL,
          "Office" VARCHAR(100) DEFAULT NULL,
          "ConOfficeID" VARCHAR(50) DEFAULT NULL,
          "ConAppOfficeID" VARCHAR(50) DEFAULT NULL,
          "ConOffice" VARCHAR(100) DEFAULT NULL,
          "PatientID" VARCHAR(50) DEFAULT NULL,
          "AppPatientID" NUMBER(38,5) DEFAULT NULL,
          "PAdmissionID" VARCHAR(500) DEFAULT NULL,
          "PName" VARCHAR(201) DEFAULT NULL,
          "PAddressID" VARCHAR(50) DEFAULT NULL,
          "PAppAddressID" NUMBER(38,5) DEFAULT NULL,
          "PAddressL1" VARCHAR(500) DEFAULT NULL,
          "PAddressL2" VARCHAR(100) DEFAULT NULL,
          "PCity" VARCHAR(255) DEFAULT NULL,
          "PAddressState" VARCHAR(100) DEFAULT NULL,
          "PZipCode" VARCHAR(100) DEFAULT NULL,
          "PCounty" VARCHAR(100) DEFAULT NULL,          
          "PLongitude" VARCHAR(50) DEFAULT NULL,          
          "PLatitude" VARCHAR(50) DEFAULT NULL,          
          "ConPatientID" VARCHAR(50) DEFAULT NULL,
          "ConAppPatientID" NUMBER(38,5) DEFAULT NULL,
          "ConPAdmissionID" VARCHAR(500) DEFAULT NULL,
          "ConPName" VARCHAR(201) DEFAULT NULL,
          "ConPAddressID" VARCHAR(50) DEFAULT NULL,
          "ConPAppAddressID" NUMBER(38,5) DEFAULT NULL,
          "ConPAddressL1" VARCHAR(500) DEFAULT NULL,
          "ConPAddressL2" VARCHAR(100) DEFAULT NULL,
          "ConPCity" VARCHAR(255) DEFAULT NULL,
          "ConPAddressState" VARCHAR(100) DEFAULT NULL,
          "ConPZipCode" VARCHAR(100) DEFAULT NULL,
          "ConPCounty" VARCHAR(100) DEFAULT NULL,
          "ConPLongitude" VARCHAR(50) DEFAULT NULL,
          "ConPLatitude" VARCHAR(50) DEFAULT NULL,
          "PayerID" VARCHAR(50) DEFAULT NULL,
          "AppPayerID" VARCHAR(50) DEFAULT NULL,
          "Contract" VARCHAR(50) DEFAULT NULL,
          "ConPayerID" VARCHAR(50) DEFAULT NULL,
          "ConAppPayerID" VARCHAR(50) DEFAULT NULL,
          "ConContract" VARCHAR(50) DEFAULT NULL,
          "BilledDate" DATETIME DEFAULT NULL,
          "ConBilledDate" DATETIME DEFAULT NULL,
          "BilledHours" NUMBER(38,3) DEFAULT NULL,
          "ConBilledHours" NUMBER(38,3) DEFAULT NULL,
          "Billed" VARCHAR(3) DEFAULT NULL,
          "ConBilled" VARCHAR(3) DEFAULT NULL,
          "MinuteDiffBetweenSch" NUMBER(38,0) DEFAULT NULL,
          "DistanceMilesFromLatLng" NUMBER(38,2) DEFAULT NULL,
          "AverageMilesPerHour" NUMBER(38,2) DEFAULT NULL,
          "ETATravleMinutes" NUMBER(38,0) DEFAULT NULL,
          "InserviceStartDate" DATETIME DEFAULT NULL,
          "InserviceEndDate" DATETIME DEFAULT NULL,
          "PTOStartDate" DATETIME DEFAULT NULL,
          "PTOEndDate" DATETIME DEFAULT NULL,
		  "ConInserviceStartDate" DATETIME DEFAULT NULL,
	      "ConInserviceEndDate" DATETIME DEFAULT NULL,
	      "ConPTOStartDate" DATETIME DEFAULT NULL,
	      "ConPTOEndDate" DATETIME DEFAULT NULL,
          "ServiceCodeID" VARCHAR(50) DEFAULT NULL,
          "AppServiceCodeID" NUMBER(38) DEFAULT NULL,
          "RateType" VARCHAR(50) DEFAULT NULL,
          "ServiceCode" VARCHAR(50) DEFAULT NULL,          
          "ConServiceCodeID" VARCHAR(50) DEFAULT NULL,
          "ConAppServiceCodeID" NUMBER(38) DEFAULT NULL,
          "ConRateType" VARCHAR(50) DEFAULT NULL,
          "ConServiceCode" VARCHAR(50) DEFAULT NULL,
          "SameSchTimeFlag" VARCHAR(5),
          "SameVisitTimeFlag" VARCHAR(5),
          "SchAndVisitTimeSameFlag" VARCHAR(5),
          "SchOverAnotherSchTimeFlag" VARCHAR(5),
          "VisitTimeOverAnotherVisitTimeFlag" VARCHAR(5),
          "SchTimeOverVisitTimeFlag" VARCHAR(5),
          "DistanceFlag" VARCHAR(5),
          "InServiceFlag" VARCHAR(5),
          "PTOFlag" VARCHAR(5),
          "StatusFlag" VARCHAR(5) DEFAULT ''U'',
          "ConStatusFlag" VARCHAR(5) DEFAULT ''U'',
          "AideFName" VARCHAR(50),
          "AideLName" VARCHAR(50),
          "ConAideFName" VARCHAR(50),
          "ConAideLName" VARCHAR(50),
          "PFName" VARCHAR(100),
          "PLName" VARCHAR(100),
          "ConPFName" VARCHAR(100),
          "ConPLName" VARCHAR(100),
          "PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
          "ConPMedicaidNumber" VARCHAR(100) DEFAULT NULL,
          "PayerState" VARCHAR(100) DEFAULT NULL,
          "ConPayerState" VARCHAR(100) DEFAULT NULL,
          "AgencyContact" VARCHAR(100) DEFAULT NULL,
          "ConAgencyContact" VARCHAR(100) DEFAULT NULL,
          "AgencyPhone" VARCHAR(30) DEFAULT NULL,
          "ConAgencyPhone" VARCHAR(30) DEFAULT NULL,
          "LastUpdatedBy" VARCHAR(100) DEFAULT NULL,
          "ConLastUpdatedBy" VARCHAR(100) DEFAULT NULL,
          "LastUpdatedDate" DATETIME DEFAULT NULL,
          "ConLastUpdatedDate" DATETIME DEFAULT NULL,
          "BilledRate" NUMBER(19,3) DEFAULT NULL,
          "TotalBilledAmount" NUMBER(19,3) DEFAULT NULL,
          "ConBilledRate" NUMBER(19,3) DEFAULT NULL,
          "ConTotalBilledAmount" NUMBER(19,3) DEFAULT NULL,
          "IsMissed" BOOLEAN,
          "MissedVisitReason" VARCHAR(500) DEFAULT NULL,
          "EVVType" VARCHAR(20) DEFAULT NULL,          
          "ConIsMissed" BOOLEAN,
          "ConMissedVisitReason" VARCHAR(500) DEFAULT NULL,
          "ConEVVType" VARCHAR(20) DEFAULT NULL,
          "PStatus" VARCHAR(50) DEFAULT NULL,
          "ConPStatus" VARCHAR(20) DEFAULT NULL,
          "AideStatus" VARCHAR(50) DEFAULT NULL,
          "ConAideStatus" VARCHAR(20) DEFAULT NULL,
          "ConNoResponseFlag" VARCHAR(10) DEFAULT NULL,
		  "ConNoResponseReasonID" NUMBER(38) DEFAULT NULL,
          "ConNoResponseTitle" VARCHAR(500) DEFAULT NULL,
          "ConNoResponseNotes" VARCHAR(500) DEFAULT NULL,  
          "LogVisitFlag" NUMBER(2) DEFAULT NULL,
          "LogConflictFlag" NUMBER(2) DEFAULT NULL,
          "P_PatientID" VARCHAR(50) DEFAULT NULL,
          "P_AppPatientID" NUMBER(38,5) DEFAULT NULL,          
          "ConP_PatientID" VARCHAR(50) DEFAULT NULL,
          "ConP_AppPatientID" NUMBER(38,5) DEFAULT NULL,          
          "PA_PatientID" VARCHAR(50) DEFAULT NULL,
          "PA_AppPatientID" NUMBER(38,5) DEFAULT NULL,          
          "ConPA_PatientID" VARCHAR(50) DEFAULT NULL,
          "ConPA_AppPatientID" NUMBER(38,5) DEFAULT NULL,
          "CreatedDate" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,          
          "P_PAdmissionID" VARCHAR(500) DEFAULT NULL,
		"P_PName" VARCHAR(201) DEFAULT NULL,
		"P_PAddressID" VARCHAR(50) DEFAULT NULL,
		"P_PAppAddressID" NUMBER(38,5) DEFAULT NULL,
		"P_PAddressL1" VARCHAR(500) DEFAULT NULL,
		"P_PAddressL2" VARCHAR(100) DEFAULT NULL,
		"P_PCity" VARCHAR(255) DEFAULT NULL,
		"P_PAddressState" VARCHAR(100) DEFAULT NULL,
		"P_PZipCode" VARCHAR(100) DEFAULT NULL,
		"P_PCounty" VARCHAR(100) DEFAULT NULL, 
		"P_PFName" VARCHAR(100) DEFAULT NULL,
		"P_PLName" VARCHAR(100) DEFAULT NULL,
		"P_PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
		"P_PStatus" VARCHAR(50) DEFAULT NULL,
		"ConP_PAdmissionID" VARCHAR(500) DEFAULT NULL,
		"ConP_PName" VARCHAR(201) DEFAULT NULL,
		"ConP_PAddressID" VARCHAR(50) DEFAULT NULL,
		"ConP_PAppAddressID" NUMBER(38,5) DEFAULT NULL,
		"ConP_PAddressL1" VARCHAR(500) DEFAULT NULL,
		"ConP_PAddressL2" VARCHAR(100) DEFAULT NULL,
		"ConP_PCity" VARCHAR(255) DEFAULT NULL,
		"ConP_PAddressState" VARCHAR(100) DEFAULT NULL,
		"ConP_PZipCode" VARCHAR(100) DEFAULT NULL,
		"ConP_PCounty" VARCHAR(100) DEFAULT NULL, 
		"ConP_PFName" VARCHAR(100) DEFAULT NULL,
		"ConP_PLName" VARCHAR(100) DEFAULT NULL,
		"ConP_PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
		"ConP_PStatus" VARCHAR(20) DEFAULT NULL,
		"PA_PAdmissionID" VARCHAR(500) DEFAULT NULL,
		"PA_PName" VARCHAR(201) DEFAULT NULL,
		"PA_PAddressID" VARCHAR(50) DEFAULT NULL,
		"PA_PAppAddressID" NUMBER(38,5) DEFAULT NULL,
		"PA_PAddressL1" VARCHAR(500) DEFAULT NULL,
		"PA_PAddressL2" VARCHAR(100) DEFAULT NULL,
		"PA_PCity" VARCHAR(255) DEFAULT NULL,
		"PA_PAddressState" VARCHAR(100) DEFAULT NULL,
		"PA_PZipCode" VARCHAR(100) DEFAULT NULL,
		"PA_PCounty" VARCHAR(100) DEFAULT NULL, 
		"PA_PFName" VARCHAR(100) DEFAULT NULL,
		"PA_PLName" VARCHAR(100) DEFAULT NULL,
		"PA_PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
		"PA_PStatus" VARCHAR(50) DEFAULT NULL,
		"ConPA_PAdmissionID" VARCHAR(500) DEFAULT NULL,
		"ConPA_PName" VARCHAR(201) DEFAULT NULL,
		"ConPA_PAddressID" VARCHAR(50) DEFAULT NULL,
		"ConPA_PAppAddressID" NUMBER(38,5) DEFAULT NULL,
		"ConPA_PAddressL1" VARCHAR(500) DEFAULT NULL,
		"ConPA_PAddressL2" VARCHAR(100) DEFAULT NULL,
		"ConPA_PCity" VARCHAR(255) DEFAULT NULL,
		"ConPA_PAddressState" VARCHAR(100) DEFAULT NULL,
		"ConPA_PZipCode" VARCHAR(100) DEFAULT NULL,
		"ConPA_PCounty" VARCHAR(100) DEFAULT NULL, 
		"ConPA_PFName" VARCHAR(100) DEFAULT NULL,
		"ConPA_PLName" VARCHAR(100) DEFAULT NULL,
		"ConPA_PMedicaidNumber" VARCHAR(100) DEFAULT NULL,
		"ConPA_PStatus" VARCHAR(20) DEFAULT NULL,
		"ContractType" VARCHAR(30) DEFAULT NULL,
		"ConContractType" VARCHAR(30) DEFAULT NULL,
		"BillRateNonBilled" NUMBER(22,6) DEFAULT NULL,
		"ConBillRateNonBilled" NUMBER(22,6) DEFAULT NULL,        
		"BillRateBoth" NUMBER(22,6) DEFAULT NULL,
		"ConBillRateBoth" NUMBER(22,6) DEFAULT NULL,
		"FlagForReview" VARCHAR(5) DEFAULT NULL,
		"FlagForReviewDate" TIMESTAMP DEFAULT NULL,
		"ConFlagForReview" VARCHAR(5) DEFAULT NULL,
		"ConFlagForReviewDate" TIMESTAMP DEFAULT NULL
      )
  `;
   try {
     var stmt = snowflake.createStatement({sqlText: "SHOW TABLES LIKE ''" + tableName + "''"});
     var resultSet = stmt.execute();
     if (!resultSet.next()) {
         snowflake.execute({sqlText: sql_command});
         return "Table " + tableName + " created successfully.";
     } else {
         return "Table " + tableName + " already exists.";
     }
 } catch (err) {
     throw "ERROR: " + err.message;
 }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_CONTACT_MAINTENANCE_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {
	    // Create table if not exists
	    var tableName = "CONTACT_MAINTENANCE";
	    var table_command = `
	   CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.CONTACT_MAINTENANCE (
	  	id NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
		RECORDEDDATETIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		"CONTACT_NAME" VARCHAR(255) DEFAULT ''U'',
		"PHONE" VARCHAR(20) DEFAULT NULL, 
		"ProviderID" VARCHAR(50) DEFAULT NULL,
		"AppProviderID" VARCHAR(50) DEFAULT NULL,
		"UPDATED_BY" NUMBER(38) DEFAULT NULL,
		"UPDATED_AT" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		"PID" VARCHAR(50) DEFAULT NULL,
		"APPLICATIONPID" VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id))`;
	    snowflake.execute({sqlText: table_command});
	    
	    return "Table " + tableName + " created or already exists.";
	} catch (err) {
	    return "Error: " + err.message;
	}
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_LOG_FIELDS_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var tableName = "LOG_FIELDS";
  var sql_command = `
      CREATE TABLE CONFLICTREPORT.PUBLIC.LOG_FIELDS (
          ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
          "FieldName" VARCHAR(50) DEFAULT NULL,
          "FieldDisplayValue" VARCHAR(50) DEFAULT NULL,
          "FieldFor" VARCHAR(10) DEFAULT NULL,
          "FieldType" VARCHAR(10) DEFAULT NULL,
		  "RestrictedFlag" NUMBER(2,0) DEFAULT NULL,
		  "NotShowInDropDown" NUMBER(2,0) DEFAULT NULL,
		  "HideColumnFlag" NUMBER(2,0) DEFAULT NULL,
		  "HideHidePayerFlag" NUMBER(2,0) DEFAULT NULL,
		  "HideForProviderFlag" NUMBER(2,0) DEFAULT NULL,
          PRIMARY KEY (ID)
      )
  `;
   try {
     var stmt = snowflake.createStatement({sqlText: "SHOW TABLES LIKE ''" + tableName + "''"});
     var resultSet = stmt.execute();
     if (!resultSet.next()) {
         snowflake.execute({sqlText: sql_command});
         return "Table " + tableName + " created successfully.";
     } else {
         return "Table " + tableName + " already exists.";
     }
 } catch (err) {
     return "Error: " + err.message;
 }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_LOG_HISTORY()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
	var resultLog = [];
	var getFieldsStmt = snowflake.createStatement({
	    sqlText: ''SELECT ID, "FieldName", "FieldFor" FROM CONFLICTREPORT."PUBLIC".LOG_FIELDS''
	});
	var fieldsRs = getFieldsStmt.execute();
	
	while (fieldsRs.next()) {
	    var row = {};
	    for (var j = 1; j <= fieldsRs.getColumnCount(); j++) {
	        var fieldName = fieldsRs.getColumnValue(2); // Assuming FieldName is in the second column
	        var FieldFor = fieldsRs.getColumnValue(3); // Assuming FieldFor is in the second column
	        var id = fieldsRs.getColumnValue(1); // Assuming ID is in the first column
	        row[fieldName] = id+''~''+FieldFor; // Store the key-value pair in the row object
	    }
	    resultLog.push(row); // Push the row object to the resultLog array (if you want an array of objects)
	}
var stmt = snowflake.createStatement({
    sqlText: `SELECT
 	TOP 3000
	T1.ID,
	T1.CONFLICTID AS "CONFLICTID1",
	T1.SSN AS "SSN1",
	T1."ProviderID" AS "ProviderID1",
	T1."AppProviderID" AS "AppProviderID1",
	T1."ProviderName" AS "ProviderName1",
	T1."VisitID" AS "VisitID1",
	T1."AppVisitID" AS "AppVisitID1",
	T1."ConProviderID" AS "ConProviderID1",
	T1."ConAppProviderID" AS "ConAppProviderID1",
	T1."ConProviderName" AS "ConProviderName1",
	T1."ConVisitID" AS "ConVisitID1",
	T1."ConAppVisitID" AS "ConAppVisitID1",
	TO_CHAR(T1."VisitDate", ''YYYY-MM-DD'') AS "VisitDate1",
	TO_CHAR(T1."SchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchStartTime1",
	TO_CHAR(T1."SchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchEndTime1",
	TO_CHAR(T1."ConSchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchStartTime1",
	TO_CHAR(T1."ConSchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchEndTime1",
	TO_CHAR(T1."VisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitStartTime1",
	TO_CHAR(T1."VisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitEndTime1",
	TO_CHAR(T1."ConVisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitStartTime1",
	TO_CHAR(T1."ConVisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitEndTime1",
	TO_CHAR(T1."EVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVStartTime1",
	TO_CHAR(T1."EVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVEndTime1",
	TO_CHAR(T1."ConEVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVStartTime1",
	TO_CHAR(T1."ConEVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVEndTime1",
	T1."CaregiverID" AS "CaregiverID1",
	T1."AppCaregiverID" AS "AppCaregiverID1",
	T1."AideCode" AS "AideCode1",
	T1."AideName" AS "AideName1",
	T1."AideSSN" AS "AideSSN1",
	T1."ConCaregiverID" AS "ConCaregiverID1",
	T1."ConAppCaregiverID" AS "ConAppCaregiverID1",
	T1."ConAideCode" AS "ConAideCode1",
	T1."ConAideName" AS "ConAideName1",
	T1."ConAideSSN" AS "ConAideSSN1",
	T1."OfficeID" AS "OfficeID1",
	T1."AppOfficeID" AS "AppOfficeID1",
	T1."Office" AS "Office1",
	T1."ConOfficeID" AS "ConOfficeID1",
	T1."ConAppOfficeID" AS "ConAppOfficeID1",
	T1."ConOffice" AS "ConOffice1",
	T1."PatientID" AS "PatientID1",
	T1."AppPatientID" AS "AppPatientID1",
	T1."PAdmissionID" AS "PAdmissionID1",
	T1."PName" AS "PName1",
	T1."PAddressID" AS "PAddressID1",
	T1."PAppAddressID" AS "PAppAddressID1",
	T1."PAddressL1" AS "PAddressL11",
	T1."PAddressL2" AS "PAddressL21",
	T1."PCity" AS "PCity1",
	T1."PAddressState" AS "PAddressState1",
	T1."PZipCode" AS "PZipCode1",
	T1."PCounty" AS "PCounty1",
	T1."PLongitude" AS "PLongitude1",
	T1."PLatitude" AS "PLatitude1",
	T1."ConPatientID" AS "ConPatientID1",
	T1."ConAppPatientID" AS "ConAppPatientID1",
	T1."ConPAdmissionID" AS "ConPAdmissionID1",
	T1."ConPName" AS "ConPName1",
	T1."ConPAddressID" AS "ConPAddressID1",
	T1."ConPAppAddressID" AS "ConPAppAddressID1",
	T1."ConPAddressL1" AS "ConPAddressL11",
	T1."ConPAddressL2" AS "ConPAddressL21",
	T1."ConPCity" AS "ConPCity1",
	T1."ConPAddressState" AS "ConPAddressState1",
	T1."ConPZipCode" AS "ConPZipCode1",
	T1."ConPCounty" AS "ConPCounty1",
	T1."ConPLongitude" AS "ConPLongitude1",
	T1."ConPLatitude" AS "ConPLatitude1",
	T1."PayerID" AS "PayerID1",
	T1."AppPayerID" AS "AppPayerID1",
	T1."Contract" AS "Contract1",
	T1."ConPayerID" AS "ConPayerID1",
	T1."ConAppPayerID" AS "ConAppPayerID1",
	T1."ConContract" AS "ConContract1",
	TO_CHAR(T1."BilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "BilledDate1",
	TO_CHAR(T1."ConBilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConBilledDate1",
	T1."BilledHours" AS "BilledHours1",
	T1."ConBilledHours" AS "ConBilledHours1",
	T1."Billed" AS "Billed1",
	T1."ConBilled" AS "ConBilled1",
	T1."MinuteDiffBetweenSch" AS "MinuteDiffBetweenSch1",
	T1."DistanceMilesFromLatLng" AS "DistanceMilesFromLatLng1",
	T1."AverageMilesPerHour" AS "AverageMilesPerHour1",
	T1."ETATravleMinutes" AS "ETATravleMinutes1",
	TO_CHAR(T1."InserviceStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceStartDate1",
	TO_CHAR(T1."InserviceEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceEndDate1",
	TO_CHAR(T1."PTOStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOStartDate1",
	TO_CHAR(T1."PTOEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOEndDate1",
	T1."SameSchTimeFlag" AS "SameSchTimeFlag1",
	T1."SameVisitTimeFlag" AS "SameVisitTimeFlag1",
	T1."SchAndVisitTimeSameFlag" AS "SchAndVisitTimeSameFlag1",
	T1."SchOverAnotherSchTimeFlag" AS "SchOverAnotherSchTimeFlag1",
	T1."VisitTimeOverAnotherVisitTimeFlag" AS "VisitTimeOverAnotherVisitTimeFlag1",
	T1."SchTimeOverVisitTimeFlag" AS "SchTimeOverVisitTimeFlag1",
	T1."DistanceFlag" AS "DistanceFlag1",
	T1."InServiceFlag" AS "InServiceFlag1",
	T1."PTOFlag" AS "PTOFlag1",
	T1."StatusFlag" AS "StatusFlag1",
	T1."AideFName" AS "AideFName1",
	T1."AideLName" AS "AideLName1",
	T1."ConAideFName" AS "ConAideFName1",
	T1."ConAideLName" AS "ConAideLName1",
	T1."PFName" AS "PFName1",
	T1."PLName" AS "PLName1",
	T1."ConPFName" AS "ConPFName1",
	T1."ConPLName" AS "ConPLName1",
	T1."PMedicaidNumber" AS "PMedicaidNumber1",
	T1."ConPMedicaidNumber" AS "ConPMedicaidNumber1",
	T1."PayerState" AS "PayerState1",
	T1."ConPayerState" AS "ConPayerState1",
	T1."AgencyContact" AS "AgencyContact1",
	T1."ConAgencyContact" AS "ConAgencyContact1",
	T1."AgencyPhone" AS "AgencyPhone1",
	T1."ConAgencyPhone" AS "ConAgencyPhone1",
	T1."LastUpdatedBy" AS "LastUpdatedBy1",
	T1."ConLastUpdatedBy" AS "ConLastUpdatedBy1",
	TO_CHAR(T1."LastUpdatedDate", ''YYYY-MM-DD HH24:MI:SS'') AS "LastUpdatedDate1",
	TO_CHAR(T1."ConLastUpdatedDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConLastUpdatedDate1",
	T1."BilledRate" AS "BilledRate1",
	T1."TotalBilledAmount" AS "TotalBilledAmount1",
	T1."ConBilledRate" AS "ConBilledRate1",
	T1."ConTotalBilledAmount" AS "ConTotalBilledAmount1",
	T1."IsMissed" AS "IsMissed1",
	T1."MissedVisitReason" AS "MissedVisitReason1",
	T1."EVVType" AS "EVVType1",
	T1."ConIsMissed" AS "ConIsMissed1",
	T1."ConMissedVisitReason" AS "ConMissedVisitReason1",
	T1."ConEVVType" AS "ConEVVType1",
	T1."PStatus" AS "PStatus1",
	T1."ConPStatus" AS "ConPStatus1",
	T1."AideStatus" AS "AideStatus1",
	T1."ConAideStatus" AS "ConAideStatus1",
    T1."ConNoResponseFlag" AS "ConNoResponseFlag1",
    T1."ConNoResponseReasonID" AS "ConNoResponseReasonID1",
    T1."ConNoResponseTitle" AS "ConNoResponseTitle1",
    T1."ConNoResponseNotes" AS "ConNoResponseNotes1",
    T1."P_PatientID" AS "P_PatientID1",
    T1."P_AppPatientID" AS "P_AppPatientID1",
    T1."ConP_PatientID" AS "ConP_PatientID1",
    T1."ConP_AppPatientID" AS "ConP_AppPatientID1",
    T1."PA_PatientID" AS "PA_PatientID1",
    T1."PA_AppPatientID" AS "PA_AppPatientID1",
    T1."ConPA_PatientID" AS "ConPA_PatientID1",
    T1."ConPA_AppPatientID" AS "ConPA_AppPatientID1",
    T1."P_PAdmissionID" AS "P_PAdmissionID1",
    T1."P_PName" AS "P_PName1",
    T1."P_PAddressID" AS "P_PAddressID1",
    T1."P_PAppAddressID" AS "P_PAppAddressID1",
    T1."P_PAddressL1" AS "P_PAddressL11",
    T1."P_PAddressL2" AS "P_PAddressL21",
    T1."P_PCity" AS "P_PCity1",
    T1."P_PAddressState" AS "P_PAddressState1",
    T1."P_PZipCode" AS "P_PZipCode1",
    T1."P_PCounty" AS "P_PCounty1",
    T1."P_PFName" AS "P_PFName1",
    T1."P_PLName" AS "P_PLName1",
    T1."P_PMedicaidNumber" AS "P_PMedicaidNumber1",
    T1."ConP_PAdmissionID" AS "ConP_PAdmissionID1",
    T1."ConP_PName" AS "ConP_PName1",
    T1."ConP_PAddressID" AS "ConP_PAddressID1",
    T1."ConP_PAppAddressID" AS "ConP_PAppAddressID1",
    T1."ConP_PAddressL1" AS "ConP_PAddressL11",
    T1."ConP_PAddressL2" AS "ConP_PAddressL21",
    T1."ConP_PCity" AS "ConP_PCity1",
    T1."ConP_PAddressState" AS "ConP_PAddressState1",
    T1."ConP_PZipCode" AS "ConP_PZipCode1",
    T1."ConP_PCounty" AS "ConP_PCounty1",
    T1."ConP_PFName" AS "ConP_PFName1",
    T1."ConP_PLName" AS "ConP_PLName1",
    T1."ConP_PMedicaidNumber" AS "ConP_PMedicaidNumber1",
    T1."PA_PAdmissionID" AS "PA_PAdmissionID1",
    T1."PA_PName" AS "PA_PName1",
    T1."PA_PAddressID" AS "PA_PAddressID1",
    T1."PA_PAppAddressID" AS "PA_PAppAddressID1",
    T1."PA_PAddressL1" AS "PA_PAddressL11",
    T1."PA_PAddressL2" AS "PA_PAddressL21",
    T1."PA_PCity" AS "PA_PCity1",
    T1."PA_PAddressState" AS "PA_PAddressState1",
    T1."PA_PZipCode" AS "PA_PZipCode1",
    T1."PA_PCounty" AS "PA_PCounty1",
    T1."PA_PFName" AS "PA_PFName1",
    T1."PA_PLName" AS "PA_PLName1",
    T1."PA_PMedicaidNumber" AS "PA_PMedicaidNumber1",
    T1."ConPA_PAdmissionID" AS "ConPA_PAdmissionID1",
    T1."ConPA_PName" AS "ConPA_PName1",
    T1."ConPA_PAddressID" AS "ConPA_PAddressID1",
    T1."ConPA_PAppAddressID" AS "ConPA_PAppAddressID1",
    T1."ConPA_PAddressL1" AS "ConPA_PAddressL11",
    T1."ConPA_PAddressL2" AS "ConPA_PAddressL21",
    T1."ConPA_PCity" AS "ConPA_PCity1",
    T1."ConPA_PAddressState" AS "ConPA_PAddressState1",
    T1."ConPA_PZipCode" AS "ConPA_PZipCode1",
    T1."ConPA_PCounty" AS "ConPA_PCounty1",
    T1."ConPA_PFName" AS "ConPA_PFName1",
    T1."ConPA_PLName" AS "ConPA_PLName1",
    T1."ConPA_PMedicaidNumber" AS "ConPA_PMedicaidNumber1",
    T1."ContractType" AS "ContractType1",
    T1."ConContractType" AS "ConContractType1",
    T1."BillRateNonBilled" AS "BillRateNonBilled1",
    T1."ConBillRateNonBilled" AS "ConBillRateNonBilled1",
    T1."BillRateBoth" AS "BillRateBoth1",
    T1."ConBillRateBoth" AS "ConBillRateBoth1",
	T2.CONFLICTID AS "CONFLICTID2",
	T2.SSN AS "SSN2",
	T2."ProviderID" AS "ProviderID2",
	T2."AppProviderID" AS "AppProviderID2",
	T2."ProviderName" AS "ProviderName2",
	T2."VisitID" AS "VisitID2",
	T2."AppVisitID" AS "AppVisitID2",
	T2."ConProviderID" AS "ConProviderID2",
	T2."ConAppProviderID" AS "ConAppProviderID2",
	T2."ConProviderName" AS "ConProviderName2",
	T2."ConVisitID" AS "ConVisitID2",
	T2."ConAppVisitID" AS "ConAppVisitID2",
	TO_CHAR(T2."VisitDate", ''YYYY-MM-DD'') AS "VisitDate2",
	TO_CHAR(T2."SchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchStartTime2",
	TO_CHAR(T2."SchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchEndTime2",
	TO_CHAR(T2."ConSchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchStartTime2",
	TO_CHAR(T2."ConSchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchEndTime2",
	TO_CHAR(T2."VisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitStartTime2",
	TO_CHAR(T2."VisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitEndTime2",
	TO_CHAR(T2."ConVisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitStartTime2",
	TO_CHAR(T2."ConVisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitEndTime2",
	TO_CHAR(T2."EVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVStartTime2",
	TO_CHAR(T2."EVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVEndTime2",
	TO_CHAR(T2."ConEVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVStartTime2",
	TO_CHAR(T2."ConEVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVEndTime2",
	T2."CaregiverID" AS "CaregiverID2",
	T2."AppCaregiverID" AS "AppCaregiverID2",
	T2."AideCode" AS "AideCode2",
	T2."AideName" AS "AideName2",
	T2."AideSSN" AS "AideSSN2",
	T2."ConCaregiverID" AS "ConCaregiverID2",
	T2."ConAppCaregiverID" AS "ConAppCaregiverID2",
	T2."ConAideCode" AS "ConAideCode2",
	T2."ConAideName" AS "ConAideName2",
	T2."ConAideSSN" AS "ConAideSSN2",
	T2."OfficeID" AS "OfficeID2",
	T2."AppOfficeID" AS "AppOfficeID2",
	T2."Office" AS "Office2",
	T2."ConOfficeID" AS "ConOfficeID2",
	T2."ConAppOfficeID" AS "ConAppOfficeID2",
	T2."ConOffice" AS "ConOffice2",
	T2."PatientID" AS "PatientID2",
	T2."AppPatientID" AS "AppPatientID2",
	T2."PAdmissionID" AS "PAdmissionID2",
	T2."PName" AS "PName2",
	T2."PAddressID" AS "PAddressID2",
	T2."PAppAddressID" AS "PAppAddressID2",
	T2."PAddressL1" AS "PAddressL12",
	T2."PAddressL2" AS "PAddressL22",
	T2."PCity" AS "PCity2",
	T2."PAddressState" AS "PAddressState2",
	T2."PZipCode" AS "PZipCode2",
	T2."PCounty" AS "PCounty2",
	T2."PLongitude" AS "PLongitude2",
	T2."PLatitude" AS "PLatitude2",
	T2."ConPatientID" AS "ConPatientID2",
	T2."ConAppPatientID" AS "ConAppPatientID2",
	T2."ConPAdmissionID" AS "ConPAdmissionID2",
	T2."ConPName" AS "ConPName2",
	T2."ConPAddressID" AS "ConPAddressID2",
	T2."ConPAppAddressID" AS "ConPAppAddressID2",
	T2."ConPAddressL1" AS "ConPAddressL12",
	T2."ConPAddressL2" AS "ConPAddressL22",
	T2."ConPCity" AS "ConPCity2",
	T2."ConPAddressState" AS "ConPAddressState2",
	T2."ConPZipCode" AS "ConPZipCode2",
	T2."ConPCounty" AS "ConPCounty2",
	T2."ConPLongitude" AS "ConPLongitude2",
	T2."ConPLatitude" AS "ConPLatitude2",
	T2."PayerID" AS "PayerID2",
	T2."AppPayerID" AS "AppPayerID2",
	T2."Contract" AS "Contract2",
	T2."ConPayerID" AS "ConPayerID2",
	T2."ConAppPayerID" AS "ConAppPayerID2",
	T2."ConContract" AS "ConContract2",
	TO_CHAR(T2."BilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "BilledDate2",
	TO_CHAR(T2."ConBilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConBilledDate2",
	T2."BilledHours" AS "BilledHours2",
	T2."ConBilledHours" AS "ConBilledHours2",
	T2."Billed" AS "Billed2",
	T2."ConBilled" AS "ConBilled2",
	T2."MinuteDiffBetweenSch" AS "MinuteDiffBetweenSch2",
	T2."DistanceMilesFromLatLng" AS "DistanceMilesFromLatLng2",
	T2."AverageMilesPerHour" AS "AverageMilesPerHour2",
	T2."ETATravleMinutes" AS "ETATravleMinutes2",
	TO_CHAR(T2."InserviceStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceStartDate2",
	TO_CHAR(T2."InserviceEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceEndDate2",
	TO_CHAR(T2."PTOStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOStartDate2",
	TO_CHAR(T2."PTOEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOEndDate2",
	T2."SameSchTimeFlag" AS "SameSchTimeFlag2",
	T2."SameVisitTimeFlag" AS "SameVisitTimeFlag2",
	T2."SchAndVisitTimeSameFlag" AS "SchAndVisitTimeSameFlag2",
	T2."SchOverAnotherSchTimeFlag" AS "SchOverAnotherSchTimeFlag2",
	T2."VisitTimeOverAnotherVisitTimeFlag" AS "VisitTimeOverAnotherVisitTimeFlag2",
	T2."SchTimeOverVisitTimeFlag" AS "SchTimeOverVisitTimeFlag2",
	T2."DistanceFlag" AS "DistanceFlag2",
	T2."InServiceFlag" AS "InServiceFlag2",
	T2."PTOFlag" AS "PTOFlag2",
	T2."StatusFlag" AS "StatusFlag2",
	T2."AideFName" AS "AideFName2",
	T2."AideLName" AS "AideLName2",
	T2."ConAideFName" AS "ConAideFName2",
	T2."ConAideLName" AS "ConAideLName2",
	T2."PFName" AS "PFName2",
	T2."PLName" AS "PLName2",
	T2."ConPFName" AS "ConPFName2",
	T2."ConPLName" AS "ConPLName2",
	T2."PMedicaidNumber" AS "PMedicaidNumber2",
	T2."ConPMedicaidNumber" AS "ConPMedicaidNumber2",
	T2."PayerState" AS "PayerState2",
	T2."ConPayerState" AS "ConPayerState2",
	T2."AgencyContact" AS "AgencyContact2",
	T2."ConAgencyContact" AS "ConAgencyContact2",
	T2."AgencyPhone" AS "AgencyPhone2",
	T2."ConAgencyPhone" AS "ConAgencyPhone2",
	T2."LastUpdatedBy" AS "LastUpdatedBy2",
	T2."ConLastUpdatedBy" AS "ConLastUpdatedBy2",
	TO_CHAR(T2."LastUpdatedDate", ''YYYY-MM-DD HH24:MI:SS'') AS "LastUpdatedDate1",
	TO_CHAR(T2."ConLastUpdatedDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConLastUpdatedDate1",
	T2."BilledRate" AS "BilledRate2",
	T2."TotalBilledAmount" AS "TotalBilledAmount2",
	T2."ConBilledRate" AS "ConBilledRate2",
	T2."ConTotalBilledAmount" AS "ConTotalBilledAmount2",
	T2."IsMissed" AS "IsMissed2",
	T2."MissedVisitReason" AS "MissedVisitReason2",
	T2."EVVType" AS "EVVType2",
	T2."ConIsMissed" AS "ConIsMissed2",
	T2."ConMissedVisitReason" AS "ConMissedVisitReason2",
	T2."ConEVVType" AS "ConEVVType2",
	T2."PStatus" AS "PStatus2",
	T2."ConPStatus" AS "ConPStatus2",
	T2."AideStatus" AS "AideStatus2",
	T2."ConAideStatus" AS "ConAideStatus2",
    T2."ConNoResponseFlag" AS "ConNoResponseFlag2",
    T2."ConNoResponseReasonID" AS "ConNoResponseReasonID2",
    T2."ConNoResponseTitle" AS "ConNoResponseTitle2",
    T2."ConNoResponseNotes" AS "ConNoResponseNotes2",
    T2."P_PatientID" AS "P_PatientID2",
    T2."P_AppPatientID" AS "P_AppPatientID2",
    T2."ConP_PatientID" AS "ConP_PatientID2",
    T2."ConP_AppPatientID" AS "ConP_AppPatientID2",
    T2."PA_PatientID" AS "PA_PatientID2",
    T2."PA_AppPatientID" AS "PA_AppPatientID2",
    T2."ConPA_PatientID" AS "ConPA_PatientID2",
    T2."ConPA_AppPatientID" AS "ConPA_AppPatientID2",
    T2."P_PAdmissionID" AS "P_PAdmissionID2",
    T2."P_PName" AS "P_PName2",
    T2."P_PAddressID" AS "P_PAddressID2",
    T2."P_PAppAddressID" AS "P_PAppAddressID2",
    T2."P_PAddressL1" AS "P_PAddressL12",
    T2."P_PAddressL2" AS "P_PAddressL22",
    T2."P_PCity" AS "P_PCity2",
    T2."P_PAddressState" AS "P_PAddressState2",
    T2."P_PZipCode" AS "P_PZipCode2",
    T2."P_PCounty" AS "P_PCounty2",
    T2."P_PFName" AS "P_PFName2",
    T2."P_PLName" AS "P_PLName2",
    T2."P_PMedicaidNumber" AS "P_PMedicaidNumber2",
    T2."ConP_PAdmissionID" AS "ConP_PAdmissionID2",
    T2."ConP_PName" AS "ConP_PName2",
    T2."ConP_PAddressID" AS "ConP_PAddressID2",
    T2."ConP_PAppAddressID" AS "ConP_PAppAddressID2",
    T2."ConP_PAddressL1" AS "ConP_PAddressL12",
    T2."ConP_PAddressL2" AS "ConP_PAddressL22",
    T2."ConP_PCity" AS "ConP_PCity2",
    T2."ConP_PAddressState" AS "ConP_PAddressState2",
    T2."ConP_PZipCode" AS "ConP_PZipCode2",
    T2."ConP_PCounty" AS "ConP_PCounty2",
    T2."ConP_PFName" AS "ConP_PFName2",
    T2."ConP_PLName" AS "ConP_PLName2",
    T2."ConP_PMedicaidNumber" AS "ConP_PMedicaidNumber2",
    T2."PA_PAdmissionID" AS "PA_PAdmissionID2",
    T2."PA_PName" AS "PA_PName2",
    T2."PA_PAddressID" AS "PA_PAddressID2",
    T2."PA_PAppAddressID" AS "PA_PAppAddressID2",
    T2."PA_PAddressL1" AS "PA_PAddressL12",
    T2."PA_PAddressL2" AS "PA_PAddressL22",
    T2."PA_PCity" AS "PA_PCity2",
    T2."PA_PAddressState" AS "PA_PAddressState2",
    T2."PA_PZipCode" AS "PA_PZipCode2",
    T2."PA_PCounty" AS "PA_PCounty2",
    T2."PA_PFName" AS "PA_PFName2",
    T2."PA_PLName" AS "PA_PLName2",
    T2."PA_PMedicaidNumber" AS "PA_PMedicaidNumber2",
    T2."ConPA_PAdmissionID" AS "ConPA_PAdmissionID2",
    T2."ConPA_PName" AS "ConPA_PName2",
    T2."ConPA_PAddressID" AS "ConPA_PAddressID2",
    T2."ConPA_PAppAddressID" AS "ConPA_PAppAddressID2",
    T2."ConPA_PAddressL1" AS "ConPA_PAddressL12",
    T2."ConPA_PAddressL2" AS "ConPA_PAddressL22",
    T2."ConPA_PCity" AS "ConPA_PCity2",
    T2."ConPA_PAddressState" AS "ConPA_PAddressState2",
    T2."ConPA_PZipCode" AS "ConPA_PZipCode2",
    T2."ConPA_PCounty" AS "ConPA_PCounty2",
    T2."ConPA_PFName" AS "ConPA_PFName2",
    T2."ConPA_PLName" AS "ConPA_PLName2",
    T2."ConPA_PMedicaidNumber" AS "ConPA_PMedicaidNumber2",
    T2."ContractType" AS "ContractType2",
    T2."ConContractType" AS "ConContractType2",
    T2."BillRateNonBilled" AS "BillRateNonBilled2",
    T2."ConBillRateNonBilled" AS "ConBillRateNonBilled2",
    T2."BillRateBoth" AS "BillRateBoth2",
    T2."ConBillRateBoth" AS "ConBillRateBoth2"
FROM
	CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS_TEMP AS T1
JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS T2 ON
	T1.ID = T2.ID
	WHERE 
		T1."LogConflictFlag" IS NULL
		AND
       NOT EXISTS (
            SELECT 1
            FROM CONFLICTREPORT."PUBLIC".LOG_HISTORY AS LHI
            WHERE
			(	            
            	LHI."CONID" = T1.ID
            	AND
				TO_CHAR(LHI."CreatedDateTime", ''YYYY-MM-DD'') = TO_CHAR(CURRENT_DATE(), ''YYYY-MM-DD'')				
			)
       )
	`
});

var rs = stmt.execute();
var IDStrs = '''';
var countval = 0;
while (rs.next()) {
	countval++;
	if(countval==500 && IDStrs!=''''){
		var getFieldsStmtA = snowflake.createStatement({
		    sqlText: `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS_TEMP SET "LogConflictFlag" = 1 WHERE ID IN (${IDStrs})`
		});
		getFieldsStmtA.execute();
		countval = 0;
		IDStrs = '''';
	}
    var id = rs.getColumnValue(1); // Assuming ID is the first COLUMN
    if(IDStrs!=''''){
    	IDStrs += '','';
    }
    IDStrs += id;
    var fieldCount = (rs.getColumnCount()-1) / 2; // Assuming the same number of fields in table1 and table2
    var insertvalue = '''';
    var insertvalue1 = '''';
	var LHID = '''';
    for (var i = 2; i <= fieldCount; i++) {
        var fieldName = rs.getColumnName(i);
		var FieldNameN = fieldName.slice(0, -1);
		var objWithID = resultLog.find(obj => obj.hasOwnProperty(FieldNameN));
		var LogID = '''';
		if(typeof objWithID !== ''undefined''){
		    var LogID = objWithID ? objWithID[FieldNameN] : '''';
		}
		
		// Use the retrieved ID
		if (LogID !== '''') {
			var LogIDS = LogID.split("~");
			var LogID = (typeof LogIDS[0] !== ''undefined'') ? LogIDS[0] : '''';
			var FieldFor = (typeof LogIDS[1] !== ''undefined'') ? LogIDS[1] : '''';
			
        	var VisitID = ''NULL''
        	var AppVisitID = ''NULL''
        	if(FieldFor==''P''){
	        	VisitID = "''"+rs.getColumnValue(7)+"''";
	        	AppVisitID = "''"+rs.getColumnValue(8)+"''";
        	}
        	if(FieldFor==''C''){
        		VisitID = "''"+rs.getColumnValue(12)+"''";
	        	AppVisitID = "''"+rs.getColumnValue(13)+"''";
        	}
	        var OldValue = rs.getColumnValue(i);
	       var OldValue = (OldValue!=''null'' && OldValue!=null) ? OldValue : '''';
	        var NewValue = rs.getColumnValue(i + fieldCount);
	       var NewValue = (NewValue!=''null'' && NewValue!=null) ? NewValue : '''';
	        if (OldValue !== NewValue) {
	        	if(LHID == ''''){
	        		var insertStmt = snowflake.createStatement({
				        sqlText: `INSERT INTO CONFLICTREPORT."PUBLIC".LOG_HISTORY ("CONID") 
				                VALUES (''${id}'')`
				    });
				    insertStmt.execute();
				   
				   	// Retrieve the maximum ID from the LOG_HISTORY table
					var maxIdStmt = snowflake.createStatement({
					    sqlText: `SELECT MAX(ID) AS "MaxID" FROM CONFLICTREPORT.PUBLIC.LOG_HISTORY`
					});
					var resultSet = maxIdStmt.execute();
					if (resultSet.next()) {
					    LHID = resultSet.getColumnValue(''MaxID'');
					}
	        	}
	        	if(LHID != ''''){
		        	if(insertvalue!=''''){
		        		insertvalue += '', '';
		        	}
		        	insertvalue += "(''"+LHID+"'', ''"+LogID+"'', ''"+OldValue+"'', ''"+NewValue+"'', "+VisitID+", "+AppVisitID+")";
	        	}
	        }
        }
    }
    if(insertvalue !=='''' && LHID !== ''''){    	
	    var insertStmt = snowflake.createStatement({
	        sqlText: `INSERT INTO CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES ("LHID", "LogID", "OldValue", "NewValue", "VisitID", "AppVisitID") 
	                  VALUES ${insertvalue}`
	    });
	    insertStmt.execute();
    }    
}
if(IDStrs!==''''){
	var getFieldsStmtA = snowflake.createStatement({
	    sqlText: `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS_TEMP SET "LogConflictFlag" = 1 WHERE ID IN (${IDStrs})`
	});
	getFieldsStmtA.execute();
}

return ''Differences logged successfully.'';
} catch (err) {
    return ''Error: '' + err.message;
}
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_LOG_HISTORY_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var tableName = "LOG_HISTORY";
  var sql_command = `
      CREATE TABLE CONFLICTREPORT.PUBLIC.LOG_HISTORY (
          ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
          "CONID" NUMBER(38,0) DEFAULT NULL,
          "CreatedDateTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		  "LogTypeFlag" VARCHAR(10) DEFAULT NULL,
          PRIMARY KEY (ID)
      )
  `;
   try {
     var stmt = snowflake.createStatement({sqlText: "SHOW TABLES LIKE ''" + tableName + "''"});
     var resultSet = stmt.execute();
     if (!resultSet.next()) {
         snowflake.execute({sqlText: sql_command});
         return "Table " + tableName + " created successfully.";
     } else {
         return "Table " + tableName + " already exists.";
     }
 } catch (err) {
     return "Error: " + err.message;
 }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_LOG_HISTORY_VALUES_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var tableName = "LOG_HISTORY_VALUES";
  var sql_command = `
      CREATE TABLE CONFLICTREPORT.PUBLIC.LOG_HISTORY_VALUES (
          ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
          "LHID" NUMBER(38,0) DEFAULT NULL,
          "LogID" NUMBER(38,0) DEFAULT NULL,
          "OldValue" VARCHAR(16777216) DEFAULT NULL,
          "NewValue" VARCHAR(16777216) DEFAULT NULL,
          "VisitID" VARCHAR(50) DEFAULT NULL,
          "AppVisitID" VARCHAR(50) DEFAULT NULL,
          PRIMARY KEY (ID)
      )
  `;
   try {
     var stmt = snowflake.createStatement({sqlText: "SHOW TABLES LIKE ''" + tableName + "''"});
     var resultSet = stmt.execute();
     if (!resultSet.next()) {
         snowflake.execute({sqlText: sql_command});
         return "Table " + tableName + " created successfully.";
     } else {
         return "Table " + tableName + " already exists.";
     }
 } catch (err) {
     return "Error: " + err.message;
 }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_NEW_LOG_HISTORY()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        // First, insert the main log history records
        var insertMainLogs = snowflake.createStatement({
            sqlText: `
                INSERT INTO CONFLICTREPORT."PUBLIC".LOG_HISTORY ("CONID", "LogTypeFlag")
				SELECT T2.ID, ''Inserted''
				FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS T2
				WHERE NOT EXISTS (
				    SELECT 1
				    FROM CONFLICTREPORT."PUBLIC".LOG_HISTORY AS LHI
				    WHERE LHI."CONID" = T2.ID AND LHI."LogTypeFlag" = ''Inserted''
				)
            `
        });
        insertMainLogs.execute();

        // Now insert the log history values using UNION ALL for each field
        var insertLogValues = snowflake.createStatement({
            sqlText: `
                INSERT INTO CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES 
				("LHID", "LogID", "OldValue", "NewValue", "VisitID", "AppVisitID")
				WITH NewLogRecords AS (
				    SELECT 
				        LH.ID as LHID,
				        T2.ID as CONID,
				        T2."VisitID" as "PVisitID",
				        T2."AppVisitID" as "PAppVisitID",
				        T2."ConVisitID" as "CVisitID",
				        T2."ConAppVisitID" as "CAppVisitID",
				        CAST(T2.CONFLICTID AS TEXT) AS "CONFLICTID",
				        CAST(T2.SSN AS TEXT) AS "SSN",
				        CAST(T2."ProviderName" AS TEXT) AS "ProviderName",
				        CAST(T2."ConProviderName" AS TEXT) AS "ConProviderName",
				        TO_CHAR(T2."VisitDate", ''YYYY-MM-DD'') AS "VisitDate", 
				        TO_CHAR(T2."SchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchStartTime", 
				        TO_CHAR(T2."SchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchEndTime", 
				        TO_CHAR(T2."ConSchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchStartTime", 
				        TO_CHAR(T2."ConSchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchEndTime", 
				        TO_CHAR(T2."VisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitStartTime", 
				        TO_CHAR(T2."VisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitEndTime", 
				        TO_CHAR(T2."ConVisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitStartTime", 
				        TO_CHAR(T2."ConVisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitEndTime", 
				        TO_CHAR(T2."EVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVStartTime", 
				        TO_CHAR(T2."EVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVEndTime", 
				        TO_CHAR(T2."ConEVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVStartTime", 
				        TO_CHAR(T2."ConEVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVEndTime",
				        CAST(T2."AideCode" AS TEXT) AS "AideCode",
				        CAST(T2."AideName" AS TEXT) AS "AideName",
				        CAST(T2."AideSSN" AS TEXT) AS "AideSSN",
				        CAST(T2."ConAideCode" AS TEXT) AS "ConAideCode",
				        CAST(T2."ConAideName" AS TEXT) AS "ConAideName",
				        CAST(T2."ConAideSSN" AS TEXT) AS "ConAideSSN",
				        CAST(T2."Office" AS TEXT) AS "Office",
				        CAST(T2."ConOffice" AS TEXT) AS "ConOffice",
				        CAST(T2."PAdmissionID" AS TEXT) AS "PAdmissionID",
				        CAST(T2."PName" AS TEXT) AS "PName",
				        CAST(T2."PAddressL1" AS TEXT) AS "PAddressL1",
				        CAST(T2."PAddressL2" AS TEXT) AS "PAddressL2",
				        CAST(T2."PCity" AS TEXT) AS "PCity",
				        CAST(T2."PAddressState" AS TEXT) AS "PAddressState",
				        CAST(T2."PZipCode" AS TEXT) AS "PZipCode",
				        CAST(T2."PCounty" AS TEXT) AS "PCounty",
				        CAST(T2."PLongitude" AS TEXT) AS "PLongitude",
				        CAST(T2."PLatitude" AS TEXT) AS "PLatitude",
				        CAST(T2."ConPAdmissionID" AS TEXT) AS "ConPAdmissionID",
				        CAST(T2."ConPName" AS TEXT) AS "ConPName",
				        CAST(T2."ConPAddressL1" AS TEXT) AS "ConPAddressL1",
				        CAST(T2."ConPAddressL2" AS TEXT) AS "ConPAddressL2",
				        CAST(T2."ConPCity" AS TEXT) AS "ConPCity",
				        CAST(T2."ConPAddressState" AS TEXT) AS "ConPAddressState",
				        CAST(T2."ConPZipCode" AS TEXT) AS "ConPZipCode",
				        CAST(T2."ConPCounty" AS TEXT) AS "ConPCounty",
				        CAST(T2."ConPLongitude" AS TEXT) AS "ConPLongitude",
				        CAST(T2."ConPLatitude" AS TEXT) AS "ConPLatitude",
				        CAST(T2."Contract" AS TEXT) AS "Contract",
				        CAST(T2."ConContract" AS TEXT) AS "ConContract",
				        TO_CHAR(T2."BilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "BilledDate", 
				        TO_CHAR(T2."ConBilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConBilledDate",
				        CAST(T2."BilledHours" AS TEXT) AS "BilledHours",
				        CAST(T2."ConBilledHours" AS TEXT) AS "ConBilledHours",
				        CAST(T2."Billed" AS TEXT) AS "Billed",
				        CAST(T2."ConBilled" AS TEXT) AS "ConBilled",
				        CAST(T2."MinuteDiffBetweenSch" AS TEXT) AS "MinuteDiffBetweenSch",
				        CAST(T2."DistanceMilesFromLatLng" AS TEXT) AS "DistanceMilesFromLatLng",
				        CAST(T2."AverageMilesPerHour" AS TEXT) AS "AverageMilesPerHour",
				        CAST(T2."ETATravleMinutes" AS TEXT) AS "ETATravleMinutes",
				        TO_CHAR(T2."InserviceStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceStartDate", 
				        TO_CHAR(T2."InserviceEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceEndDate", 
				        TO_CHAR(T2."PTOStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOStartDate", 
				        TO_CHAR(T2."PTOEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOEndDate",
				        CAST(T2."SameSchTimeFlag" AS TEXT) AS "SameSchTimeFlag",
				        CAST(T2."SameVisitTimeFlag" AS TEXT) AS "SameVisitTimeFlag",
				        CAST(T2."SchAndVisitTimeSameFlag" AS TEXT) AS "SchAndVisitTimeSameFlag",
				        CAST(T2."SchOverAnotherSchTimeFlag" AS TEXT) AS "SchOverAnotherSchTimeFlag",
				        CAST(T2."VisitTimeOverAnotherVisitTimeFlag" AS TEXT) AS "VisitTimeOverAnotherVisitTimeFlag",
				        CAST(T2."SchTimeOverVisitTimeFlag" AS TEXT) AS "SchTimeOverVisitTimeFlag",
				        CAST(T2."DistanceFlag" AS TEXT) AS "DistanceFlag",
				        CAST(T2."InServiceFlag" AS TEXT) AS "InServiceFlag",
				        CAST(T2."PTOFlag" AS TEXT) AS "PTOFlag",
				        CAST(T2."StatusFlag" AS TEXT) AS "ConStatusFlag",
				        CAST(T2."AideFName" AS TEXT) AS "AideFName",
				        CAST(T2."AideLName" AS TEXT) AS "AideLName",
				        CAST(T2."ConAideFName" AS TEXT) AS "ConAideFName",
				        CAST(T2."ConAideLName" AS TEXT) AS "ConAideLName",
				        CAST(T2."PFName" AS TEXT) AS "PFName",
				        CAST(T2."PLName" AS TEXT) AS "PLName",
				        CAST(T2."ConPFName" AS TEXT) AS "ConPFName",
				        CAST(T2."ConPLName" AS TEXT) AS "ConPLName",
				        CAST(T2."PMedicaidNumber" AS TEXT) AS "PMedicaidNumber",
				        CAST(T2."ConPMedicaidNumber" AS TEXT) AS "ConPMedicaidNumber",
				        CAST(T2."PayerState" AS TEXT) AS "PayerState",
				        CAST(T2."ConPayerState" AS TEXT) AS "ConPayerState",
				        CAST(T2."AgencyContact" AS TEXT) AS "AgencyContact",
				        CAST(T2."ConAgencyContact" AS TEXT) AS "ConAgencyContact",
				        CAST(T2."AgencyPhone" AS TEXT) AS "AgencyPhone",
				        CAST(T2."ConAgencyPhone" AS TEXT) AS "ConAgencyPhone",
				        CAST(T2."LastUpdatedBy" AS TEXT) AS "LastUpdatedBy",
				        CAST(T2."ConLastUpdatedBy" AS TEXT) AS "ConLastUpdatedBy",
				        CAST(T2."LastUpdatedDate" AS TEXT) AS "LastUpdatedDate",
				        CAST(T2."ConLastUpdatedDate" AS TEXT) AS "ConLastUpdatedDate",
				        CAST(T2."BilledRate" AS TEXT) AS "BilledRate",
				        CAST(T2."TotalBilledAmount" AS TEXT) AS "TotalBilledAmount",
				        CAST(T2."ConBilledRate" AS TEXT) AS "ConBilledRate",
				        CAST(T2."ConTotalBilledAmount" AS TEXT) AS "ConTotalBilledAmount",
				        CAST(T2."IsMissed" AS TEXT) AS "IsMissed",
				        CAST(T2."MissedVisitReason" AS TEXT) AS "MissedVisitReason",
				        CAST(T2."EVVType" AS TEXT) AS "EVVType",
				        CAST(T2."ConIsMissed" AS TEXT) AS "ConIsMissed",
				        CAST(T2."ConMissedVisitReason" AS TEXT) AS "ConMissedVisitReason",
				        CAST(T2."ConEVVType" AS TEXT) AS "ConEVVType",
				        CAST(T2."PStatus" AS TEXT) AS "PStatus",
				        CAST(T2."ConPStatus" AS TEXT) AS "ConPStatus",
				        CAST(T2."AideStatus" AS TEXT) AS "AideStatus",
				        CAST(T2."ConAideStatus" AS TEXT) AS "ConAideStatus",
				        CAST(T2."ConNoResponseFlag" AS TEXT) AS "ConNoResponseFlag",
				        CAST(T2."ConNoResponseTitle" AS TEXT) AS "ConNoResponseTitle",
				        CAST(T2."ConNoResponseNotes" AS TEXT) AS "ConNoResponseNotes",
				        CAST(T2."P_PAdmissionID" AS TEXT) AS "P_PAdmissionID",
				        CAST(T2."P_PName" AS TEXT) AS "P_PName",
				        CAST(T2."P_PAddressL1" AS TEXT) AS "P_PAddressL1",
				        CAST(T2."P_PAddressL2" AS TEXT) AS "P_PAddressL2",
				        CAST(T2."P_PCity" AS TEXT) AS "P_PCity",
				        CAST(T2."P_PAddressState" AS TEXT) AS "P_PAddressState",
				        CAST(T2."P_PZipCode" AS TEXT) AS "P_PZipCode",
				        CAST(T2."P_PCounty" AS TEXT) AS "P_PCounty",
				        CAST(T2."P_PFName" AS TEXT) AS "P_PFName",
				        CAST(T2."P_PLName" AS TEXT) AS "P_PLName",
				        CAST(T2."P_PMedicaidNumber" AS TEXT) AS "P_PMedicaidNumber",
				        CAST(T2."ConP_PAdmissionID" AS TEXT) AS "ConP_PAdmissionID",
				        CAST(T2."ConP_PName" AS TEXT) AS "ConP_PName",
				        CAST(T2."ConP_PAddressL1" AS TEXT) AS "ConP_PAddressL1",
				        CAST(T2."ConP_PAddressL2" AS TEXT) AS "ConP_PAddressL2",
				        CAST(T2."ConP_PCity" AS TEXT) AS "ConP_PCity",
				        CAST(T2."ConP_PAddressState" AS TEXT) AS "ConP_PAddressState",
				        CAST(T2."ConP_PZipCode" AS TEXT) AS "ConP_PZipCode",
				        CAST(T2."ConP_PCounty" AS TEXT) AS "ConP_PCounty",
				        CAST(T2."ConP_PFName" AS TEXT) AS "ConP_PFName",
				        CAST(T2."ConP_PLName" AS TEXT) AS "ConP_PLName",
				        CAST(T2."ConP_PMedicaidNumber" AS TEXT) AS "ConP_PMedicaidNumber",
				        CAST(T2."PA_PAdmissionID" AS TEXT) AS "PA_PAdmissionID",
				        CAST(T2."PA_PName" AS TEXT) AS "PA_PName",
				        CAST(T2."PA_PAddressL1" AS TEXT) AS "PA_PAddressL1",
				        CAST(T2."PA_PAddressL2" AS TEXT) AS "PA_PAddressL2",
				        CAST(T2."PA_PCity" AS TEXT) AS "PA_PCity",
				        CAST(T2."PA_PAddressState" AS TEXT) AS "PA_PAddressState",
				        CAST(T2."PA_PZipCode" AS TEXT) AS "PA_PZipCode",
				        CAST(T2."PA_PCounty" AS TEXT) AS "PA_PCounty",
				        CAST(T2."PA_PFName" AS TEXT) AS "PA_PFName",
				        CAST(T2."PA_PLName" AS TEXT) AS "PA_PLName",
				        CAST(T2."PA_PMedicaidNumber" AS TEXT) AS "PA_PMedicaidNumber",
				        CAST(T2."ConPA_PAdmissionID" AS TEXT) AS "ConPA_PAdmissionID",
				        CAST(T2."ConPA_PName" AS TEXT) AS "ConPA_PName",
				        CAST(T2."ConPA_PAddressL1" AS TEXT) AS "ConPA_PAddressL1",
				        CAST(T2."ConPA_PAddressL2" AS TEXT) AS "ConPA_PAddressL2",
				        CAST(T2."ConPA_PCity" AS TEXT) AS "ConPA_PCity",
				        CAST(T2."ConPA_PAddressState" AS TEXT) AS "ConPA_PAddressState",
				        CAST(T2."ConPA_PZipCode" AS TEXT) AS "ConPA_PZipCode",
				        CAST(T2."ConPA_PCounty" AS TEXT) AS "ConPA_PCounty",
				        CAST(T2."ConPA_PFName" AS TEXT) AS "ConPA_PFName",
				        CAST(T2."ConPA_PLName" AS TEXT) AS "ConPA_PLName",
				        CAST(T2."ConPA_PMedicaidNumber" AS TEXT) AS "ConPA_PMedicaidNumber",
				        CAST(T2."ContractType" AS TEXT) AS "ContractType",
				        CAST(T2."ConContractType" AS TEXT) AS "ConContractType",
				        CAST(T2."BillRateNonBilled" AS TEXT) AS "BillRateNonBilled",
				        CAST(T2."ConBillRateNonBilled" AS TEXT) AS "ConBillRateNonBilled",
				        CAST(T2."BillRateBoth" AS TEXT) AS "BillRateBoth",
				        CAST(T2."ConBillRateBoth" AS TEXT) AS "ConBillRateBoth",
				        CAST(T2."FederalTaxNumber" AS TEXT) AS "FederalTaxNumber",
				        CAST(T2."ConFederalTaxNumber" AS TEXT) AS "ConFederalTaxNumber",
				        CAST(T3."StatusFlag" AS TEXT) AS "StatusFlag",
				        CAST(T3."FlagForReview" AS TEXT) AS "FlagForReview",
				        TO_CHAR(T3."FlagForReviewDate", ''YYYY-MM-DD HH24:MI:SS'') AS "FlagForReviewDate",
				        CAST(T2."FlagForReview" AS TEXT) AS "ConFlagForReview",
				        TO_CHAR(T2."FlagForReviewDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConFlagForReviewDate",
				        TO_CHAR(T2."ConInserviceStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConInserviceStartDate", 
				        TO_CHAR(T2."ConInserviceEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConInserviceEndDate", 
				        TO_CHAR(T2."ConPTOStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConPTOStartDate", 
				        TO_CHAR(T2."ConPTOEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConPTOEndDate"
				    FROM CONFLICTREPORT."PUBLIC".LOG_HISTORY LH
				    JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS T2 ON T2.ID = LH."CONID"
					JOIN CONFLICTREPORT."PUBLIC".CONFLICTS T3 ON T3.CONFLICTID = T2.CONFLICTID
				    WHERE LH."LogTypeFlag" = ''Inserted'' AND NOT EXISTS (
				            SELECT 1
				            FROM CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES AS LHV
				            WHERE
							(
				            	LHV."LHID" = LH.ID			
							)
				       )
				),
				LogFields AS (
				    SELECT ID as "LogID", "FieldName", "FieldFor"
				    FROM CONFLICTREPORT."PUBLIC".LOG_FIELDS
				),
				UnpivotedData AS (
				    SELECT 
				        LHID,
				        "PVisitID",
				        "PAppVisitID",
				        "CVisitID",
				        "CAppVisitID",
				        column_name,
				        column_value
				    FROM NewLogRecords
				    UNPIVOT(
				        column_value FOR column_name IN (
				            "CONFLICTID", 
				            "SSN", 
				            "ProviderName", 
				            "ConProviderName",
				            "VisitDate",
				            "SchStartTime",
				            "SchEndTime",
				            "ConSchStartTime",
				            "ConSchEndTime",
				            "VisitStartTime",
				            "VisitEndTime",
				            "ConVisitStartTime",
				            "ConVisitEndTime",
				            "EVVStartTime",
				            "EVVEndTime",
				            "ConEVVStartTime",
				            "ConEVVEndTime",
				            "AideCode",
				            "AideName",
				            "AideSSN",
				            "ConAideCode",
				            "ConAideName",
				            "ConAideSSN",
				            "Office",
				            "ConOffice",
				            "PAdmissionID",
				            "PName",
				            "PAddressL1",
				            "PAddressL2",
				            "PCity",
				            "PAddressState",
				            "PZipCode",
				            "PCounty",
				            "PLongitude",
				            "PLatitude",
				            "ConPAdmissionID",
				            "ConPName",
				            "ConPAddressL1",
				            "ConPAddressL2",
				            "ConPCity",
				            "ConPAddressState",
				            "ConPZipCode",
				            "ConPCounty",
				            "ConPLongitude",
				            "ConPLatitude",
				            "Contract",
				            "ConContract",
				            "BilledDate",
				            "ConBilledDate",
				            "BilledHours",
				            "ConBilledHours",
				            "Billed",
				            "ConBilled",
				            "MinuteDiffBetweenSch",
				            "DistanceMilesFromLatLng",
				            "AverageMilesPerHour",
				            "ETATravleMinutes",
				            "InserviceStartDate",
				            "InserviceEndDate",
				            "PTOStartDate",
				            "PTOEndDate",
				            "SameSchTimeFlag",
				            "SameVisitTimeFlag",
				            "SchAndVisitTimeSameFlag",
				            "SchOverAnotherSchTimeFlag",
				            "VisitTimeOverAnotherVisitTimeFlag",
				            "SchTimeOverVisitTimeFlag",
				            "DistanceFlag",
				            "InServiceFlag",
				            "PTOFlag",
				            "StatusFlag",
				            "ConStatusFlag",
				            "AideFName",
				            "AideLName",
				            "ConAideFName",
				            "ConAideLName",
				            "PFName",
				            "PLName",
				            "ConPFName",
				            "ConPLName",
				            "PMedicaidNumber",
				            "ConPMedicaidNumber",
				            "PayerState",
				            "ConPayerState",
				            "AgencyContact",
				            "ConAgencyContact",
				            "AgencyPhone",
				            "ConAgencyPhone",
				            "LastUpdatedBy",
				            "ConLastUpdatedBy",
				            "LastUpdatedDate",
				            "ConLastUpdatedDate",
				            "BilledRate",
				            "TotalBilledAmount",
				            "ConBilledRate",
				            "ConTotalBilledAmount",
				            "IsMissed",
				            "MissedVisitReason",
				            "EVVType",
				            "ConIsMissed",
				            "ConMissedVisitReason",
				            "ConEVVType",
				            "PStatus",
				            "ConPStatus",
				            "AideStatus",
				            "ConAideStatus",
				            "ConNoResponseFlag",
				            "ConNoResponseTitle",
				            "ConNoResponseNotes",
				            "P_PAdmissionID",
				            "P_PName",
				            "P_PAddressL1",
				            "P_PAddressL2",
				            "P_PCity",
				            "P_PAddressState",
				            "P_PZipCode",
				            "P_PCounty",
				            "P_PFName",
				            "P_PLName",
				            "P_PMedicaidNumber",
				            "ConP_PAdmissionID",
				            "ConP_PName",
				            "ConP_PAddressL1",
				            "ConP_PAddressL2",
				            "ConP_PCity",
				            "ConP_PAddressState",
				            "ConP_PZipCode",
				            "ConP_PCounty",
				            "ConP_PFName",
				            "ConP_PLName",
				            "ConP_PMedicaidNumber",
				            "PA_PAdmissionID",
				            "PA_PName",
				            "PA_PAddressL1",
				            "PA_PAddressL2",
				            "PA_PCity",
				            "PA_PAddressState",
				            "PA_PZipCode",
				            "PA_PCounty",
				            "PA_PFName",
				            "PA_PLName",
				            "PA_PMedicaidNumber",
				            "ConPA_PAdmissionID",
				            "ConPA_PName",
				            "ConPA_PAddressL1",
				            "ConPA_PAddressL2",
				            "ConPA_PCity",
				            "ConPA_PAddressState",
				            "ConPA_PZipCode",
				            "ConPA_PCounty",
				            "ConPA_PFName",
				            "ConPA_PLName",
				            "ConPA_PMedicaidNumber",
				            "ContractType",
				            "ConContractType",
				            "BillRateNonBilled",
				            "ConBillRateNonBilled",
				            "BillRateBoth",
				            "ConBillRateBoth",
				            "FederalTaxNumber",
				            "ConFederalTaxNumber",
							"FlagForReview",
							"FlagForReviewDate",
							"ConFlagForReview",
							"ConFlagForReviewDate",
							"ConInserviceStartDate",
							"ConInserviceEndDate",
							"ConPTOStartDate",
							"ConPTOEndDate"
				        )
				    )
				)
				
				SELECT 
				    U.LHID,
				    LF."LogID",
				    '''' as "OldValue",    
				    U.column_value as "NewValue",
				    CASE 
				        WHEN LF."FieldFor" = ''P'' THEN U."PVisitID"
				        WHEN LF."FieldFor" = ''C'' THEN U."CVisitID"
				    END as "VisitID",
				    CASE 
				        WHEN LF."FieldFor" = ''P'' THEN U."PAppVisitID"
				        WHEN LF."FieldFor" = ''C'' THEN U."CAppVisitID"
				    END as "AppVisitID"
				FROM UnpivotedData U
				JOIN LogFields LF ON LF."FieldName" = U.column_name
            `
        });
        insertLogValues.execute();

        return ''Records inserted successfully'';
    }
    catch (err) {
		var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
	  // If an error occurs, capture it and raise it with a custom message
	  throw ''ERROR: '' + err.message;  // Returns the error message to the caller
    }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_NOTIFICATIONS_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	
	    // Create table if not exists
	    var tableName = "NOTIFICATIONS";
	    var table_command = `
	   	CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.NOTIFICATIONS (
	  	ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
		CONFLICTID NUMBER(38,0) DEFAULT NULL,
		"ProviderID" VARCHAR(50) DEFAULT NULL,
		"AppProviderID" VARCHAR(50) DEFAULT NULL,
		"NotificationType" VARCHAR(50) DEFAULT NULL,
		"CreatedDate" DATE DEFAULT NULL,
		"CreatedDateTime" TIMESTAMP DEFAULT NULL,
		"ReadUnreadFlag" NUMBER(3,0) DEFAULT NULL,
		"Contract" VARCHAR(100) DEFAULT NULL,
		PRIMARY KEY (ID))`;

		var tableName1 = "PAYER_PROVIDER_REMINDERS";
	    var table_command1 = `
	   	CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PAYER_PROVIDER_REMINDERS (
	  	ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
		"PayerID" VARCHAR(50) DEFAULT NULL,
		"AppPayerID" VARCHAR(50) DEFAULT NULL,
		"Contract" VARCHAR(100) DEFAULT NULL,
		"ProviderID" VARCHAR(50) DEFAULT NULL,
		"AppProviderID" VARCHAR(50) DEFAULT NULL,
		"ProviderName" VARCHAR(100) DEFAULT NULL,
		"CreatedDateTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		"NumberOfDays" NUMBER(38,0) DEFAULT NULL,
		PRIMARY KEY (ID))`;

		var tableNamea1 = "CONFLICT_COMMU_INTERS";
	    var table_commanda1 = `
	   	CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.CONFLICT_COMMU_INTERS (
	  	"id" NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
		"CONFLICTID" NUMBER(38,0) DEFAULT NULL COMMENT ''For provider internal notes'',
		"GroupID" NUMBER(38,0) DEFAULT NULL COMMENT ''for payer login internalnotes'',
		"ReverseUUID" VARCHAR(100) DEFAULT NULL COMMENT ''For communication'',
		"Description" VARCHAR(1000) DEFAULT NULL,
		"CommentType" NUMBER(3,0) DEFAULT NULL COMMENT ''1 = Communications 2 = Internal Notes'',
		"Attachmenturl" VARCHAR(500) DEFAULT NULL,
		"communications_type" NUMBER(3,0) DEFAULT NULL COMMENT ''1 = Provider 2 = Payer'',
		"created_at" TIMESTAMP DEFAULT NULL,
		"updated_at" TIMESTAMP DEFAULT NULL,
		"created_by" NUMBER(38,0) DEFAULT NULL,
		"updated_by" NUMBER(38,0) DEFAULT NULL,
		"created_by_name" VARCHAR(200) DEFAULT NULL,
		"updated_by_name" VARCHAR(200) DEFAULT NULL,
		"OriginalFileName" VARCHAR(200) DEFAULT NULL,
		"FileSize" NUMBER(38,0) DEFAULT NULL,
		PRIMARY KEY ("id"))`;

	try {
	    snowflake.execute({sqlText: table_command});
	    snowflake.execute({sqlText: table_command1});
	    snowflake.execute({sqlText: table_commanda1});
	    
	    return "Table " + tableName + ", "+tableName1+" and "+tableNamea1+" created or already exists.";
	} catch (err) {
	    return "Error: " + err.message;
	}
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_PAYER_DASHBOARD_TABLE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var SQL1 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_TOP (
        PAYERID VARCHAR(50) DEFAULT NULL,
        STATUS VARCHAR(20) DEFAULT NULL,
        STATUS_VALUE VARCHAR(50) DEFAULT NULL,        
        SEVEN_TO NUMBER(38,0) DEFAULT NULL,
        SEVEN_SP NUMBER(38,2) DEFAULT NULL,
        SEVEN_OP NUMBER(38,2) DEFAULT NULL,
        SEVEN_FP NUMBER(38,2) DEFAULT NULL,
        THIRTY_TO NUMBER(38,0) DEFAULT NULL,
        THIRTY_SP NUMBER(38,2) DEFAULT NULL,
        THIRTY_OP NUMBER(38,2) DEFAULT NULL,
        THIRTY_FP NUMBER(38,2) DEFAULT NULL,        
        SIXTY_TO NUMBER(38,0) DEFAULT NULL,
        SIXTY_SP NUMBER(38,2) DEFAULT NULL,
        SIXTY_OP NUMBER(38,2) DEFAULT NULL,
        SIXTY_FP NUMBER(38,2) DEFAULT NULL,        
        NINETY_TO NUMBER(38,0) DEFAULT NULL,
        NINETY_SP NUMBER(38,2) DEFAULT NULL,
        NINETY_OP NUMBER(38,2) DEFAULT NULL,
        NINETY_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL2 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP (
        PAYERID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
		CONTYPE VARCHAR(50) DEFAULT NULL,
		CONTYPES VARCHAR(50) DEFAULT NULL,
        CO_TO NUMBER(38,0) DEFAULT NULL,
	    CO_SP NUMBER(38,2) DEFAULT NULL,
	    CO_OP NUMBER(38,2) DEFAULT NULL,
	    CO_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL3 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY (
        PAYERID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        PROVIDERID VARCHAR(50) DEFAULT NULL,
        P_NAME VARCHAR(100) DEFAULT NULL,
        TIN VARCHAR(100) DEFAULT NULL,
        CON_TO NUMBER(38,0) DEFAULT NULL,
	    CON_SP NUMBER(38,2) DEFAULT NULL,
	    CON_OP NUMBER(38,2) DEFAULT NULL,
	    CON_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL4 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER (
        PAYERID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        CAREGIVERID VARCHAR(50) DEFAULT NULL,
        C_NAME VARCHAR(100) DEFAULT NULL,
		C_LNAME VARCHAR(100) DEFAULT NULL,
		C_FNAME VARCHAR(100) DEFAULT NULL,
        CON_TO NUMBER(38,0) DEFAULT NULL,
	    CON_SP NUMBER(38,2) DEFAULT NULL,
	    CON_OP NUMBER(38,2) DEFAULT NULL,
	    CON_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL5 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT (
        PAYERID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        PATIENTID VARCHAR(50) DEFAULT NULL,
        PFNAME VARCHAR(100) DEFAULT NULL,
        PLNAME VARCHAR(100) DEFAULT NULL,
        PNAME VARCHAR(100) DEFAULT NULL,
        ADMISSIONID VARCHAR(100) DEFAULT NULL,
        CON_TO NUMBER(38,0) DEFAULT NULL,
	    CON_SP NUMBER(38,2) DEFAULT NULL,
	    CON_OP NUMBER(38,2) DEFAULT NULL,
	    CON_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL6 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER (
        PAYERID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        CONPAYERID VARCHAR(50) DEFAULT NULL,
        PNAME VARCHAR(100) DEFAULT NULL,
        CON_TO NUMBER(38,0) DEFAULT NULL,
	    CON_SP NUMBER(38,2) DEFAULT NULL,
	    CON_OP NUMBER(38,2) DEFAULT NULL,
	    CON_FP NUMBER(38,2) DEFAULT NULL
      )`;

  try {
      snowflake.execute({ sqlText: SQL1 });
      snowflake.execute({ sqlText: SQL2 });
      snowflake.execute({ sqlText: SQL3 });
      snowflake.execute({ sqlText: SQL4 });
      snowflake.execute({ sqlText: SQL5 });
      snowflake.execute({ sqlText: SQL6 });
      return "Table created successfully (or already exists).";
  } catch (err) {
      throw "ERROR: " + err.message;
  }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_PROCEDURE_LOG_HISTORY_VALUES_TEMP()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {
	    // Create table if not exists
	    var tableName = "LOG_HISTORY_VALUES_TEMP";
	    var table_command = `
	   CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.LOG_HISTORY_VALUES_TEMP (
	  	CONID NUMBER(38,0) NOT NULL,
        "LogID" NUMBER(38,0) DEFAULT NULL,
        "OldValue" VARCHAR(16777216) DEFAULT NULL,
        "NewValue" VARCHAR(16777216) DEFAULT NULL,
        "VisitID" VARCHAR(50) DEFAULT NULL,
        "AppVisitID" VARCHAR(50) DEFAULT NULL)`;
	    snowflake.execute({sqlText: table_command});
	    
	    return "Table " + tableName + " created or already exists.";
	} catch (err) {
	    throw "ERROR: " + err.message;
	}
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_PROCEDURE_TASK_HISTORY()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {
	    // Create table if not exists
	    var tableName = "TASK_HISTORY_LIST";
	    var table_command = `
	   CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.TASK_HISTORY_LIST (
	  	ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
		"NAME" VARCHAR(100) DEFAULT NULL,
		"UNNAME" VARCHAR(100) DEFAULT NULL,
		"EMAILSENT" VARCHAR(10) DEFAULT NULL, 
		"CREATED_AT" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (ID))`;
	    snowflake.execute({sqlText: table_command});
	    
	    return "Table " + tableName + " created or already exists.";
	} catch (err) {
	    throw "ERROR: " + err.message;
	}
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_PROVIDER_DASHBOARD_TABLE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var SQL1 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_TOP (
        PROVIDERID VARCHAR(50) DEFAULT NULL,
        OFFICEID VARCHAR(50) DEFAULT NULL,
        TODAYTOTAL NUMBER(38,0) DEFAULT NULL,
        TODAYSHIFTPRICE NUMBER(38,2) DEFAULT NULL,
        TODAYOVERLAPPRICE NUMBER(38,2) DEFAULT NULL,
        SEVENTOTAL NUMBER(38,0) DEFAULT NULL,
        SEVENFINALPRICE NUMBER(38,2) DEFAULT NULL,
        THIRTYTOTAL NUMBER(38,0) DEFAULT NULL,
        THIRTYFINALPRICE NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL2 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CON_TYP (
        PROVIDERID VARCHAR(50) DEFAULT NULL,
        OFFICEID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        EX_ST_MATCH_TO NUMBER(38,0) DEFAULT NULL,
	    EX_ST_MATCH_SP NUMBER(38,2) DEFAULT NULL,
	    EX_ST_MATCH_OP NUMBER(38,2) DEFAULT NULL,
	    EX_ST_MATCH_FP NUMBER(38,2) DEFAULT NULL,
	    EX_VT_MATCH_TO NUMBER(38,0) DEFAULT NULL,
	    EX_VT_MATCH_SP NUMBER(38,2) DEFAULT NULL,
	    EX_VT_MATCH_OP NUMBER(38,2) DEFAULT NULL,
	    EX_VT_MATCH_FP NUMBER(38,2) DEFAULT NULL,	
	    EX_ST_VT_MATCH_TO NUMBER(38,0) DEFAULT NULL,
	    EX_ST_VT_MATCH_SP NUMBER(38,2) DEFAULT NULL,
	    EX_ST_VT_MATCH_OP NUMBER(38,2) DEFAULT NULL,
	    EX_ST_VT_MATCH_FP NUMBER(38,2) DEFAULT NULL,	
	    ST_OVR_TO NUMBER(38,0) DEFAULT NULL,
	    ST_OVR_SP NUMBER(38,2) DEFAULT NULL,
	    ST_OVR_OP NUMBER(38,2) DEFAULT NULL,
	    ST_OVR_FP NUMBER(38,2) DEFAULT NULL,	
	    VT_OVR_TO NUMBER(38,0) DEFAULT NULL,
	    VT_OVR_SP NUMBER(38,2) DEFAULT NULL,
	    VT_OVR_OP NUMBER(38,2) DEFAULT NULL,
	    VT_OVR_FP NUMBER(38,2) DEFAULT NULL,	
	    ST_VT_OVR_TO NUMBER(38,0) DEFAULT NULL,
	    ST_VT_OVR_SP NUMBER(38,2) DEFAULT NULL,
	    ST_VT_OVR_OP NUMBER(38,2) DEFAULT NULL,
	    ST_VT_OVR_FP NUMBER(38,2) DEFAULT NULL,	
	    TD_TO NUMBER(38,0) DEFAULT NULL,
	    TD_SP NUMBER(38,2) DEFAULT NULL,
	    TD_OP NUMBER(38,2) DEFAULT NULL,
	    TD_FP NUMBER(38,2) DEFAULT NULL,	
	    IN_TO NUMBER(38,0) DEFAULT NULL,
	    IN_SP NUMBER(38,2) DEFAULT NULL,
	    IN_OP NUMBER(38,2) DEFAULT NULL,
	    IN_FP NUMBER(38,2) DEFAULT NULL,	
	    PT_TO NUMBER(38,0) DEFAULT NULL,
	    PT_SP NUMBER(38,2) DEFAULT NULL,
	    PT_OP NUMBER(38,2) DEFAULT NULL,
	    PT_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL3 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_AGENCY (
        PROVIDERID VARCHAR(50) DEFAULT NULL,
        OFFICEID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        CONPROVIDERID VARCHAR(50) DEFAULT NULL,
        CON_P_NAME VARCHAR(100) DEFAULT NULL,
        CON_TIN VARCHAR(100) DEFAULT NULL,
        CON_TO NUMBER(38,0) DEFAULT NULL,
	    CON_SP NUMBER(38,2) DEFAULT NULL,
	    CON_OP NUMBER(38,2) DEFAULT NULL,
	    CON_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL4 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CAREGIVER (
        PROVIDERID VARCHAR(50) DEFAULT NULL,
        OFFICEID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        CAREGIVERID VARCHAR(50) DEFAULT NULL,
        C_CODE VARCHAR(100) DEFAULT NULL,
        C_NAME VARCHAR(100) DEFAULT NULL,
        CON_TO NUMBER(38,0) DEFAULT NULL,
	    CON_SP NUMBER(38,2) DEFAULT NULL,
	    CON_OP NUMBER(38,2) DEFAULT NULL,
	    CON_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL5 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_PATIENT (
        PROVIDERID VARCHAR(50) DEFAULT NULL,
        OFFICEID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        PATIENTID VARCHAR(50) DEFAULT NULL,
        PFNAME VARCHAR(100) DEFAULT NULL,
        PLNAME VARCHAR(100) DEFAULT NULL,
        PNAME VARCHAR(100) DEFAULT NULL,
        CON_TO NUMBER(38,0) DEFAULT NULL,
	    CON_SP NUMBER(38,2) DEFAULT NULL,
	    CON_OP NUMBER(38,2) DEFAULT NULL,
	    CON_FP NUMBER(38,2) DEFAULT NULL
      )`;

	
  var SQL6 = `CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_PAYER (
        PROVIDERID VARCHAR(50) DEFAULT NULL,
        OFFICEID VARCHAR(50) DEFAULT NULL,
        CRDATEUNIQUE DATE DEFAULT NULL,
        PAYERID VARCHAR(50) DEFAULT NULL,
        PNAME VARCHAR(100) DEFAULT NULL,
        CON_TO NUMBER(38,0) DEFAULT NULL,
	    CON_SP NUMBER(38,2) DEFAULT NULL,
	    CON_OP NUMBER(38,2) DEFAULT NULL,
	    CON_FP NUMBER(38,2) DEFAULT NULL
      )`;

  try {
      snowflake.execute({ sqlText: SQL1 });
      snowflake.execute({ sqlText: SQL2 });
      snowflake.execute({ sqlText: SQL3 });
      snowflake.execute({ sqlText: SQL4 });
      snowflake.execute({ sqlText: SQL5 });
      snowflake.execute({ sqlText: SQL6 });
      return "Table created successfully (or already exists).";
  } catch (err) {
      throw "ERROR: " + err.message;
  }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.CREATE_SETTINGS_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var tableName = "SETTINGS";
  var sql_command = `
      CREATE TABLE IF NOT EXISTS CONFLICTREPORT.PUBLIC.SETTINGS (
          "ExtraDistance" NUMBER DEFAULT 25,
          "ExtraDistancePer" NUMBER(38,2) DEFAULT 1.25,
          "NORESPONSELIMITTIME" NUMBER DEFAULT 0,
          "UpdateCronFlag" NUMBER DEFAULT NULL,
          "InsertCronFlag" NUMBER DEFAULT NULL,
          "ConflictIDFlag" NUMBER DEFAULT NULL,
          "GroupIDFlag" NUMBER DEFAULT NULL,
          "VisitHistoryFlag" NUMBER DEFAULT NULL,
		  "LastLoadDate" TIMESTAMP DEFAULT NULL
      )
  `;
	var sql_command2 = `UPDATE CONFLICTREPORT.PUBLIC.SETTINGS SET "UpdateCronFlag" = NULL, "InsertCronFlag" = NULL, "ConflictIDFlag" = NULL, "GroupIDFlag" = NULL, "VisitHistoryFlag" = NULL`;
   	try {
		snowflake.execute({sqlText: sql_command});
		snowflake.execute({sqlText: sql_command2});
        return "Table " + tableName + " created or updated successfully.";
	 } catch (err) {
	     return "Error: " + err.message;
	 }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.FETCH_LOG_FIELDS()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	var resultLog = [];
	var getFieldsStmt = snowflake.createStatement({
	    sqlText: ''SELECT ID, "FieldName" FROM CONFLICTREPORT."PUBLIC".LOG_FIELDS''
	});
	var fieldsRs = getFieldsStmt.execute();
	
	while (fieldsRs.next()) {
	    var row = {};
	    for (var j = 1; j <= fieldsRs.getColumnCount(); j++) {
	        var fieldName = fieldsRs.getColumnValue(2); // Assuming FieldName is in the second column
	        var id = fieldsRs.getColumnValue(1); // Assuming ID is in the first column
	        row[fieldName] = id; // Store the key-value pair in the row object
	    }
	    resultLog.push(row); // Push the row object to the resultLog array (if you want an array of objects)
	}

	var FieldName = ''PTOFlag'';
	var objWithID = resultLog.find(obj => obj.hasOwnProperty(FieldName));
	var FieldID = '''';
	if(typeof objWithID !== ''undefined''){
	    var FieldID = objWithID ? objWithID[FieldName] : '''';
	}

	return FieldID;
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.INSERT_CONFLICTS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {
	
	var table_command2 = `INSERT INTO CONFLICTREPORT."PUBLIC".PAYER_PROVIDER_REMINDERS
		("PayerID", "AppPayerID", "Contract", "ProviderID", "AppProviderID", "ProviderName", "CreatedDateTime", "NumberOfDays")
		SELECT DISTINCT DPP."Payer Id" AS "PayerID", DPP."Application Payer Id" AS "AppPayerID", DPA."Payer Name" AS "Contract", DPP."Provider Id" AS "ProviderID", DPP."Application Provider Id" AS "AppProviderID", DP."Provider Name" AS "ProviderName", CURRENT_TIMESTAMP AS "CreatedDateTime", CAST(NULL AS NUMBER) "NumberOfDays"
		FROM ANALYTICS.BI.DIMPROVIDER AS DP
		INNER JOIN ANALYTICS.BI.DIMPAYERPROVIDER AS DPP ON CONCAT(DPP."Provider Id" , ''~'', DPP."Application Provider Id") = CONCAT(DP."Provider Id", ''~'', DP."Application Provider Id")
		INNER JOIN ANALYTICS.BI.DIMPAYER AS DPA ON CONCAT(DPA."Payer Id" , ''~'', DPA."Application Payer Id") = CONCAT(DPP."Payer Id", ''~'', DPP."Application Payer Id")
		WHERE NOT EXISTS (
		    SELECT 1 
		    FROM CONFLICTREPORT."PUBLIC".PAYER_PROVIDER_REMINDERS AS PPR_N 
		    WHERE CONCAT(PPR_N."PayerID", ''~'', PPR_N."AppPayerID") = CONCAT(DPP."Payer Id", ''~'', DPP."Application Payer Id")
		    AND CONCAT(PPR_N."ProviderID", ''~'', PPR_N."AppProviderID") = CONCAT(DPP."Provider Id" , ''~'', DPP."Application Provider Id")
		)`;
	
	var table_command3 = `UPDATE CONFLICTREPORT."PUBLIC".PAYER_PROVIDER_REMINDERS AS PPR
		SET 
		    PPR."Contract" = DPA."Payer Name",
		    PPR."ProviderName" = DP."Provider Name"
		FROM ANALYTICS.BI.DIMPROVIDER AS DP
		INNER JOIN ANALYTICS.BI.DIMPAYERPROVIDER AS DPP 
		    ON CONCAT(DPP."Provider Id", ''~'', DPP."Application Provider Id") = CONCAT(DP."Provider Id", ''~'', DP."Application Provider Id")
		INNER JOIN ANALYTICS.BI.DIMPAYER AS DPA 
		    ON CONCAT(DPA."Payer Id", ''~'', DPA."Application Payer Id") = CONCAT(DPP."Payer Id", ''~'', DPP."Application Payer Id")
		WHERE 
		    PPR."PayerID" = DPP."Payer Id" 
		    AND PPR."AppPayerID" = DPP."Application Payer Id"
		    AND PPR."ProviderID" = DPP."Provider Id"
		    AND PPR."AppProviderID" = DPP."Application Provider Id"`;


  	var sql_query = `INSERT INTO CONFLICTREPORT."PUBLIC".CONFLICTS
		(CONFLICTID, "CreatedDate")
		SELECT cvm."CONFLICTID", CURRENT_DATE
		FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS cvm
		WHERE NOT EXISTS (
		    SELECT 1
		    FROM CONFLICTREPORT.PUBLIC.CONFLICTS c
		    WHERE cvm."CONFLICTID" = c."CONFLICTID"
		)
		AND cvm."CONFLICTID" IS NOT NULL
		GROUP BY cvm."CONFLICTID"
		ORDER BY cvm."CONFLICTID"`;
                   
	var sql_queryseconds1 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
		SET CF."StatusFlag" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN ''D''
				WHEN CVM."IsMissed" = TRUE THEN ''R''
				ELSE CF."StatusFlag"
			END,
			CF."ResolveDate" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
				WHEN CVM."IsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
				ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
			END,
			CF."ResolvedBy" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
				WHEN CVM."IsMissed" = TRUE THEN COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
				ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
			END
		FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
		WHERE CVM.CONFLICTID = CF.CONFLICTID`;
		
	var sql_queryseconds2 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
		SET CVM."StatusFlag" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN ''D''
				WHEN CVM."ConIsMissed" = TRUE THEN ''R''
				ELSE CVM."StatusFlag"
			END,
			CVM."ResolveDate" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
				WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
				ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
			END,
			CVM."ResolvedBy" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
				WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
				ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
			END`;	

	var sql_queryseconds2_A = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
            SET CF."StatusFlag" = ''U''
            WHERE CF.CONFLICTID IN (
                SELECT CF.CONFLICTID 
                FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF 
                INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
                WHERE CF."StatusFlag" NOT IN (''D'', ''I'', ''W'', ''U'') AND CVM."StatusFlag" IN(''U'') AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
                GROUP BY CF.CONFLICTID
            )`;
			
	var sql_queryseconds3 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
		SET CVM."StatusFlag" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN ''D''
				WHEN CVM."ConIsMissed" = TRUE THEN ''R''
				ELSE ''R''
			END,
			CVM."ResolveDate" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
				WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
				ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
			END,
			CVM."ResolvedBy" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
				WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
				ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
			END
		WHERE CVM.CONFLICTID IN (
			SELECT CF.CONFLICTID 
			FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF 
			INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
			WHERE CF."StatusFlag" IN (''R'', ''D'') 
			GROUP BY CF.CONFLICTID 
			HAVING COUNT(CVM.ID) = 1
		)`;
			
	var sql_queryseconds4 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
		SET CF."StatusFlag" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN ''D''
				WHEN CVM."IsMissed" = TRUE THEN ''R''
				ELSE ''R''
			END,
			CF."ResolveDate" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
				WHEN CVM."IsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
				ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
			END,
			CF."ResolvedBy" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
				WHEN CVM."IsMissed" = TRUE THEN COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
				ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
			END
		FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
		WHERE CVM.CONFLICTID = CF.CONFLICTID AND CF.CONFLICTID IN(
			SELECT 
				DISTINCT CVM.CONFLICTID
			FROM 
				CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
			WHERE 
				CVM.CONFLICTID IN (
					SELECT DISTINCT CVM.CONFLICTID
					FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM WHERE CVM."StatusFlag" IN(''R'', ''D'')
				)
				GROUP BY CVM.CONFLICTID
				HAVING COUNT(CVM.ID) = 1
		)`;
			
	var sql_queryseconds5 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
		SET CVM."StatusFlag" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN ''D''
				WHEN CVM."ConIsMissed" = TRUE THEN ''R''
				ELSE ''R''
			END,
			CVM."ResolveDate" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
				WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
				ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
			END,
			CVM."ResolvedBy" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
				WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
				ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
			END
		WHERE CVM.CONFLICTID IN (
		  SELECT CF.CONFLICTID
		  FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF
		  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID
		  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID AND CVM1."StatusFlag" IN(''R'', ''D'')
		  WHERE CF."StatusFlag" IN(''R'', ''D'')
		  GROUP BY CF.CONFLICTID
		  HAVING COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID)
		)`;
			
	var sql_queryseconds6 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
		SET CVM."StatusFlag" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN ''D''
				WHEN CVM."ConIsMissed" = TRUE THEN ''R''
				ELSE ''R''
			END,
			CVM."ResolveDate" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
				WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
				ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
			END,
			CVM."ResolvedBy" = CASE 
				WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
				WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
				ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
			END
		WHERE CVM.CONFLICTID IN (
		  SELECT CF.CONFLICTID
		  FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF
		  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID
		  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID AND CVM1."StatusFlag" IN(''R'', ''D'')
		  WHERE CF."StatusFlag" IN(''R'', ''D'')
		  GROUP BY CF.CONFLICTID
		  HAVING COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID) OR (COUNT(DISTINCT CVM.ID)-1) = COUNT(DISTINCT CVM1.ID)
		)`;
			
	var sql_queryseconds7 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
		SET CF."StatusFlag" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN ''D''
				WHEN CVM."IsMissed" = TRUE THEN ''R''
				ELSE ''R''
			END,
			CF."ResolveDate" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
				WHEN CVM."IsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
				ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
			END,
			CF."ResolvedBy" = CASE 
				WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
				WHEN CVM."IsMissed" = TRUE THEN COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
				ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
			END
		FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
		WHERE CVM.CONFLICTID = CF.CONFLICTID AND CF.CONFLICTID IN (
		  SELECT CF.CONFLICTID
		  FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF
		  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID
		  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID AND CVM1."StatusFlag" IN(''R'', ''D'')
		  WHERE CF."StatusFlag" NOT IN(''R'', ''D'')
		  GROUP BY CF.CONFLICTID
		  HAVING COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID) AND COUNT(DISTINCT CVM.ID) > 0 AND COUNT(DISTINCT CVM1.ID) > 0
		)`;
			
	var sql_queryseconds8 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
		SET CF."StatusFlag" = CASE 
				WHEN CF."NoResponseFlag" = ''Yes'' THEN ''N''
				ELSE CF."StatusFlag"
			END,
			CF."ResolveDate" = NULL,
			CF."ResolvedBy" = NULL
		FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM WHERE CVM.CONFLICTID = CF.CONFLICTID AND CF."StatusFlag" IN (''U'', ''N'', ''W'', ''I'')`;
			
	var sql_queryseconds9 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
		SET CVM."StatusFlag" = 
			CASE 
				WHEN CVM."ConNoResponseFlag" = ''Yes'' THEN ''N''
				ELSE CVM."StatusFlag"
			END,
			CVM."ResolveDate" = NULL,
			CVM."ResolvedBy" = NULL
		WHERE CVM."StatusFlag" IN (''U'', ''N'', ''W'', ''I'')`;	

	var sql_queryseconds10 = `INSERT INTO CONFLICTREPORT."PUBLIC".NOTIFICATIONS (CONFLICTID, "ProviderID", "AppProviderID", "NotificationType", "CreatedDate", "CreatedDateTime")
		SELECT DISTINCT C.CONFLICTID, CVM."ProviderID", CVM."AppProviderID", ''New Conflict'' AS "NotificationType", CURRENT_DATE AS "CreatedDate", CURRENT_TIMESTAMP AS "CreatedDateTime" FROM CONFLICTREPORT."PUBLIC".CONFLICTS AS C 
		INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM.CONFLICTID = C.CONFLICTID WHERE DATE(C.RECORDEDDATETIME) = CURRENT_DATE AND NOT EXISTS (
	        SELECT 1 
	        FROM CONFLICTREPORT.PUBLIC.NOTIFICATIONS AS N 
	        WHERE N.CONFLICTID = C.CONFLICTID
	        AND N."ProviderID" = CVM."ProviderID"
	        AND N."AppProviderID" = CVM."AppProviderID" AND "NotificationType" = ''New Conflict''
	    )`;		

	var sql_queryseconds11 = `INSERT INTO CONFLICTREPORT."PUBLIC".NOTIFICATIONS (CONFLICTID, "ProviderID", "AppProviderID", "NotificationType", "CreatedDate", "CreatedDateTime")
		SELECT DISTINCT C.CONFLICTID, CVM."ProviderID", CVM."AppProviderID", ''Resolved'' AS "NotificationType", CURRENT_DATE AS "CreatedDate", CURRENT_TIMESTAMP AS "CreatedDateTime"
		FROM CONFLICTREPORT."PUBLIC".CONFLICTS AS C
		INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM.CONFLICTID = C.CONFLICTID
		WHERE DATE(C."ResolveDate") = CURRENT_DATE AND C."StatusFlag" IN(''R'', ''D'') AND NOT EXISTS (
	        SELECT 1 
	        FROM CONFLICTREPORT.PUBLIC.NOTIFICATIONS AS N 
	        WHERE N.CONFLICTID = C.CONFLICTID
	        AND N."ProviderID" = CVM."ProviderID"
	        AND N."AppProviderID" = CVM."AppProviderID" AND "NotificationType" = ''Resolved''
	    ) HAVING (SELECT COUNT(ID) AS TOTAL FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS WHERE CONFLICTID = C.CONFLICTID) = (SELECT COUNT(ID) AS TOTAL FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS WHERE CONFLICTID = C.CONFLICTID AND "StatusFlag" IN(''R'', ''D''))`;

  	var stmt2 = snowflake.createStatement({sqlText: table_command2});
    var res2 = stmt2.execute();
    var stmt3 = snowflake.createStatement({sqlText: table_command3});
    var res3 = stmt3.execute();
    var stmt = snowflake.createStatement({sqlText: sql_query});
    var res = stmt.execute();
      var queryseconds_stmt1 = snowflake.createStatement({sqlText: sql_queryseconds1});
    var queryseconds_res1 = queryseconds_stmt1.execute();
    var queryseconds_stmt2 = snowflake.createStatement({sqlText: sql_queryseconds2});
    var queryseconds_res2 = queryseconds_stmt2.execute();	
    var queryseconds_stmt2_A = snowflake.createStatement({sqlText: sql_queryseconds2_A});
    var queryseconds_res2_A = queryseconds_stmt2_A.execute();
    var queryseconds_stmt3 = snowflake.createStatement({sqlText: sql_queryseconds3});
    var queryseconds_res3 = queryseconds_stmt3.execute();
    var queryseconds_stmt4 = snowflake.createStatement({sqlText: sql_queryseconds4});
    var queryseconds_res4 = queryseconds_stmt4.execute();
    var queryseconds_stmt5 = snowflake.createStatement({sqlText: sql_queryseconds5});
    var queryseconds_res5 = queryseconds_stmt5.execute();
    var queryseconds_stmt6 = snowflake.createStatement({sqlText: sql_queryseconds6});
    var queryseconds_res6 = queryseconds_stmt6.execute();
    var queryseconds_stmt7 = snowflake.createStatement({sqlText: sql_queryseconds7});
    var queryseconds_res7 = queryseconds_stmt7.execute();
    var queryseconds_stmt8 = snowflake.createStatement({sqlText: sql_queryseconds8});
    var queryseconds_res8 = queryseconds_stmt8.execute();
    var queryseconds_stmt9 = snowflake.createStatement({sqlText: sql_queryseconds9});
    var queryseconds_res9 = queryseconds_stmt9.execute();
    var queryseconds_stmt10 = snowflake.createStatement({sqlText: sql_queryseconds10});
    var queryseconds_res10 = queryseconds_stmt10.execute();
    var queryseconds_stmt11 = snowflake.createStatement({sqlText: sql_queryseconds11});
    var queryseconds_res11 = queryseconds_stmt11.execute();

    return "Success";
  } catch (err) {

	var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
	snowflake.execute({ sqlText: updatesetting });
	  // If an error occurs, capture it and raise it with a custom message
	  throw ''ERROR: '' + err.message;  // Returns the error message to the caller
  }
 ';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  try 
    {
        var sql_query = `INSERT INTO CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS
    ("CONFLICTID", "SSN", "ProviderID", "AppProviderID", "ProviderName", 
    "VisitID", "AppVisitID", "ConProviderID", "ConAppProviderID", "ConProviderName",
    "ConVisitID", "ConAppVisitID", "VisitDate", "SchStartTime", "SchEndTime", "ConSchStartTime", 
    "ConSchEndTime", "VisitStartTime", "VisitEndTime", "ConVisitStartTime", "ConVisitEndTime", 
    "EVVStartTime", "EVVEndTime", "ConEVVStartTime", "ConEVVEndTime", "CaregiverID", "AppCaregiverID", 
    "AideCode", "AideName", "AideSSN", "ConCaregiverID", "ConAppCaregiverID", "ConAideCode", "ConAideName",
    "ConAideSSN", "OfficeID", "AppOfficeID", "Office", "ConOfficeID", "ConAppOfficeID", "ConOffice", "PatientID",
    "AppPatientID", "PAdmissionID", "PName", "PAddressID", "PAppAddressID", "PAddressL1", "PAddressL2", "PCity", 
    "PAddressState", "PZipCode", "PCounty", "PLongitude", "PLatitude", "ConPatientID", "ConAppPatientID", "ConPAdmissionID", 
    "ConPName", "ConPAddressID", "ConPAppAddressID", "ConPAddressL1", "ConPAddressL2", "ConPCity", "ConPAddressState",
    "ConPZipCode", "ConPCounty", "ConPLongitude", "ConPLatitude", "PayerID", "AppPayerID", "Contract", "ConPayerID", "ConAppPayerID",
    "ConContract", "BilledDate", "ConBilledDate", "BilledHours", "ConBilledHours", "Billed", "ConBilled", "MinuteDiffBetweenSch", 
    "DistanceMilesFromLatLng", "AverageMilesPerHour", "ETATravleMinutes", "ServiceCodeID", "AppServiceCodeID", "RateType", 
    "ServiceCode", "ConServiceCodeID", "ConAppServiceCodeID", "ConRateType", "ConServiceCode", "SameSchTimeFlag", "SameVisitTimeFlag",
    "SchAndVisitTimeSameFlag", "SchOverAnotherSchTimeFlag", "VisitTimeOverAnotherVisitTimeFlag", "SchTimeOverVisitTimeFlag", "DistanceFlag",
    "InServiceFlag", "PTOFlag", "AideFName", "AideLName", "ConAideFName", "ConAideLName", "PFName", "PLName", "ConPFName", "ConPLName", 
    "PMedicaidNumber", "ConPMedicaidNumber", "PayerState", "ConPayerState", "AgencyContact", "ConAgencyContact", "AgencyPhone",
    "ConAgencyPhone", "LastUpdatedBy", "ConLastUpdatedBy", "LastUpdatedDate", "ConLastUpdatedDate", "BilledRate", "TotalBilledAmount", 
    "ConBilledRate", "ConTotalBilledAmount", "IsMissed", "MissedVisitReason", "EVVType", "ConIsMissed", "ConMissedVisitReason", 
    "ConEVVType", "PStatus", "ConPStatus", "AideStatus", "ConAideStatus", "P_PatientID", "P_AppPatientID", "ConP_PatientID",
    "ConP_AppPatientID", "PA_PatientID", "PA_AppPatientID", "ConPA_PatientID", "ConPA_AppPatientID", "P_PAdmissionID", "P_PName",
    "P_PAddressID", "P_PAppAddressID", "P_PAddressL1", "P_PAddressL2", "P_PCity", "P_PAddressState", "P_PZipCode", "P_PCounty",
    "P_PFName", "P_PLName", "P_PMedicaidNumber", "ConP_PAdmissionID", "ConP_PName", "ConP_PAddressID", "ConP_PAppAddressID", 
    "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity", "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty", "ConP_PFName",
    "ConP_PLName", "ConP_PMedicaidNumber", "PA_PAdmissionID", "PA_PName", "PA_PAddressID", "PA_PAppAddressID", "PA_PAddressL1",
    "PA_PAddressL2", "PA_PCity", "PA_PAddressState", "PA_PZipCode", "PA_PCounty", "PA_PFName", "PA_PLName", "PA_PMedicaidNumber",
    "ConPA_PAdmissionID", "ConPA_PName", "ConPA_PAddressID", "ConPA_PAppAddressID", "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity", 
    "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty", "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber", "ContractType",
    "ConContractType", "P_PStatus", "ConP_PStatus", "PA_PStatus", "ConPA_PStatus", "BillRateNonBilled", "ConBillRateNonBilled",
    "BillRateBoth", "ConBillRateBoth", "FederalTaxNumber", "ConFederalTaxNumber")
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
            ABS(DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime")) AS "MinuteDiffBetweenSch",
            ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer", 2) AS "DistanceMilesFromLatLng",
            MPH."AverageMilesPerHour",
            ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) as "ETATravleMinutes",
            V1."ServiceCodeID" AS "ServiceCodeID",
          V1."AppServiceCodeID" AS "AppServiceCodeID",
          V1."RateType" AS "RateType",
          V1."ServiceCode" AS "ServiceCode",          
          V2."ServiceCodeID" AS "ConServiceCodeID",
          V2."AppServiceCodeID" AS "ConAppServiceCodeID",
          V2."RateType" AS "ConRateType",
          V2."ServiceCode" AS "ConServiceCode",
             CASE WHEN V1."ProviderID" != V2."ProviderID" AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") = CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") AND V1."VisitDate" >= CURRENT_DATE THEN ''Y'' ELSE ''N'' END AS "SameSchTimeFlag",
             CASE WHEN V1."ProviderID" != V2."ProviderID" AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL AND CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime") = CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime") THEN ''Y'' ELSE ''N'' END AS "SameVisitTimeFlag",
             CASE WHEN V1."ProviderID" != V2."ProviderID" AND ((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") = CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")) OR (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL AND CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") = CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime"))) THEN ''Y'' ELSE ''N'' END AS "SchVisitTimeSame",
            CASE WHEN V1."ProviderID" != V2."ProviderID" AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL AND (V1."SchStartTime" < V2."SchEndTime" AND V1."SchEndTime" > V2."SchStartTime") AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") != CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") AND V1."VisitDate" >= CURRENT_DATE THEN ''Y'' ELSE ''N'' END AS "SchOverAnotherSchTimeFlag",
            CASE WHEN V1."ProviderID" != V2."ProviderID" AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL AND (V1."VisitStartTime" < V2."VisitEndTime" AND V1."VisitEndTime" > V2."VisitStartTime") AND CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime") != CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime") THEN ''Y'' ELSE ''N'' END AS "VisitTimeOverAnotherVisitTimeFlag",
			 CASE WHEN V1."ProviderID" != V2."ProviderID" AND (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL AND ( V1."SchStartTime" < V2."VisitEndTime" AND V1."SchEndTime" > V2."VisitStartTime" ) AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") != CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")) OR (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL AND ( V2."SchStartTime" < V1."VisitEndTime" AND V2."SchEndTime" > V1."VisitStartTime" ) AND CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") != CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime")) THEN ''Y'' ELSE ''N'' END AS "SchTimeOverVisitTimeFlag",
             CASE WHEN V1."ProviderID" != V2."ProviderID"
       AND
       V1."Longitude" IS NOT NULL
       AND
       V1."Latitude" IS NOT NULL
       AND
       V2."Longitude" IS NOT NULL
       AND
       V2."Latitude" IS NOT NULL
       AND
       V1."VisitStartTime" IS NOT NULL
       AND 
       V1."VisitEndTime" IS NOT NULL
       AND 
       V2."VisitStartTime" IS NOT NULL
       AND 
       V2."VisitEndTime" IS NOT NULL
       AND
       (
         (
           V1."P_PZipCode" IS NOT NULL
           AND
           V2."P_PZipCode" IS NOT NULL
           AND
           V1."P_PZipCode" != V2."P_PZipCode"
         )
         OR
         (
           V1."P_PZipCode" IS NULL
           OR
           V2."P_PZipCode" IS NULL
         )
       )
       AND
       MPH."AverageMilesPerHour" IS NOT NULL
       AND
       (
         (DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime") > 0
          AND ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) > DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime"))
         OR
         (DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime") > 0
          AND ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V2."Longitude", V2."Latitude"), ST_MAKEPOINT(V1."Longitude", V1."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) > DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime"))
       ) THEN ''Y'' ELSE ''N'' END AS "DistanceFlag",
      ''N'' AS "InServiceFlag",
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
            SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty",
			CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL 
			AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(
			SPLIT(
			CR1."Call Out GPS Coordinates",
			'','')[1], ''"'', CAST(NULL AS NUMBER)) 
			WHEN CR1."Call In GPS Coordinates" IS NOT NULL
			AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(
			SPLIT(
			CR1."Call In GPS Coordinates",
			'','')[1], ''"'', CAST(NULL AS NUMBER)) 
			ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(NULL AS TIMESTAMP) "InserviceStartDate", CAST(NULL AS TIMESTAMP) "InserviceEndDate", CAST(NULL AS STRING) "AppCaregiverInserviceID", CAST(NULL AS TIMESTAMP) "PTOStartDate", CAST(NULL AS TIMESTAMP) "PTOEndDate", CAST(NULL AS STRING) "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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
            WHERE CR1."Is Missed" = FALSE AND DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND 
            --CR1."Application Provider Id" IN (651, 891, 532, 7646) AND
            CR1."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
        ) AS V1
        LEFT JOIN
        (
            SELECT DISTINCT CAST(NULL AS NUMBER) "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") AS "SSN",  CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName",  CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID",  CAST(NULL AS STRING) "PAdmissionID",  CAST(NULL AS STRING) "PName",  CAST(NULL AS STRING) "PFName",  CAST(NULL AS STRING) "PLName",  CAST(NULL AS STRING) "PMedicaidNumber",  CAST(NULL AS STRING) "PAddressID",  CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(NULL AS TIMESTAMP) "InserviceStartDate", CAST(NULL AS TIMESTAMP) "InserviceEndDate", CAST(NULL AS STRING) "AppCaregiverInserviceID", CAST(NULL AS STRING) "FCSVisitID", CAST(NULL AS STRING) "FCSAppVisitID", CAST(NULL AS STRING) "FCAVisitID", CAST(NULL AS STRING) "FCAAppVisitID", CAST(NULL AS TIMESTAMP) "PTOStartDate", CAST(NULL AS TIMESTAMP) "PTOEndDate", CAST(NULL AS STRING) "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1 
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
        LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
            LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
        WHERE CR1."Is Missed" = FALSE AND DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND 
            --CR1."Application Provider Id" IN (651, 891, 532, 7646) AND
            CR1."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
        ) AS V2 ON V1.SSN = V2.SSN AND V1."VisitDate" = V2."VisitDate" AND V1."ProviderID" != V2."ProviderID" AND V1."VisitID" != V2."VisitID"
        CROSS JOIN CONFLICTREPORT."PUBLIC"."SETTINGS" AS SETT
        LEFT JOIN CONFLICTREPORT."PUBLIC".MPH AS MPH ON ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer", 2) BETWEEN MPH."From" AND MPH."To"
        WHERE (
         --SameSchTimeFlag  RULE 1
           (
         V1."VisitStartTime" IS NULL
         AND
         V1."VisitEndTime" IS NULL
         AND
         V2."VisitStartTime" IS NULL
         AND
         V2."VisitEndTime" IS NULL
         AND
         CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") = CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime")
         AND
         V1."VisitDate" >= CURRENT_DATE
           )
           OR
      --SameVisitTimeFlag Rule 2
           (
         V1."VisitStartTime" IS NOT NULL
         AND
         V1."VisitEndTime" IS NOT NULL
         AND
         V2."VisitStartTime" IS NOT NULL
         AND
         V2."VisitEndTime" IS NOT NULL
         AND
         CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime") = CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")
           )
           OR
      --SchVisitTimeSame Rule 3
           ((V2."VisitStartTime" IS NULL
			 	AND
			 	V2."VisitEndTime" IS NULL
			 	AND
			 	V1."VisitStartTime" IS NOT NULL
			 	AND
			 	V1."VisitEndTime" IS NOT NULL
			 	AND
			 	CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") = CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime")
				)OR(
			 	V1."VisitStartTime" IS NULL
			 	AND
			 	V1."VisitEndTime" IS NULL
			 	AND
			 	V2."VisitStartTime" IS NOT NULL
			 	AND
			 	V2."VisitEndTime" IS NOT NULL
			 	AND
			 	CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") = CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")
       		 ))
           OR
            --SchOverAnotherSchTimeFlag    Rule 4
           (
        V1."VisitStartTime" IS NULL
        AND
        V1."VisitEndTime" IS NULL
        AND
        V2."VisitStartTime" IS NULL
        AND
        V2."VisitEndTime" IS NULL
             AND
        (
          V1."SchStartTime" < V2."SchEndTime"
          AND
          V1."SchEndTime" > V2."SchStartTime"
        )
        AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") != CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime")
        AND
        V1."VisitDate" >= CURRENT_DATE
           )
           OR
            --VisitTimeOverAnotherVisitTimeFlag    Rule 5
           (
        V1."VisitStartTime" IS NOT NULL
        AND
        V1."VisitEndTime" IS NOT NULL
        AND
        V2."VisitStartTime" IS NOT NULL
        AND
        V2."VisitEndTime" IS NOT NULL
        AND
             (
          V1."VisitStartTime" < V2."VisitEndTime"
          AND
          V1."VisitEndTime" > V2."VisitStartTime"
        )
        AND CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime") != CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")                   
           )
           OR
      --SchTimeOverVisitTimeFlag    Rule 6
           ((V1."VisitStartTime" IS NULL 
         AND
         V1."VisitEndTime" IS NULL
         AND
         V2."VisitStartTime" IS NOT NULL
         AND
         V2."VisitEndTime" IS NOT NULL
         AND
         (
           V1."SchStartTime" < V2."VisitEndTime"
           AND
           V1."SchEndTime" > V2."VisitStartTime"
         )
        AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") != CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")
           )
		   OR
		   (
         V2."VisitStartTime" IS NULL 
         AND
         V2."VisitEndTime" IS NULL
         AND
         V1."VisitStartTime" IS NOT NULL
         AND
         V1."VisitEndTime" IS NOT NULL
         AND
         (
           V2."SchStartTime" < V1."VisitEndTime"
           AND
           V2."SchEndTime" > V1."VisitStartTime"
         )
        AND CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") != CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime")
           ))      
		  OR 
      -- DistanceFlag Rule 7   
            (
              V1."Longitude" IS NOT NULL
         AND
          V1."Latitude" IS NOT NULL
         AND
          V2."Longitude" IS NOT NULL
         AND
          V2."Latitude" IS NOT NULL
         AND
          V1."VisitStartTime" IS NOT NULL
         AND 
          V1."VisitEndTime" IS NOT NULL
         AND 
          V2."VisitStartTime" IS NOT NULL
         AND 
          V2."VisitEndTime" IS NOT NULL
         AND
          (
             (
               V1."P_PZipCode" IS NOT NULL
             AND
             V2."P_PZipCode" IS NOT NULL
             AND
             V1."P_PZipCode" != V2."P_PZipCode"
           )
           OR
           (
             V1."P_PZipCode" IS NULL
             OR
             V2."P_PZipCode" IS NULL
           )
         )
         AND
          MPH."AverageMilesPerHour" IS NOT NULL
         AND
         (
           (DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime") > 0
            AND ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) > DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime"))
           OR
           (DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime") > 0
            AND ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V2."Longitude", V2."Latitude"), ST_MAKEPOINT(V1."Longitude", V1."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) > DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime"))
         )
            )
        )
        AND  
        NOT EXISTS (
          SELECT 1
          FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
          WHERE 
            NVL(NULLIF(CVM."VisitID", ''''), ''9999999'') = NVL(NULLIF(V1."VisitID", ''''), ''9999999'')
            AND
            NVL(NULLIF(CVM."ConVisitID", ''''), ''9999999'') = NVL(NULLIF(V2."VisitID", ''''), ''9999999'')          
      )`;
        snowflake.execute({sqlText: sql_query});
   
        return "Procedure executed successfully.";
    } catch (err) {
    var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
    snowflake.execute({ sqlText: updatesetting });
    // If an error occurs, capture it and raise it with a custom message
    throw "ERROR: " + err.message;  // Returns the error message to the caller
    }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_1()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {
	return true;
  var sql_query_reverse_pto = `INSERT INTO CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS ("CONFLICTID", "SSN", "ProviderID", "AppProviderID", "ProviderName", "VisitID", "AppVisitID", "ConProviderID", "ConAppProviderID", "ConProviderName", "ConVisitID", "ConAppVisitID", "VisitDate", "SchStartTime", "SchEndTime", "ConSchStartTime", "ConSchEndTime", "VisitStartTime", "VisitEndTime", "ConVisitStartTime", "ConVisitEndTime", "EVVStartTime", "EVVEndTime", "ConEVVStartTime", "ConEVVEndTime", "CaregiverID", "AppCaregiverID", "AideCode", "AideName", "AideSSN", "ConCaregiverID", "ConAppCaregiverID", "ConAideCode", "ConAideName", "ConAideSSN", "OfficeID", "AppOfficeID", "Office", "ConOfficeID", "ConAppOfficeID", "ConOffice", "PatientID", "AppPatientID", "PAdmissionID", "PName", "PAddressID", "PAppAddressID", "PAddressL1", "PAddressL2", "PCity", "PAddressState", "PZipCode", "PCounty", "PLongitude", "PLatitude", "ConPatientID", "ConAppPatientID", "ConPAdmissionID", "ConPName", "ConPAddressID", "ConPAppAddressID", "ConPAddressL1", "ConPAddressL2", "ConPCity", "ConPAddressState", "ConPZipCode", "ConPCounty", "ConPLongitude", "ConPLatitude", "PayerID", "AppPayerID", "Contract", "ConPayerID", "ConAppPayerID", "ConContract", "BilledDate", "ConBilledDate", "BilledHours", "ConBilledHours", "Billed", "ConBilled", "MinuteDiffBetweenSch", "DistanceMilesFromLatLng", "AverageMilesPerHour", "ETATravleMinutes", "ServiceCodeID", "AppServiceCodeID", "RateType", "ServiceCode", "ConServiceCodeID", "ConAppServiceCodeID", "ConRateType", "ConServiceCode", "SameSchTimeFlag", "SameVisitTimeFlag", "SchAndVisitTimeSameFlag", "SchOverAnotherSchTimeFlag", "VisitTimeOverAnotherVisitTimeFlag", "SchTimeOverVisitTimeFlag", "DistanceFlag", "InServiceFlag", "PTOFlag", "AideFName", "AideLName", "ConAideFName", "ConAideLName", "PFName", "PLName", "ConPFName", "ConPLName", "PMedicaidNumber", "ConPMedicaidNumber", "PayerState", "ConPayerState", "AgencyContact", "ConAgencyContact", "AgencyPhone", "ConAgencyPhone", "LastUpdatedBy", "ConLastUpdatedBy", "LastUpdatedDate", "ConLastUpdatedDate", "BilledRate", "TotalBilledAmount", "ConBilledRate", "ConTotalBilledAmount", "IsMissed", "MissedVisitReason", "EVVType", "ConIsMissed", "ConMissedVisitReason", "ConEVVType", "PStatus", "ConPStatus", "AideStatus", "ConAideStatus", "P_PatientID", "P_AppPatientID", "ConP_PatientID", "ConP_AppPatientID", "PA_PatientID", "PA_AppPatientID", "ConPA_PatientID", "ConPA_AppPatientID", "P_PAdmissionID", "P_PName", "P_PAddressID", "P_PAppAddressID", "P_PAddressL1", "P_PAddressL2", "P_PCity", "P_PAddressState", "P_PZipCode", "P_PCounty", "P_PFName", "P_PLName", "P_PMedicaidNumber", "ConP_PAdmissionID", "ConP_PName", "ConP_PAddressID", "ConP_PAppAddressID", "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity", "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty", "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber", "PA_PAdmissionID", "PA_PName", "PA_PAddressID", "PA_PAppAddressID", "PA_PAddressL1", "PA_PAddressL2", "PA_PCity", "PA_PAddressState", "PA_PZipCode", "PA_PCounty", "PA_PFName", "PA_PLName", "PA_PMedicaidNumber", "ConPA_PAdmissionID", "ConPA_PName", "ConPA_PAddressID", "ConPA_PAppAddressID", "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity", "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty", "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber", "ContractType", "ConContractType", "P_PStatus", "ConP_PStatus", "PA_PStatus", "ConPA_PStatus", "BillRateNonBilled", "ConBillRateNonBilled", "BillRateBoth", "ConBillRateBoth", "FederalTaxNumber", "ConFederalTaxNumber", "PTOStartDate", "PTOEndDate", "ConPTOStartDate", "ConPTOEndDate")
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
			''N'' AS "InServiceFlag",
			''Y'' AS "PTOFlag",
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
			V1."PTOStartDate" AS "PTOStartDate",
			V1."PTOEndDate" AS "PTOEndDate",
			V2."PTOStartDate" AS "ConPTOStartDate",
			V2."PTOEndDate" AS "ConPTOEndDate"
		FROM
       (SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) as "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(NULL AS TIMESTAMP) "InserviceStartDate", CAST(NULL AS TIMESTAMP) "InserviceEndDate", CAST(NULL AS STRING) "AppCaregiverInserviceID", CAST(FCA."Start Date" AS timestamp) AS "PTOStartDate", CAST(FCA."End Date" AS timestamp) AS "PTOEndDate", CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1 
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
        
		LEFT JOIN ANALYTICS.BI.FACTCAREGIVERABSENCE AS FCA ON FCA."Global Caregiver Id" = CR1."Caregiver Id" AND FCA."Provider Id" = CR1."Provider Id" AND (CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL AND CAST(CR1."Visit Start Time" AS DATE) <= CAST(FCA."End Date" AS DATE) AND CAST(CR1."Visit End Time" AS DATE) >= CAST(FCA."Start Date" AS DATE))

        WHERE CR1."Is Missed" = FALSE AND FCA."Caregiver Vacation Id" IS NULL AND DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND 
        
        CR1."Visit Start Time" IS NOT NULL
        AND
        CR1."Visit End Time" IS NOT NULL
        AND
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
		MD5(CONCAT(''P'', CAST(FCA."Caregiver Vacation Id" AS VARCHAR))) AS "VisitID", 
		CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "AppVisitID",
		CAST(FCA."Start Date" AS DATE) AS "VisitDate", 
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
		CAST(NULL AS TIMESTAMP) "InserviceStartDate",
		CAST(NULL AS TIMESTAMP) "InserviceEndDate",
		CAST(NULL AS STRING) "AppCaregiverInserviceID",
		CAST(FCA."Start Date" AS timestamp) AS "PTOStartDate",
		CAST(FCA."End Date" AS timestamp) AS "PTOEndDate",
		CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "PTOVacationID",
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
	   ANALYTICS.BI.FACTCAREGIVERABSENCE AS FCA
	   
	   INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = FCA."Global Caregiver Id" AND TRIM(CAR."SSN") IS NOT NULL AND TRIM(CAR."SSN")!=''''

	   INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = FCA."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
	   
	   LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = FCA."Office Id" AND DOF."Is Active" = TRUE

	   LEFT JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM1 ON CVM1."VisitID" = MD5(CONCAT(''P'', CAST(FCA."Caregiver Vacation Id" AS VARCHAR))) AND CVM1."CONFLICTID" IS NOT NULL
	   
	   WHERE CAST(FCA."Start Date" AS DATE) BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND 
       
       DPR."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
       ) AS V2 ON
       V1.SSN = V2.SSN
       AND 
	   (CAST(V1."VisitStartTime" AS DATE) <= CAST(V2."PTOEndDate" AS DATE) AND CAST(V1."VisitEndTime" AS DATE) >= CAST(V2."PTOStartDate" AS DATE)) AND V1."ProviderID" IS NOT NULL
		AND
		V1."AppProviderID" IS NOT NULL
		AND V2."ProviderID" != V1."ProviderID"
		AND
		V2."PTOVacationID" IS NOT NULL
		AND
		V1."PTOVacationID" IS NULL
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

        var insertpto = `
        INSERT INTO CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS ("CONFLICTID", "SSN", "ProviderID", "AppProviderID", "ProviderName", "VisitID", "AppVisitID", "ConProviderID", "ConAppProviderID", "ConProviderName", "ConVisitID", "ConAppVisitID", "VisitDate", "SchStartTime", "SchEndTime", "ConSchStartTime", "ConSchEndTime", "VisitStartTime", "VisitEndTime", "ConVisitStartTime", "ConVisitEndTime", "EVVStartTime", "EVVEndTime", "ConEVVStartTime", "ConEVVEndTime", "CaregiverID", "AppCaregiverID", "AideCode", "AideName", "AideSSN", "ConCaregiverID", "ConAppCaregiverID", "ConAideCode", "ConAideName", "ConAideSSN", "OfficeID", "AppOfficeID", "Office", "ConOfficeID", "ConAppOfficeID", "ConOffice", "PatientID", "AppPatientID", "PAdmissionID", "PName", "PAddressID", "PAppAddressID", "PAddressL1", "PAddressL2", "PCity", "PAddressState", "PZipCode", "PCounty", "PLongitude", "PLatitude", "ConPatientID", "ConAppPatientID", "ConPAdmissionID", "ConPName", "ConPAddressID", "ConPAppAddressID", "ConPAddressL1", "ConPAddressL2", "ConPCity", "ConPAddressState", "ConPZipCode", "ConPCounty", "ConPLongitude", "ConPLatitude", "PayerID", "AppPayerID", "Contract", "ConPayerID", "ConAppPayerID", "ConContract", "BilledDate", "ConBilledDate", "BilledHours", "ConBilledHours", "Billed", "ConBilled", "MinuteDiffBetweenSch", "DistanceMilesFromLatLng", "AverageMilesPerHour", "ETATravleMinutes", "InserviceStartDate", "InserviceEndDate", "PTOStartDate", "PTOEndDate", "ConPTOStartDate", "ConPTOEndDate", "ServiceCodeID", "AppServiceCodeID", "RateType", "ServiceCode", "ConServiceCodeID", "ConAppServiceCodeID", "ConRateType", "ConServiceCode", "SameSchTimeFlag", "SameVisitTimeFlag", "SchAndVisitTimeSameFlag", "SchOverAnotherSchTimeFlag", "VisitTimeOverAnotherVisitTimeFlag", "SchTimeOverVisitTimeFlag", "DistanceFlag", "InServiceFlag", "PTOFlag", "AideFName", "AideLName", "ConAideFName", "ConAideLName", "PFName", "PLName", "ConPFName", "ConPLName", "PMedicaidNumber", "ConPMedicaidNumber", "PayerState", "ConPayerState", "AgencyContact", "ConAgencyContact", "AgencyPhone", "ConAgencyPhone", "LastUpdatedBy", "ConLastUpdatedBy", "LastUpdatedDate", "ConLastUpdatedDate", "BilledRate", "TotalBilledAmount", "ConBilledRate", "ConTotalBilledAmount", "IsMissed", "MissedVisitReason", "EVVType", "ConIsMissed", "ConMissedVisitReason", "ConEVVType", "PStatus", "ConPStatus", "AideStatus", "ConAideStatus", "P_PatientID", "P_AppPatientID", "ConP_PatientID", "ConP_AppPatientID", "PA_PatientID", "PA_AppPatientID", "ConPA_PatientID", "ConPA_AppPatientID", "P_PAdmissionID", "P_PName", "P_PAddressID", "P_PAppAddressID", "P_PAddressL1", "P_PAddressL2", "P_PCity", "P_PAddressState", "P_PZipCode", "P_PCounty", "P_PFName", "P_PLName", "P_PMedicaidNumber", "ConP_PAdmissionID", "ConP_PName", "ConP_PAddressID", "ConP_PAppAddressID", "ConP_PAddressL1", "ConP_PAddressL2", "ConP_PCity", "ConP_PAddressState", "ConP_PZipCode", "ConP_PCounty", "ConP_PFName", "ConP_PLName", "ConP_PMedicaidNumber", "PA_PAdmissionID", "PA_PName", "PA_PAddressID", "PA_PAppAddressID", "PA_PAddressL1", "PA_PAddressL2", "PA_PCity", "PA_PAddressState", "PA_PZipCode", "PA_PCounty", "PA_PFName", "PA_PLName", "PA_PMedicaidNumber", "ConPA_PAdmissionID", "ConPA_PName", "ConPA_PAddressID", "ConPA_PAppAddressID", "ConPA_PAddressL1", "ConPA_PAddressL2", "ConPA_PCity", "ConPA_PAddressState", "ConPA_PZipCode", "ConPA_PCounty", "ConPA_PFName", "ConPA_PLName", "ConPA_PMedicaidNumber", "ContractType", "ConContractType", "P_PStatus", "ConP_PStatus", "PA_PStatus", "ConPA_PStatus", "BillRateNonBilled", "ConBillRateNonBilled", "BillRateBoth", "ConBillRateBoth", "FederalTaxNumber", "ConFederalTaxNumber")
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
            V1."PTOStartDate" AS "PTOStartDate",
			V1."PTOEndDate" AS "PTOEndDate",
            V2."PTOStartDate" AS "ConPTOStartDate",
			V2."PTOEndDate" AS "ConPTOEndDate",
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
            ''N'' AS "InServiceFlag",
            ''Y'' AS "PTOFlag",
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
		MD5(CONCAT(''P'', CAST(FCA."Caregiver Vacation Id" AS VARCHAR))) AS "VisitID", 
		CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "AppVisitID",
		CAST(FCA."Start Date" AS DATE) AS "VisitDate", 
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
		CAST(NULL AS TIMESTAMP) "InserviceStartDate",
		CAST(NULL AS TIMESTAMP) "InserviceEndDate",
		CAST(NULL AS STRING) "AppCaregiverInserviceID",
		CAST(FCA."Start Date" AS timestamp) AS "PTOStartDate",
		CAST(FCA."End Date" AS timestamp) AS "PTOEndDate",
		CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "PTOVacationID",
		CAST(NULL AS STRING) "ServiceCodeID",
		CAST(NULL AS STRING) "AppServiceCodeID",
		CAST(NULL AS STRING) "RateType",
		CAST(NULL AS STRING) "ServiceCode",
		CAST(NULL AS STRING) "LastUpdatedDate",
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
	   ANALYTICS.BI.FACTCAREGIVERABSENCE AS FCA
	   
	   INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = FCA."Global Caregiver Id" AND TRIM(CAR."SSN") IS NOT NULL AND TRIM(CAR."SSN")!=''''

	   INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = FCA."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
	   
	   LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = FCA."Office Id" AND DOF."Is Active" = TRUE

	   LEFT JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM1 ON CVM1."VisitID" = MD5(CONCAT(''P'', CAST(FCA."Caregiver Vacation Id" AS VARCHAR))) AND CVM1."CONFLICTID" IS NOT NULL
	   
	   WHERE CAST(FCA."Start Date" AS DATE) BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND 
       
       DPR."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
       ) AS V1
       INNER JOIN
       (
			SELECT DISTINCT CAST(NULL AS NUMBER) "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") AS "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(NULL AS TIMESTAMP) "InserviceStartDate", CAST(NULL AS TIMESTAMP) "InserviceEndDate", CAST(NULL AS STRING) "AppCaregiverInserviceID", CAST(FCA."Start Date" AS timestamp) AS "PTOStartDate", CAST(FCA."End Date" AS timestamp) AS "PTOEndDate", CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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
			LEFT JOIN ANALYTICS.BI.FACTCAREGIVERABSENCE AS FCA ON FCA."Global Caregiver Id" = CR1."Caregiver Id" AND FCA."Provider Id" = CR1."Provider Id" AND (CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL AND CAST(CR1."Visit Start Time" AS DATE) <= CAST(FCA."End Date" AS DATE) AND CAST(CR1."Visit End Time" AS DATE) >= CAST(FCA."Start Date" AS DATE))

			LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
			LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
			WHERE CR1."Is Missed" = FALSE AND CAST(FCA."Caregiver Vacation Id" AS VARCHAR) IS NULL AND DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
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
		AND (CAST(V2."VisitStartTime" AS DATE) <= CAST(V1."PTOEndDate" AS DATE) AND CAST(V2."VisitEndTime" AS DATE) >= CAST(V1."PTOStartDate" AS DATE)) AND V2."ProviderID" IS NOT NULL
				AND V1."ProviderID" != V2."ProviderID"
				AND
				V1."PTOVacationID" IS NOT NULL
				AND
				V2."PTOVacationID" IS NULL
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
    snowflake.execute({sqlText: insertpto});
    snowflake.execute({sqlText: sql_query_reverse_pto});   
    return "Procedure executed successfully.";
  } catch (err) {
		var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
	  // If an error occurs, capture it and raise it with a custom message
	  throw "ERROR: " + err.message;  // Returns the error message to the caller
  }
';

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

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.LOAD_PAYER_CONFLICT_SUMMARY()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	// Section 1: Truncate All Target Tables
    var SQL_TRUNCATE_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT;`;
	var SQL_TRUNCATE_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_CONFLICT_SUMMARY_IMPACT;`;
	
	// Section 2: Load COUNT Data
    var SQL_INSERT_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT (
			PAYERID, PROVIDERID,CONPAYERID,PROVIDER_NAME, TIN, CONTRACT, PATIENT_FNAME, PATIENT_LNAME, ADMISSIONID,
			CAREGIVER_NAME, CONTYPE, CONTYPEDESC, VISITDATE, CRDATEUNIQUE, STATUSFLAG, VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"ConPayerID" AS CONPAYERID,
			"ProviderName" AS PROVIDER_NAME,
			"FederalTaxNumber" AS TIN,
			"ConContract" AS CONTRACT,
			"PA_PFName" AS PATIENT_FNAME,
			"PA_PLName" AS PATIENT_LNAME,
			"PA_PAdmissionID" AS ADMISSIONID,
			MAX(CONCAT("AideFName",'' '', "AideLName")) AS CAREGIVER_NAME,
			CONTYPE,
			CASE 
				WHEN CONTYPE = ''only_to'' THEN ''Time Overlap Only''
				WHEN CONTYPE = ''only_td'' THEN ''Time Distance Only''
				WHEN CONTYPE = ''only_is'' THEN ''In Service Only''
				WHEN CONTYPE = ''both_to_td'' THEN ''Time Overlap and Time Distance''
				WHEN CONTYPE = ''both_to_is'' THEN ''Time Overlap and In Service''
				WHEN CONTYPE = ''both_td_is'' THEN ''Time Distance and In Service''
				WHEN CONTYPE = ''all_to_td_is'' THEN ''All Three (Time Overlap, Time Distance, and In Service)''
				ELSE NULL
			END AS CONTYPEDESC,
			"VisitDate" AS VISITDATE,
			"G_CRDATEUNIQUE"::DATE AS CRDATEUNIQUE,
			"StatusFlag" AS STATUSFLAG,
			VISIT_KEY
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "ProviderName" IS NOT NULL AND  "PA_PFName" IS NOT NULL AND "PA_PLName" IS NOT NULL AND "Contract" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"ConPayerID",
			"ProviderName",
			"FederalTaxNumber",
			"ConContract",
			"PA_PFName",
			"PA_PLName",
			"PA_PAdmissionID",
			CONTYPE,
			"VisitDate",
			"G_CRDATEUNIQUE",
			"StatusFlag",
			VISIT_KEY;
    `;
	
	// Section 2: Load IMPACT Data
    var SQL_INSERT_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_CONFLICT_SUMMARY_IMPACT (
			PAYERID, PROVIDERID,CONPAYERID,PROVIDER_NAME, TIN, CONTRACT, PATIENT_FNAME, PATIENT_LNAME, ADMISSIONID,
			CAREGIVER_NAME, CONTYPE, CONTYPEDESC, VISITDATE, CRDATEUNIQUE, STATUSFLAG, CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"ConPayerID" AS CONPAYERID,
			"ProviderName" AS PROVIDER_NAME,
			"FederalTaxNumber" AS TIN,
			"ConContract" AS CONTRACT,
			"PA_PFName" AS PATIENT_FNAME,
			"PA_PLName" AS PATIENT_LNAME,
			"PA_PAdmissionID" AS ADMISSIONID,
			MAX(CONCAT("AideFName",'' '', "AideLName")) AS CAREGIVER_NAME,
			CONTYPE,
			CASE 
				WHEN CONTYPE = ''only_to'' THEN ''Time Overlap Only''
				WHEN CONTYPE = ''only_td'' THEN ''Time Distance Only''
				WHEN CONTYPE = ''only_is'' THEN ''In Service Only''
				WHEN CONTYPE = ''both_to_td'' THEN ''Time Overlap and Time Distance''
				WHEN CONTYPE = ''both_to_is'' THEN ''Time Overlap and In Service''
				WHEN CONTYPE = ''both_td_is'' THEN ''Time Distance and In Service''
				WHEN CONTYPE = ''all_to_td_is'' THEN ''All Three (Time Overlap, Time Distance, and In Service)''
				ELSE NULL
			END AS CONTYPEDESC,
			"VisitDate" AS VISITDATE,
			"G_CRDATEUNIQUE"::DATE AS CRDATEUNIQUE,
			"StatusFlag" AS STATUSFLAG,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP			
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "ProviderName" IS NOT NULL AND  "PA_PFName" IS NOT NULL AND "PA_PLName" IS NOT NULL AND "Contract" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"ConPayerID",
			"ProviderName",
			"FederalTaxNumber",
			"ConContract",
			"PA_PFName",
			"PA_PLName",
			"PA_PAdmissionID",
			CONTYPE,
			"VisitDate",
			"G_CRDATEUNIQUE",
			"StatusFlag";
    `;
			
    try {
        // Execute all truncates first
        snowflake.execute({ sqlText: SQL_TRUNCATE_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_IMPACT });
		
        // Execute all inserts
        snowflake.execute({ sqlText: SQL_INSERT_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_IMPACT });

        return "PAYER Conflict Summary Data Loaded Successfully.";
    } catch (err) {
        throw "ERROR: " + err.message;
    }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.LOAD_PAYER_DASHBOARD_CHART_DATA()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	// Section 1: Truncate All Target Tables
    var SQL_TRUNCATE_CON_TYPE_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_CON_TYPE_COUNT;`;
	var SQL_TRUNCATE_CON_TYPE_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_CON_TYPE_IMPACT;`;
	var SQL_TRUNCATE_AGENCY_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_AGENCY_COUNT;`;
	var SQL_TRUNCATE_AGENCY_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_AGENCY_IMPACT;`;
	var SQL_TRUNCATE_PATIENT_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_PATIENT_COUNT;`;
	var SQL_TRUNCATE_PATIENT_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_PATIENT_IMPACT;`;
	var SQL_TRUNCATE_PAYER_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_PAYER_COUNT;`;
	var SQL_TRUNCATE_PAYER_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_PAYER_IMPACT;`;
	var SQL_TRUNCATE_CAREGIVER_COUNT= `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_CAREGIVER_COUNT;`;
	var SQL_TRUNCATE_CAREGIVER_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_CAREGIVER_IMPACT;`;
	var SQL_TRUNCATE_PAYER_PAYER_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_CHART_COUNT;`;
	var SQL_TRUNCATE_PAYER_PAYER_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_CHART_IMPACT;`;
	
	// Section 2: Load CON_TYPE_COUNT Data
    var SQL_INSERT_CON_TYPE_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_CON_TYPE_COUNT (
			PAYERID, PROVIDERID, VISITDATE, CONTYPE, CONTYPEDESC, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			CONTYPE,
			CASE 
				WHEN CONTYPE = ''only_to'' THEN ''Time Overlap Only''
				WHEN CONTYPE = ''only_td'' THEN ''Time Distance Only''
				WHEN CONTYPE = ''only_is'' THEN ''In Service Only''
				WHEN CONTYPE = ''both_to_td'' THEN ''Time Overlap and Time Distance''
				WHEN CONTYPE = ''both_to_is'' THEN ''Time Overlap and In Service''
				WHEN CONTYPE = ''both_td_is'' THEN ''Time Distance and In Service''
				WHEN CONTYPE = ''all_to_td_is'' THEN ''All Three (Time Overlap, Time Distance, and In Service)''
				ELSE NULL
			END AS CONTYPEDESC,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			VISIT_KEY,
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		GROUP BY 
			"PayerID",
			"ProviderID",
			"VisitDate",
			CONTYPE,
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode",
			VISIT_KEY;
    `;
	
	// Section 3: Load CON_TYPE_IMPACT Data
    var SQL_INSERT_CON_TYPE_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_CON_TYPE_IMPACT (
			PAYERID, PROVIDERID, VISITDATE, CONTYPE, CONTYPEDESC, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			CONTYPE,
			CASE 
				WHEN CONTYPE = ''only_to'' THEN ''Time Overlap Only''
				WHEN CONTYPE = ''only_td'' THEN ''Time Distance Only''
				WHEN CONTYPE = ''only_is'' THEN ''In Service Only''
				WHEN CONTYPE = ''both_to_td'' THEN ''Time Overlap and Time Distance''
				WHEN CONTYPE = ''both_to_is'' THEN ''Time Overlap and In Service''
				WHEN CONTYPE = ''both_td_is'' THEN ''Time Distance and In Service''
				WHEN CONTYPE = ''all_to_td_is'' THEN ''All Three (Time Overlap, Time Distance, and In Service)''
				ELSE NULL
			END AS CONTYPEDESC,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		GROUP BY 
			"PayerID",
			"ProviderID",
			"VisitDate",
			CONTYPE,
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode";
    `;
	// Section 4: Load AGENCY_COUNT Data
    var SQL_INSERT_AGENCY_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_AGENCY_COUNT (
			PAYERID, PROVIDERID, VISITDATE,P_NAME, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			"ProviderName" AS P_NAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			VISIT_KEY,
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "ProviderName" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"ProviderName",
			"VisitDate",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode",
			VISIT_KEY;
    `;
	// Section 5: Load AGENCY_IMPACT Data
    var SQL_INSERT_AGENCY_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_AGENCY_IMPACT (
			PAYERID, PROVIDERID, VISITDATE, P_NAME, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			"ProviderName" AS P_NAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "ProviderName" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"ProviderName",
			"VisitDate",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode";
    `;
	
	
	// Section 6: Load PATIENT_COUNT Data
    var SQL_INSERT_PATIENT_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_PATIENT_COUNT (
			PAYERID, PROVIDERID, VISITDATE, PATIENTID, PNAME, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			"PA_PatientID" AS PATIENTID, 
			"PA_PName" AS PNAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			VISIT_KEY,
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "PA_PName" IS NOT NULL 
		GROUP BY 
			"PayerID",
			"ProviderID",
			"VisitDate",
			"PA_PatientID",
			"PA_PName",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode",
			VISIT_KEY;
    `;
	
	// Section 7: Load PATIENT_IMPACT Data
    var SQL_INSERT_PATIENT_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_PATIENT_IMPACT (
			PAYERID, PROVIDERID, VISITDATE, PATIENTID, PNAME, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			"PA_PatientID" AS PATIENTID, 
			"PA_PName" AS PNAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "PA_PName" IS NOT NULL 
		GROUP BY 
			"PayerID",
			"ProviderID",
			"PA_PatientID",
			"PA_PName",
			"VisitDate",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode";
    `;
	// Section 8: Load PAYER_COUNT Data
    var SQL_INSERT_PAYER_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_PAYER_COUNT (
			PAYERID, PROVIDERID, VISITDATE,PNAME, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			"Contract" AS PNAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			VISIT_KEY,
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "Contract" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"VisitDate",
			"Contract",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode",
			VISIT_KEY;
    `;
	
	// Section 9: Load PAYER_IMPACT Data
    var SQL_INSERT_PAYER_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_PAYER_IMPACT (
			PAYERID, PROVIDERID, VISITDATE, PNAME, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			"Contract" AS PNAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "Contract" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"Contract",
			"VisitDate",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode";
    `;
	
	// Section 10: Load CAREGIVER_COUNT Data
    var SQL_INSERT_CAREGIVER_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_CAREGIVER_COUNT (
			PAYERID, PROVIDERID, VISITDATE, SSN, C_NAME, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			"SSN",
			MAX(CONCAT("AideFName",'' '', "AideLName")) AS C_NAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			VISIT_KEY,
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "SSN" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"VisitDate",
			"SSN",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode",
			VISIT_KEY;
    `;
	// Section 11: Load CAREGIVER_IMPACT Data
    var SQL_INSERT_CAREGIVER_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.STATE_DASHBOARD_CAREGIVER_IMPACT (
			PAYERID, PROVIDERID, VISITDATE, SSN, C_NAME, STATUSFLAG, COSTTYPE, VISITTYPE, COUNTY, SERVICECODE,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"ProviderID" AS PROVIDERID,
			"VisitDate",
			"SSN",
			MAX(CONCAT("AideFName",'' '', "AideLName")) AS C_NAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, '''') AS COUNTY,
			"ServiceCode" AS SERVICECODE,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "SSN" IS NOT NULL
		GROUP BY 
			"PayerID",
			"ProviderID",
			"VisitDate",
			"SSN",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			NULLIF(COUNTY, ''''),
			"ServiceCode";
    `;
	
	// Section 12: Load PAYER_PAYER_COUNT Data
	var SQL_INSERT_PAYER_PAYER_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_CHART_COUNT (
			PAYERID, VISITDATE, CONPAYERID,PNAME, STATUSFLAG, COSTTYPE, VISITTYPE, 
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate" AS VISITDATE,
			"ConPayerID" AS CONPAYERID,
			"ConContract" AS PNAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			VISIT_KEY
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "ConPayerID" != ''0'' AND "Contract" IS NOT NULL AND "ContractType" != ''Internal'' AND "ConContractType" != ''Internal''
		GROUP BY 
			"VisitDate",
			"PayerID",
			"ConPayerID",
			"ConContract",
			"StatusFlag",
			COSTTYPE,
			VISITTYPE,
			VISIT_KEY;
    `;
	// Section 9: Load PAYER__PAYER_IMPACT Data
    var SQL_INSERT_PAYER_PAYER_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_CHART_IMPACT (
			PAYERID,  VISITDATE,CONPAYERID, PNAME, STATUSFLAG, COSTTYPE, VISITTYPE,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate"  AS VISITDATE,
			"ConPayerID" AS CONPAYERID,
			"ConContract"  AS PNAME,
			"StatusFlag" AS STATUSFLAG,
			COSTTYPE,
			VISITTYPE,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "ConPayerID" != ''0'' AND "Contract" IS NOT NULL AND "ContractType" != ''Internal'' AND "ConContractType" != ''Internal''
		GROUP BY 
			"VisitDate","PayerID","ConPayerID",
			"ConContract", "StatusFlag",COSTTYPE,
			VISITTYPE;
    `;
	


		
    try {
        // Execute all truncates first
        snowflake.execute({ sqlText: SQL_TRUNCATE_CON_TYPE_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_CON_TYPE_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_AGENCY_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_AGENCY_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PATIENT_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PATIENT_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PAYER_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PAYER_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_CAREGIVER_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_CAREGIVER_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PAYER_PAYER_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PAYER_PAYER_IMPACT });
		
        // Execute all inserts
        snowflake.execute({ sqlText: SQL_INSERT_CON_TYPE_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_CON_TYPE_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_AGENCY_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_AGENCY_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_PATIENT_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_PATIENT_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_PAYER_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_PAYER_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_CAREGIVER_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_CAREGIVER_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_PAYER_PAYER_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_PAYER_PAYER_IMPACT });
		
		
        return "State Dashboard Data Loaded Successfully.";
    } catch (err) {
        throw "ERROR: " + err.message;
    }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.LOAD_PAYER_DASHBOARD_DATA()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	// Section 1: Truncate All Target Tables
    var SQL_TRUNCATE_CON_TYPE_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP_COUNT;`;
	var SQL_TRUNCATE_CON_TYPE_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP_IMPACT;`;
	var SQL_TRUNCATE_AGENCY_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY_COUNT;`;
	var SQL_TRUNCATE_AGENCY_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY_IMPACT;`;
	var SQL_TRUNCATE_PATIENT_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT_COUNT;`;
	var SQL_TRUNCATE_PATIENT_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT_IMPACT;`;
	var SQL_TRUNCATE_PAYER_COUNT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_COUNT;`;
	var SQL_TRUNCATE_PAYER_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_IMPACT;`;
	var SQL_TRUNCATE_CAREGIVER_COUNT= `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER_COUNT;`;
	var SQL_TRUNCATE_CAREGIVER_IMPACT = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER_IMPACT;`;
	
	// Section 2: Load CON_TYPE_COUNT Data
    var SQL_INSERT_CON_TYPE_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP_COUNT (
			PAYERID,
			VISITDATE,
			CONTYPE,
			CONTYPEDESC,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate" AS VISITDATE,
			CONTYPE,
			CASE 
				WHEN CONTYPE = ''only_to'' THEN ''Time Overlap Only''
				WHEN CONTYPE = ''only_td'' THEN ''Time Distance Only''
				WHEN CONTYPE = ''only_is'' THEN ''In Service Only''
				WHEN CONTYPE = ''both_to_td'' THEN ''Time Overlap and Time Distance''
				WHEN CONTYPE = ''both_to_is'' THEN ''Time Overlap and In Service''
				WHEN CONTYPE = ''both_td_is'' THEN ''Time Distance and In Service''
				WHEN CONTYPE = ''all_to_td_is'' THEN ''All Three (Time Overlap, Time Distance, and In Service)''
				ELSE NULL
			END AS CONTYPEDESC,
			VISIT_KEY
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		GROUP BY 
			"PayerID",
			"VisitDate" ,
			CONTYPE,
			VISIT_KEY;
    `;
	
	// Section 3: Load CON_TYPE_IMPACT Data
    var SQL_INSERT_CON_TYPE_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP_IMPACT (
			PAYERID, VISITDATE, CONTYPE, CONTYPEDESC,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate" AS VISITDATE,
			CONTYPE,
			CASE 
				WHEN CONTYPE = ''only_to'' THEN ''Time Overlap Only''
				WHEN CONTYPE = ''only_td'' THEN ''Time Distance Only''
				WHEN CONTYPE = ''only_is'' THEN ''In Service Only''
				WHEN CONTYPE = ''both_to_td'' THEN ''Time Overlap and Time Distance''
				WHEN CONTYPE = ''both_to_is'' THEN ''Time Overlap and In Service''
				WHEN CONTYPE = ''both_td_is'' THEN ''Time Distance and In Service''
				WHEN CONTYPE = ''all_to_td_is'' THEN ''All Three (Time Overlap, Time Distance, and In Service)''
				ELSE NULL
			END AS CONTYPEDESC,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		GROUP BY 
			"VisitDate",
			"PayerID",
			CONTYPE;
    `;
	// Section 4: Load AGENCY_COUNT Data
    var SQL_INSERT_AGENCY_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY_COUNT (
			PAYERID ,
			VISITDATE,
			PROVIDERID,
			P_NAME,
			TIN,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate"  AS VISITDATE,
			"ProviderID" AS PROVIDERID,
			"ProviderName" AS P_NAME,
			"FederalTaxNumber" AS TIN,
			VISIT_KEY
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "ProviderName" IS NOT NULL
		GROUP BY 
			"VisitDate",
			"PayerID",
			"ProviderID",
			"ProviderName",
			"FederalTaxNumber",
			VISIT_KEY;
    `;
	// Section 5: Load AGENCY_IMPACT Data
    var SQL_INSERT_AGENCY_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY_IMPACT (
			PAYERID,
			VISITDATE,PROVIDERID, P_NAME,TIN,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate"  AS VISITDATE,
			"ProviderID" AS PROVIDERID,			
			"ProviderName" AS P_NAME,
			"FederalTaxNumber" AS TIN,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "ProviderName" IS NOT NULL
		GROUP BY 
			"VisitDate",
			"PayerID",
			"ProviderID",
			"ProviderName",
			"FederalTaxNumber";
    `;
	
	
	// Section 6: Load PATIENT_COUNT Data
    var SQL_INSERT_PATIENT_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT_COUNT (
			PAYERID,
			VISITDATE,
			PATIENTID,
			PFNAME,
			PLNAME,
			PNAME,
			ADMISSIONID,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate" AS VISITDATE,
			"PA_PatientID" AS PATIENTID,
			"PA_PFName" AS PFNAME,
			"PA_PLName" AS PLNAME,
			"PA_PName" AS PNAME,
			"PA_PAdmissionID" AS ADMISSIONID,
			VISIT_KEY
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "PA_PName" IS NOT NULL
		GROUP BY
			"VisitDate",
			"PayerID",
			"PA_PatientID",
			"PA_PFName", 
			"PA_PLName",
			"PA_PName",
			"PA_PAdmissionID",
			VISIT_KEY;
    `;
	
	// Section 7: Load PATIENT_IMPACT Data
    var SQL_INSERT_PATIENT_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT_IMPACT (
			PAYERID, VISITDATE, PATIENTID,PFNAME,
			PLNAME, PNAME,ADMISSIONID,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate",
			"PA_PatientID" AS PATIENTID, 
			"PA_PFName" AS PFNAME,
			"PA_PLName" AS PLNAME,
			"PA_PName" AS PNAME,
			"PA_PAdmissionID" AS ADMISSIONID,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "PA_PName" IS NOT NULL
		GROUP BY 
			"VisitDate",
			"PayerID",
			"PA_PatientID",
			"PA_PFName", 
			"PA_PLName",
			"PA_PName",
			"PA_PAdmissionID";
    `;
	// Section 8: Load PAYER_COUNT Data
    var SQL_INSERT_PAYER_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_COUNT (
			PAYERID, VISITDATE, CONPAYERID,PNAME,
			VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate" AS VISITDATE,
			"ConPayerID" AS CONPAYERID,
			"ConContract" AS PNAME,
			VISIT_KEY
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "ConPayerID" != ''0'' AND "Contract" IS NOT NULL AND "ContractType" != ''Internal'' AND "ConContractType" != ''Internal''
		GROUP BY 
			"VisitDate",
			"PayerID",
			"ConPayerID",
			"ConContract",
			VISIT_KEY;
    `;
	
	// Section 9: Load PAYER_IMPACT Data
    var SQL_INSERT_PAYER_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_IMPACT (
			PAYERID,  VISITDATE,CONPAYERID, PNAME,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate"  AS VISITDATE,
			"ConPayerID" AS CONPAYERID,
			"ConContract"  AS PNAME,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE "ConPayerID" != ''0'' AND "Contract" IS NOT NULL AND "ContractType" != ''Internal'' AND "ConContractType" != ''Internal''
		GROUP BY 
			"VisitDate","PayerID","ConPayerID",
			"ConContract";
    `;
	
	// Section 10: Load CAREGIVER_COUNT Data
    var SQL_INSERT_CAREGIVER_COUNT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER_COUNT (
			PAYERID, VISITDATE, SSN, C_NAME, VISIT_KEY
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate"  AS VISITDATE,
			"SSN",
			MAX(CONCAT("AideFName",'' '', "AideLName")) AS C_NAME,
			VISIT_KEY
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "SSN" IS NOT NULL
		GROUP BY 
			"PayerID",
			"VisitDate",
			"SSN",
			VISIT_KEY;
    `;
	// Section 11: Load CAREGIVER_IMPACT Data
    var SQL_INSERT_CAREGIVER_IMPACT = `
		INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER_IMPACT (
			PAYERID, VISITDATE, SSN, C_NAME,
			CON_SP, CON_OP, CON_FP
		)
		SELECT 
			"PayerID" AS PAYERID,
			"VisitDate"  AS VISITDATE,
			"SSN",
			MAX(CONCAT("AideFName",'' '', "AideLName")) AS C_NAME,
			SUM(FULL_SHIFT_AMOUNT) AS CON_SP,
			SUM(OVERLAP_AMOUNT) AS CON_OP,
			SUM(CASE WHEN "StatusFlag" = ''R'' THEN OVERLAP_AMOUNT ELSE 0 END) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.DT_PAYER_CONFLICTS_COMMON
		WHERE  "SSN" IS NOT NULL
		GROUP BY
            "VisitDate",		
			"PayerID",
			"SSN";
    `;	
    try {
        // Execute all truncates first
        snowflake.execute({ sqlText: SQL_TRUNCATE_CON_TYPE_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_CON_TYPE_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_AGENCY_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_AGENCY_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PATIENT_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PATIENT_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PAYER_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_PAYER_IMPACT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_CAREGIVER_COUNT });
		snowflake.execute({ sqlText: SQL_TRUNCATE_CAREGIVER_IMPACT });
		
        // Execute all inserts
        snowflake.execute({ sqlText: SQL_INSERT_CON_TYPE_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_CON_TYPE_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_AGENCY_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_AGENCY_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_PATIENT_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_PATIENT_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_PAYER_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_PAYER_IMPACT });
		snowflake.execute({ sqlText: SQL_INSERT_CAREGIVER_COUNT });
		snowflake.execute({ sqlText: SQL_INSERT_CAREGIVER_IMPACT });
		
		
		
        return "Payer Dashboard Data Loaded Successfully.";
    } catch (err) {
        throw "ERROR: " + err.message;
    }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.LOAD_PAYER_DASHBOARD_DATA_NEW()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {
		// Step 1: TRUNCATE the table
		var truncate1Stmt = snowflake.createStatement({
			sqlText: `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP_NEW`
		});
		truncate1Stmt.execute();
		
		var truncate2Stmt = snowflake.createStatement({
			sqlText: `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY_NEW`
		});
		truncate2Stmt.execute();
		
		var truncate3Stmt = snowflake.createStatement({
			sqlText: `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT_NEW`
		});
		truncate3Stmt.execute();
		
		var truncate4Stmt = snowflake.createStatement({
			sqlText: `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_NEW`
		});
		truncate4Stmt.execute();
		
		var truncate5Stmt = snowflake.createStatement({
			sqlText: `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER_NEW`
		});
		truncate5Stmt.execute();
		
		// Step 2: Fetch payer IDs
		var payerStmt = snowflake.createStatement({
			sqlText: `
				SELECT DISTINCT V1."PayerID" AS APID
				FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
				INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 
					ON V2."CONFLICTID" = V1."CONFLICTID"
				INNER JOIN ANALYTICS.BI.DIMPAYER AS P 
					ON P."Payer Id" = V1."PayerID"
				WHERE P."Is Active" = TRUE 
					AND P."Is Demo" = FALSE
			`
		});

		var payerResult = payerStmt.execute();

		// Step 3: Loop through result set
		while (payerResult.next()) {
			var payerId = payerResult.getColumnValue(1);
			
			//-------------------------PAYER CON TYPE---------------------
			var insercontypes = `
				INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CON_TYP_NEW(
					PAYERID, CRDATEUNIQUE, CONTYPE, CONTYPES, STATUSFLAG, 
					COSTTYPE, VISITTYPE, CO_TO, CO_SP, CO_OP, CO_FP
				)
				SELECT *
				FROM (
					-- Time Overlap Query (Consolidated from ConflictTypeF 1-6)
					SELECT
						''${payerId}'' AS PAYERID,
						a."CRDATEUNIQUE" AS "CRDATEUNIQUE",
						''Time Overlap'' AS "ConflictType",
						''100'' AS "ConflictTypeF",
						a."StatusFlag" AS "STATUSFLAG",
						CASE 
							WHEN a."Billed" = ''yes'' THEN ''Recovery'' 
							ELSE ''Avoidance'' 
						END AS "COSTTYPE",
						CASE 
							WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
						END AS "VISITTYPE",
						COUNT(DISTINCT a."GroupID") AS "Total",
						SUM(
							CASE 
								WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND a."BILLABLEMINUTESFULLSHIFT" IS NOT NULL 
									THEN a."BILLABLEMINUTESFULLSHIFT" * a."BilledRateMinute" 
								WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 
									THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
								ELSE 0 
							END
						) AS "ShiftPrice",
						SUM(
						  CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') 
							THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							ELSE 0 
						  END
						) AS "OverlapPrice",
						SUM(
						  CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND a."StatusFlag" = ''R'' AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."StatusFlag" = ''R'' AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							ELSE 0 
						  END
						) AS "FinalPrice"

					FROM (
						SELECT DISTINCT 
							V1."GroupID",
							V1."CONFLICTID",
							V1."ShVTSTTime",
							V1."ShVTENTime",
							V1."BilledRateMinute",
							V1."G_CRDATEUNIQUE",
							V1."BILLABLEMINUTESFULLSHIFT",
							V1."BILLABLEMINUTESOVERLAP",
							V1."DistanceFlag",
							TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
							V1."PayerID" AS APID,
							CASE
								WHEN V2."StatusFlag" IN(''R'', ''D'') THEN ''R''
								WHEN V2."StatusFlag" IN (''N'') THEN ''N''
								ELSE ''U''
							END AS "StatusFlag",
							V1."Billed",
							V1."VisitStartTime",
							grp."GroupSize"
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
						INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 
							ON V2."CONFLICTID" = V1."CONFLICTID"
						INNER JOIN (
							SELECT "GroupID", COUNT(DISTINCT "CONFLICTID") AS "GroupSize"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "GroupID" IN (
								SELECT DISTINCT "GroupID"
								FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
								WHERE "PayerID" = ''${payerId}''
							)
							GROUP BY "GroupID"
						) grp ON grp."GroupID" = V1."GroupID"
						WHERE V1."GroupID" IN (
							SELECT DISTINCT "GroupID"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "PayerID" = ''${payerId}''
								AND (
									"SameSchTimeFlag" = ''Y'' OR 
									"SameVisitTimeFlag" = ''Y'' OR 
									"SchAndVisitTimeSameFlag" = ''Y'' OR 
									"SchOverAnotherSchTimeFlag" = ''Y'' OR 
									"VisitTimeOverAnotherVisitTimeFlag" = ''Y'' OR 
									"SchTimeOverVisitTimeFlag" = ''Y''
								)
						)
					) a
					LEFT JOIN (
						SELECT DISTINCT 
							V1."GroupID",
							V1."CONFLICTID",
							V1."ShVTSTTime",
							V1."ShVTENTime",
							TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
						INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 
							ON V2."CONFLICTID" = V1."CONFLICTID"
						WHERE V1."GroupID" IN (
							SELECT DISTINCT "GroupID"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "PayerID" = ''${payerId}''
						)
					) b ON a.CONFLICTID <> b.CONFLICTID AND a."GroupID" = b."GroupID"
					GROUP BY 
						a.CRDATEUNIQUE, 
						a."StatusFlag", 
						CASE WHEN a."Billed" = ''yes'' THEN ''Recovery'' ELSE ''Avoidance'' END, 
						CASE 
							WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
						END

					UNION ALL

					-- Time-Distance Query (ConflictTypeF 7)
					SELECT
						''${payerId}'' AS PAYERID,
						a."CRDATEUNIQUE" AS "CRDATEUNIQUE",
						''Time- Distance'' AS "ConflictType",
						''7'' AS "ConflictTypeF",
						a."StatusFlag" AS "STATUSFLAG",
						CASE 
							WHEN a."Billed" = ''yes'' THEN ''Recovery'' 
							ELSE ''Avoidance'' 
						END AS "COSTTYPE",
						CASE 
							WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
						END AS "VISITTYPE",
						COUNT(DISTINCT a."GroupID") AS "Total",
						SUM(
							CASE 
								WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND a."BILLABLEMINUTESFULLSHIFT" IS NOT NULL 
									THEN a."BILLABLEMINUTESFULLSHIFT" * a."BilledRateMinute" 
								WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 
									THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
								ELSE 0 
							END
						) AS "ShiftPrice",
						SUM(
						  CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') 
							THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							ELSE 0 
						  END
						) AS "OverlapPrice",
						SUM(
						  CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND a."StatusFlag" = ''R'' AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."StatusFlag" = ''R'' AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							ELSE 0 
						  END
						) AS "FinalPrice"

					FROM (
						SELECT DISTINCT 
							V1."GroupID",
							V1."CONFLICTID",
							V1."ShVTSTTime",
							V1."ShVTENTime",
							V1."BilledRateMinute",
							V1."G_CRDATEUNIQUE",
							TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
							V1."PayerID" AS APID,
							V1."BILLABLEMINUTESFULLSHIFT",
							V1."BILLABLEMINUTESOVERLAP",
							CASE
								WHEN V2."StatusFlag" IN(''R'', ''D'') THEN ''R''
								WHEN V2."StatusFlag" IN (''N'') THEN ''N''
								ELSE ''U''
							END AS "StatusFlag",
							V1."Billed",
							V1."VisitStartTime",
							V1."DistanceFlag",
							grp."GroupSize"
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
						INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 
							ON V2."CONFLICTID" = V1."CONFLICTID"
						INNER JOIN (
							SELECT "GroupID", COUNT(DISTINCT "CONFLICTID") AS "GroupSize"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "GroupID" IN (
								SELECT DISTINCT "GroupID"
								FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
								WHERE "PayerID" = ''${payerId}''
							)
							GROUP BY "GroupID"
						) grp ON grp."GroupID" = V1."GroupID"
						WHERE V1."GroupID" IN (
							SELECT DISTINCT "GroupID"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "PayerID" = ''${payerId}'' AND "DistanceFlag" = ''Y''
						)
					) a
					LEFT JOIN (
						SELECT DISTINCT 
							V1."GroupID",
							V1."CONFLICTID",
							V1."ShVTSTTime",
							V1."ShVTENTime",
							TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
						INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 
							ON V2."CONFLICTID" = V1."CONFLICTID"
						WHERE V1."GroupID" IN (
							SELECT DISTINCT "GroupID"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "PayerID" = ''${payerId}''
						)
					) b ON a.CONFLICTID <> b.CONFLICTID AND a."GroupID" = b."GroupID"
					GROUP BY 
						a.CRDATEUNIQUE, 
						a."StatusFlag", 
						CASE WHEN a."Billed" = ''yes'' THEN ''Recovery'' ELSE ''Avoidance'' END, 
						CASE 
							WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
						END

					UNION ALL

					-- In-Service Query (ConflictTypeF 8)
					SELECT
						''${payerId}'' AS PAYERID,
						a."CRDATEUNIQUE" AS "CRDATEUNIQUE",
						''In-Service'' AS "ConflictType",
						''8'' AS "ConflictTypeF",
						a."StatusFlag" AS "STATUSFLAG",
						CASE 
							WHEN a."Billed" = ''yes'' THEN ''Recovery'' 
							ELSE ''Avoidance'' 
						END AS "COSTTYPE",
						CASE 
							WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
						END AS "VISITTYPE",
						COUNT(DISTINCT a."GroupID") AS "Total",
						SUM(
							CASE 
								WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND a."BILLABLEMINUTESFULLSHIFT" IS NOT NULL 
									THEN a."BILLABLEMINUTESFULLSHIFT" * a."BilledRateMinute" 
								WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 
									THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
								ELSE 0 
							END
						) AS "ShiftPrice",
						SUM(
						  CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') 
							THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							ELSE 0 
						  END
						) AS "OverlapPrice",
						SUM(
						  CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND a."StatusFlag" = ''R'' AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."StatusFlag" = ''R'' AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							ELSE 0 
						  END
						) AS "FinalPrice"

					FROM (
						SELECT DISTINCT 
							V1."GroupID",
							V1."CONFLICTID",
							V1."ShVTSTTime",
							V1."ShVTENTime",
							V1."BilledRateMinute",
							V1."G_CRDATEUNIQUE",
							V1."BILLABLEMINUTESFULLSHIFT",
							V1."BILLABLEMINUTESOVERLAP",
							TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
							V1."PayerID" AS APID,
							V1."DistanceFlag",
							CASE
								WHEN V2."StatusFlag" IN(''R'', ''D'') THEN ''R''
								WHEN V2."StatusFlag" IN (''N'') THEN ''N''
								ELSE ''U''
							END AS "StatusFlag",
							V1."Billed",
							V1."VisitStartTime",
							grp."GroupSize"
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
						INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 
							ON V2."CONFLICTID" = V1."CONFLICTID"
						INNER JOIN (
							SELECT "GroupID", COUNT(DISTINCT "CONFLICTID") AS "GroupSize"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "GroupID" IN (
								SELECT DISTINCT "GroupID"
								FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
								WHERE "PayerID" = ''${payerId}''
							)
							GROUP BY "GroupID"
						) grp ON grp."GroupID" = V1."GroupID"
						WHERE V1."GroupID" IN (
							SELECT DISTINCT "GroupID"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "PayerID" = ''${payerId}'' AND "InServiceFlag" = ''Y''
						)
					) a
					LEFT JOIN (
						SELECT DISTINCT 
							V1."GroupID",
							V1."CONFLICTID",
							V1."ShVTSTTime",
							V1."ShVTENTime",
							TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
						INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 
							ON V2."CONFLICTID" = V1."CONFLICTID"
						WHERE V1."GroupID" IN (
							SELECT DISTINCT "GroupID"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "PayerID" = ''${payerId}''
						)
					) b ON a.CONFLICTID <> b.CONFLICTID AND a."GroupID" = b."GroupID"
					GROUP BY 
						a.CRDATEUNIQUE, 
						a."StatusFlag", 
						CASE WHEN a."Billed" = ''yes'' THEN ''Recovery'' ELSE ''Avoidance'' END, 
						CASE 
							WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
							WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
						END
				)
			`;

			var dashboard1Stmt = snowflake.createStatement({
				sqlText: insercontypes
			});

			dashboard1Stmt.execute();
			//-------------------------END PAYER CON TYPE---------------------
			
			
			
			//-------------------------PAYER AGENCY----------------------------
			
			var insertAgencySql = `
				INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_AGENCY_NEW(
					PAYERID, CRDATEUNIQUE, PROVIDERID, P_NAME, TIN, 
					STATUSFLAG, COSTTYPE, VISITTYPE, 
					CON_TO, CON_SP, CON_OP, CON_FP
				)
				SELECT
					''${payerId}'' AS PAYERID,
					a.CRDATEUNIQUE,
					a."APRID" AS PROVIDERID,
					a."ProviderName" AS P_NAME,
					a."FederalTaxNumber" AS TIN,
					a."StatusFlag" AS STATUSFLAG,
					CASE 
						WHEN a."Billed" = ''yes'' THEN ''Recovery'' 
						ELSE ''Avoidance'' 
					END AS COSTTYPE,
					CASE 
						WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
						WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
						WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
					END AS VISITTYPE,
					COUNT(DISTINCT a."GroupID") AS Total,
					SUM(
						CASE 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND a."BILLABLEMINUTESFULLSHIFT" IS NOT NULL 
								THEN a."BILLABLEMINUTESFULLSHIFT" * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 
								THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
							ELSE 0 
						END
					) AS ShiftPrice,
					SUM(
						CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') 
							THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							ELSE 0 
						END
					) AS OverlapPrice,
					SUM(
						CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND a."StatusFlag" = ''R'' AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."StatusFlag" = ''R'' AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
							WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
							ELSE 0 
						END
					) AS FinalPrice
				FROM
					(
					SELECT
						DISTINCT V1."GroupID",
						V1."CONFLICTID",
						V1."ShVTSTTime",
						V1."ShVTENTime",
						V1."BilledRateMinute",
						V1."ProviderID" AS APRID,
						V1."ProviderName",
						V1."FederalTaxNumber",
						V1."G_CRDATEUNIQUE",
						V1."BILLABLEMINUTESFULLSHIFT",
						V1."BILLABLEMINUTESOVERLAP",
						TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
						V1."PayerID" AS APID,
						V1."Billed",
						V1."VisitStartTime",
						CASE
							WHEN V2."StatusFlag" IN(''R'', ''D'') THEN ''R''
							WHEN V2."StatusFlag" IN (''N'') THEN ''N''
							ELSE ''U''
						END AS "StatusFlag",
						V1."DistanceFlag",
						grp."GroupSize"
					FROM
						CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
					INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
						V2."CONFLICTID" = V1."CONFLICTID"
					INNER JOIN (
						  SELECT "GroupID", COUNT(DISTINCT "CONFLICTID") AS "GroupSize"
						  FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
						  WHERE "GroupID" IN (
							  SELECT DISTINCT "GroupID"
							  FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							  WHERE "PayerID" = ''${payerId}''
						  )
						  GROUP BY "GroupID"
					  ) grp ON grp."GroupID" = V1."GroupID"
					WHERE
						V1."GroupID" IN (
							SELECT DISTINCT "GroupID"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "PayerID" = ''${payerId}''
						)
					) a
				LEFT JOIN (
					SELECT
						DISTINCT V1."GroupID",
						V1."CONFLICTID",
						V1."ShVTSTTime",
						V1."ShVTENTime",
						TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE
					FROM
						CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
					INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
						V2."CONFLICTID" = V1."CONFLICTID"
					WHERE
						V1."GroupID" IN (
							SELECT DISTINCT "GroupID"
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
							WHERE "PayerID" = ''${payerId}''
						)
					) b ON a.CONFLICTID <> b.CONFLICTID AND a."GroupID" = b."GroupID"
				GROUP BY
					a.CRDATEUNIQUE,
					a."APRID",
					a."ProviderName",
					a."FederalTaxNumber",
					a."StatusFlag",
					COSTTYPE,
					VISITTYPE
			`;

			var dashboard2Stmt = snowflake.createStatement({
				sqlText: insertAgencySql
			});

			dashboard2Stmt.execute();
			
			//-------------------------END PAYER AGENCY----------------------------
			
			
			
			//-------------------------PAYER PATIENT-------------------------------------
			
			var insertPatientSql = `
			INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PATIENT_NEW(
				PAYERID, CRDATEUNIQUE, PATIENTID, PFNAME, PLNAME, PNAME, ADMISSIONID,
				STATUSFLAG, COSTTYPE, VISITTYPE,
				CON_TO, CON_SP, CON_OP, CON_FP
			)
			SELECT
				''${payerId}'' AS PAYERID,
				a.CRDATEUNIQUE,
				a.APAID AS PATIENTID,
				a."PA_PFName" AS PFNAME,
				a."PA_PLName" AS PLNAME,
				a."PA_PName" AS PNAME,
				a."PA_PAdmissionID" AS ADMISSIONID,
				a."StatusFlag" AS STATUSFLAG,
				CASE
					WHEN a."Billed" = ''yes'' THEN ''Recovery''
					ELSE ''Avoidance''
				END AS COSTTYPE,
				CASE
					WHEN a."VisitStartTime" IS NULL THEN ''Scheduled''
					WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed''
					WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed''
				END AS VISITTYPE,
				COUNT(DISTINCT a."GroupID") AS CON_TO,
				SUM(
					CASE
						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND a."BILLABLEMINUTESFULLSHIFT" IS NOT NULL
							THEN a."BILLABLEMINUTESFULLSHIFT" * a."BilledRateMinute"
						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0
							THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
						ELSE 0
					END
				) AS CON_SP,
				SUM(
					CASE
						WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') 
						THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
						
						WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."DistanceFlag" = ''Y'' THEN 0
						
						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0
							AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL
							THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
						ELSE 0
					END
				) AS CON_OP,
				SUM(
					CASE
						WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND a."StatusFlag" = ''R'' AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') 
						THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
						
						WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."StatusFlag" = ''R'' AND a."DistanceFlag" = ''Y'' THEN 0
						
						WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0
							AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL
							THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"

						WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0
							AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL)
							AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime"
							THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
						ELSE 0
					END
				) AS CON_FP
			FROM
				(
				SELECT
					DISTINCT V1."GroupID",
					V1."CONFLICTID",
					V1."ShVTSTTime",
					V1."ShVTENTime",
					V1."BilledRateMinute",
					V1."PA_PatientID" AS APAID,
					V1."PA_PName",
					V1."PA_PFName",
					V1."PA_PLName",
					V1."G_CRDATEUNIQUE",
					V1."BILLABLEMINUTESFULLSHIFT",
					V1."BILLABLEMINUTESOVERLAP",
					V1."Billed",
					V1."VisitStartTime",
					TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
					V1."PayerID" AS APID,
					grp."GroupSize",
					V1."DistanceFlag",
					CASE
						WHEN V2."StatusFlag" IN(''R'', ''D'') THEN ''R''
						WHEN V2."StatusFlag" IN (''N'') THEN ''N''
						ELSE ''U''
					END AS "StatusFlag",
					(CASE
						WHEN V1."PayerID" = ''${payerId}''
						AND V1."PA_PAdmissionID" != '''' THEN V1."PA_PAdmissionID"
						WHEN V1."PayerID" = ''${payerId}''
						AND V1."PA_PAdmissionID" = '''' THEN ''''
						ELSE ''Restricted''
					END) AS "PA_PAdmissionID"
				FROM
					CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
				INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
					V2."CONFLICTID" = V1."CONFLICTID"
				INNER JOIN (
					SELECT "GroupID", COUNT(DISTINCT "CONFLICTID") AS "GroupSize"
					FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
					WHERE "GroupID" IN (
						SELECT DISTINCT "GroupID"
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
						WHERE "PayerID" = ''${payerId}''
					)
					GROUP BY "GroupID"
				) grp ON grp."GroupID" = V1."GroupID"
				WHERE
					V1."GroupID" IN (
						SELECT DISTINCT "GroupID"
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
						WHERE "PayerID" = ''${payerId}''
					)
				) a
			LEFT JOIN (
				SELECT
					DISTINCT V1."GroupID",
					V1."CONFLICTID",
					V1."ShVTSTTime",
					V1."ShVTENTime"
				FROM
					CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
				INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
					V2."CONFLICTID" = V1."CONFLICTID"
				WHERE
					V1."GroupID" IN (
						SELECT DISTINCT "GroupID"
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
						WHERE "PayerID" = ''${payerId}''
					)
				) b ON a.CONFLICTID <> b.CONFLICTID AND a."GroupID" = b."GroupID"
			WHERE
				a."PA_PName" IS NOT NULL
				AND a."PA_PAdmissionID" != ''Restricted''
			GROUP BY
				a.CRDATEUNIQUE,
				a.APAID,
				a."PA_PName",
				a."PA_PFName",
				a."PA_PLName",
				a."PA_PAdmissionID",
				a."StatusFlag",
				COSTTYPE,
				VISITTYPE
			`;

			var dashboard3stmt = snowflake.createStatement({
				sqlText: insertPatientSql
			});

			dashboard3stmt.execute();
			
			//-------------------------END PAYER PATIENT--------------------------------------------------------------------------
			
			
			
			//-------------------------PAYER PAYER-------------------------------
			
			var inpayer = `
			  INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_PAYER_NEW(
				PAYERID, CRDATEUNIQUE, CONPAYERID, PNAME,
				STATUSFLAG, COSTTYPE, VISITTYPE,
				CON_TO, CON_SP, CON_OP, CON_FP
			  )
			  SELECT
				''${payerId}'' AS PAYERID,
				a.CRDATEUNIQUE,
				a.APID AS CONPAYERID,
				a."Contract" AS PNAME,
				a."StatusFlag" AS STATUSFLAG,
				CASE
				  WHEN a."Billed" = ''yes'' THEN ''Recovery''
				  ELSE ''Avoidance''
				END AS COSTTYPE,
				CASE
				  WHEN a."VisitStartTime" IS NULL THEN ''Scheduled''
				  WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed''
				  WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed''
				END AS VISITTYPE,
				COUNT(DISTINCT a."GroupID") AS CON_TO,
				SUM(
					CASE 
						WHEN a."BilledRateMinute" > 0 AND a."BILLABLEMINUTESFULLSHIFT" IS NOT NULL 
							THEN a."BILLABLEMINUTESFULLSHIFT" * a."BilledRateMinute" 
						WHEN a."BilledRateMinute" > 0 
							THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
						ELSE 0 
					END
				) AS CON_SP,
				SUM(
					CASE 
						WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') 
						THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
						WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."DistanceFlag" = ''Y'' THEN 0
						WHEN a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
							THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
						WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
						WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
						WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
						WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
						WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
						WHEN a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
						ELSE 0 
					END
				) AS CON_OP,
				SUM(
					CASE 
						WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND a."StatusFlag" = ''R'' AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
						WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."StatusFlag" = ''R'' AND a."DistanceFlag" = ''Y'' THEN 0
						WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
							THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute"
						WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
						WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
						WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
						WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
						WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute"
						WHEN a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute"
						ELSE 0 
					END
				) AS CON_FP
			  FROM
			  (
				SELECT
				  DISTINCT -- THIS IS THE MISSING PIECE TO ALIGN WITH THE OLD PROCEDURE
				  V1."GroupID", V1."CONFLICTID", V1."ShVTSTTime", V1."ShVTENTime",
				  V1."BilledRateMinute", V1."BILLABLEMINUTESFULLSHIFT", V1."BILLABLEMINUTESOVERLAP",
				  V1."Billed", V1."VisitStartTime",
				  TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
				  V1."PayerID" AS APID, V1."Contract",
				  CASE
					WHEN V2."StatusFlag" IN(''R'', ''D'') THEN ''R''
					WHEN V2."StatusFlag" IN (''N'') THEN ''N''
					ELSE ''U''
				  END AS "StatusFlag",
				  V1."DistanceFlag",
				  grp."GroupSize"
				FROM
				  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
				  INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = V1."CONFLICTID"
				  INNER JOIN (
					  SELECT "GroupID", COUNT(DISTINCT "CONFLICTID") AS "GroupSize"
					  FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
					  WHERE "GroupID" IN (
						  SELECT DISTINCT "GroupID"
						  FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
						  WHERE "PayerID" = ''${payerId}''
					  )
					  GROUP BY "GroupID"
				  ) grp ON grp."GroupID" = V1."GroupID"
				WHERE
				  V1."AppPayerID" != ''0''
				  AND V1."GroupID" IN (
					  SELECT DISTINCT "GroupID"
					  FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
					  WHERE "PayerID" = ''${payerId}''
				  )
			  ) a
			  LEFT JOIN (
				SELECT
				  "GroupID", "CONFLICTID", "ShVTSTTime", "ShVTENTime"
				FROM
				  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
				WHERE "PayerID" = ''${payerId}''
			  ) b
				ON a.CONFLICTID <> b.CONFLICTID
				AND a."GroupID" = b."GroupID"
			  GROUP BY
				a.APID,
				a."Contract",
				a.CRDATEUNIQUE,
				a."StatusFlag",
				COSTTYPE,
				VISITTYPE
			`;

			var dashboard4Stmt = snowflake.createStatement({
			  sqlText: inpayer
			});
			dashboard4Stmt.execute();
			
			//-------------------------END PAYER PAYER-------------------------------
			
			
			
			//-------------------------PAYER CAREGIVER------------------------------
			
			var insertCaregiverSql = `
				INSERT INTO CONFLICTREPORT.PUBLIC.PAYER_DASHBOARD_CAREGIVER_NEW (
					PAYERID, CRDATEUNIQUE, SSN, CAREGIVERID, C_NAME, C_LNAME, C_FNAME,
					STATUSFLAG, COSTTYPE, VISITTYPE, 
					CON_TO, CON_SP, CON_OP, CON_FP
				)
				SELECT
					''${payerId}'' AS PAYERID,
                    a.CRDATEUNIQUE,
					a."SSN",
					NULL AS CAREGIVERID,
					MAX(a."AideName") AS C_NAME,
					NULL AS C_LNAME,
					NULL AS C_FNAME,
					a."StatusFlag" AS STATUSFLAG,
					CASE 
						WHEN a."Billed" = ''yes'' THEN ''Recovery'' 
						ELSE ''Avoidance'' 
					END AS COSTTYPE,
					CASE 
						WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
						WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
						WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
					END AS VISITTYPE,
					COUNT(DISTINCT a."GroupID") AS CON_TO,
					SUM(
						CASE 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND a."BILLABLEMINUTESFULLSHIFT" IS NOT NULL 
								THEN a."BILLABLEMINUTESFULLSHIFT" * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 
								THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
							ELSE 0 
						END
					) AS CON_SP,
					SUM(
						CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute" 
							ELSE 0
						END
					) AS CON_OP,
					SUM(
						CASE 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NOT NULL AND a."StatusFlag" = ''R'' AND (a."GroupSize" <= 2 OR a."DistanceFlag" = ''Y'') THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a."BILLABLEMINUTESOVERLAP" IS NULL AND a."StatusFlag" = ''R'' AND a."DistanceFlag" = ''Y'' THEN 0
							WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND a."GroupSize" <= 2 AND a."BILLABLEMINUTESOVERLAP" IS NOT NULL 
								THEN a."BILLABLEMINUTESOVERLAP" * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTSTTime" <= a."ShVTENTime" AND b."ShVTENTime" > a."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTSTTime" <= b."ShVTENTime" AND a."ShVTENTime" > b."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" >= a."ShVTSTTime" AND b."ShVTENTime" <= a."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" >= b."ShVTSTTime" AND a."ShVTENTime" <= b."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND b."ShVTSTTime" < a."ShVTSTTime" AND b."ShVTENTime" > a."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, a."ShVTSTTime", a."ShVTENTime") * a."BilledRateMinute" 
							WHEN a.APID = ''${payerId}'' AND a."StatusFlag" = ''R'' AND a."BilledRateMinute" > 0 AND (a."GroupSize" > 2 OR a."BILLABLEMINUTESOVERLAP" IS NULL) AND a."ShVTSTTime" < b."ShVTSTTime" AND a."ShVTENTime" > b."ShVTENTime" 
								THEN TIMESTAMPDIFF(MINUTE, b."ShVTSTTime", b."ShVTENTime") * a."BilledRateMinute" 
							ELSE 0 
						END
					) AS CON_FP
				FROM
					(
					-- Subquery ''a'': Gathers all the base visit and caregiver data for the given payer.
					SELECT
						V1."GroupID", V1."CONFLICTID", V1."ShVTSTTime", V1."ShVTENTime",
						V1."BilledRateMinute", V1."CaregiverID", V1."SSN",
						INITCAP(LOWER(V1."AideName")) AS "AideName",
						INITCAP(LOWER(V1."AideFName")) AS "AideFName",
						INITCAP(LOWER(V1."AideLName")) AS "AideLName",
						V1."BILLABLEMINUTESFULLSHIFT", V1."BILLABLEMINUTESOVERLAP",
                        TO_CHAR(V1."G_CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
						V1."PayerID" AS APID, V1."Billed", V1."VisitStartTime",
						CASE
							WHEN V2."StatusFlag" IN(''R'', ''D'') THEN ''R''
							WHEN V2."StatusFlag" IN (''N'') THEN ''N''
							ELSE ''U''
						END AS "StatusFlag",
						V1."DistanceFlag",
						grp."GroupSize"
					FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS V1
					INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
						V2."CONFLICTID" = V1."CONFLICTID"
					INNER JOIN (
						SELECT "GroupID", COUNT(DISTINCT "CONFLICTID") AS "GroupSize"
						FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
						WHERE "PayerID" = ''${payerId}''
						GROUP BY "GroupID"
					) grp ON grp."GroupID" = V1."GroupID"
					WHERE V1."PayerID" = ''${payerId}''
					) a
				LEFT JOIN
					(
					-- Subquery ''b'': Gathers overlapping visit times for the join.
					SELECT
						"GroupID", "CONFLICTID", "ShVTSTTime", "ShVTENTime"
					FROM
						CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
					WHERE
						"PayerID" = ''${payerId}''
					) b ON a.CONFLICTID <> b.CONFLICTID AND a."GroupID" = b."GroupID"
				-- The GROUP BY clause is now updated to match the new uniqueness requirement.
				GROUP BY
                    a.CRDATEUNIQUE,
					a."SSN",
					a."StatusFlag",
					CASE 
						WHEN a."Billed" = ''yes'' THEN ''Recovery'' 
						ELSE ''Avoidance'' 
					END,
					CASE 
						WHEN a."VisitStartTime" IS NULL THEN ''Scheduled'' 
						WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" != ''yes'' THEN ''Confirmed'' 
						WHEN a."VisitStartTime" IS NOT NULL AND a."Billed" = ''yes'' THEN ''Billed'' 
					END
			`;

			var dashboard5Stmt = snowflake.createStatement({
				sqlText: insertCaregiverSql
			});

			dashboard5Stmt.execute();
			
			//-------------------------END PAYER CAREGIVER------------------------------
		}

		return `Inserted rows successfully.`;

	} catch (err) {
		throw err;
	}
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.LOAD_PROVIDER_DASHBOARD_DATA()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	var SQL1 = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_TOP`;
  var SQL2 = `INSERT INTO CONFLICTREPORT."PUBLIC".PROVIDER_DASHBOARD_TOP (PROVIDERID, OFFICEID, TODAYTOTAL, TODAYSHIFTPRICE, TODAYOVERLAPPRICE, SEVENTOTAL, SEVENFINALPRICE, THIRTYTOTAL, THIRTYFINALPRICE)
	SELECT
	CVM."ProviderID" AS PROVIDERID,
	CVM."OfficeID" AS OFFICEID,
	COUNT(DISTINCT CASE WHEN TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') = TO_CHAR(CURRENT_DATE, ''YYYY-MM-DD'') THEN CVM.CONFLICTID END) AS TodayTotal,
	SUM( CASE WHEN TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') = TO_CHAR(CURRENT_DATE, ''YYYY-MM-DD'') THEN CVMCH."ShiftPrice" ELSE 0 END ) AS TodayShiftPrice,
	SUM( CASE WHEN TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') = TO_CHAR(CURRENT_DATE, ''YYYY-MM-DD'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS TodayOverlapPrice,
	COUNT(DISTINCT CASE WHEN TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') BETWEEN TO_CHAR(CURRENT_DATE - 7, ''YYYY-MM-DD'') AND TO_CHAR(CURRENT_DATE, ''YYYY-MM-DD'') AND V2."StatusFlag" IN (''R'', ''D'') THEN CVM.CONFLICTID END) AS SevenTotal,
	SUM( CASE WHEN TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') BETWEEN TO_CHAR(CURRENT_DATE - 7, ''YYYY-MM-DD'') AND TO_CHAR(CURRENT_DATE, ''YYYY-MM-DD'') AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS SevenFinalPrice,
	COUNT(DISTINCT CASE WHEN TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') BETWEEN TO_CHAR(CURRENT_DATE - 30, ''YYYY-MM-DD'') AND TO_CHAR(CURRENT_DATE, ''YYYY-MM-DD'') AND V2."StatusFlag" IN (''R'', ''D'') THEN CVM.CONFLICTID END) AS ThirtyTotal,
	SUM( CASE WHEN TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') BETWEEN TO_CHAR(CURRENT_DATE - 30, ''YYYY-MM-DD'') AND TO_CHAR(CURRENT_DATE, ''YYYY-MM-DD'') AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS ThirtyFinalPrice,
FROM
	CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM
INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
	V2."CONFLICTID" = CVM."CONFLICTID"
LEFT JOIN (
	SELECT
		CVM1.ID,
		CASE
		    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESFULLSHIFT" IS NOT NULL THEN CVM1."BILLABLEMINUTESFULLSHIFT" * CVM1."BilledRateMinute"
			WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
			ELSE 0
		END AS "ShiftPrice",
		ROW_NUMBER() OVER (PARTITION BY CVM1."CONFLICTID"
	ORDER BY
		CASE
			WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
			AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
			AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime")
			WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
				AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
				AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime")
				WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
					AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
					WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
						AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
						WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
							AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
							WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
								AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
								ELSE 0
							END DESC) AS RN,
		CASE
		    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESOVERLAP" IS NOT NULL THEN CVM1."BILLABLEMINUTESOVERLAP" * CVM1."BilledRateMinute"
			WHEN CVM1."BilledRateMinute" > 0
				AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
				AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
				AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
				WHEN CVM1."BilledRateMinute" > 0
					AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
					AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
					AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
					WHEN CVM1."BilledRateMinute" > 0
						AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
						AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
						WHEN CVM1."BilledRateMinute" > 0
							AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
							AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
							WHEN CVM1."BilledRateMinute" > 0
								AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
								AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
								WHEN CVM1."BilledRateMinute" > 0
									AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
									AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
									ELSE 0
								END AS "OverlapPrice"
							FROM
								CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
							
	) AS CVMCH ON CVMCH.ID = CVM.ID	AND CVMCH.RN = 1
WHERE
	TO_CHAR(CVM."CRDATEUNIQUE",
	''YYYY-MM-DD'') BETWEEN TO_CHAR(CURRENT_DATE - 30,
	''YYYY-MM-DD'') AND TO_CHAR(CURRENT_DATE,
	''YYYY-MM-DD'')
	--AND (CVM."SchOverAnotherSchTimeFlag" = ''Y''
	--	OR CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'')
	GROUP BY CVM."ProviderID", CVM."OfficeID"
	`;
	var SQL3 = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CON_TYP`;
	var SQL4 = `INSERT INTO CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CON_TYP (PROVIDERID, OFFICEID, CRDATEUNIQUE, EX_ST_MATCH_TO, EX_ST_MATCH_SP, EX_ST_MATCH_OP, EX_ST_MATCH_FP, EX_VT_MATCH_TO, EX_VT_MATCH_SP, EX_VT_MATCH_OP, EX_VT_MATCH_FP, EX_ST_VT_MATCH_TO, EX_ST_VT_MATCH_SP, EX_ST_VT_MATCH_OP, EX_ST_VT_MATCH_FP, ST_OVR_TO, ST_OVR_SP, ST_OVR_OP, ST_OVR_FP, VT_OVR_TO, VT_OVR_SP, VT_OVR_OP, VT_OVR_FP, ST_VT_OVR_TO, ST_VT_OVR_SP, ST_VT_OVR_OP, ST_VT_OVR_FP, TD_TO, TD_SP, TD_OP, TD_FP, IN_TO, IN_SP, IN_OP, IN_FP, PT_TO, PT_SP, PT_OP, PT_FP)
	SELECT
		CVM."ProviderID" AS PROVIDERID,
		CVM."OfficeID" AS OFFICEID,
		TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
		COUNT(DISTINCT CASE WHEN CVM."SameSchTimeFlag" = ''Y'' THEN CVM.CONFLICTID END) AS EX_ST_MATCH_TO,
		SUM( CASE WHEN CVM."SameSchTimeFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS EX_ST_MATCH_SP,
		SUM( CASE WHEN CVM."SameSchTimeFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS EX_ST_MATCH_OP,
		SUM( CASE WHEN CVM."SameSchTimeFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS EX_ST_MATCH_FP,
		COUNT(DISTINCT CASE WHEN CVM."SameVisitTimeFlag" = ''Y'' THEN CVM.CONFLICTID END) AS EX_VT_MATCH_TO,
		SUM( CASE WHEN CVM."SameVisitTimeFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS EX_VT_MATCH_SP,
		SUM( CASE WHEN CVM."SameVisitTimeFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS EX_VT_MATCH_OP,
		SUM( CASE WHEN CVM."SameVisitTimeFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS EX_VT_MATCH_FP,
		COUNT(DISTINCT CASE WHEN CVM."SchAndVisitTimeSameFlag" = ''Y'' THEN CVM.CONFLICTID END) AS EX_ST_VT_MATCH_TO,
		SUM( CASE WHEN CVM."SchAndVisitTimeSameFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS EX_ST_VT_MATCH_SP,
		SUM( CASE WHEN CVM."SchAndVisitTimeSameFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS EX_ST_VT_MATCH_OP,
		SUM( CASE WHEN CVM."SchAndVisitTimeSameFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'')THEN CVMCH."OverlapPrice" ELSE 0 END ) AS EX_ST_VT_MATCH_FP,
		COUNT(DISTINCT CASE WHEN CVM."SchOverAnotherSchTimeFlag" = ''Y'' THEN CVM.CONFLICTID END) AS ST_OVR_TO,
		SUM( CASE WHEN CVM."SchOverAnotherSchTimeFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS ST_OVR_SP,
		SUM( CASE WHEN CVM."SchOverAnotherSchTimeFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS ST_OVR_OP,
		SUM( CASE WHEN CVM."SchOverAnotherSchTimeFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS ST_OVR_FP,
		COUNT(DISTINCT CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'' THEN CVM.CONFLICTID END) AS VT_OVR_TO,
		SUM( CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS VT_OVR_SP,
		SUM( CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS VT_OVR_OP,
		SUM( CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS VT_OVR_FP,
		COUNT(DISTINCT CASE WHEN CVM."SchTimeOverVisitTimeFlag" = ''Y'' THEN CVM.CONFLICTID END) AS ST_VT_OVR_TO,
		SUM( CASE WHEN CVM."SchTimeOverVisitTimeFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS ST_VT_OVR_SP,
		SUM( CASE WHEN CVM."SchTimeOverVisitTimeFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS ST_VT_OVR_OP,
		SUM( CASE WHEN CVM."SchTimeOverVisitTimeFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS ST_VT_OVR_FP,
		COUNT(DISTINCT CASE WHEN CVM."DistanceFlag" = ''Y'' THEN CVM.CONFLICTID END) AS TD_TO,
		SUM( CASE WHEN CVM."DistanceFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS TD_SP,
		SUM( CASE WHEN CVM."DistanceFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS TD_OP,
		SUM( CASE WHEN CVM."DistanceFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS TD_FP,
		COUNT(DISTINCT CASE WHEN CVM."InServiceFlag" = ''Y'' THEN CVM.CONFLICTID END) AS IN_TO,
		SUM( CASE WHEN CVM."InServiceFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS IN_SP,
		SUM( CASE WHEN CVM."InServiceFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS IN_OP,
		SUM( CASE WHEN CVM."InServiceFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS IN_FP,
		COUNT(DISTINCT CASE WHEN CVM."PTOFlag" = ''Y'' THEN CVM.CONFLICTID END) AS PT_TO,
		SUM( CASE WHEN CVM."PTOFlag" = ''Y'' THEN CVMCH."ShiftPrice" ELSE 0 END ) AS PT_SP,
		SUM( CASE WHEN CVM."PTOFlag" = ''Y'' THEN CVMCH."OverlapPrice" ELSE 0 END ) AS PT_OP,
		SUM( CASE WHEN CVM."PTOFlag" = ''Y'' AND V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverlapPrice" ELSE 0 END ) AS PT_FP
	FROM
		CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM
	INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
		V2."CONFLICTID" = CVM."CONFLICTID"
	LEFT JOIN (
		SELECT
			CVM1.ID,
			CASE
			    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESFULLSHIFT" IS NOT NULL THEN CVM1."BILLABLEMINUTESFULLSHIFT" * CVM1."BilledRateMinute"
				WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
				ELSE 0
			END AS "ShiftPrice",
			ROW_NUMBER() OVER (PARTITION BY CVM1."CONFLICTID"
		ORDER BY
			CASE
				WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
				AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
				AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime")
				WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
					AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
					AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime")
					WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
						AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
						WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
							AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
							WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
								AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
								WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
									AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
									ELSE 0
								END DESC) AS RN,
			CASE
			    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESOVERLAP" IS NOT NULL THEN CVM1."BILLABLEMINUTESOVERLAP" * CVM1."BilledRateMinute"
				WHEN CVM1."BilledRateMinute" > 0
					AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
					AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
					AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
					WHEN CVM1."BilledRateMinute" > 0
						AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
						AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
						AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
						WHEN CVM1."BilledRateMinute" > 0
							AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
							AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
							WHEN CVM1."BilledRateMinute" > 0
								AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
								AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
								WHEN CVM1."BilledRateMinute" > 0
									AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
									AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
									WHEN CVM1."BilledRateMinute" > 0
										AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
										AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
										ELSE 0
									END AS "OverlapPrice"
		FROM
			CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
		 ) AS CVMCH ON CVMCH.ID = CVM.ID AND CVMCH.RN = 1
		GROUP BY TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD''), CVM."ProviderID", CVM."OfficeID"
	`;
	
	var SQL5 = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_AGENCY`;
	var SQL6 = `INSERT INTO CONFLICTREPORT."PUBLIC".PROVIDER_DASHBOARD_AGENCY (PROVIDERID, OFFICEID, CRDATEUNIQUE, CONPROVIDERID, CON_P_NAME, CON_TIN, CON_TO, CON_SP, CON_OP, CON_FP)
	SELECT
		CVM."ProviderID" AS PROVIDERID,
		CVM."OfficeID" AS OFFICEID,
		TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
		CVM."ConProviderID" AS CONPROVIDERID,
		CVM."ConProviderName" AS CON_P_NAME,
		CVM."ConFederalTaxNumber" AS CON_TIN,
		COUNT(DISTINCT CVM.CONFLICTID) AS CON_TO,
		SUM( CASE WHEN CVMCH."ShiftAmount" IS NOT NULL THEN CVMCH."ShiftAmount" ELSE 0 END ) AS CON_SP,
		SUM( CASE WHEN CVMCH."OverLapAmount" IS NOT NULL THEN CVMCH."OverLapAmount" ELSE 0 END ) AS CON_OP,
		SUM( CASE WHEN V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverLapAmount" ELSE 0 END ) AS CON_FP
	FROM
		CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM
	INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
		V2."CONFLICTID" = CVM."CONFLICTID"
	LEFT JOIN (
		SELECT
			CVM1.ID,
			CASE
			    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESFULLSHIFT" IS NOT NULL THEN CVM1."BILLABLEMINUTESFULLSHIFT" * CVM1."BilledRateMinute"
				WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
				ELSE 0
			END AS "ShiftAmount",
			ROW_NUMBER() OVER (PARTITION BY CVM1."CONFLICTID"
		ORDER BY
			CASE
				WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
				AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
				AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime")
				WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
					AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
					AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime")
					WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
						AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
						WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
							AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
							WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
								AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
								WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
									AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
									ELSE 0
								END DESC) AS RN,
			CASE
			    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESOVERLAP" IS NOT NULL THEN CVM1."BILLABLEMINUTESOVERLAP" * CVM1."BilledRateMinute"
				WHEN CVM1."BilledRateMinute" > 0
					AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
					AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
					AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
					WHEN CVM1."BilledRateMinute" > 0
						AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
						AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
						AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
						WHEN CVM1."BilledRateMinute" > 0
							AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
							AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
							WHEN CVM1."BilledRateMinute" > 0
								AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
								AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
								WHEN CVM1."BilledRateMinute" > 0
									AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
									AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
									WHEN CVM1."BilledRateMinute" > 0
										AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
										AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
										ELSE 0
									END AS "OverLapAmount"
								FROM
									CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
			) AS CVMCH ON
		CVMCH.ID = CVM.ID
		AND CVMCH.RN = 1
	WHERE
		CVM."ConProviderID" IS NOT NULL
		AND
		CVM."ConProviderName" IS NOT NULL
		--AND (CVM."SchOverAnotherSchTimeFlag" = ''Y''
		--	OR CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'')
		GROUP BY
		CVM."ConProviderID",
		CVM."ConFederalTaxNumber",
		CVM."ConProviderName",
		TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD''),
		CVM."ProviderID",
		CVM."OfficeID"`;
	var SQL7 = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_CAREGIVER`;
	var SQL8 = `INSERT INTO CONFLICTREPORT."PUBLIC".PROVIDER_DASHBOARD_CAREGIVER (PROVIDERID, OFFICEID, CRDATEUNIQUE, CAREGIVERID, C_CODE, C_NAME, CON_TO, CON_SP, CON_OP, CON_FP)
	SELECT
		CVM."ProviderID" AS PROVIDERID,
		CVM."OfficeID" AS OFFICEID,
		TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
		CVM."CaregiverID" AS CAREGIVERID,
		CVM."AideCode" AS C_CODE,
		CVM."AideName" AS C_NAME,
		COUNT(DISTINCT CVM.CONFLICTID) AS CON_TO,
		SUM( CASE WHEN CVMCH."ShiftAmount" IS NOT NULL THEN CVMCH."ShiftAmount" ELSE 0 END ) AS CON_SP,
		SUM( CASE WHEN CVMCH."OverLapAmount" IS NOT NULL THEN CVMCH."OverLapAmount" ELSE 0 END ) AS CON_OP,
		SUM( CASE WHEN V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverLapAmount" ELSE 0 END ) AS CON_FP
	FROM
		CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM
	INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
		V2."CONFLICTID" = CVM."CONFLICTID"
	LEFT JOIN (
		SELECT
			CVM1.ID,
			CASE
			    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESFULLSHIFT" IS NOT NULL THEN CVM1."BILLABLEMINUTESFULLSHIFT" * CVM1."BilledRateMinute"
				WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
				ELSE 0
			END AS "ShiftAmount",
			ROW_NUMBER() OVER (PARTITION BY CVM1."CONFLICTID"
		ORDER BY
			CASE
				WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
				AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
				AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime")
				WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
					AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
					AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime")
					WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
						AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
						WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
							AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
							WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
								AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
								WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
									AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
									ELSE 0
								END DESC) AS RN,
			CASE
			    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESOVERLAP" IS NOT NULL THEN CVM1."BILLABLEMINUTESOVERLAP" * CVM1."BilledRateMinute"
				WHEN CVM1."BilledRateMinute" > 0
					AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
					AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
					AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
					WHEN CVM1."BilledRateMinute" > 0
						AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
						AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
						AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
						WHEN CVM1."BilledRateMinute" > 0
							AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
							AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
							WHEN CVM1."BilledRateMinute" > 0
								AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
								AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
								WHEN CVM1."BilledRateMinute" > 0
									AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
									AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
									WHEN CVM1."BilledRateMinute" > 0
										AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
										AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
										ELSE 0
									END AS "OverLapAmount"
								FROM
									CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1) AS CVMCH ON
		CVMCH.ID = CVM.ID
		AND CVMCH.RN = 1
	--WHERE
	--	(CVM."SchOverAnotherSchTimeFlag" = ''Y'' OR CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'')
		
	GROUP BY
		CVM."CaregiverID",
		CVM."AideCode",
		CVM."AideName",
		TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD''),
		CVM."ProviderID",
		CVM."OfficeID"
	`;

	
	var SQL9 = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_PATIENT`;
	var SQL10 = `INSERT INTO CONFLICTREPORT."PUBLIC".PROVIDER_DASHBOARD_PATIENT (PROVIDERID, OFFICEID, CRDATEUNIQUE, PATIENTID, PFNAME, PLNAME, PNAME, CON_TO, CON_SP, CON_OP, CON_FP)
	SELECT
		CVM."ProviderID" AS PROVIDERID,
		CVM."OfficeID" AS OFFICEID,
		TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
		CVM."P_PatientID" AS PATIENTID,
		CVM."P_PFName" AS PFNAME,
		CVM."P_PLName" AS PLNAME,
		CONCAT(CVM."P_PLName", '' '', CVM."P_PFName") AS PNAME,
		COUNT(DISTINCT CVM.CONFLICTID) AS CON_TO,
		SUM( CASE WHEN CVMCH."ShiftAmount" IS NOT NULL THEN CVMCH."ShiftAmount" ELSE 0 END ) AS CON_SP,
		SUM( CASE WHEN CVMCH."OverLapAmount" IS NOT NULL THEN CVMCH."OverLapAmount" ELSE 0 END ) AS CON_OP,
		SUM( CASE WHEN V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverLapAmount" ELSE 0 END ) AS CON_FP
	FROM
		CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM
	INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON
		V2."CONFLICTID" = CVM."CONFLICTID"
	LEFT JOIN (
		SELECT
			CVM1.ID,
			CASE
			    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESFULLSHIFT" IS NOT NULL THEN CVM1."BILLABLEMINUTESFULLSHIFT" * CVM1."BilledRateMinute" 
				WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
				ELSE 0
			END AS "ShiftAmount",
			ROW_NUMBER() OVER (PARTITION BY CVM1."CONFLICTID"
		ORDER BY
			CASE
				WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
				AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
				AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime")
				WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
					AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
					AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime")
					WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
						AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
						WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
							AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
							WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
								AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
								WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
									AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
									ELSE 0
								END DESC) AS RN,
			CASE
			    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESOVERLAP" IS NOT NULL THEN CVM1."BILLABLEMINUTESOVERLAP" * CVM1."BilledRateMinute"
				WHEN CVM1."BilledRateMinute" > 0
					AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
					AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
					AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
					WHEN CVM1."BilledRateMinute" > 0
						AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
						AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
						AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
						WHEN CVM1."BilledRateMinute" > 0
							AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
							AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
							WHEN CVM1."BilledRateMinute" > 0
								AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
								AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
								WHEN CVM1."BilledRateMinute" > 0
									AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
									AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
									WHEN CVM1."BilledRateMinute" > 0
										AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
										AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
										ELSE 0
									END AS "OverLapAmount"
								FROM
									CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
		) AS CVMCH ON CVMCH.ID = CVM.ID	AND CVMCH.RN = 1
	WHERE
		--(CVM."SchOverAnotherSchTimeFlag" = ''Y'' OR CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'') AND 
		CVM."P_PatientID" IS NOT NULL
	GROUP BY
		CVM."P_PatientID",
		CVM."P_PFName",
		CVM."P_PLName",
		TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD''),
		CVM."ProviderID",
		CVM."OfficeID"
	`;

	var SQL11 = `TRUNCATE TABLE CONFLICTREPORT.PUBLIC.PROVIDER_DASHBOARD_PAYER`;
	var SQL12 = `INSERT INTO CONFLICTREPORT."PUBLIC".PROVIDER_DASHBOARD_PAYER (PROVIDERID, OFFICEID, CRDATEUNIQUE, PAYERID, PNAME, CON_TO, CON_SP, CON_OP, CON_FP)
	SELECT
			CVM."ProviderID" AS PROVIDERID,
			CVM."OfficeID" AS OFFICEID,
			TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD'') AS CRDATEUNIQUE,
			CVM."PayerID" AS PAYERID,
			CVM."Contract" AS PNAME,
			COUNT(DISTINCT CVM.CONFLICTID) AS CON_TO,
			SUM( CASE WHEN CVMCH."ShiftAmount" IS NOT NULL THEN CVMCH."ShiftAmount" ELSE 0 END ) AS CON_SP,
			SUM( CASE WHEN CVMCH."OverLapAmount" IS NOT NULL THEN CVMCH."OverLapAmount" ELSE 0 END ) AS CON_OP,
			SUM( CASE WHEN V2."StatusFlag" IN (''R'', ''D'') THEN CVMCH."OverLapAmount" ELSE 0 END ) AS CON_FP
		FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM
		INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTS AS V2 ON V2."CONFLICTID" = CVM."CONFLICTID"
		LEFT JOIN (
			SELECT
				CVM1.ID,
				CASE
				    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESFULLSHIFT" IS NOT NULL THEN CVM1."BILLABLEMINUTESFULLSHIFT" * CVM1."BilledRateMinute"
					WHEN CVM1."BilledRateMinute" > 0 THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
					ELSE 0
				END AS "ShiftAmount",
				ROW_NUMBER() OVER (PARTITION BY CVM1."CONFLICTID"
			ORDER BY
				CASE
					WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
					AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
					AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime")
					WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
						AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
						AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime")
						WHEN CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
							AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
							WHEN CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
								AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
								WHEN CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
									AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime")
									WHEN CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
										AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime")
										ELSE 0
									END DESC) AS RN,
				CASE
				    WHEN CVM1."BilledRateMinute" > 0 AND CVM1."BILLABLEMINUTESOVERLAP" IS NOT NULL THEN CVM1."BILLABLEMINUTESOVERLAP" * CVM1."BilledRateMinute"
					WHEN CVM1."BilledRateMinute" > 0
						AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
						AND CVM1."CShVTSTTime" <= CVM1."ShVTENTime"
						AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
						WHEN CVM1."BilledRateMinute" > 0
							AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
							AND CVM1."ShVTSTTime" <= CVM1."CShVTENTime"
							AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
							WHEN CVM1."BilledRateMinute" > 0
								AND CVM1."CShVTSTTime" >= CVM1."ShVTSTTime"
								AND CVM1."CShVTENTime" <= CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
								WHEN CVM1."BilledRateMinute" > 0
									AND CVM1."ShVTSTTime" >= CVM1."CShVTSTTime"
									AND CVM1."ShVTENTime" <= CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
									WHEN CVM1."BilledRateMinute" > 0
										AND CVM1."CShVTSTTime" < CVM1."ShVTSTTime"
										AND CVM1."CShVTENTime" > CVM1."ShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."ShVTSTTime", CVM1."ShVTENTime") * CVM1."BilledRateMinute"
										WHEN CVM1."BilledRateMinute" > 0
											AND CVM1."ShVTSTTime" < CVM1."CShVTSTTime"
											AND CVM1."ShVTENTime" > CVM1."CShVTENTime" THEN TIMESTAMPDIFF(MINUTE, CVM1."CShVTSTTime", CVM1."CShVTENTime") * CVM1."BilledRateMinute"
											ELSE 0
										END AS "OverLapAmount"
									FROM
										CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CVM1
			) AS CVMCH ON CVMCH.ID = CVM.ID	AND CVMCH.RN = 1
		WHERE
			--(CVM."SchOverAnotherSchTimeFlag" = ''Y'' OR CVM."VisitTimeOverAnotherVisitTimeFlag" = ''Y'')
			--AND
			CVM."AppPayerID" != ''0''
		GROUP BY
			CVM."PayerID",
			CVM."Contract",
			TO_CHAR(CVM."CRDATEUNIQUE", ''YYYY-MM-DD''),
			CVM."ProviderID",
			CVM."OfficeID"`;

  try {
      snowflake.execute({ sqlText: SQL1 });
		snowflake.execute({ sqlText: SQL2 });
      snowflake.execute({ sqlText: SQL3 });
		snowflake.execute({ sqlText: SQL4 });
      snowflake.execute({ sqlText: SQL5 });
		snowflake.execute({ sqlText: SQL6 });
      snowflake.execute({ sqlText: SQL7 });
		snowflake.execute({ sqlText: SQL8 });
      snowflake.execute({ sqlText: SQL9 });
		snowflake.execute({ sqlText: SQL10 });
      snowflake.execute({ sqlText: SQL11 });
		snowflake.execute({ sqlText: SQL12 });
      return "Provider Dashboard Data Loaded Successfully.";
  } catch (err) {
      throw "ERROR: " + err.message;
  }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.SEND_TASK_STATUS_EMAIL()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var sqlQuery = `
        WITH TaskStats AS (
            SELECT
                NAME AS TASK_NAME,
                COUNT(*) AS TOTAL_RUNS,
                COUNT(CASE WHEN STATE = ''FAILED'' THEN 1 ELSE NULL END) AS FAILURES_LAST_2DAYS,
                AVG(DATEDIFF(''second'', QUERY_START_TIME, COMPLETED_TIME)) AS AVG_DURATION_SECONDS,
                MAX(COMPLETED_TIME) AS LAST_COMPLETED_TIME,
                MAX(QUERY_START_TIME) AS LAST_START_TIME,
                MAX(STATE) AS LAST_STATE,
                TO_CHAR(
                    TO_TIMESTAMP(DATEDIFF(''second'', MAX(QUERY_START_TIME), MAX(COMPLETED_TIME))),
                    ''HH24:MI:SS''
                ) AS LAST_DURATION_TIME,
                TO_CHAR(
                    TO_TIMESTAMP(AVG(DATEDIFF(''second'', QUERY_START_TIME, COMPLETED_TIME))),
                    ''HH24:MI:SS''
                ) AS AVG_DURATION_TIME
            FROM 
                SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE
                NAME IN (
                    ''ENABLE_ONE_TIME_TASK'', ''ONE_TIME_TASK'', ''TASK_1_COPY_DATA'',
                    ''TASK_2_UPDATE_DATA'', ''TASK_2_UPDATE_DATA_1'', ''TASK_2_UPDATE_DATA_2'', ''TASK_2_UPDATE_DATA_3'',
                    ''TASK_3_INSERT_DATA'', ''TASK_3_INSERT_DATA_1'', ''TASK_3_INSERT_DATA_2'',
                    ''TASK_4_UPDATE_MAPS'', ''TASK_5_INSERT_CONFLICTS'', ''TASK_6_ASSIGN_GROUP_IDS'',
                    ''TASK_7_UPDATE_PHONE_CONTACT'', ''TASK_8_CREATE_NEW_LOG_HISTORY'', ''TASK_9_CREATE_LOG_HISTORY''
                )
                AND QUERY_START_TIME > CURRENT_TIMESTAMP - INTERVAL ''2 DAYS''
            GROUP BY NAME
        )
        SELECT
            TASK_NAME,
            TOTAL_RUNS,
            FAILURES_LAST_2DAYS,
            AVG_DURATION_SECONDS,
            LAST_STATE,
            LAST_START_TIME,
            LAST_COMPLETED_TIME,
            LAST_DURATION_TIME,
            AVG_DURATION_TIME 
        FROM TaskStats
        ORDER BY CASE TASK_NAME
            WHEN ''ENABLE_ONE_TIME_TASK'' THEN 1
            WHEN ''ONE_TIME_TASK'' THEN 2
            WHEN ''TASK_1_COPY_DATA'' THEN 3
            WHEN ''TASK_2_UPDATE_DATA'' THEN 4
            WHEN ''TASK_2_UPDATE_DATA_1'' THEN 5
            WHEN ''TASK_2_UPDATE_DATA_2'' THEN 6
            WHEN ''TASK_2_UPDATE_DATA_3'' THEN 7
            WHEN ''TASK_3_INSERT_DATA'' THEN 8
            WHEN ''TASK_3_INSERT_DATA_1'' THEN 9
            WHEN ''TASK_3_INSERT_DATA_2'' THEN 10
            WHEN ''TASK_4_UPDATE_MAPS'' THEN 11
            WHEN ''TASK_5_INSERT_CONFLICTS'' THEN 12
            WHEN ''TASK_6_ASSIGN_GROUP_IDS'' THEN 13
            WHEN ''TASK_7_UPDATE_PHONE_CONTACT'' THEN 14
            WHEN ''TASK_8_CREATE_NEW_LOG_HISTORY'' THEN 15
            WHEN ''TASK_9_CREATE_LOG_HISTORY'' THEN 16
            ELSE 999
        END
    `;

    var stmt = snowflake.createStatement({ sqlText: sqlQuery });
    var resultSet = stmt.execute();

    var emailBody = `
        <html>
        <head>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    color: #333;
                    line-height: 1.6;
                    background-color: #f4f4f9;
                    margin: 0;
                    padding: 20px;
                }
                h1 {
                    color: #0056b3;
                    text-align: center;
                    margin-bottom: 20px;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 20px;
                }
                th, td {
                    padding: 12px;
                    text-align: left;
                    border: 1px solid #ddd;
                }
                th {
                    background-color: #0073e6;
                    color: white;
                }
                tr:nth-child(even) {
                    background-color: #f2f2f2;
                }
                .highlight-failure {
                    background-color: #ff6666;  /* Red for failures */
                }
                .highlight-abnormal {
                    background-color: #ffcc66;  /* Orange for abnormal durations */
                }
            </style>
        </head>
        <body>
            <h1>Production Task Running Status Report</h1>
            <p>Dear Team,</p>
            <p>Please find below the current status of the production tasks over the past 2 days:</p>
            <table>
                <thead>
                    <tr>
                        <th>Task Name</th>
                        <th>Total Runs</th>
                        <th>Failures (Last 2 Days)</th>
                        <th>Average Duration (HH:MM:SS)</th>
                        <th>Last State</th>
                        <th>Last Start Time</th>
                        <th>Last Completed Time</th>
                        <th>Last Duration (HH:MM:SS)</th>
                    </tr>
                </thead>
                <tbody>
    `;

    while (resultSet.next()) {
        var failures = resultSet.getColumnValue(''FAILURES_LAST_2DAYS'');
        var avgDuration = resultSet.getColumnValue(''AVG_DURATION_TIME'');
        var lastDuration = resultSet.getColumnValue(''LAST_DURATION_TIME'');
        
        var rowClass = failures > 0 ? ''highlight-failure'' : '''';

        if (lastDuration && avgDuration) {
            var lastDurationInSeconds = convertToSeconds(lastDuration);
            var avgDurationInSeconds = convertToSeconds(avgDuration);

            var fifteenMinutesInSeconds = 15 * 60;  // 15 minutes in seconds
            if ((lastDurationInSeconds - avgDurationInSeconds) > fifteenMinutesInSeconds) {
                rowClass += '' highlight-abnormal'';
            }
        }

        emailBody += `
            <tr class="${rowClass}">
                <td>${resultSet.getColumnValue(''TASK_NAME'')}</td>
                <td>${resultSet.getColumnValue(''TOTAL_RUNS'')}</td>
                <td>${resultSet.getColumnValue(''FAILURES_LAST_2DAYS'')}</td>
                <td>${resultSet.getColumnValue(''AVG_DURATION_TIME'')}</td>
                <td>${resultSet.getColumnValue(''LAST_STATE'')}</td>
                <td>${resultSet.getColumnValue(''LAST_START_TIME'')}</td>
                <td>${resultSet.getColumnValue(''LAST_COMPLETED_TIME'')}</td>
                <td>${resultSet.getColumnValue(''LAST_DURATION_TIME'')}</td>
            </tr>
        `;
    }

    emailBody += `</tbody></table>
            <p>Best regards,<br>Dishant Modh (SRE)</p>
        </body>
        </html>
    `;

    snowflake.execute({
        sqlText: `CALL SYSTEM$SEND_EMAIL(
            ''VALIDATION_EMAIL_INT'',
            ''dmodh@hhaexchange.com, hmakwana@hhaexchange.com'',
            ''Task Running Status Report'',
            :1,
            ''text/html''
        )`,
        binds: [emailBody]
    });

    return ''Email sent successfully!'';

    function convertToSeconds(duration) {
        var timeParts = duration.split('':'');
        var hours = parseInt(timeParts[0]);
        var minutes = parseInt(timeParts[1]);
        var seconds = parseInt(timeParts[2]);
        return (hours * 3600) + (minutes * 60) + seconds;
    }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.SP_GET_FINAL_BILLABLE_UNITS_OPTIMIZED()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'get_billable_units'
EXTERNAL_ACCESS_INTEGRATIONS = (EAI_HHA_REVENUE_API)
EXECUTE AS OWNER
AS '
from decimal import Decimal
import requests
import snowflake.snowpark as snowpark
from datetime import datetime, timedelta
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def clean_json(obj):
    if isinstance(obj, dict):
        return {k: clean_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_json(i) for i in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def get_token():
    token_url = "https://idp.cloud.hhaexchange.com/connect/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": "HHAeXchange.Revenue.Api",
        "client_secret": "HT5N6DP6v5eTGP5uJdtmvs5fSRV1uM",
        "scope": "all:read"
    }
    token_headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    token_response = requests.post(token_url, data=token_data, headers=token_headers, verify=False)
    token_response.raise_for_status()
    return token_response.json().get("access_token")

def format_time(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def adjust_shift_endtime_for_time_distance(row):
    """
    Adjusts the shift end time by adding ETA travel minutes for distance-based visits
    Returns the adjusted end time or original end time if no adjustment needed
    """
    if row["DistanceFlag"] == "Y" and row["ETATravleMinutes"] is not None and row["ShVTENTime"] is not None:
        try:
            travel_minutes = int(row["ETATravleMinutes"])
            adjusted_end_time = row["ShVTENTime"] + timedelta(minutes=travel_minutes)
            return adjusted_end_time
        except (ValueError, TypeError):
            return row["ShVTENTime"]
    return row["ShVTENTime"]

def get_billable_units(session: snowpark.Session) -> str:
    update_rows = []
    failed_log_rows = []
    failed_ids = []
    BATCH_SIZE = 5000
    MAX_WORKERS = 500
    CHUNK_SIZE = 10000
    try:
        rows_df = session.sql("""
            SELECT 
                c."AppProviderID", 
                c."AppPayerID", 
                d."Environemnt", 
                c."AppVisitID", 
                c."AppOfficeID", 
                c."AppServiceCodeID", 
                c."AppPatientID", 
                c."ShVTSTTime", 
                c."ShVTENTime", 
                c."CShVTSTTime", 
                c."CShVTENTime",
                c.BILLABLEMINUTESFULLSHIFT,
                c.BILLABLEUNITSFULLSHIFT,
                c.BILLABLEMINUTESOVERLAP,
                c.BILLABLEUNITSOVERLAP,
                c.ID,
                c."CONFLICTID",
                c."DistanceFlag",
                c."ETATravleMinutes",
                cr."Application Contract Id" AS ContractIdForInternal
            FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS c
            LEFT JOIN ANALYTICS.BI.DIMPROVIDER d 
                ON c."ProviderID" = d."Provider Id"
            LEFT JOIN ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR cr
                ON c."VisitID" = cr."Visit Id"  
            WHERE d."Environemnt" IN (''PROD:APP'', ''PROD-APP2:AP2'', ''PROD-AWS:CLO'')
                AND c."PTOFlag" = ''N''
                AND c."InServiceFlag" = ''N''
				AND (
                    "SameSchTimeFlag" = ''Y'' OR 
                    "SameVisitTimeFlag" = ''Y'' OR 
                    "SchAndVisitTimeSameFlag" = ''Y'' OR 
                    "SchOverAnotherSchTimeFlag" = ''Y'' OR 
                    "VisitTimeOverAnotherVisitTimeFlag" = ''Y'' OR 
                    "SchTimeOverVisitTimeFlag" = ''Y'' or "DistanceFlag" = ''Y''
                )
				AND c.BILLABLEMINUTESFULLSHIFT IS NULL
                AND c."AppVisitID" IS NOT NULL
				AND c."AppServiceCodeID" IS NOT NULL
				AND (c."P_PAddressState" = ''NY'' OR c."ConP_PAddressState" = ''NY'')
                AND (c.FAILEDON IS NULL OR c.FAILEDON >= DATEADD(DAY, -2, CURRENT_TIMESTAMP()))	  
        """).collect()
		
        if not rows_df:
            return "No data to process."

        token = get_token()
        token_expiry = datetime.now() + timedelta(minutes=8)

        def safe_int(val):
            try:
                return int(val) if val is not None else None
            except:
                return None

        def log_failure(visit_id, payer_id, contract_id, payload, response, failure_type, error_msg, conflict_id, row_id):
            if row_id is None:
                return
            failed_ids.append(row_id)
            failed_log_rows.append((
                visit_id,
                payer_id,
                contract_id,
                json.dumps(payload).replace("''", "''''"),
                json.dumps(response if response else {}).replace("''", "''''"),
                failure_type,
                error_msg.replace("''", "''''"),
                conflict_id
            ))

        def process_row(row):
            nonlocal token, token_expiry
            
            if datetime.now() >= token_expiry:
                token = get_token()
                token_expiry = datetime.now() + timedelta(minutes=8)

            env = row["Environemnt"]
            if env == "PROD:APP":
                api_url = "https://revenueapiapp.hhaexchange.com/v1/billable-units"
            elif env == "PROD-APP2:AP2":
                api_url = "https://revenueapiapp2.hhaexchange.com/v1/billable-units"
            elif env == "PROD-AWS:CLO":
                api_url = "https://revenueapicloud.hhaexchange.com/v1/billable-units"
            else:
                return None

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            payer_id = safe_int(row["AppPayerID"])
            contract_id = safe_int(row["CONTRACTIDFORINTERNAL"])
            app_visit_id = safe_int(row["AppVisitID"])
            row_id = safe_int(row["ID"])
            conflict_id = safe_int(row["CONFLICTID"])
            adjusted_end_time = adjust_shift_endtime_for_time_distance(row)

            if not app_visit_id:
                log_failure(app_visit_id, contract_id, payer_id, None, None, "MissingData", "AppVisitID is null or invalid", conflict_id, row_id)
                return None

            payer_id_to_send = contract_id if (not payer_id or payer_id == 0) else payer_id
            if not payer_id_to_send:
                log_failure(app_visit_id, contract_id, payer_id, None, None, "MissingData", "Both PayerID and ContractID are null or invalid", conflict_id, row_id)
                return None

            try:
                st = row["ShVTSTTime"]
                et0 = row["ShVTENTime"]
                et = adjusted_end_time
                cst = row["CShVTSTTime"]
                cet = row["CShVTENTime"]

                if not st or not et:
                    log_failure(app_visit_id, contract_id, payer_id, None, None, "MissingData", "Start time or end time is null", conflict_id, row_id)
                    return None

                visit_payloads = [{
                    "visitID": app_visit_id,
                    "scheduleIdentifier": f"{row_id}_full",
                    "scheduleStartTime": format_time(st),
                    "scheduleEndTime": format_time(et0),
                    "visitStartTime": format_time(st),
                    "visitEndTime": format_time(et0),
                    "ApprovedTravelTimeMinutes": 0,
                    "adjMinutes": 0,
                    "BankedMinutes": 0
                }]

                if cst and cet:
                    overlap_start, overlap_end = st, et
                    if cst >= st and cst <= et and cet > et:
                        overlap_start, overlap_end = cst, et
                    elif st >= cst and st <= cet and et > cet:
                        overlap_start, overlap_end = st, cet
                    elif cst >= st and cet <= et:
                        overlap_start, overlap_end = cst, cet
                    elif st >= cst and et <= cet:
                        overlap_start, overlap_end = st, et
                    elif cst < st and cet > et:
                        overlap_start, overlap_end = st, et
                    elif st < cst and et > cet:
                        overlap_start, overlap_end = cst, cet

                    visit_payloads.append({
                        "visitID": app_visit_id,
                        "scheduleIdentifier": f"{row_id}_overlap",
                        "scheduleStartTime": format_time(overlap_start),
                        "scheduleEndTime": format_time(overlap_end),
                        "visitStartTime": format_time(overlap_start),
                        "visitEndTime": format_time(overlap_end),
                        "ApprovedTravelTimeMinutes": 0,
                        "adjMinutes": 0,
                        "BankedMinutes": 0
                    })

                payload = {
                    "vendorID": safe_int(row["AppProviderID"]),
                    "payerID": payer_id_to_send,
                    "officeID": safe_int(row["AppOfficeID"]),
                    "servicecodeID": safe_int(row["AppServiceCodeID"]),
                    "patientID": safe_int(row["AppPatientID"]),
                    "userID": 7,
                    "callerInfo": "Conflict",
                    "visits": visit_payloads
                }

                if not payload["vendorID"] or not payload["officeID"] or not payload["servicecodeID"] or not payload["patientID"]:
                    log_failure(app_visit_id, contract_id, payer_id, payload, None, "MissingData", "Required fields (vendorID, officeID, servicecodeID, patientID) are null or invalid", conflict_id, row_id)
                    return None

                for attempt in range(3):
                    try:
                        api_response = requests.post(api_url, json=clean_json(payload), headers=headers, verify=False, timeout=30)
                        break
                    except requests.exceptions.Timeout:
                        if attempt == 2:
                            log_failure(app_visit_id, contract_id, payer_id, payload, None, "Timeout", "API request timed out after 3 attempts", conflict_id, row_id)
                            return None
                        time.sleep(1)
                    except requests.exceptions.RequestException as e:
                        log_failure(app_visit_id, contract_id, payer_id, payload, None, "RequestException", str(e), conflict_id, row_id)
                        return None

                if api_response.status_code == 200:
                    data = api_response.json()
                    visits = data.get("visits", [])
                    
                    config_errors = []
                    for v in visits:
                        if "Message" in v and v["Message"] and "configuration" in v["Message"].lower().strip():
                            config_errors.append(v["Message"])
                    
                    if config_errors:
                        error_msg = "; ".join(config_errors)
                        log_failure(app_visit_id, contract_id, payer_id, payload, data, "ConfigNotFound", error_msg, conflict_id, row_id)
                        return None

                    full = next((v for v in visits if v.get("scheduleIdentifier", "").endswith("_full")), {})
                    overlap = next((v for v in visits if v.get("scheduleIdentifier", "").endswith("_overlap")), {})
                    return (row_id, full.get("billableMinutes", 0), full.get("billableUnits", 0), overlap.get("billableMinutes", 0), overlap.get("billableUnits", 0))
                else:
                    log_failure(app_visit_id, contract_id, payer_id, payload, api_response.text, "API Error", f"Status: {api_response.status_code}", conflict_id, row_id)
                    return None
            except Exception as e:
                log_failure(app_visit_id, contract_id, payer_id, payload, None, "Exception", str(e), conflict_id, row_id)
                return None

        total_processed = 0
        for i in range(0, len(rows_df), BATCH_SIZE):
            batch = rows_df[i:i + BATCH_SIZE]
            batch_start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_row, row) for row in batch]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        update_rows.append(result)
                        total_processed += 1
            
            batch_time = time.time() - batch_start_time
            print(f"Processed batch {i//BATCH_SIZE + 1}: {len(batch)} records in {batch_time:.2f} seconds")

        if update_rows:
            CHUNK_SIZE = 10000
            for i in range(0, len(update_rows), CHUNK_SIZE):
                chunk = update_rows[i:i + CHUNK_SIZE]
                df = session.create_dataframe(chunk, schema=["ID", "BMF", "BUF", "BMO", "BUO"])
                df.write.save_as_table("CONFLICTREPORT.PUBLIC.TMP_BILLABLE_UPDATES", mode="overwrite")
                session.sql("""
                    MERGE INTO CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS t
                    USING CONFLICTREPORT.PUBLIC.TMP_BILLABLE_UPDATES s
                    ON t."ID" = s.ID
                    WHEN MATCHED THEN UPDATE SET 
                        BILLABLEMINUTESFULLSHIFT = s.BMF,
                        BILLABLEUNITSFULLSHIFT = s.BUF,
                        BILLABLEMINUTESOVERLAP = s.BMO,
                        BILLABLEUNITSOVERLAP = s.BUO,
                        FAILEDON = NULL
                """).collect()

        if failed_log_rows:
            for i in range(0, len(failed_log_rows), CHUNK_SIZE):
                chunk = failed_log_rows[i:i + CHUNK_SIZE]
                df_logs = session.create_dataframe(chunk, schema=[
                    "APPVISITID", "APPPAYERID", "CONTRACT_ID_INTERNAL", 
                    "PAYLOAD", "RESPONSE", "FAILURE_TYPE", "ERROR_MESSAGE",
                    "CONFLICTID"
                ])
                df_logs.write.mode("append").save_as_table("CONFLICTREPORT.PUBLIC.FAILED_API_LOGS")

        if failed_ids:
            for i in range(0, len(failed_ids), CHUNK_SIZE):
                chunk_ids = failed_ids[i:i + CHUNK_SIZE]
                id_list = ",".join(str(i) for i in chunk_ids)
                session.sql(f"""
                    UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
                    SET FAILEDON = CURRENT_TIMESTAMP()
                    WHERE "ID" IN ({id_list}) AND FAILEDON IS NULL
                """).collect()

        return f"Processing completed. Total processed: {total_processed}, Failed: {len(failed_ids)}"

    except Exception as e:
        return f"General exception: {str(e)}"
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.SP_GET_REVENUE_API_TOKEN()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'get_token'
EXTERNAL_ACCESS_INTEGRATIONS = (EAI_HHA_REVENUE_API)
EXECUTE AS OWNER
AS '
import requests

def get_token():
    token_url = "https://idp.cloud.hhaexchange.com/connect/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": "HHAeXchange.Revenue.Api",
        "client_secret": "HT5N6DP6v5eTGP5uJdtmvs5fSRV1uM",
        "scope": "all:read"
    }
    token_headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(token_url, data=token_data, headers=token_headers, verify=False)
    response.raise_for_status()
    return response.json().get("access_token")
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.UPDATE_CONFLICT_VISIT_MAPS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
 var sql_query = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS target
                   SET target."CONFLICTID" = source."CONFLICTID"
                   FROM (
                       SELECT
                           "VisitID",
                           "AppVisitID",
                           ROW_NUMBER() OVER (ORDER BY "VisitID", "AppVisitID") + COALESCE((SELECT MAX(CONFLICTID) FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS), 0) AS "CONFLICTID"
                       FROM
                           CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
						WHERE "CONFLICTID" IS NULL
                       GROUP BY
                           "VisitID", "AppVisitID"
                   ) AS source
                   WHERE target."VisitID" = source."VisitID" AND target."AppVisitID" = source."AppVisitID"`;
                  
                 	var sql_query1 = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS cv
					SET CRDATEUNIQUE = (
					    SELECT MIN(v."CreatedDate")
					    FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS v
					    WHERE v.CONFLICTID = cv.CONFLICTID
					) WHERE CRDATEUNIQUE IS NULL`;
					 
   var stmt = snowflake.createStatement({sqlText: sql_query});
   var res = stmt.execute();
   var stmt1 = snowflake.createStatement({sqlText: sql_query1});
   var res1 = stmt1.execute();
   return "ConflictIDs assigned successfully.";
 } catch (err) {
	var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
  // If an error occurs, capture it and raise it with a custom message
  throw ''ERROR: '' + err.message;  // Returns the error message to the caller
 }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.UPDATE_CREATE_LOG_HISTORY()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	try {
		// First, insert the main log history records
        var insertMainLogsUT = snowflake.createStatement({
            sqlText: `
                TRUNCATE TABLE CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES_TEMP
            `
        });
        insertMainLogsUT.execute();

		var inupdatequery = `INSERT INTO CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES_TEMP (CONID, "LogID", "OldValue", "NewValue", "VisitID", "AppVisitID")
		WITH NewLogRecords AS (
			SELECT
				T2.ID as CONID,
				T2."VisitID" as "PVisitID",
				T2."AppVisitID" as "PAppVisitID",
				T2."ConVisitID" as "CVisitID",
				T2."ConAppVisitID" as "CAppVisitID",
				CAST(T2.CONFLICTID AS TEXT) AS "CONFLICTID",
				CAST(T2.SSN AS TEXT) AS "SSN",
				CAST(T2."ProviderName" AS TEXT) AS "ProviderName",
				CAST(T2."ConProviderName" AS TEXT) AS "ConProviderName",
				TO_CHAR(T2."VisitDate", ''YYYY-MM-DD'') AS "VisitDate",
				TO_CHAR(T2."SchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchStartTime",
				TO_CHAR(T2."SchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchEndTime",
				TO_CHAR(T2."ConSchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchStartTime",
				TO_CHAR(T2."ConSchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchEndTime",
				TO_CHAR(T2."VisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitStartTime",
				TO_CHAR(T2."VisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitEndTime",
				TO_CHAR(T2."ConVisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitStartTime",
				TO_CHAR(T2."ConVisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitEndTime",
				TO_CHAR(T2."EVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVStartTime",
				TO_CHAR(T2."EVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVEndTime",
				TO_CHAR(T2."ConEVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVStartTime",
				TO_CHAR(T2."ConEVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVEndTime",
				CAST(T2."AideCode" AS TEXT) AS "AideCode",
				CAST(T2."AideName" AS TEXT) AS "AideName",
				CAST(T2."AideSSN" AS TEXT) AS "AideSSN",
				CAST(T2."ConAideCode" AS TEXT) AS "ConAideCode",
				CAST(T2."ConAideName" AS TEXT) AS "ConAideName",
				CAST(T2."ConAideSSN" AS TEXT) AS "ConAideSSN",
				CAST(T2."Office" AS TEXT) AS "Office",
				CAST(T2."ConOffice" AS TEXT) AS "ConOffice",
				CAST(T2."PAdmissionID" AS TEXT) AS "PAdmissionID",
				CAST(T2."PName" AS TEXT) AS "PName",
				CAST(T2."PAddressL1" AS TEXT) AS "PAddressL1",
				CAST(T2."PAddressL2" AS TEXT) AS "PAddressL2",
				CAST(T2."PCity" AS TEXT) AS "PCity",
				CAST(T2."PAddressState" AS TEXT) AS "PAddressState",
				CAST(T2."PZipCode" AS TEXT) AS "PZipCode",
				CAST(T2."PCounty" AS TEXT) AS "PCounty",
				CAST(T2."PLongitude" AS TEXT) AS "PLongitude",
				CAST(T2."PLatitude" AS TEXT) AS "PLatitude",
				CAST(T2."ConPAdmissionID" AS TEXT) AS "ConPAdmissionID",
				CAST(T2."ConPName" AS TEXT) AS "ConPName",
				CAST(T2."ConPAddressL1" AS TEXT) AS "ConPAddressL1",
				CAST(T2."ConPAddressL2" AS TEXT) AS "ConPAddressL2",
				CAST(T2."ConPCity" AS TEXT) AS "ConPCity",
				CAST(T2."ConPAddressState" AS TEXT) AS "ConPAddressState",
				CAST(T2."ConPZipCode" AS TEXT) AS "ConPZipCode",
				CAST(T2."ConPCounty" AS TEXT) AS "ConPCounty",
				CAST(T2."ConPLongitude" AS TEXT) AS "ConPLongitude",
				CAST(T2."ConPLatitude" AS TEXT) AS "ConPLatitude",
				CAST(T2."Contract" AS TEXT) AS "Contract",
				CAST(T2."ConContract" AS TEXT) AS "ConContract",
				TO_CHAR(T2."BilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "BilledDate", 
				TO_CHAR(T2."ConBilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConBilledDate",
				CAST(T2."BilledHours" AS TEXT) AS "BilledHours",
				CAST(T2."ConBilledHours" AS TEXT) AS "ConBilledHours",
				CAST(T2."Billed" AS TEXT) AS "Billed",
				CAST(T2."ConBilled" AS TEXT) AS "ConBilled",
				CAST(T2."MinuteDiffBetweenSch" AS TEXT) AS "MinuteDiffBetweenSch",
				CAST(T2."DistanceMilesFromLatLng" AS TEXT) AS "DistanceMilesFromLatLng",
				CAST(T2."AverageMilesPerHour" AS TEXT) AS "AverageMilesPerHour",
				CAST(T2."ETATravleMinutes" AS TEXT) AS "ETATravleMinutes",
				TO_CHAR(T2."InserviceStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceStartDate", 
				TO_CHAR(T2."InserviceEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceEndDate", 
				TO_CHAR(T2."PTOStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOStartDate", 
				TO_CHAR(T2."PTOEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOEndDate",
				CAST(T2."SameSchTimeFlag" AS TEXT) AS "SameSchTimeFlag",
				CAST(T2."SameVisitTimeFlag" AS TEXT) AS "SameVisitTimeFlag",
				CAST(T2."SchAndVisitTimeSameFlag" AS TEXT) AS "SchAndVisitTimeSameFlag",
				CAST(T2."SchOverAnotherSchTimeFlag" AS TEXT) AS "SchOverAnotherSchTimeFlag",
				CAST(T2."VisitTimeOverAnotherVisitTimeFlag" AS TEXT) AS "VisitTimeOverAnotherVisitTimeFlag",
				CAST(T2."SchTimeOverVisitTimeFlag" AS TEXT) AS "SchTimeOverVisitTimeFlag",
				CAST(T2."DistanceFlag" AS TEXT) AS "DistanceFlag",
				CAST(T2."InServiceFlag" AS TEXT) AS "InServiceFlag",
				CAST(T2."PTOFlag" AS TEXT) AS "PTOFlag",
				CAST(T2."StatusFlag" AS TEXT) AS "ConStatusFlag",
				CAST(T2."AideFName" AS TEXT) AS "AideFName",
				CAST(T2."AideLName" AS TEXT) AS "AideLName",
				CAST(T2."ConAideFName" AS TEXT) AS "ConAideFName",
				CAST(T2."ConAideLName" AS TEXT) AS "ConAideLName",
				CAST(T2."PFName" AS TEXT) AS "PFName",
				CAST(T2."PLName" AS TEXT) AS "PLName",
				CAST(T2."ConPFName" AS TEXT) AS "ConPFName",
				CAST(T2."ConPLName" AS TEXT) AS "ConPLName",
				CAST(T2."PMedicaidNumber" AS TEXT) AS "PMedicaidNumber",
				CAST(T2."ConPMedicaidNumber" AS TEXT) AS "ConPMedicaidNumber",
				CAST(T2."PayerState" AS TEXT) AS "PayerState",
				CAST(T2."ConPayerState" AS TEXT) AS "ConPayerState",
				CAST(T2."AgencyContact" AS TEXT) AS "AgencyContact",
				CAST(T2."ConAgencyContact" AS TEXT) AS "ConAgencyContact",
				CAST(T2."AgencyPhone" AS TEXT) AS "AgencyPhone",
				CAST(T2."ConAgencyPhone" AS TEXT) AS "ConAgencyPhone",
				CAST(T2."LastUpdatedBy" AS TEXT) AS "LastUpdatedBy",
				CAST(T2."ConLastUpdatedBy" AS TEXT) AS "ConLastUpdatedBy",
				CAST(T2."LastUpdatedDate" AS TEXT) AS "LastUpdatedDate",
				CAST(T2."ConLastUpdatedDate" AS TEXT) AS "ConLastUpdatedDate",
				CAST(T2."BilledRate" AS TEXT) AS "BilledRate",
				CAST(T2."TotalBilledAmount" AS TEXT) AS "TotalBilledAmount",
				CAST(T2."ConBilledRate" AS TEXT) AS "ConBilledRate",
				CAST(T2."ConTotalBilledAmount" AS TEXT) AS "ConTotalBilledAmount",
				CAST(T2."IsMissed" AS TEXT) AS "IsMissed",
				CAST(T2."MissedVisitReason" AS TEXT) AS "MissedVisitReason",
				CAST(T2."EVVType" AS TEXT) AS "EVVType",
				CAST(T2."ConIsMissed" AS TEXT) AS "ConIsMissed",
				CAST(T2."ConMissedVisitReason" AS TEXT) AS "ConMissedVisitReason",
				CAST(T2."ConEVVType" AS TEXT) AS "ConEVVType",
				CAST(T2."PStatus" AS TEXT) AS "PStatus",
				CAST(T2."ConPStatus" AS TEXT) AS "ConPStatus",
				CAST(T2."AideStatus" AS TEXT) AS "AideStatus",
				CAST(T2."ConAideStatus" AS TEXT) AS "ConAideStatus",
				CAST(T2."ConNoResponseFlag" AS TEXT) AS "ConNoResponseFlag",
				CAST(T2."ConNoResponseTitle" AS TEXT) AS "ConNoResponseTitle",
				CAST(T2."ConNoResponseNotes" AS TEXT) AS "ConNoResponseNotes",
				CAST(T2."P_PAdmissionID" AS TEXT) AS "P_PAdmissionID",
				CAST(T2."P_PName" AS TEXT) AS "P_PName",
				CAST(T2."P_PAddressL1" AS TEXT) AS "P_PAddressL1",
				CAST(T2."P_PAddressL2" AS TEXT) AS "P_PAddressL2",
				CAST(T2."P_PCity" AS TEXT) AS "P_PCity",
				CAST(T2."P_PAddressState" AS TEXT) AS "P_PAddressState",
				CAST(T2."P_PZipCode" AS TEXT) AS "P_PZipCode",
				CAST(T2."P_PCounty" AS TEXT) AS "P_PCounty",
				CAST(T2."P_PFName" AS TEXT) AS "P_PFName",
				CAST(T2."P_PLName" AS TEXT) AS "P_PLName",
				CAST(T2."P_PMedicaidNumber" AS TEXT) AS "P_PMedicaidNumber",
				CAST(T2."ConP_PAdmissionID" AS TEXT) AS "ConP_PAdmissionID",
				CAST(T2."ConP_PName" AS TEXT) AS "ConP_PName",
				CAST(T2."ConP_PAddressL1" AS TEXT) AS "ConP_PAddressL1",
				CAST(T2."ConP_PAddressL2" AS TEXT) AS "ConP_PAddressL2",
				CAST(T2."ConP_PCity" AS TEXT) AS "ConP_PCity",
				CAST(T2."ConP_PAddressState" AS TEXT) AS "ConP_PAddressState",
				CAST(T2."ConP_PZipCode" AS TEXT) AS "ConP_PZipCode",
				CAST(T2."ConP_PCounty" AS TEXT) AS "ConP_PCounty",
				CAST(T2."ConP_PFName" AS TEXT) AS "ConP_PFName",
				CAST(T2."ConP_PLName" AS TEXT) AS "ConP_PLName",
				CAST(T2."ConP_PMedicaidNumber" AS TEXT) AS "ConP_PMedicaidNumber",
				CAST(T2."PA_PAdmissionID" AS TEXT) AS "PA_PAdmissionID",
				CAST(T2."PA_PName" AS TEXT) AS "PA_PName",
				CAST(T2."PA_PAddressL1" AS TEXT) AS "PA_PAddressL1",
				CAST(T2."PA_PAddressL2" AS TEXT) AS "PA_PAddressL2",
				CAST(T2."PA_PCity" AS TEXT) AS "PA_PCity",
				CAST(T2."PA_PAddressState" AS TEXT) AS "PA_PAddressState",
				CAST(T2."PA_PZipCode" AS TEXT) AS "PA_PZipCode",
				CAST(T2."PA_PCounty" AS TEXT) AS "PA_PCounty",
				CAST(T2."PA_PFName" AS TEXT) AS "PA_PFName",
				CAST(T2."PA_PLName" AS TEXT) AS "PA_PLName",
				CAST(T2."PA_PMedicaidNumber" AS TEXT) AS "PA_PMedicaidNumber",
				CAST(T2."ConPA_PAdmissionID" AS TEXT) AS "ConPA_PAdmissionID",
				CAST(T2."ConPA_PName" AS TEXT) AS "ConPA_PName",
				CAST(T2."ConPA_PAddressL1" AS TEXT) AS "ConPA_PAddressL1",
				CAST(T2."ConPA_PAddressL2" AS TEXT) AS "ConPA_PAddressL2",
				CAST(T2."ConPA_PCity" AS TEXT) AS "ConPA_PCity",
				CAST(T2."ConPA_PAddressState" AS TEXT) AS "ConPA_PAddressState",
				CAST(T2."ConPA_PZipCode" AS TEXT) AS "ConPA_PZipCode",
				CAST(T2."ConPA_PCounty" AS TEXT) AS "ConPA_PCounty",
				CAST(T2."ConPA_PFName" AS TEXT) AS "ConPA_PFName",
				CAST(T2."ConPA_PLName" AS TEXT) AS "ConPA_PLName",
				CAST(T2."ConPA_PMedicaidNumber" AS TEXT) AS "ConPA_PMedicaidNumber",
				CAST(T2."ContractType" AS TEXT) AS "ContractType",
				CAST(T2."ConContractType" AS TEXT) AS "ConContractType",
				CAST(T2."BillRateNonBilled" AS TEXT) AS "BillRateNonBilled",
				CAST(T2."ConBillRateNonBilled" AS TEXT) AS "ConBillRateNonBilled",
				CAST(T2."BillRateBoth" AS TEXT) AS "BillRateBoth",
				CAST(T2."ConBillRateBoth" AS TEXT) AS "ConBillRateBoth",
				CAST(T2."FederalTaxNumber" AS TEXT) AS "FederalTaxNumber",
				CAST(T2."ConFederalTaxNumber" AS TEXT) AS "ConFederalTaxNumber",
				CAST(T1."StatusFlag" AS TEXT) AS "StatusFlag",
				CAST(T1."FlagForReview" AS TEXT) AS "FlagForReview",
				TO_CHAR(T1."FlagForReviewDate", ''YYYY-MM-DD HH24:MI:SS'') AS "FlagForReviewDate",
				CAST(T2."FlagForReview" AS TEXT) AS "ConFlagForReview",
				TO_CHAR(T2."FlagForReviewDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConFlagForReviewDate",
				TO_CHAR(T2."ConInserviceStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConInserviceStartDate", 
				TO_CHAR(T2."ConInserviceEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConInserviceEndDate", 
				TO_CHAR(T2."ConPTOStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConPTOStartDate", 
				TO_CHAR(T2."ConPTOEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConPTOEndDate"
			FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS T2
			JOIN CONFLICTREPORT."PUBLIC".CONFLICTS T1 ON T1.CONFLICTID = T2.CONFLICTID WHERE DATE(T2."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
		),
		TempConflictValues AS (
			SELECT
				T2.ID as CONID,
				T2."VisitID" as "PVisitID",
				T2."AppVisitID" as "PAppVisitID",
				T2."ConVisitID" as "CVisitID",
				T2."ConAppVisitID" as "CAppVisitID",
				CAST(T2.CONFLICTID AS TEXT) AS "CONFLICTID",
				CAST(T2.SSN AS TEXT) AS "SSN",
				CAST(T2."ProviderName" AS TEXT) AS "ProviderName",
				CAST(T2."ConProviderName" AS TEXT) AS "ConProviderName",
				TO_CHAR(T2."VisitDate", ''YYYY-MM-DD'') AS "VisitDate",
				TO_CHAR(T2."SchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchStartTime",
				TO_CHAR(T2."SchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "SchEndTime",
				TO_CHAR(T2."ConSchStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchStartTime",
				TO_CHAR(T2."ConSchEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConSchEndTime",
				TO_CHAR(T2."VisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitStartTime",
				TO_CHAR(T2."VisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "VisitEndTime",
				TO_CHAR(T2."ConVisitStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitStartTime",
				TO_CHAR(T2."ConVisitEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConVisitEndTime",
				TO_CHAR(T2."EVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVStartTime",
				TO_CHAR(T2."EVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "EVVEndTime",
				TO_CHAR(T2."ConEVVStartTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVStartTime",
				TO_CHAR(T2."ConEVVEndTime", ''YYYY-MM-DD HH24:MI:SS'') AS "ConEVVEndTime",
				CAST(T2."AideCode" AS TEXT) AS "AideCode",
				CAST(T2."AideName" AS TEXT) AS "AideName",
				CAST(T2."AideSSN" AS TEXT) AS "AideSSN",
				CAST(T2."ConAideCode" AS TEXT) AS "ConAideCode",
				CAST(T2."ConAideName" AS TEXT) AS "ConAideName",
				CAST(T2."ConAideSSN" AS TEXT) AS "ConAideSSN",
				CAST(T2."Office" AS TEXT) AS "Office",
				CAST(T2."ConOffice" AS TEXT) AS "ConOffice",
				CAST(T2."PAdmissionID" AS TEXT) AS "PAdmissionID",
				CAST(T2."PName" AS TEXT) AS "PName",
				CAST(T2."PAddressL1" AS TEXT) AS "PAddressL1",
				CAST(T2."PAddressL2" AS TEXT) AS "PAddressL2",
				CAST(T2."PCity" AS TEXT) AS "PCity",
				CAST(T2."PAddressState" AS TEXT) AS "PAddressState",
				CAST(T2."PZipCode" AS TEXT) AS "PZipCode",
				CAST(T2."PCounty" AS TEXT) AS "PCounty",
				CAST(T2."PLongitude" AS TEXT) AS "PLongitude",
				CAST(T2."PLatitude" AS TEXT) AS "PLatitude",
				CAST(T2."ConPAdmissionID" AS TEXT) AS "ConPAdmissionID",
				CAST(T2."ConPName" AS TEXT) AS "ConPName",
				CAST(T2."ConPAddressL1" AS TEXT) AS "ConPAddressL1",
				CAST(T2."ConPAddressL2" AS TEXT) AS "ConPAddressL2",
				CAST(T2."ConPCity" AS TEXT) AS "ConPCity",
				CAST(T2."ConPAddressState" AS TEXT) AS "ConPAddressState",
				CAST(T2."ConPZipCode" AS TEXT) AS "ConPZipCode",
				CAST(T2."ConPCounty" AS TEXT) AS "ConPCounty",
				CAST(T2."ConPLongitude" AS TEXT) AS "ConPLongitude",
				CAST(T2."ConPLatitude" AS TEXT) AS "ConPLatitude",
				CAST(T2."Contract" AS TEXT) AS "Contract",
				CAST(T2."ConContract" AS TEXT) AS "ConContract",
				TO_CHAR(T2."BilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "BilledDate", 
				TO_CHAR(T2."ConBilledDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConBilledDate",
				CAST(T2."BilledHours" AS TEXT) AS "BilledHours",
				CAST(T2."ConBilledHours" AS TEXT) AS "ConBilledHours",
				CAST(T2."Billed" AS TEXT) AS "Billed",
				CAST(T2."ConBilled" AS TEXT) AS "ConBilled",
				CAST(T2."MinuteDiffBetweenSch" AS TEXT) AS "MinuteDiffBetweenSch",
				CAST(T2."DistanceMilesFromLatLng" AS TEXT) AS "DistanceMilesFromLatLng",
				CAST(T2."AverageMilesPerHour" AS TEXT) AS "AverageMilesPerHour",
				CAST(T2."ETATravleMinutes" AS TEXT) AS "ETATravleMinutes",
				TO_CHAR(T2."InserviceStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceStartDate", 
				TO_CHAR(T2."InserviceEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "InserviceEndDate", 
				TO_CHAR(T2."PTOStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOStartDate", 
				TO_CHAR(T2."PTOEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "PTOEndDate",
				CAST(T2."SameSchTimeFlag" AS TEXT) AS "SameSchTimeFlag",
				CAST(T2."SameVisitTimeFlag" AS TEXT) AS "SameVisitTimeFlag",
				CAST(T2."SchAndVisitTimeSameFlag" AS TEXT) AS "SchAndVisitTimeSameFlag",
				CAST(T2."SchOverAnotherSchTimeFlag" AS TEXT) AS "SchOverAnotherSchTimeFlag",
				CAST(T2."VisitTimeOverAnotherVisitTimeFlag" AS TEXT) AS "VisitTimeOverAnotherVisitTimeFlag",
				CAST(T2."SchTimeOverVisitTimeFlag" AS TEXT) AS "SchTimeOverVisitTimeFlag",
				CAST(T2."DistanceFlag" AS TEXT) AS "DistanceFlag",
				CAST(T2."InServiceFlag" AS TEXT) AS "InServiceFlag",
				CAST(T2."PTOFlag" AS TEXT) AS "PTOFlag",
				CAST(T2."ConStatusFlag" AS TEXT) AS "ConStatusFlag",
				CAST(T2."AideFName" AS TEXT) AS "AideFName",
				CAST(T2."AideLName" AS TEXT) AS "AideLName",
				CAST(T2."ConAideFName" AS TEXT) AS "ConAideFName",
				CAST(T2."ConAideLName" AS TEXT) AS "ConAideLName",
				CAST(T2."PFName" AS TEXT) AS "PFName",
				CAST(T2."PLName" AS TEXT) AS "PLName",
				CAST(T2."ConPFName" AS TEXT) AS "ConPFName",
				CAST(T2."ConPLName" AS TEXT) AS "ConPLName",
				CAST(T2."PMedicaidNumber" AS TEXT) AS "PMedicaidNumber",
				CAST(T2."ConPMedicaidNumber" AS TEXT) AS "ConPMedicaidNumber",
				CAST(T2."PayerState" AS TEXT) AS "PayerState",
				CAST(T2."ConPayerState" AS TEXT) AS "ConPayerState",
				CAST(T2."AgencyContact" AS TEXT) AS "AgencyContact",
				CAST(T2."ConAgencyContact" AS TEXT) AS "ConAgencyContact",
				CAST(T2."AgencyPhone" AS TEXT) AS "AgencyPhone",
				CAST(T2."ConAgencyPhone" AS TEXT) AS "ConAgencyPhone",
				CAST(T2."LastUpdatedBy" AS TEXT) AS "LastUpdatedBy",
				CAST(T2."ConLastUpdatedBy" AS TEXT) AS "ConLastUpdatedBy",
				CAST(T2."LastUpdatedDate" AS TEXT) AS "LastUpdatedDate",
				CAST(T2."ConLastUpdatedDate" AS TEXT) AS "ConLastUpdatedDate",
				CAST(T2."BilledRate" AS TEXT) AS "BilledRate",
				CAST(T2."TotalBilledAmount" AS TEXT) AS "TotalBilledAmount",
				CAST(T2."ConBilledRate" AS TEXT) AS "ConBilledRate",
				CAST(T2."ConTotalBilledAmount" AS TEXT) AS "ConTotalBilledAmount",
				CAST(T2."IsMissed" AS TEXT) AS "IsMissed",
				CAST(T2."MissedVisitReason" AS TEXT) AS "MissedVisitReason",
				CAST(T2."EVVType" AS TEXT) AS "EVVType",
				CAST(T2."ConIsMissed" AS TEXT) AS "ConIsMissed",
				CAST(T2."ConMissedVisitReason" AS TEXT) AS "ConMissedVisitReason",
				CAST(T2."ConEVVType" AS TEXT) AS "ConEVVType",
				CAST(T2."PStatus" AS TEXT) AS "PStatus",
				CAST(T2."ConPStatus" AS TEXT) AS "ConPStatus",
				CAST(T2."AideStatus" AS TEXT) AS "AideStatus",
				CAST(T2."ConAideStatus" AS TEXT) AS "ConAideStatus",
				CAST(T2."ConNoResponseFlag" AS TEXT) AS "ConNoResponseFlag",
				CAST(T2."ConNoResponseTitle" AS TEXT) AS "ConNoResponseTitle",
				CAST(T2."ConNoResponseNotes" AS TEXT) AS "ConNoResponseNotes",
				CAST(T2."P_PAdmissionID" AS TEXT) AS "P_PAdmissionID",
				CAST(T2."P_PName" AS TEXT) AS "P_PName",
				CAST(T2."P_PAddressL1" AS TEXT) AS "P_PAddressL1",
				CAST(T2."P_PAddressL2" AS TEXT) AS "P_PAddressL2",
				CAST(T2."P_PCity" AS TEXT) AS "P_PCity",
				CAST(T2."P_PAddressState" AS TEXT) AS "P_PAddressState",
				CAST(T2."P_PZipCode" AS TEXT) AS "P_PZipCode",
				CAST(T2."P_PCounty" AS TEXT) AS "P_PCounty",
				CAST(T2."P_PFName" AS TEXT) AS "P_PFName",
				CAST(T2."P_PLName" AS TEXT) AS "P_PLName",
				CAST(T2."P_PMedicaidNumber" AS TEXT) AS "P_PMedicaidNumber",
				CAST(T2."ConP_PAdmissionID" AS TEXT) AS "ConP_PAdmissionID",
				CAST(T2."ConP_PName" AS TEXT) AS "ConP_PName",
				CAST(T2."ConP_PAddressL1" AS TEXT) AS "ConP_PAddressL1",
				CAST(T2."ConP_PAddressL2" AS TEXT) AS "ConP_PAddressL2",
				CAST(T2."ConP_PCity" AS TEXT) AS "ConP_PCity",
				CAST(T2."ConP_PAddressState" AS TEXT) AS "ConP_PAddressState",
				CAST(T2."ConP_PZipCode" AS TEXT) AS "ConP_PZipCode",
				CAST(T2."ConP_PCounty" AS TEXT) AS "ConP_PCounty",
				CAST(T2."ConP_PFName" AS TEXT) AS "ConP_PFName",
				CAST(T2."ConP_PLName" AS TEXT) AS "ConP_PLName",
				CAST(T2."ConP_PMedicaidNumber" AS TEXT) AS "ConP_PMedicaidNumber",
				CAST(T2."PA_PAdmissionID" AS TEXT) AS "PA_PAdmissionID",
				CAST(T2."PA_PName" AS TEXT) AS "PA_PName",
				CAST(T2."PA_PAddressL1" AS TEXT) AS "PA_PAddressL1",
				CAST(T2."PA_PAddressL2" AS TEXT) AS "PA_PAddressL2",
				CAST(T2."PA_PCity" AS TEXT) AS "PA_PCity",
				CAST(T2."PA_PAddressState" AS TEXT) AS "PA_PAddressState",
				CAST(T2."PA_PZipCode" AS TEXT) AS "PA_PZipCode",
				CAST(T2."PA_PCounty" AS TEXT) AS "PA_PCounty",
				CAST(T2."PA_PFName" AS TEXT) AS "PA_PFName",
				CAST(T2."PA_PLName" AS TEXT) AS "PA_PLName",
				CAST(T2."PA_PMedicaidNumber" AS TEXT) AS "PA_PMedicaidNumber",
				CAST(T2."ConPA_PAdmissionID" AS TEXT) AS "ConPA_PAdmissionID",
				CAST(T2."ConPA_PName" AS TEXT) AS "ConPA_PName",
				CAST(T2."ConPA_PAddressL1" AS TEXT) AS "ConPA_PAddressL1",
				CAST(T2."ConPA_PAddressL2" AS TEXT) AS "ConPA_PAddressL2",
				CAST(T2."ConPA_PCity" AS TEXT) AS "ConPA_PCity",
				CAST(T2."ConPA_PAddressState" AS TEXT) AS "ConPA_PAddressState",
				CAST(T2."ConPA_PZipCode" AS TEXT) AS "ConPA_PZipCode",
				CAST(T2."ConPA_PCounty" AS TEXT) AS "ConPA_PCounty",
				CAST(T2."ConPA_PFName" AS TEXT) AS "ConPA_PFName",
				CAST(T2."ConPA_PLName" AS TEXT) AS "ConPA_PLName",
				CAST(T2."ConPA_PMedicaidNumber" AS TEXT) AS "ConPA_PMedicaidNumber",
				CAST(T2."ContractType" AS TEXT) AS "ContractType",
				CAST(T2."ConContractType" AS TEXT) AS "ConContractType",
				CAST(T2."BillRateNonBilled" AS TEXT) AS "BillRateNonBilled",
				CAST(T2."ConBillRateNonBilled" AS TEXT) AS "ConBillRateNonBilled",
				CAST(T2."BillRateBoth" AS TEXT) AS "BillRateBoth",
				CAST(T2."ConBillRateBoth" AS TEXT) AS "ConBillRateBoth",
				CAST(T2."FederalTaxNumber" AS TEXT) AS "FederalTaxNumber",
				CAST(T2."ConFederalTaxNumber" AS TEXT) AS "ConFederalTaxNumber",
				CAST(T2."StatusFlag" AS TEXT) AS "StatusFlag",
				CAST(T2."FlagForReview" AS TEXT) AS "FlagForReview",
				TO_CHAR(T2."FlagForReviewDate", ''YYYY-MM-DD HH24:MI:SS'') AS "FlagForReviewDate",
				CAST(T2."ConFlagForReview" AS TEXT) AS "ConFlagForReview",
				TO_CHAR(T2."ConFlagForReviewDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConFlagForReviewDate",
				TO_CHAR(T2."ConInserviceStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConInserviceStartDate", 
				TO_CHAR(T2."ConInserviceEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConInserviceEndDate", 
				TO_CHAR(T2."ConPTOStartDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConPTOStartDate", 
				TO_CHAR(T2."ConPTOEndDate", ''YYYY-MM-DD HH24:MI:SS'') AS "ConPTOEndDate"
			FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS_TEMP T2
		),
		LogFields AS (
			SELECT ID as "LogID", "FieldName", "FieldFor"
			FROM CONFLICTREPORT."PUBLIC".LOG_FIELDS
		),
		UnpivotedData AS (
			SELECT 
				CONID,
				"PVisitID",
				"PAppVisitID",
				"CVisitID",
				"CAppVisitID",
				column_name,
				column_value
			FROM NewLogRecords
			UNPIVOT(
				column_value FOR column_name IN (
					"CONFLICTID", 
					"SSN", 
					"ProviderName", 
					"ConProviderName",
					"VisitDate",
					"SchStartTime",
					"SchEndTime",
					"ConSchStartTime",
					"ConSchEndTime",
					"VisitStartTime",
					"VisitEndTime",
					"ConVisitStartTime",
					"ConVisitEndTime",
					"EVVStartTime",
					"EVVEndTime",
					"ConEVVStartTime",
					"ConEVVEndTime",
					"AideCode",
					"AideName",
					"AideSSN",
					"ConAideCode",
					"ConAideName",
					"ConAideSSN",
					"Office",
					"ConOffice",
					"PAdmissionID",
					"PName",
					"PAddressL1",
					"PAddressL2",
					"PCity",
					"PAddressState",
					"PZipCode",
					"PCounty",
					"PLongitude",
					"PLatitude",
					"ConPAdmissionID",
					"ConPName",
					"ConPAddressL1",
					"ConPAddressL2",
					"ConPCity",
					"ConPAddressState",
					"ConPZipCode",
					"ConPCounty",
					"ConPLongitude",
					"ConPLatitude",
					"Contract",
					"ConContract",
					"BilledDate",
					"ConBilledDate",
					"BilledHours",
					"ConBilledHours",
					"Billed",
					"ConBilled",
					"MinuteDiffBetweenSch",
					"DistanceMilesFromLatLng",
					"AverageMilesPerHour",
					"ETATravleMinutes",
					"InserviceStartDate",
					"InserviceEndDate",
					"PTOStartDate",
					"PTOEndDate",
					"SameSchTimeFlag",
					"SameVisitTimeFlag",
					"SchAndVisitTimeSameFlag",
					"SchOverAnotherSchTimeFlag",
					"VisitTimeOverAnotherVisitTimeFlag",
					"SchTimeOverVisitTimeFlag",
					"DistanceFlag",
					"InServiceFlag",
					"PTOFlag",
					"StatusFlag",
					"ConStatusFlag",
					"AideFName",
					"AideLName",
					"ConAideFName",
					"ConAideLName",
					"PFName",
					"PLName",
					"ConPFName",
					"ConPLName",
					"PMedicaidNumber",
					"ConPMedicaidNumber",
					"PayerState",
					"ConPayerState",
					"AgencyContact",
					"ConAgencyContact",
					"AgencyPhone",
					"ConAgencyPhone",
					"LastUpdatedBy",
					"ConLastUpdatedBy",
					"LastUpdatedDate",
					"ConLastUpdatedDate",
					"BilledRate",
					"TotalBilledAmount",
					"ConBilledRate",
					"ConTotalBilledAmount",
					"IsMissed",
					"MissedVisitReason",
					"EVVType",
					"ConIsMissed",
					"ConMissedVisitReason",
					"ConEVVType",
					"PStatus",
					"ConPStatus",
					"AideStatus",
					"ConAideStatus",
					"ConNoResponseFlag",
					"ConNoResponseTitle",
					"ConNoResponseNotes",
					"P_PAdmissionID",
					"P_PName",
					"P_PAddressL1",
					"P_PAddressL2",
					"P_PCity",
					"P_PAddressState",
					"P_PZipCode",
					"P_PCounty",
					"P_PFName",
					"P_PLName",
					"P_PMedicaidNumber",
					"ConP_PAdmissionID",
					"ConP_PName",
					"ConP_PAddressL1",
					"ConP_PAddressL2",
					"ConP_PCity",
					"ConP_PAddressState",
					"ConP_PZipCode",
					"ConP_PCounty",
					"ConP_PFName",
					"ConP_PLName",
					"ConP_PMedicaidNumber",
					"PA_PAdmissionID",
					"PA_PName",
					"PA_PAddressL1",
					"PA_PAddressL2",
					"PA_PCity",
					"PA_PAddressState",
					"PA_PZipCode",
					"PA_PCounty",
					"PA_PFName",
					"PA_PLName",
					"PA_PMedicaidNumber",
					"ConPA_PAdmissionID",
					"ConPA_PName",
					"ConPA_PAddressL1",
					"ConPA_PAddressL2",
					"ConPA_PCity",
					"ConPA_PAddressState",
					"ConPA_PZipCode",
					"ConPA_PCounty",
					"ConPA_PFName",
					"ConPA_PLName",
					"ConPA_PMedicaidNumber",
					"ContractType",
					"ConContractType",
					"BillRateNonBilled",
					"ConBillRateNonBilled",
					"BillRateBoth",
					"ConBillRateBoth",
					"FederalTaxNumber",
					"ConFederalTaxNumber",
					"FlagForReview",
					"FlagForReviewDate",
					"ConFlagForReview",
					"ConFlagForReviewDate",
					"ConInserviceStartDate",
					"ConInserviceEndDate",
					"ConPTOStartDate",
					"ConPTOEndDate"
				)
			)
		),
		UnpivotedDataTemp AS (
			SELECT 
				CONID,
				"PVisitID",
				"PAppVisitID",
				"CVisitID",
				"CAppVisitID",
				column_name,
				column_value
			FROM TempConflictValues
			UNPIVOT(
				column_value FOR column_name IN (
					"CONFLICTID", 
					"SSN", 
					"ProviderName", 
					"ConProviderName",
					"VisitDate",
					"SchStartTime",
					"SchEndTime",
					"ConSchStartTime",
					"ConSchEndTime",
					"VisitStartTime",
					"VisitEndTime",
					"ConVisitStartTime",
					"ConVisitEndTime",
					"EVVStartTime",
					"EVVEndTime",
					"ConEVVStartTime",
					"ConEVVEndTime",
					"AideCode",
					"AideName",
					"AideSSN",
					"ConAideCode",
					"ConAideName",
					"ConAideSSN",
					"Office",
					"ConOffice",
					"PAdmissionID",
					"PName",
					"PAddressL1",
					"PAddressL2",
					"PCity",
					"PAddressState",
					"PZipCode",
					"PCounty",
					"PLongitude",
					"PLatitude",
					"ConPAdmissionID",
					"ConPName",
					"ConPAddressL1",
					"ConPAddressL2",
					"ConPCity",
					"ConPAddressState",
					"ConPZipCode",
					"ConPCounty",
					"ConPLongitude",
					"ConPLatitude",
					"Contract",
					"ConContract",
					"BilledDate",
					"ConBilledDate",
					"BilledHours",
					"ConBilledHours",
					"Billed",
					"ConBilled",
					"MinuteDiffBetweenSch",
					"DistanceMilesFromLatLng",
					"AverageMilesPerHour",
					"ETATravleMinutes",
					"InserviceStartDate",
					"InserviceEndDate",
					"PTOStartDate",
					"PTOEndDate",
					"SameSchTimeFlag",
					"SameVisitTimeFlag",
					"SchAndVisitTimeSameFlag",
					"SchOverAnotherSchTimeFlag",
					"VisitTimeOverAnotherVisitTimeFlag",
					"SchTimeOverVisitTimeFlag",
					"DistanceFlag",
					"InServiceFlag",
					"PTOFlag",
					"StatusFlag",
					"ConStatusFlag",
					"AideFName",
					"AideLName",
					"ConAideFName",
					"ConAideLName",
					"PFName",
					"PLName",
					"ConPFName",
					"ConPLName",
					"PMedicaidNumber",
					"ConPMedicaidNumber",
					"PayerState",
					"ConPayerState",
					"AgencyContact",
					"ConAgencyContact",
					"AgencyPhone",
					"ConAgencyPhone",
					"LastUpdatedBy",
					"ConLastUpdatedBy",
					"LastUpdatedDate",
					"ConLastUpdatedDate",
					"BilledRate",
					"TotalBilledAmount",
					"ConBilledRate",
					"ConTotalBilledAmount",
					"IsMissed",
					"MissedVisitReason",
					"EVVType",
					"ConIsMissed",
					"ConMissedVisitReason",
					"ConEVVType",
					"PStatus",
					"ConPStatus",
					"AideStatus",
					"ConAideStatus",
					"ConNoResponseFlag",
					"ConNoResponseTitle",
					"ConNoResponseNotes",
					"P_PAdmissionID",
					"P_PName",
					"P_PAddressL1",
					"P_PAddressL2",
					"P_PCity",
					"P_PAddressState",
					"P_PZipCode",
					"P_PCounty",
					"P_PFName",
					"P_PLName",
					"P_PMedicaidNumber",
					"ConP_PAdmissionID",
					"ConP_PName",
					"ConP_PAddressL1",
					"ConP_PAddressL2",
					"ConP_PCity",
					"ConP_PAddressState",
					"ConP_PZipCode",
					"ConP_PCounty",
					"ConP_PFName",
					"ConP_PLName",
					"ConP_PMedicaidNumber",
					"PA_PAdmissionID",
					"PA_PName",
					"PA_PAddressL1",
					"PA_PAddressL2",
					"PA_PCity",
					"PA_PAddressState",
					"PA_PZipCode",
					"PA_PCounty",
					"PA_PFName",
					"PA_PLName",
					"PA_PMedicaidNumber",
					"ConPA_PAdmissionID",
					"ConPA_PName",
					"ConPA_PAddressL1",
					"ConPA_PAddressL2",
					"ConPA_PCity",
					"ConPA_PAddressState",
					"ConPA_PZipCode",
					"ConPA_PCounty",
					"ConPA_PFName",
					"ConPA_PLName",
					"ConPA_PMedicaidNumber",
					"ContractType",
					"ConContractType",
					"BillRateNonBilled",
					"ConBillRateNonBilled",
					"BillRateBoth",
					"ConBillRateBoth",
					"FederalTaxNumber",
					"ConFederalTaxNumber",
					"FlagForReview",
					"FlagForReviewDate",
					"ConFlagForReview",
					"ConFlagForReviewDate",
					"ConInserviceStartDate",
					"ConInserviceEndDate",
					"ConPTOStartDate",
					"ConPTOEndDate"
				)
			)
		)
		
		SELECT TB1.CONID, TB1."LogID", TB2."NewValue" AS "OldValue", TB1."NewValue", TB1."VisitID", TB1."AppVisitID" FROM
		(
			SELECT 
				U.CONID,
				LF."LogID",
				U.column_value as "NewValue",
				CASE 
					WHEN LF."FieldFor" = ''P'' THEN U."PVisitID"
					WHEN LF."FieldFor" = ''C'' THEN U."CVisitID"
				END as "VisitID",
				CASE 
					WHEN LF."FieldFor" = ''P'' THEN U."PAppVisitID"
					WHEN LF."FieldFor" = ''C'' THEN U."CAppVisitID"
				END as "AppVisitID"
			FROM UnpivotedData U
			JOIN LogFields LF ON LF."FieldName" = U.column_name
		) AS TB1
		INNER JOIN 
		(
			SELECT 
				U.CONID,
				LF."LogID",
				U.column_value as "NewValue",
				CASE 
					WHEN LF."FieldFor" = ''P'' THEN U."PVisitID"
					WHEN LF."FieldFor" = ''C'' THEN U."CVisitID"
				END as "VisitID",
				CASE 
					WHEN LF."FieldFor" = ''P'' THEN U."PAppVisitID"
					WHEN LF."FieldFor" = ''C'' THEN U."CAppVisitID"
				END as "AppVisitID"
			FROM UnpivotedDataTemp U
			JOIN LogFields LF ON LF."FieldName" = U.column_name
		) AS TB2 ON TB1."LogID" = TB2."LogID" AND TB1.CONID = TB2.CONID AND TB1."VisitID" = TB2."VisitID" AND TB1."AppVisitID" = TB2."AppVisitID"
		WHERE TB1."NewValue" != TB2."NewValue"`;
		
		snowflake.execute({ sqlText: inupdatequery });


		// First, insert the main log history records
        var insertMainLogs = snowflake.createStatement({
            sqlText: `
                INSERT INTO CONFLICTREPORT."PUBLIC".LOG_HISTORY ("CONID", "LogTypeFlag")
				SELECT distinct T2.CONID, ''UpdatedNew''
				FROM CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES_TEMP T2
            `
        });
        insertMainLogs.execute();

		// First, insert the main log history records
        var insertTempToMainLogs = snowflake.createStatement({
            sqlText: `
                INSERT INTO CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES
				(LHID, "LogID", "OldValue", "NewValue", "VisitID", "AppVisitID")
				SELECT LH.ID, LHVT."LogID", LHVT."OldValue", LHVT."NewValue", LHVT."VisitID", LHVT."AppVisitID"
				FROM CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES_TEMP AS LHVT INNER JOIN CONFLICTREPORT."PUBLIC".LOG_HISTORY AS LH ON LH.CONID = LHVT.CONID AND "LogTypeFlag" = ''UpdatedNew'';
            `
        });
        insertTempToMainLogs.execute();

		// First, insert the main log history records
        var insertMainLogsU = snowflake.createStatement({
            sqlText: `
                UPDATE CONFLICTREPORT."PUBLIC".LOG_HISTORY SET "LogTypeFlag" = ''Updated'' WHERE "LogTypeFlag" = ''UpdatedNew''
            `
        });
        insertMainLogsU.execute();

		// First, insert the main log history records
        var insertMainLogsUT = snowflake.createStatement({
            sqlText: `
                TRUNCATE TABLE CONFLICTREPORT."PUBLIC".LOG_HISTORY_VALUES_TEMP
            `
        });
        insertMainLogsUT.execute();
		
		var finalupdate = snowflake.createStatement({
            sqlText: `UPDATE CONFLICTREPORT.PUBLIC.SETTINGS SET "LastLoadDate" = CURRENT_TIMESTAMP, "InProgressFlag" = 2`
        });
		finalupdate.execute();
        return ''Records inserted successfully'';
    }
    catch (err) {
		var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
	  // If an error occurs, capture it and raise it with a custom message
	  throw ''ERROR: '' + err.message;  // Returns the error message to the caller
    }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.UPDATE_DATA_CONFLICTVISITMAPS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='user-defined procedure'
EXECUTE AS CALLER
AS '
  try {
	var update_query = `
    UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM SET CVM."UpdateFlag" = 1 WHERE CVM."CONFLICTID" IS NOT NULL AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
   
  var sql_query = `    
  	UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.CONFLICTID = ALLDATA.CONFLICTID, CVM.SSN = ALLDATA.SSN, CVM."ProviderID" = ALLDATA."ProviderID", CVM."AppProviderID" = ALLDATA."AppProviderID", CVM."ProviderName" = ALLDATA."ProviderName", CVM."VisitID" = ALLDATA."VisitID", CVM."AppVisitID" = ALLDATA."AppVisitID", CVM."ConProviderID" = ALLDATA."ConProviderID", CVM."ConAppProviderID" = ALLDATA."ConAppProviderID", CVM."ConProviderName" = ALLDATA."ConProviderName", CVM."ConVisitID" = ALLDATA."ConVisitID", CVM."ConAppVisitID" = ALLDATA."ConAppVisitID", CVM."VisitDate" = ALLDATA."VisitDate", CVM."SchStartTime" = ALLDATA."SchStartTime", CVM."SchEndTime" = ALLDATA."SchEndTime", CVM."ConSchStartTime" = ALLDATA."ConSchStartTime", CVM."ConSchEndTime" = ALLDATA."ConSchEndTime", CVM."VisitStartTime" = ALLDATA."VisitStartTime", CVM."VisitEndTime" = ALLDATA."VisitEndTime", CVM."ConVisitStartTime" = ALLDATA."ConVisitStartTime", CVM."ConVisitEndTime" = ALLDATA."ConVisitEndTime", CVM."EVVStartTime" = ALLDATA."EVVStartTime", CVM."EVVEndTime" = ALLDATA."EVVEndTime", CVM."ConEVVStartTime" = ALLDATA."ConEVVStartTime", CVM."ConEVVEndTime" = ALLDATA."ConEVVEndTime", CVM."CaregiverID" = ALLDATA."CaregiverID", CVM."AppCaregiverID" = ALLDATA."AppCaregiverID", CVM."AideCode" = ALLDATA."AideCode", CVM."AideName" = ALLDATA."AideName", CVM."AideSSN" = ALLDATA."AideSSN", CVM."ConCaregiverID" = ALLDATA."ConCaregiverID", CVM."ConAppCaregiverID" = ALLDATA."ConAppCaregiverID", CVM."ConAideCode" = ALLDATA."ConAideCode", CVM."ConAideName" = ALLDATA."ConAideName", CVM."ConAideSSN" = ALLDATA."ConAideSSN", CVM."OfficeID" = ALLDATA."OfficeID", CVM."AppOfficeID" = ALLDATA."AppOfficeID", CVM."Office" = ALLDATA."Office", CVM."ConOfficeID" = ALLDATA."ConOfficeID", CVM."ConAppOfficeID" = ALLDATA."ConAppOfficeID", CVM."ConOffice" = ALLDATA."ConOffice", CVM."PatientID" = ALLDATA."PatientID", CVM."AppPatientID" = ALLDATA."AppPatientID", CVM."PAdmissionID" = ALLDATA."PAdmissionID", CVM."PName" = ALLDATA."PName", CVM."PAddressID" = ALLDATA."PAddressID", CVM."PAppAddressID" = ALLDATA."PAppAddressID", CVM."PAddressL1" = ALLDATA."PAddressL1", CVM."PAddressL2" = ALLDATA."PAddressL2", CVM."PCity" = ALLDATA."PCity", CVM."PAddressState" = ALLDATA."PAddressState", CVM."PZipCode" = ALLDATA."PZipCode", CVM."PCounty" = ALLDATA."PCounty", CVM."PLongitude" = ALLDATA."PLongitude", CVM."PLatitude" = ALLDATA."PLatitude", CVM."ConPatientID" = ALLDATA."ConPatientID", CVM."ConAppPatientID" = ALLDATA."ConAppPatientID", CVM."ConPAdmissionID" = ALLDATA."ConPAdmissionID", CVM."ConPName" = ALLDATA."ConPName", CVM."ConPAddressID" = ALLDATA."ConPAddressID", CVM."ConPAppAddressID" = ALLDATA."ConPAppAddressID", CVM."ConPAddressL1" = ALLDATA."ConPAddressL1", CVM."ConPAddressL2" = ALLDATA."ConPAddressL2", CVM."ConPCity" = ALLDATA."ConPCity", CVM."ConPAddressState" = ALLDATA."ConPAddressState", CVM."ConPZipCode" = ALLDATA."ConPZipCode", CVM."ConPCounty" = ALLDATA."ConPCounty", CVM."ConPLongitude" = ALLDATA."ConPLongitude", CVM."ConPLatitude" = ALLDATA."ConPLatitude", CVM."PayerID" = ALLDATA."PayerID", CVM."AppPayerID" = ALLDATA."AppPayerID", CVM."Contract" = ALLDATA."Contract", CVM."ConPayerID" = ALLDATA."ConPayerID", CVM."ConAppPayerID" = ALLDATA."ConAppPayerID", CVM."ConContract" = ALLDATA."ConContract", CVM."BilledDate" = ALLDATA."BilledDate", CVM."ConBilledDate" = ALLDATA."ConBilledDate", CVM."BilledHours" = ALLDATA."BilledHours", CVM."ConBilledHours" = ALLDATA."ConBilledHours", CVM."Billed" = ALLDATA."Billed", CVM."ConBilled" = ALLDATA."ConBilled", CVM."MinuteDiffBetweenSch" = ALLDATA."MinuteDiffBetweenSch", CVM."DistanceMilesFromLatLng" = ALLDATA."DistanceMilesFromLatLng", CVM."AverageMilesPerHour" = ALLDATA."AverageMilesPerHour", CVM."ETATravleMinutes" = ALLDATA."ETATravleMinutes", CVM."ServiceCodeID" = ALLDATA."ServiceCodeID", CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID", CVM."RateType" = ALLDATA."RateType", CVM."ServiceCode" = ALLDATA."ServiceCode", CVM."ConServiceCodeID" = ALLDATA."ConServiceCodeID", CVM."ConAppServiceCodeID" = ALLDATA."ConAppServiceCodeID", CVM."ConRateType" = ALLDATA."ConRateType", CVM."ConServiceCode" = ALLDATA."ConServiceCode", CVM."UpdateFlag" = NULL, CVM."UpdatedDate" = CURRENT_TIMESTAMP(), CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" NOT IN (''W'', ''I'') THEN ''U'' ELSE CVM."StatusFlag" END, CVM."ResolveDate" = NULL, CVM."AideFName" = ALLDATA."AideFName", CVM."AideLName" = ALLDATA."AideLName", CVM."ConAideFName" = ALLDATA."ConAideFName", CVM."ConAideLName" = ALLDATA."ConAideLName", CVM."PFName" = ALLDATA."PFName", CVM."PLName" = ALLDATA."PLName", CVM."ConPFName" = ALLDATA."ConPFName", CVM."ConPLName" = ALLDATA."ConPLName", CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber", CVM."ConPMedicaidNumber" = ALLDATA."ConPMedicaidNumber", CVM."PayerState" = ALLDATA."PayerState", CVM."ConPayerState" = ALLDATA."ConPayerState", CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy", CVM."ConLastUpdatedBy" = ALLDATA."ConLastUpdatedBy", CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate", CVM."ConLastUpdatedDate" = ALLDATA."ConLastUpdatedDate", CVM."BilledRate" = ALLDATA."BilledRate", CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount", CVM."ConBilledRate" = ALLDATA."ConBilledRate", CVM."ConTotalBilledAmount" = ALLDATA."ConTotalBilledAmount", CVM."IsMissed" = ALLDATA."IsMissed", CVM."MissedVisitReason" = ALLDATA."MissedVisitReason", CVM."EVVType" = ALLDATA."EVVType", CVM."ConIsMissed" = ALLDATA."ConIsMissed", CVM."ConMissedVisitReason" = ALLDATA."ConMissedVisitReason", CVM."ConEVVType" = ALLDATA."ConEVVType", CVM."PStatus" = ALLDATA."PStatus", CVM."ConPStatus" = ALLDATA."ConPStatus", CVM."AideStatus" = ALLDATA."AideStatus", CVM."ConAideStatus" = ALLDATA."ConAideStatus", CVM."P_PatientID" = ALLDATA."P_PatientID", CVM."P_AppPatientID" = ALLDATA."P_AppPatientID", CVM."ConP_PatientID" = ALLDATA."ConP_PatientID", CVM."ConP_AppPatientID" = ALLDATA."ConP_AppPatientID", CVM."PA_PatientID" = ALLDATA."PA_PatientID", CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID", CVM."ConPA_PatientID" = ALLDATA."ConPA_PatientID", CVM."ConPA_AppPatientID" = ALLDATA."ConPA_AppPatientID", CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID", CVM."P_PName" = ALLDATA."P_PName", CVM."P_PAddressID" = ALLDATA."P_PAddressID", CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID", CVM."P_PAddressL1" = ALLDATA."P_PAddressL1", CVM."P_PAddressL2" = ALLDATA."P_PAddressL2", CVM."P_PCity" = ALLDATA."P_PCity", CVM."P_PAddressState" = ALLDATA."P_PAddressState", CVM."P_PZipCode" = ALLDATA."P_PZipCode", CVM."P_PCounty" = ALLDATA."P_PCounty", CVM."P_PFName" = ALLDATA."P_PFName", CVM."P_PLName" = ALLDATA."P_PLName", CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", CVM."ConP_PAdmissionID" = ALLDATA."ConP_PAdmissionID", CVM."ConP_PName" = ALLDATA."ConP_PName", CVM."ConP_PAddressID" = ALLDATA."ConP_PAddressID", CVM."ConP_PAppAddressID" = ALLDATA."ConP_PAppAddressID", CVM."ConP_PAddressL1" = ALLDATA."ConP_PAddressL1", CVM."ConP_PAddressL2" = ALLDATA."ConP_PAddressL2", CVM."ConP_PCity" = ALLDATA."ConP_PCity", CVM."ConP_PAddressState" = ALLDATA."ConP_PAddressState", CVM."ConP_PZipCode" = ALLDATA."ConP_PZipCode", CVM."ConP_PCounty" = ALLDATA."ConP_PCounty", CVM."ConP_PFName" = ALLDATA."ConP_PFName", CVM."ConP_PLName" = ALLDATA."ConP_PLName", CVM."ConP_PMedicaidNumber" = ALLDATA."ConP_PMedicaidNumber", CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", CVM."PA_PName" = ALLDATA."PA_PName", CVM."PA_PAddressID" = ALLDATA."PA_PAddressID", CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1", CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2", CVM."PA_PCity" = ALLDATA."PA_PCity", CVM."PA_PAddressState" = ALLDATA."PA_PAddressState", CVM."PA_PZipCode" = ALLDATA."PA_PZipCode", CVM."PA_PCounty" = ALLDATA."PA_PCounty", CVM."PA_PFName" = ALLDATA."PA_PFName", CVM."PA_PLName" = ALLDATA."PA_PLName", CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", CVM."ConPA_PAdmissionID" = ALLDATA."ConPA_PAdmissionID", CVM."ConPA_PName" = ALLDATA."ConPA_PName", CVM."ConPA_PAddressID" = ALLDATA."ConPA_PAddressID", CVM."ConPA_PAppAddressID" = ALLDATA."ConPA_PAppAddressID", CVM."ConPA_PAddressL1" = ALLDATA."ConPA_PAddressL1", CVM."ConPA_PAddressL2" = ALLDATA."ConPA_PAddressL2", CVM."ConPA_PCity" = ALLDATA."ConPA_PCity", CVM."ConPA_PAddressState" = ALLDATA."ConPA_PAddressState", CVM."ConPA_PZipCode" = ALLDATA."ConPA_PZipCode", CVM."ConPA_PCounty" = ALLDATA."ConPA_PCounty", CVM."ConPA_PFName" = ALLDATA."ConPA_PFName", CVM."ConPA_PLName" = ALLDATA."ConPA_PLName", CVM."ConPA_PMedicaidNumber" = ALLDATA."ConPA_PMedicaidNumber", CVM."ContractType" = ALLDATA."ContractType", CVM."ConContractType" = ALLDATA."ConContractType", CVM."SameSchTimeFlag" = CASE WHEN CVM."SameSchTimeFlag" = ''N'' THEN ALLDATA."SameSchTimeFlag" ELSE CVM."SameSchTimeFlag" END, CVM."SameVisitTimeFlag" = CASE WHEN CVM."SameVisitTimeFlag" = ''N'' THEN ALLDATA."SameVisitTimeFlag" ELSE CVM."SameVisitTimeFlag" END, CVM."SchAndVisitTimeSameFlag" = CASE WHEN CVM."SchAndVisitTimeSameFlag" = ''N'' THEN ALLDATA."SchVisitTimeSame" ELSE CVM."SchAndVisitTimeSameFlag" END, CVM."SchOverAnotherSchTimeFlag" = CASE WHEN CVM."SchOverAnotherSchTimeFlag" = ''N'' THEN ALLDATA."SchOverAnotherSchTimeFlag" ELSE CVM."SchOverAnotherSchTimeFlag" END, CVM."VisitTimeOverAnotherVisitTimeFlag" = CASE WHEN CVM."VisitTimeOverAnotherVisitTimeFlag" = ''N'' THEN ALLDATA."VisitTimeOverAnotherVisitTimeFlag" ELSE CVM."VisitTimeOverAnotherVisitTimeFlag" END, CVM."SchTimeOverVisitTimeFlag" = CASE WHEN CVM."SchTimeOverVisitTimeFlag" = ''N'' THEN ALLDATA."SchTimeOverVisitTimeFlag" ELSE CVM."SchTimeOverVisitTimeFlag" END, CVM."DistanceFlag" = CASE WHEN CVM."DistanceFlag" = ''N'' THEN ALLDATA."DistanceFlag" ELSE CVM."DistanceFlag" END, CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled", CVM."ConBillRateNonBilled" = ALLDATA."ConBillRateNonBilled", CVM."BillRateBoth" = ALLDATA."BillRateBoth", CVM."ConBillRateBoth" = ALLDATA."ConBillRateBoth", CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber", CVM."ConFederalTaxNumber" = ALLDATA."ConFederalTaxNumber"
  	FROM (
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
            CASE 
                WHEN DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime") > 0 
                     AND DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime") > 0
                THEN LEAST(DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime"), 
                          DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime"))
                WHEN DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime") > 0
                THEN DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime")
                WHEN DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime") > 0
                THEN DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime")
                ELSE 0 
            END AS "MinuteDiffBetweenSch",
            ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer", 2) AS "DistanceMilesFromLatLng",
            MPH."AverageMilesPerHour",
            ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) as "ETATravleMinutes",
			V1."ServiceCodeID" AS "ServiceCodeID",
	        V1."AppServiceCodeID" AS "AppServiceCodeID",
	        V1."RateType" AS "RateType",
	        V1."ServiceCode" AS "ServiceCode",          
	        V2."ServiceCodeID" AS "ConServiceCodeID",
	        V2."AppServiceCodeID" AS "ConAppServiceCodeID",
	        V2."RateType" AS "ConRateType",
	        V2."ServiceCode" AS "ConServiceCode",
            CASE WHEN V1."ProviderID" != V2."ProviderID" AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") = CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") THEN ''Y'' ELSE ''N'' END AS "SameSchTimeFlag",
            CASE WHEN V1."ProviderID" != V2."ProviderID" AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL AND CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime") = CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime") THEN ''Y'' ELSE ''N'' END AS "SameVisitTimeFlag",
            CASE WHEN V1."ProviderID" != V2."ProviderID" AND ((V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") = CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")) OR (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL AND CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") = CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime"))) THEN ''Y'' ELSE ''N'' END AS "SchVisitTimeSame",
            CASE WHEN V1."ProviderID" != V2."ProviderID" AND V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL AND V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL AND (V1."SchStartTime" < V2."SchEndTime" AND V1."SchEndTime" > V2."SchStartTime") AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") != CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") THEN ''Y'' ELSE ''N'' END AS "SchOverAnotherSchTimeFlag",
            CASE WHEN V1."ProviderID" != V2."ProviderID" AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL AND (V1."VisitStartTime" < V2."VisitEndTime" AND V1."VisitEndTime" > V2."VisitStartTime") AND CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime") != CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime") THEN ''Y'' ELSE ''N'' END AS "VisitTimeOverAnotherVisitTimeFlag",
            CASE WHEN V1."ProviderID" != V2."ProviderID" AND (V1."VisitStartTime" IS NULL AND V1."VisitEndTime" IS NULL AND V2."VisitStartTime" IS NOT NULL AND V2."VisitEndTime" IS NOT NULL AND ( V1."SchStartTime" < V2."VisitEndTime" AND V1."SchEndTime" > V2."VisitStartTime" ) AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") != CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")) OR (V2."VisitStartTime" IS NULL AND V2."VisitEndTime" IS NULL AND V1."VisitStartTime" IS NOT NULL AND V1."VisitEndTime" IS NOT NULL AND ( V2."SchStartTime" < V1."VisitEndTime" AND V2."SchEndTime" > V1."VisitStartTime" ) AND CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") != CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime")) THEN ''Y'' ELSE ''N'' END AS "SchTimeOverVisitTimeFlag",
			CASE WHEN V1."ProviderID" != V2."ProviderID"
			 AND
			 V1."Longitude" IS NOT NULL
			 AND
			 V1."Latitude" IS NOT NULL
			 AND
			 V2."Longitude" IS NOT NULL
			 AND
			 V2."Latitude" IS NOT NULL
			 AND
			 V1."VisitStartTime" IS NOT NULL
			 AND 
			 V1."VisitEndTime" IS NOT NULL
			 AND 
			 V2."VisitStartTime" IS NOT NULL
			 AND 
			 V2."VisitEndTime" IS NOT NULL
			 AND
			 (
			 	(
			 		V1."PZipCode" IS NOT NULL
			 		AND
			 		V2."PZipCode" IS NOT NULL
			 		AND
			 		V1."PZipCode" != V2."PZipCode"
			 	)
			 	OR
			 	(
			 		V1."PZipCode" IS NULL
			 		OR
			 		V2."PZipCode" IS NULL
			 	)
			 )
			 AND
			 MPH."AverageMilesPerHour" IS NOT NULL
			 AND
			 (
			 	(DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime") > 0
			 	AND
			 	ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) > DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime"))
			 	OR
			 	(DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime") > 0
			 	AND
			 	ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V2."Longitude", V2."Latitude"), ST_MAKEPOINT(V1."Longitude", V1."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) > DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime"))
			 ) THEN ''Y'' ELSE ''N'' END AS "DistanceFlag",
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
       (SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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
		 INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = CR1."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE LEFT JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM1 ON CVM1."VisitID" = CR1."Visit Id" AND CVM1."AppVisitID" = CR1."Application Visit Id" AND CVM1."CONFLICTID" IS NOT NULL
		  LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
		  LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
		  WHERE DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
		  AND
		  CR1."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY)
		  AND
		  NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
		) AS V1
       	LEFT JOIN
       	(
			SELECT DISTINCT CAST(NULL AS NUMBER) "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") AS "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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
	   LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
	   LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
	   WHERE DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
	   AND
	   CR1."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
	   ) AS V2 ON
       V1."VisitDate" = V2."VisitDate" AND V1."VisitID" != V2."VisitID" AND V1.SSN = V2.SSN AND V1."ProviderID" != V2."ProviderID"
       CROSS JOIN CONFLICTREPORT."PUBLIC"."SETTINGS" AS SETT
       LEFT JOIN CONFLICTREPORT."PUBLIC".MPH AS MPH ON ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer", 2) BETWEEN MPH."From" AND MPH."To"
       WHERE (
	   		--SameSchTimeFlag	RULE 1
       		 (
			 	V1."VisitStartTime" IS NULL
			 	AND
			 	V1."VisitEndTime" IS NULL
			 	AND
			 	V2."VisitStartTime" IS NULL
			 	AND
			 	V2."VisitEndTime" IS NULL
			 	AND
			 	CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") = CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime")
       		 )
       		 OR
			--SameVisitTimeFlag Rule 2
       		 (
			 	V1."VisitStartTime" IS NOT NULL
			 	AND
			 	V1."VisitEndTime" IS NOT NULL
			 	AND
			 	V2."VisitStartTime" IS NOT NULL
			 	AND
			 	V2."VisitEndTime" IS NOT NULL
			 	AND
			 	CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime") = CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")
       		 )
       		 OR
			 -- SchVisitTimeSame Rule 3
       		 ((V2."VisitStartTime" IS NULL
			 	AND
			 	V2."VisitEndTime" IS NULL
			 	AND
			 	V1."VisitStartTime" IS NOT NULL
			 	AND
			 	V1."VisitEndTime" IS NOT NULL
			 	AND
			 	CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") = CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime")
				)OR(
			 	V1."VisitStartTime" IS NULL
			 	AND
			 	V1."VisitEndTime" IS NULL
			 	AND
			 	V2."VisitStartTime" IS NOT NULL
			 	AND
			 	V2."VisitEndTime" IS NOT NULL
			 	AND
			 	CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") = CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")
       		 ))
       		 OR
			 --SchOverAnotherSchTimeFlag		Rule 4
       		(
				V1."VisitStartTime" IS NULL
				AND
				V1."VisitEndTime" IS NULL
				AND
				V2."VisitStartTime" IS NULL
				AND
				V2."VisitEndTime" IS NULL
       			AND
				(
					V1."SchStartTime" < V2."SchEndTime"
					AND
					V1."SchEndTime" > V2."SchStartTime"
				)
				AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") != CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime")
       		)
       		OR
			--VisitTimeOverAnotherVisitTimeFlag		Rule 5
       		(
       			V1."VisitStartTime" IS NOT NULL
				AND
				V1."VisitEndTime" IS NOT NULL
				AND
				V2."VisitStartTime" IS NOT NULL
				AND
				V2."VisitEndTime" IS NOT NULL
				AND
       			(V1."VisitStartTime" < V2."VisitEndTime" AND V1."VisitEndTime" > V2."VisitStartTime")
				AND CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime") != CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")				       		
       		)
       		 OR
			 --SchTimeOverVisitTimeFlag		Rule 6
       		 (
		   (V1."VisitStartTime" IS NULL 
         AND
         V1."VisitEndTime" IS NULL
         AND
         V2."VisitStartTime" IS NOT NULL
         AND
         V2."VisitEndTime" IS NOT NULL
         AND
         (
           V1."SchStartTime" < V2."VisitEndTime"
           AND
           V1."SchEndTime" > V2."VisitStartTime"
         )
        AND CONCAT(V1."SchStartTime", ''~'', V1."SchEndTime") != CONCAT(V2."VisitStartTime", ''~'', V2."VisitEndTime")
           )
		   OR
		   (
         V2."VisitStartTime" IS NULL 
         AND
         V2."VisitEndTime" IS NULL
         AND
         V1."VisitStartTime" IS NOT NULL
         AND
         V1."VisitEndTime" IS NOT NULL
         AND
         (
           V2."SchStartTime" < V1."VisitEndTime"
           AND
           V2."SchEndTime" > V1."VisitStartTime"
         )
        AND CONCAT(V2."SchStartTime", ''~'', V2."SchEndTime") != CONCAT(V1."VisitStartTime", ''~'', V1."VisitEndTime")
           )
		   )		
		     OR  
			 --DistanceFlag Rule 7   
             (
	             V1."Longitude" IS NOT NULL
			 	AND
			     V1."Latitude" IS NOT NULL
			 	AND
			     V2."Longitude" IS NOT NULL
			 	AND
			     V2."Latitude" IS NOT NULL
			 	AND
			     V1."VisitStartTime" IS NOT NULL
			 	AND 
			     V1."VisitEndTime" IS NOT NULL
			 	AND 
			     V2."VisitStartTime" IS NOT NULL
			 	AND 
			     V2."VisitEndTime" IS NOT NULL
			 	AND
			     (
			     	(
			     		V1."PZipCode" IS NOT NULL
			 			AND
			 			V2."PZipCode" IS NOT NULL
			 			AND
			 			V1."PZipCode" != V2."PZipCode"
			 		)
			 		OR
			 		(
			 			V1."PZipCode" IS NULL
			 			OR
			 			V2."PZipCode" IS NULL
			 		)
			 	)
			 	AND
			     MPH."AverageMilesPerHour" IS NOT NULL
			 	AND
			 	(
			 		(DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime") > 0
			 		AND
			 		ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V1."Longitude", V1."Latitude"), ST_MAKEPOINT(V2."Longitude", V2."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) > DATEDIFF(minute, V1."VisitEndTime", V2."VisitStartTime"))
			 		OR
			 		(DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime") > 0
			 		AND
			 		ROUND(((ROUND((ST_DISTANCE(ST_MAKEPOINT(V2."Longitude", V2."Latitude"), ST_MAKEPOINT(V1."Longitude", V1."Latitude")) / 1609)*SETT."ExtraDistancePer")/MPH."AverageMilesPerHour")*60), 2) > DATEDIFF(minute, V2."VisitEndTime", V1."VisitStartTime"))
			 	)
             )
        )
  	) AS ALLDATA WHERE (CVM."VisitID" = ALLDATA."VisitID" AND CVM."ConVisitID" = ALLDATA."ConVisitID") OR (CVM."VisitID" = ALLDATA."VisitID" AND CVM."ConVisitID" IS NULL AND ALLDATA."ConVisitID" IS NULL) AND CVM."InserviceStartDate" IS NULL AND CVM."InserviceEndDate" IS NULL AND CVM."PTOStartDate" IS NULL AND CVM."PTOEndDate" IS NULL AND CVM."ConInserviceStartDate" IS NULL AND CVM."ConInserviceEndDate" IS NULL AND CVM."ConPTOStartDate" IS NULL AND CVM."ConPTOEndDate" IS NULL AND CVM."UpdateFlag" = 1`;

  		
  	snowflake.execute({ sqlText: update_query });
  	snowflake.execute({ sqlText: sql_query });  
    
    return "CONFLICTVISITMAPS table updated successfully.";
  } catch (err) {
	var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
    // If an error occurs, capture it and raise it with a custom message
  	throw "ERROR: " + err.message;  // Returns the error message to the caller
  }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.UPDATE_DATA_CONFLICTVISITMAPS_1()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  try {

    var sql_query_reverse_inservice_update = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.CONFLICTID = ALLDATA.CONFLICTID, CVM.SSN = ALLDATA.SSN, CVM."ProviderID" = ALLDATA."ProviderID", CVM."AppProviderID" = ALLDATA."AppProviderID", CVM."ProviderName" = ALLDATA."ProviderName", CVM."VisitID" = ALLDATA."VisitID", CVM."AppVisitID" = ALLDATA."AppVisitID", CVM."ConProviderID" = ALLDATA."ConProviderID", CVM."ConAppProviderID" = ALLDATA."ConAppProviderID", CVM."ConProviderName" = ALLDATA."ConProviderName", CVM."ConVisitID" = ALLDATA."ConVisitID", CVM."ConAppVisitID" = ALLDATA."ConAppVisitID", CVM."VisitDate" = ALLDATA."VisitDate", CVM."SchStartTime" = ALLDATA."SchStartTime", CVM."SchEndTime" = ALLDATA."SchEndTime", CVM."ConSchStartTime" = ALLDATA."ConSchStartTime", CVM."ConSchEndTime" = ALLDATA."ConSchEndTime", CVM."VisitStartTime" = ALLDATA."VisitStartTime", CVM."VisitEndTime" = ALLDATA."VisitEndTime", CVM."ConVisitStartTime" = ALLDATA."ConVisitStartTime", CVM."ConVisitEndTime" = ALLDATA."ConVisitEndTime", CVM."EVVStartTime" = ALLDATA."EVVStartTime", CVM."EVVEndTime" = ALLDATA."EVVEndTime", CVM."ConEVVStartTime" = ALLDATA."ConEVVStartTime", CVM."ConEVVEndTime" = ALLDATA."ConEVVEndTime", CVM."CaregiverID" = ALLDATA."CaregiverID", CVM."AppCaregiverID" = ALLDATA."AppCaregiverID", CVM."AideCode" = ALLDATA."AideCode", CVM."AideName" = ALLDATA."AideName", CVM."AideSSN" = ALLDATA."AideSSN", CVM."ConCaregiverID" = ALLDATA."ConCaregiverID", CVM."ConAppCaregiverID" = ALLDATA."ConAppCaregiverID", CVM."ConAideCode" = ALLDATA."ConAideCode", CVM."ConAideName" = ALLDATA."ConAideName", CVM."ConAideSSN" = ALLDATA."ConAideSSN", CVM."OfficeID" = ALLDATA."OfficeID", CVM."AppOfficeID" = ALLDATA."AppOfficeID", CVM."Office" = ALLDATA."Office", CVM."ConOfficeID" = ALLDATA."ConOfficeID", CVM."ConAppOfficeID" = ALLDATA."ConAppOfficeID", CVM."ConOffice" = ALLDATA."ConOffice", CVM."PatientID" = ALLDATA."PatientID", CVM."AppPatientID" = ALLDATA."AppPatientID", CVM."PAdmissionID" = ALLDATA."PAdmissionID", CVM."PName" = ALLDATA."PName", CVM."PAddressID" = ALLDATA."PAddressID", CVM."PAppAddressID" = ALLDATA."PAppAddressID", CVM."PAddressL1" = ALLDATA."PAddressL1", CVM."PAddressL2" = ALLDATA."PAddressL2", CVM."PCity" = ALLDATA."PCity", CVM."PAddressState" = ALLDATA."PAddressState", CVM."PZipCode" = ALLDATA."PZipCode", CVM."PCounty" = ALLDATA."PCounty", CVM."PLongitude" = ALLDATA."PLongitude", CVM."PLatitude" = ALLDATA."PLatitude", CVM."ConPatientID" = ALLDATA."ConPatientID", CVM."ConAppPatientID" = ALLDATA."ConAppPatientID", CVM."ConPAdmissionID" = ALLDATA."ConPAdmissionID", CVM."ConPName" = ALLDATA."ConPName", CVM."ConPAddressID" = ALLDATA."ConPAddressID", CVM."ConPAppAddressID" = ALLDATA."ConPAppAddressID", CVM."ConPAddressL1" = ALLDATA."ConPAddressL1", CVM."ConPAddressL2" = ALLDATA."ConPAddressL2", CVM."ConPCity" = ALLDATA."ConPCity", CVM."ConPAddressState" = ALLDATA."ConPAddressState", CVM."ConPZipCode" = ALLDATA."ConPZipCode", CVM."ConPCounty" = ALLDATA."ConPCounty", CVM."ConPLongitude" = ALLDATA."ConPLongitude", CVM."ConPLatitude" = ALLDATA."ConPLatitude", CVM."PayerID" = ALLDATA."PayerID", CVM."AppPayerID" = ALLDATA."AppPayerID", CVM."Contract" = ALLDATA."Contract", CVM."ConPayerID" = ALLDATA."ConPayerID", CVM."ConAppPayerID" = ALLDATA."ConAppPayerID", CVM."ConContract" = ALLDATA."ConContract", CVM."BilledDate" = ALLDATA."BilledDate", CVM."ConBilledDate" = ALLDATA."ConBilledDate", CVM."BilledHours" = ALLDATA."BilledHours", CVM."ConBilledHours" = ALLDATA."ConBilledHours", CVM."Billed" = ALLDATA."Billed", CVM."ConBilled" = ALLDATA."ConBilled", CVM."MinuteDiffBetweenSch" = ALLDATA."MinuteDiffBetweenSch", CVM."DistanceMilesFromLatLng" = ALLDATA."DistanceMilesFromLatLng", CVM."AverageMilesPerHour" = ALLDATA."AverageMilesPerHour", CVM."ETATravleMinutes" = ALLDATA."ETATravleMinutes", CVM."ServiceCodeID" = ALLDATA."ServiceCodeID", CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID", CVM."RateType" = ALLDATA."RateType", CVM."ServiceCode" = ALLDATA."ServiceCode", CVM."ConServiceCodeID" = ALLDATA."ConServiceCodeID", CVM."ConAppServiceCodeID" = ALLDATA."ConAppServiceCodeID", CVM."ConRateType" = ALLDATA."ConRateType", CVM."ConServiceCode" = ALLDATA."ConServiceCode", CVM."UpdateFlag" = NULL, CVM."UpdatedDate" = CURRENT_TIMESTAMP(), CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" NOT IN (''W'', ''I'') THEN ''U'' ELSE CVM."StatusFlag" END, CVM."ResolveDate" = NULL, CVM."AideFName" = ALLDATA."AideFName", CVM."AideLName" = ALLDATA."AideLName", CVM."ConAideFName" = ALLDATA."ConAideFName", CVM."ConAideLName" = ALLDATA."ConAideLName", CVM."PFName" = ALLDATA."PFName", CVM."PLName" = ALLDATA."PLName", CVM."ConPFName" = ALLDATA."ConPFName", CVM."ConPLName" = ALLDATA."ConPLName", CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber", CVM."ConPMedicaidNumber" = ALLDATA."ConPMedicaidNumber", CVM."PayerState" = ALLDATA."PayerState", CVM."ConPayerState" = ALLDATA."ConPayerState", CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy", CVM."ConLastUpdatedBy" = ALLDATA."ConLastUpdatedBy", CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate", CVM."ConLastUpdatedDate" = ALLDATA."ConLastUpdatedDate", CVM."BilledRate" = ALLDATA."BilledRate", CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount", CVM."ConBilledRate" = ALLDATA."ConBilledRate", CVM."ConTotalBilledAmount" = ALLDATA."ConTotalBilledAmount", CVM."IsMissed" = ALLDATA."IsMissed", CVM."MissedVisitReason" = ALLDATA."MissedVisitReason", CVM."EVVType" = ALLDATA."EVVType", CVM."ConIsMissed" = ALLDATA."ConIsMissed", CVM."ConMissedVisitReason" = ALLDATA."ConMissedVisitReason", CVM."ConEVVType" = ALLDATA."ConEVVType", CVM."PStatus" = ALLDATA."PStatus", CVM."ConPStatus" = ALLDATA."ConPStatus", CVM."AideStatus" = ALLDATA."AideStatus", CVM."ConAideStatus" = ALLDATA."ConAideStatus", CVM."P_PatientID" = ALLDATA."P_PatientID", CVM."P_AppPatientID" = ALLDATA."P_AppPatientID", CVM."ConP_PatientID" = ALLDATA."ConP_PatientID", CVM."ConP_AppPatientID" = ALLDATA."ConP_AppPatientID", CVM."PA_PatientID" = ALLDATA."PA_PatientID", CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID", CVM."ConPA_PatientID" = ALLDATA."ConPA_PatientID", CVM."ConPA_AppPatientID" = ALLDATA."ConPA_AppPatientID", CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID", CVM."P_PName" = ALLDATA."P_PName", CVM."P_PAddressID" = ALLDATA."P_PAddressID", CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID", CVM."P_PAddressL1" = ALLDATA."P_PAddressL1", CVM."P_PAddressL2" = ALLDATA."P_PAddressL2", CVM."P_PCity" = ALLDATA."P_PCity", CVM."P_PAddressState" = ALLDATA."P_PAddressState", CVM."P_PZipCode" = ALLDATA."P_PZipCode", CVM."P_PCounty" = ALLDATA."P_PCounty", CVM."P_PFName" = ALLDATA."P_PFName", CVM."P_PLName" = ALLDATA."P_PLName", CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", CVM."ConP_PAdmissionID" = ALLDATA."ConP_PAdmissionID", CVM."ConP_PName" = ALLDATA."ConP_PName", CVM."ConP_PAddressID" = ALLDATA."ConP_PAddressID", CVM."ConP_PAppAddressID" = ALLDATA."ConP_PAppAddressID", CVM."ConP_PAddressL1" = ALLDATA."ConP_PAddressL1", CVM."ConP_PAddressL2" = ALLDATA."ConP_PAddressL2", CVM."ConP_PCity" = ALLDATA."ConP_PCity", CVM."ConP_PAddressState" = ALLDATA."ConP_PAddressState", CVM."ConP_PZipCode" = ALLDATA."ConP_PZipCode", CVM."ConP_PCounty" = ALLDATA."ConP_PCounty", CVM."ConP_PFName" = ALLDATA."ConP_PFName", CVM."ConP_PLName" = ALLDATA."ConP_PLName", CVM."ConP_PMedicaidNumber" = ALLDATA."ConP_PMedicaidNumber", CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", CVM."PA_PName" = ALLDATA."PA_PName", CVM."PA_PAddressID" = ALLDATA."PA_PAddressID", CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1", CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2", CVM."PA_PCity" = ALLDATA."PA_PCity", CVM."PA_PAddressState" = ALLDATA."PA_PAddressState", CVM."PA_PZipCode" = ALLDATA."PA_PZipCode", CVM."PA_PCounty" = ALLDATA."PA_PCounty", CVM."PA_PFName" = ALLDATA."PA_PFName", CVM."PA_PLName" = ALLDATA."PA_PLName", CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", CVM."ConPA_PAdmissionID" = ALLDATA."ConPA_PAdmissionID", CVM."ConPA_PName" = ALLDATA."ConPA_PName", CVM."ConPA_PAddressID" = ALLDATA."ConPA_PAddressID", CVM."ConPA_PAppAddressID" = ALLDATA."ConPA_PAppAddressID", CVM."ConPA_PAddressL1" = ALLDATA."ConPA_PAddressL1", CVM."ConPA_PAddressL2" = ALLDATA."ConPA_PAddressL2", CVM."ConPA_PCity" = ALLDATA."ConPA_PCity", CVM."ConPA_PAddressState" = ALLDATA."ConPA_PAddressState", CVM."ConPA_PZipCode" = ALLDATA."ConPA_PZipCode", CVM."ConPA_PCounty" = ALLDATA."ConPA_PCounty", CVM."ConPA_PFName" = ALLDATA."ConPA_PFName", CVM."ConPA_PLName" = ALLDATA."ConPA_PLName", CVM."ConPA_PMedicaidNumber" = ALLDATA."ConPA_PMedicaidNumber", CVM."ContractType" = ALLDATA."ContractType", CVM."ConContractType" = ALLDATA."ConContractType", CVM."InServiceFlag" = CASE WHEN CVM."InServiceFlag" = ''N'' THEN ALLDATA."InServiceFlag" ELSE CVM."InServiceFlag" END, CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled", CVM."ConBillRateNonBilled" = ALLDATA."ConBillRateNonBilled", CVM."BillRateBoth" = ALLDATA."BillRateBoth", CVM."ConBillRateBoth" = ALLDATA."ConBillRateBoth", CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber", CVM."ConFederalTaxNumber" = ALLDATA."ConFederalTaxNumber", CVM."InserviceStartDate" = ALLDATA."InserviceStartDate", CVM."InserviceEndDate" = ALLDATA."InserviceEndDate", CVM."ConInserviceStartDate" = ALLDATA."ConInserviceStartDate", CVM."ConInserviceEndDate" = ALLDATA."ConInserviceEndDate"
    FROM (    
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
			''Y'' AS "InServiceFlag",
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
        CR1."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)) AS V1
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
		CAST(NULL AS STRING)"PA_PAppAddressID",
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
		AND 
		V1."ProviderID" IS NOT NULL
		AND
		V2."ProviderID" != V1."ProviderID"
		AND
		V2."AppCaregiverInserviceID" IS NOT NULL
		AND
		V1."AppCaregiverInserviceID" IS NULL
       ) AS ALLDATA WHERE CVM."VisitID" = ALLDATA."VisitID" AND CVM."ConVisitID" = ALLDATA."ConVisitID" AND CVM."InserviceStartDate" IS NULL AND CVM."InserviceEndDate" IS NULL AND CVM."ConInserviceStartDate" IS NOT NULL AND CVM."ConInserviceEndDate" IS NOT NULL AND CVM."UpdateFlag" = 1`;

       var sql_query_new_inservice_update = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.CONFLICTID = ALLDATA.CONFLICTID, CVM.SSN = ALLDATA.SSN, CVM."ProviderID" = ALLDATA."ProviderID", CVM."AppProviderID" = ALLDATA."AppProviderID", CVM."ProviderName" = ALLDATA."ProviderName", CVM."VisitID" = ALLDATA."VisitID", CVM."AppVisitID" = ALLDATA."AppVisitID", CVM."ConProviderID" = ALLDATA."ConProviderID", CVM."ConAppProviderID" = ALLDATA."ConAppProviderID", CVM."ConProviderName" = ALLDATA."ConProviderName", CVM."ConVisitID" = ALLDATA."ConVisitID", CVM."ConAppVisitID" = ALLDATA."ConAppVisitID", CVM."VisitDate" = ALLDATA."VisitDate", CVM."SchStartTime" = ALLDATA."SchStartTime", CVM."SchEndTime" = ALLDATA."SchEndTime", CVM."ConSchStartTime" = ALLDATA."ConSchStartTime", CVM."ConSchEndTime" = ALLDATA."ConSchEndTime", CVM."VisitStartTime" = ALLDATA."VisitStartTime", CVM."VisitEndTime" = ALLDATA."VisitEndTime", CVM."ConVisitStartTime" = ALLDATA."ConVisitStartTime", CVM."ConVisitEndTime" = ALLDATA."ConVisitEndTime", CVM."EVVStartTime" = ALLDATA."EVVStartTime", CVM."EVVEndTime" = ALLDATA."EVVEndTime", CVM."ConEVVStartTime" = ALLDATA."ConEVVStartTime", CVM."ConEVVEndTime" = ALLDATA."ConEVVEndTime", CVM."CaregiverID" = ALLDATA."CaregiverID", CVM."AppCaregiverID" = ALLDATA."AppCaregiverID", CVM."AideCode" = ALLDATA."AideCode", CVM."AideName" = ALLDATA."AideName", CVM."AideSSN" = ALLDATA."AideSSN", CVM."ConCaregiverID" = ALLDATA."ConCaregiverID", CVM."ConAppCaregiverID" = ALLDATA."ConAppCaregiverID", CVM."ConAideCode" = ALLDATA."ConAideCode", CVM."ConAideName" = ALLDATA."ConAideName", CVM."ConAideSSN" = ALLDATA."ConAideSSN", CVM."OfficeID" = ALLDATA."OfficeID", CVM."AppOfficeID" = ALLDATA."AppOfficeID", CVM."Office" = ALLDATA."Office", CVM."ConOfficeID" = ALLDATA."ConOfficeID", CVM."ConAppOfficeID" = ALLDATA."ConAppOfficeID", CVM."ConOffice" = ALLDATA."ConOffice", CVM."PatientID" = ALLDATA."PatientID", CVM."AppPatientID" = ALLDATA."AppPatientID", CVM."PAdmissionID" = ALLDATA."PAdmissionID", CVM."PName" = ALLDATA."PName", CVM."PAddressID" = ALLDATA."PAddressID", CVM."PAppAddressID" = ALLDATA."PAppAddressID", CVM."PAddressL1" = ALLDATA."PAddressL1", CVM."PAddressL2" = ALLDATA."PAddressL2", CVM."PCity" = ALLDATA."PCity", CVM."PAddressState" = ALLDATA."PAddressState", CVM."PZipCode" = ALLDATA."PZipCode", CVM."PCounty" = ALLDATA."PCounty", CVM."PLongitude" = ALLDATA."PLongitude", CVM."PLatitude" = ALLDATA."PLatitude", CVM."ConPatientID" = ALLDATA."ConPatientID", CVM."ConAppPatientID" = ALLDATA."ConAppPatientID", CVM."ConPAdmissionID" = ALLDATA."ConPAdmissionID", CVM."ConPName" = ALLDATA."ConPName", CVM."ConPAddressID" = ALLDATA."ConPAddressID", CVM."ConPAppAddressID" = ALLDATA."ConPAppAddressID", CVM."ConPAddressL1" = ALLDATA."ConPAddressL1", CVM."ConPAddressL2" = ALLDATA."ConPAddressL2", CVM."ConPCity" = ALLDATA."ConPCity", CVM."ConPAddressState" = ALLDATA."ConPAddressState", CVM."ConPZipCode" = ALLDATA."ConPZipCode", CVM."ConPCounty" = ALLDATA."ConPCounty", CVM."ConPLongitude" = ALLDATA."ConPLongitude", CVM."ConPLatitude" = ALLDATA."ConPLatitude", CVM."PayerID" = ALLDATA."PayerID", CVM."AppPayerID" = ALLDATA."AppPayerID", CVM."Contract" = ALLDATA."Contract", CVM."ConPayerID" = ALLDATA."ConPayerID", CVM."ConAppPayerID" = ALLDATA."ConAppPayerID", CVM."ConContract" = ALLDATA."ConContract", CVM."BilledDate" = ALLDATA."BilledDate", CVM."ConBilledDate" = ALLDATA."ConBilledDate", CVM."BilledHours" = ALLDATA."BilledHours", CVM."ConBilledHours" = ALLDATA."ConBilledHours", CVM."Billed" = ALLDATA."Billed", CVM."ConBilled" = ALLDATA."ConBilled", CVM."MinuteDiffBetweenSch" = ALLDATA."MinuteDiffBetweenSch", CVM."DistanceMilesFromLatLng" = ALLDATA."DistanceMilesFromLatLng", CVM."AverageMilesPerHour" = ALLDATA."AverageMilesPerHour", CVM."ETATravleMinutes" = ALLDATA."ETATravleMinutes", CVM."ServiceCodeID" = ALLDATA."ServiceCodeID", CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID", CVM."RateType" = ALLDATA."RateType", CVM."ServiceCode" = ALLDATA."ServiceCode", CVM."ConServiceCodeID" = ALLDATA."ConServiceCodeID", CVM."ConAppServiceCodeID" = ALLDATA."ConAppServiceCodeID", CVM."ConRateType" = ALLDATA."ConRateType", CVM."ConServiceCode" = ALLDATA."ConServiceCode", CVM."UpdateFlag" = NULL, CVM."UpdatedDate" = CURRENT_TIMESTAMP(), CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" NOT IN (''W'', ''I'') THEN ''U'' ELSE CVM."StatusFlag" END, CVM."ResolveDate" = NULL, CVM."AideFName" = ALLDATA."AideFName", CVM."AideLName" = ALLDATA."AideLName", CVM."ConAideFName" = ALLDATA."ConAideFName", CVM."ConAideLName" = ALLDATA."ConAideLName", CVM."PFName" = ALLDATA."PFName", CVM."PLName" = ALLDATA."PLName", CVM."ConPFName" = ALLDATA."ConPFName", CVM."ConPLName" = ALLDATA."ConPLName", CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber", CVM."ConPMedicaidNumber" = ALLDATA."ConPMedicaidNumber", CVM."PayerState" = ALLDATA."PayerState", CVM."ConPayerState" = ALLDATA."ConPayerState", CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy", CVM."ConLastUpdatedBy" = ALLDATA."ConLastUpdatedBy", CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate", CVM."ConLastUpdatedDate" = ALLDATA."ConLastUpdatedDate", CVM."BilledRate" = ALLDATA."BilledRate", CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount", CVM."ConBilledRate" = ALLDATA."ConBilledRate", CVM."ConTotalBilledAmount" = ALLDATA."ConTotalBilledAmount", CVM."IsMissed" = ALLDATA."IsMissed", CVM."MissedVisitReason" = ALLDATA."MissedVisitReason", CVM."EVVType" = ALLDATA."EVVType", CVM."ConIsMissed" = ALLDATA."ConIsMissed", CVM."ConMissedVisitReason" = ALLDATA."ConMissedVisitReason", CVM."ConEVVType" = ALLDATA."ConEVVType", CVM."PStatus" = ALLDATA."PStatus", CVM."ConPStatus" = ALLDATA."ConPStatus", CVM."AideStatus" = ALLDATA."AideStatus", CVM."ConAideStatus" = ALLDATA."ConAideStatus", CVM."P_PatientID" = ALLDATA."P_PatientID", CVM."P_AppPatientID" = ALLDATA."P_AppPatientID", CVM."ConP_PatientID" = ALLDATA."ConP_PatientID", CVM."ConP_AppPatientID" = ALLDATA."ConP_AppPatientID", CVM."PA_PatientID" = ALLDATA."PA_PatientID", CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID", CVM."ConPA_PatientID" = ALLDATA."ConPA_PatientID", CVM."ConPA_AppPatientID" = ALLDATA."ConPA_AppPatientID", CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID", CVM."P_PName" = ALLDATA."P_PName", CVM."P_PAddressID" = ALLDATA."P_PAddressID", CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID", CVM."P_PAddressL1" = ALLDATA."P_PAddressL1", CVM."P_PAddressL2" = ALLDATA."P_PAddressL2", CVM."P_PCity" = ALLDATA."P_PCity", CVM."P_PAddressState" = ALLDATA."P_PAddressState", CVM."P_PZipCode" = ALLDATA."P_PZipCode", CVM."P_PCounty" = ALLDATA."P_PCounty", CVM."P_PFName" = ALLDATA."P_PFName", CVM."P_PLName" = ALLDATA."P_PLName", CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", CVM."ConP_PAdmissionID" = ALLDATA."ConP_PAdmissionID", CVM."ConP_PName" = ALLDATA."ConP_PName", CVM."ConP_PAddressID" = ALLDATA."ConP_PAddressID", CVM."ConP_PAppAddressID" = ALLDATA."ConP_PAppAddressID", CVM."ConP_PAddressL1" = ALLDATA."ConP_PAddressL1", CVM."ConP_PAddressL2" = ALLDATA."ConP_PAddressL2", CVM."ConP_PCity" = ALLDATA."ConP_PCity", CVM."ConP_PAddressState" = ALLDATA."ConP_PAddressState", CVM."ConP_PZipCode" = ALLDATA."ConP_PZipCode", CVM."ConP_PCounty" = ALLDATA."ConP_PCounty", CVM."ConP_PFName" = ALLDATA."ConP_PFName", CVM."ConP_PLName" = ALLDATA."ConP_PLName", CVM."ConP_PMedicaidNumber" = ALLDATA."ConP_PMedicaidNumber", CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", CVM."PA_PName" = ALLDATA."PA_PName", CVM."PA_PAddressID" = ALLDATA."PA_PAddressID", CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1", CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2", CVM."PA_PCity" = ALLDATA."PA_PCity", CVM."PA_PAddressState" = ALLDATA."PA_PAddressState", CVM."PA_PZipCode" = ALLDATA."PA_PZipCode", CVM."PA_PCounty" = ALLDATA."PA_PCounty", CVM."PA_PFName" = ALLDATA."PA_PFName", CVM."PA_PLName" = ALLDATA."PA_PLName", CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", CVM."ConPA_PAdmissionID" = ALLDATA."ConPA_PAdmissionID", CVM."ConPA_PName" = ALLDATA."ConPA_PName", CVM."ConPA_PAddressID" = ALLDATA."ConPA_PAddressID", CVM."ConPA_PAppAddressID" = ALLDATA."ConPA_PAppAddressID", CVM."ConPA_PAddressL1" = ALLDATA."ConPA_PAddressL1", CVM."ConPA_PAddressL2" = ALLDATA."ConPA_PAddressL2", CVM."ConPA_PCity" = ALLDATA."ConPA_PCity", CVM."ConPA_PAddressState" = ALLDATA."ConPA_PAddressState", CVM."ConPA_PZipCode" = ALLDATA."ConPA_PZipCode", CVM."ConPA_PCounty" = ALLDATA."ConPA_PCounty", CVM."ConPA_PFName" = ALLDATA."ConPA_PFName", CVM."ConPA_PLName" = ALLDATA."ConPA_PLName", CVM."ConPA_PMedicaidNumber" = ALLDATA."ConPA_PMedicaidNumber", CVM."ContractType" = ALLDATA."ContractType", CVM."ConContractType" = ALLDATA."ConContractType", CVM."InServiceFlag" = CASE WHEN CVM."InServiceFlag" = ''N'' THEN ALLDATA."InServiceFlag" ELSE CVM."InServiceFlag" END, CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled", CVM."ConBillRateNonBilled" = ALLDATA."ConBillRateNonBilled", CVM."BillRateBoth" = ALLDATA."BillRateBoth", CVM."ConBillRateBoth" = ALLDATA."ConBillRateBoth", CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber", CVM."ConFederalTaxNumber" = ALLDATA."ConFederalTaxNumber", CVM."InserviceStartDate" = ALLDATA."InserviceStartDate", CVM."InserviceEndDate" = ALLDATA."InserviceEndDate", CVM."ConInserviceStartDate" = ALLDATA."ConInserviceStartDate", CVM."ConInserviceEndDate" = ALLDATA."ConInserviceEndDate"
    FROM (    
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
		CAST(NULL AS STRING) "IsMissed", 
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
		CAST(NULL AS STRING) "BilledDate",
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
		) AS V1
       INNER JOIN
       (
			SELECT DISTINCT CAST(NULL AS NUMBER) "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") AS "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(FCS."Inservice start date" AS timestamp) AS "InserviceStartDate", CAST(FCS."Inservice end date" AS timestamp) AS "InserviceEndDate", CAST(FCS."Application Caregiver Inservice Id" AS VARCHAR) AS "AppCaregiverInserviceID", CAST(NULL AS STRING) "PTOStartDate", CAST(NULL AS STRING) "PTOEndDate", CAST(NULL AS STRING) "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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
       			AND
       			V2."AppProviderID" IS NOT NULL
				AND V1."ProviderID" != V2."ProviderID"
				AND
				V1."AppCaregiverInserviceID" IS NOT NULL
				AND
				V2."AppCaregiverInserviceID" IS NULL
       ) AS ALLDATA WHERE CVM."VisitID" = ALLDATA."VisitID" AND CVM."ConVisitID" = ALLDATA."ConVisitID" AND CVM."InserviceStartDate" IS NOT NULL AND CVM."InserviceEndDate" IS NOT NULL AND CVM."ConInserviceStartDate" IS NULL AND CVM."ConInserviceEndDate" IS NULL AND CVM."UpdateFlag" = 1`;


  	snowflake.execute({ sqlText: sql_query_reverse_inservice_update });
  	snowflake.execute({ sqlText: sql_query_new_inservice_update });  
    
    return "CONFLICTVISITMAPS table updated successfully.";
  } catch (err) {
	var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
    // If an error occurs, capture it and raise it with a custom message
  	throw "ERROR: " + err.message;  // Returns the error message to the caller
  }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.UPDATE_DATA_CONFLICTVISITMAPS_2()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	return true;
  try {
       var sql_query_reverse_pto_update = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.CONFLICTID = ALLDATA.CONFLICTID, CVM.SSN = ALLDATA.SSN, CVM."ProviderID" = ALLDATA."ProviderID", CVM."AppProviderID" = ALLDATA."AppProviderID", CVM."ProviderName" = ALLDATA."ProviderName", CVM."VisitID" = ALLDATA."VisitID", CVM."AppVisitID" = ALLDATA."AppVisitID", CVM."ConProviderID" = ALLDATA."ConProviderID", CVM."ConAppProviderID" = ALLDATA."ConAppProviderID", CVM."ConProviderName" = ALLDATA."ConProviderName", CVM."ConVisitID" = ALLDATA."ConVisitID", CVM."ConAppVisitID" = ALLDATA."ConAppVisitID", CVM."VisitDate" = ALLDATA."VisitDate", CVM."SchStartTime" = ALLDATA."SchStartTime", CVM."SchEndTime" = ALLDATA."SchEndTime", CVM."ConSchStartTime" = ALLDATA."ConSchStartTime", CVM."ConSchEndTime" = ALLDATA."ConSchEndTime", CVM."VisitStartTime" = ALLDATA."VisitStartTime", CVM."VisitEndTime" = ALLDATA."VisitEndTime", CVM."ConVisitStartTime" = ALLDATA."ConVisitStartTime", CVM."ConVisitEndTime" = ALLDATA."ConVisitEndTime", CVM."EVVStartTime" = ALLDATA."EVVStartTime", CVM."EVVEndTime" = ALLDATA."EVVEndTime", CVM."ConEVVStartTime" = ALLDATA."ConEVVStartTime", CVM."ConEVVEndTime" = ALLDATA."ConEVVEndTime", CVM."CaregiverID" = ALLDATA."CaregiverID", CVM."AppCaregiverID" = ALLDATA."AppCaregiverID", CVM."AideCode" = ALLDATA."AideCode", CVM."AideName" = ALLDATA."AideName", CVM."AideSSN" = ALLDATA."AideSSN", CVM."ConCaregiverID" = ALLDATA."ConCaregiverID", CVM."ConAppCaregiverID" = ALLDATA."ConAppCaregiverID", CVM."ConAideCode" = ALLDATA."ConAideCode", CVM."ConAideName" = ALLDATA."ConAideName", CVM."ConAideSSN" = ALLDATA."ConAideSSN", CVM."OfficeID" = ALLDATA."OfficeID", CVM."AppOfficeID" = ALLDATA."AppOfficeID", CVM."Office" = ALLDATA."Office", CVM."ConOfficeID" = ALLDATA."ConOfficeID", CVM."ConAppOfficeID" = ALLDATA."ConAppOfficeID", CVM."ConOffice" = ALLDATA."ConOffice", CVM."PatientID" = ALLDATA."PatientID", CVM."AppPatientID" = ALLDATA."AppPatientID", CVM."PAdmissionID" = ALLDATA."PAdmissionID", CVM."PName" = ALLDATA."PName", CVM."PAddressID" = ALLDATA."PAddressID", CVM."PAppAddressID" = ALLDATA."PAppAddressID", CVM."PAddressL1" = ALLDATA."PAddressL1", CVM."PAddressL2" = ALLDATA."PAddressL2", CVM."PCity" = ALLDATA."PCity", CVM."PAddressState" = ALLDATA."PAddressState", CVM."PZipCode" = ALLDATA."PZipCode", CVM."PCounty" = ALLDATA."PCounty", CVM."PLongitude" = ALLDATA."PLongitude", CVM."PLatitude" = ALLDATA."PLatitude", CVM."ConPatientID" = ALLDATA."ConPatientID", CVM."ConAppPatientID" = ALLDATA."ConAppPatientID", CVM."ConPAdmissionID" = ALLDATA."ConPAdmissionID", CVM."ConPName" = ALLDATA."ConPName", CVM."ConPAddressID" = ALLDATA."ConPAddressID", CVM."ConPAppAddressID" = ALLDATA."ConPAppAddressID", CVM."ConPAddressL1" = ALLDATA."ConPAddressL1", CVM."ConPAddressL2" = ALLDATA."ConPAddressL2", CVM."ConPCity" = ALLDATA."ConPCity", CVM."ConPAddressState" = ALLDATA."ConPAddressState", CVM."ConPZipCode" = ALLDATA."ConPZipCode", CVM."ConPCounty" = ALLDATA."ConPCounty", CVM."ConPLongitude" = ALLDATA."ConPLongitude", CVM."ConPLatitude" = ALLDATA."ConPLatitude", CVM."PayerID" = ALLDATA."PayerID", CVM."AppPayerID" = ALLDATA."AppPayerID", CVM."Contract" = ALLDATA."Contract", CVM."ConPayerID" = ALLDATA."ConPayerID", CVM."ConAppPayerID" = ALLDATA."ConAppPayerID", CVM."ConContract" = ALLDATA."ConContract", CVM."BilledDate" = ALLDATA."BilledDate", CVM."ConBilledDate" = ALLDATA."ConBilledDate", CVM."BilledHours" = ALLDATA."BilledHours", CVM."ConBilledHours" = ALLDATA."ConBilledHours", CVM."Billed" = ALLDATA."Billed", CVM."ConBilled" = ALLDATA."ConBilled", CVM."MinuteDiffBetweenSch" = ALLDATA."MinuteDiffBetweenSch", CVM."DistanceMilesFromLatLng" = ALLDATA."DistanceMilesFromLatLng", CVM."AverageMilesPerHour" = ALLDATA."AverageMilesPerHour", CVM."ETATravleMinutes" = ALLDATA."ETATravleMinutes", CVM."ServiceCodeID" = ALLDATA."ServiceCodeID", CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID", CVM."RateType" = ALLDATA."RateType", CVM."ServiceCode" = ALLDATA."ServiceCode", CVM."ConServiceCodeID" = ALLDATA."ConServiceCodeID", CVM."ConAppServiceCodeID" = ALLDATA."ConAppServiceCodeID", CVM."ConRateType" = ALLDATA."ConRateType", CVM."ConServiceCode" = ALLDATA."ConServiceCode", CVM."UpdateFlag" = NULL, CVM."UpdatedDate" = CURRENT_TIMESTAMP(), CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" NOT IN (''W'', ''I'') THEN ''U'' ELSE CVM."StatusFlag" END, CVM."ResolveDate" = NULL, CVM."AideFName" = ALLDATA."AideFName", CVM."AideLName" = ALLDATA."AideLName", CVM."ConAideFName" = ALLDATA."ConAideFName", CVM."ConAideLName" = ALLDATA."ConAideLName", CVM."PFName" = ALLDATA."PFName", CVM."PLName" = ALLDATA."PLName", CVM."ConPFName" = ALLDATA."ConPFName", CVM."ConPLName" = ALLDATA."ConPLName", CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber", CVM."ConPMedicaidNumber" = ALLDATA."ConPMedicaidNumber", CVM."PayerState" = ALLDATA."PayerState", CVM."ConPayerState" = ALLDATA."ConPayerState", CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy", CVM."ConLastUpdatedBy" = ALLDATA."ConLastUpdatedBy", CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate", CVM."ConLastUpdatedDate" = ALLDATA."ConLastUpdatedDate", CVM."BilledRate" = ALLDATA."BilledRate", CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount", CVM."ConBilledRate" = ALLDATA."ConBilledRate", CVM."ConTotalBilledAmount" = ALLDATA."ConTotalBilledAmount", CVM."IsMissed" = ALLDATA."IsMissed", CVM."MissedVisitReason" = ALLDATA."MissedVisitReason", CVM."EVVType" = ALLDATA."EVVType", CVM."ConIsMissed" = ALLDATA."ConIsMissed", CVM."ConMissedVisitReason" = ALLDATA."ConMissedVisitReason", CVM."ConEVVType" = ALLDATA."ConEVVType", CVM."PStatus" = ALLDATA."PStatus", CVM."ConPStatus" = ALLDATA."ConPStatus", CVM."AideStatus" = ALLDATA."AideStatus", CVM."ConAideStatus" = ALLDATA."ConAideStatus", CVM."P_PatientID" = ALLDATA."P_PatientID", CVM."P_AppPatientID" = ALLDATA."P_AppPatientID", CVM."ConP_PatientID" = ALLDATA."ConP_PatientID", CVM."ConP_AppPatientID" = ALLDATA."ConP_AppPatientID", CVM."PA_PatientID" = ALLDATA."PA_PatientID", CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID", CVM."ConPA_PatientID" = ALLDATA."ConPA_PatientID", CVM."ConPA_AppPatientID" = ALLDATA."ConPA_AppPatientID", CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID", CVM."P_PName" = ALLDATA."P_PName", CVM."P_PAddressID" = ALLDATA."P_PAddressID", CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID", CVM."P_PAddressL1" = ALLDATA."P_PAddressL1", CVM."P_PAddressL2" = ALLDATA."P_PAddressL2", CVM."P_PCity" = ALLDATA."P_PCity", CVM."P_PAddressState" = ALLDATA."P_PAddressState", CVM."P_PZipCode" = ALLDATA."P_PZipCode", CVM."P_PCounty" = ALLDATA."P_PCounty", CVM."P_PFName" = ALLDATA."P_PFName", CVM."P_PLName" = ALLDATA."P_PLName", CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", CVM."ConP_PAdmissionID" = ALLDATA."ConP_PAdmissionID", CVM."ConP_PName" = ALLDATA."ConP_PName", CVM."ConP_PAddressID" = ALLDATA."ConP_PAddressID", CVM."ConP_PAppAddressID" = ALLDATA."ConP_PAppAddressID", CVM."ConP_PAddressL1" = ALLDATA."ConP_PAddressL1", CVM."ConP_PAddressL2" = ALLDATA."ConP_PAddressL2", CVM."ConP_PCity" = ALLDATA."ConP_PCity", CVM."ConP_PAddressState" = ALLDATA."ConP_PAddressState", CVM."ConP_PZipCode" = ALLDATA."ConP_PZipCode", CVM."ConP_PCounty" = ALLDATA."ConP_PCounty", CVM."ConP_PFName" = ALLDATA."ConP_PFName", CVM."ConP_PLName" = ALLDATA."ConP_PLName", CVM."ConP_PMedicaidNumber" = ALLDATA."ConP_PMedicaidNumber", CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", CVM."PA_PName" = ALLDATA."PA_PName", CVM."PA_PAddressID" = ALLDATA."PA_PAddressID", CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1", CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2", CVM."PA_PCity" = ALLDATA."PA_PCity", CVM."PA_PAddressState" = ALLDATA."PA_PAddressState", CVM."PA_PZipCode" = ALLDATA."PA_PZipCode", CVM."PA_PCounty" = ALLDATA."PA_PCounty", CVM."PA_PFName" = ALLDATA."PA_PFName", CVM."PA_PLName" = ALLDATA."PA_PLName", CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", CVM."ConPA_PAdmissionID" = ALLDATA."ConPA_PAdmissionID", CVM."ConPA_PName" = ALLDATA."ConPA_PName", CVM."ConPA_PAddressID" = ALLDATA."ConPA_PAddressID", CVM."ConPA_PAppAddressID" = ALLDATA."ConPA_PAppAddressID", CVM."ConPA_PAddressL1" = ALLDATA."ConPA_PAddressL1", CVM."ConPA_PAddressL2" = ALLDATA."ConPA_PAddressL2", CVM."ConPA_PCity" = ALLDATA."ConPA_PCity", CVM."ConPA_PAddressState" = ALLDATA."ConPA_PAddressState", CVM."ConPA_PZipCode" = ALLDATA."ConPA_PZipCode", CVM."ConPA_PCounty" = ALLDATA."ConPA_PCounty", CVM."ConPA_PFName" = ALLDATA."ConPA_PFName", CVM."ConPA_PLName" = ALLDATA."ConPA_PLName", CVM."ConPA_PMedicaidNumber" = ALLDATA."ConPA_PMedicaidNumber", CVM."ContractType" = ALLDATA."ContractType", CVM."ConContractType" = ALLDATA."ConContractType", CVM."InServiceFlag" = CASE WHEN CVM."InServiceFlag" = ''N'' THEN ALLDATA."InServiceFlag" ELSE CVM."InServiceFlag" END, CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled", CVM."ConBillRateNonBilled" = ALLDATA."ConBillRateNonBilled", CVM."BillRateBoth" = ALLDATA."BillRateBoth", CVM."ConBillRateBoth" = ALLDATA."ConBillRateBoth", CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber", CVM."ConFederalTaxNumber" = ALLDATA."ConFederalTaxNumber", CVM."PTOStartDate" = ALLDATA."PTOStartDate", CVM."PTOEndDate" = ALLDATA."PTOEndDate", CVM."ConPTOStartDate" = ALLDATA."ConPTOStartDate", CVM."ConPTOEndDate" = ALLDATA."ConPTOEndDate"
    FROM (    
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
			''N'' AS "InServiceFlag",
			''Y'' AS "PTOFlag",
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
			V1."PTOStartDate" AS "PTOStartDate",
			V1."PTOEndDate" AS "PTOEndDate",
			V2."PTOStartDate" AS "ConPTOStartDate",
			V2."PTOEndDate" AS "ConPTOEndDate"
		FROM
       (SELECT DISTINCT CVM1."CONFLICTID" as "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(NULL AS TIMESTAMP) "InserviceStartDate", CAST(NULL AS TIMESTAMP) "InserviceEndDate", CAST(NULL AS STRING) "AppCaregiverInserviceID", CAST(FCA."Start Date" AS timestamp) AS "PTOStartDate", CAST(FCA."End Date" AS timestamp) AS "PTOEndDate", CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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

		LEFT JOIN ANALYTICS.BI.FACTCAREGIVERABSENCE AS FCA ON FCA."Global Caregiver Id" = CR1."Caregiver Id" AND FCA."Provider Id" = CR1."Provider Id" AND (CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL AND CAST(CR1."Visit Start Time" AS DATE) <= CAST(FCA."End Date" AS DATE) AND CAST(CR1."Visit End Time" AS DATE) >= CAST(FCA."Start Date" AS DATE))

        WHERE CR1."Is Missed" = FALSE AND FCA."Caregiver Vacation Id" IS NULL AND DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND 
        
        CR1."Visit Start Time" IS NOT NULL
        AND
        CR1."Visit End Time" IS NOT NULL
        AND
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
		CAST(NULL AS STRING) "IsMissed", 
		CAST(NULL AS STRING) "EVVType", 
		CAST(NULL AS NUMBER) "BilledRate", 
		CAST(NULL AS NUMBER) "TotalBilledAmount",
		DPR."Provider Id" as "ProviderID", 
		DPR."Application Provider Id" as "AppProviderID", 
		DPR."Provider Name" AS "ProviderName", 
		CAST(NULL AS STRING) "AgencyContact", 
		DPR."Phone Number 1" AS "AgencyPhone", 
		DPR."Federal Tax Number" AS "FederalTaxNumber",
		MD5(CONCAT(''P'', CAST(FCA."Caregiver Vacation Id" AS VARCHAR))) AS "VisitID", 
		CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "AppVisitID",
		CAST(FCA."Start Date" AS DATE) AS "VisitDate", 
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
		CAST(NULL AS STRING) "BilledDate",
		CAST(NULL AS NUMBER) "BilledHours",
		CAST(NULL AS STRING) "Billed",
		CAST(NULL AS TIMESTAMP) "InserviceStartDate",
		CAST(NULL AS TIMESTAMP) "InserviceEndDate",
		CAST(NULL AS STRING) "AppCaregiverInserviceID",
		CAST(FCA."Start Date" AS timestamp) AS "PTOStartDate",
		CAST(FCA."End Date" AS timestamp) AS "PTOEndDate",
		CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "PTOVacationID",
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
	   ANALYTICS.BI.FACTCAREGIVERABSENCE AS FCA
	   
	   INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = FCA."Global Caregiver Id" AND TRIM(CAR."SSN") IS NOT NULL AND TRIM(CAR."SSN")!=''''

	   INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = FCA."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
	   
	   LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = FCA."Office Id" AND DOF."Is Active" = TRUE

	   LEFT JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM1 ON CVM1."VisitID" = MD5(CONCAT(''P'', CAST(FCA."Caregiver Vacation Id" AS VARCHAR))) AND CVM1."CONFLICTID" IS NOT NULL
	   
	   WHERE CAST(FCA."Start Date" AS DATE) BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND 
       
       DPR."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
	   ) AS V2 ON
       V1.SSN = V2.SSN
       AND 
	   (CAST(V1."VisitStartTime" AS DATE) <= CAST(V2."PTOEndDate" AS DATE) AND CAST(V1."VisitEndTime" AS DATE) >= CAST(V2."PTOStartDate" AS DATE))
	   AND V1."ProviderID" IS NOT NULL
		AND V2."ProviderID" != V1."ProviderID"
		AND
		V2."PTOVacationID" IS NOT NULL
		AND
		V1."PTOVacationID" IS NULL
       ) AS ALLDATA WHERE CVM."VisitID" = ALLDATA."VisitID" AND CVM."ConVisitID" = ALLDATA."ConVisitID" AND CVM."PTOStartDate" IS NULL AND CVM."PTOEndDate" IS NULL AND CVM."ConPTOStartDate" IS NOT NULL AND CVM."ConPTOEndDate" IS NOT NULL AND CVM."UpdateFlag" = 1`;


       var sql_query_insert_pto_update = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.CONFLICTID = ALLDATA.CONFLICTID, CVM.SSN = ALLDATA.SSN, CVM."ProviderID" = ALLDATA."ProviderID", CVM."AppProviderID" = ALLDATA."AppProviderID", CVM."ProviderName" = ALLDATA."ProviderName", CVM."VisitID" = ALLDATA."VisitID", CVM."AppVisitID" = ALLDATA."AppVisitID", CVM."ConProviderID" = ALLDATA."ConProviderID", CVM."ConAppProviderID" = ALLDATA."ConAppProviderID", CVM."ConProviderName" = ALLDATA."ConProviderName", CVM."ConVisitID" = ALLDATA."ConVisitID", CVM."ConAppVisitID" = ALLDATA."ConAppVisitID", CVM."VisitDate" = ALLDATA."VisitDate", CVM."SchStartTime" = ALLDATA."SchStartTime", CVM."SchEndTime" = ALLDATA."SchEndTime", CVM."ConSchStartTime" = ALLDATA."ConSchStartTime", CVM."ConSchEndTime" = ALLDATA."ConSchEndTime", CVM."VisitStartTime" = ALLDATA."VisitStartTime", CVM."VisitEndTime" = ALLDATA."VisitEndTime", CVM."ConVisitStartTime" = ALLDATA."ConVisitStartTime", CVM."ConVisitEndTime" = ALLDATA."ConVisitEndTime", CVM."EVVStartTime" = ALLDATA."EVVStartTime", CVM."EVVEndTime" = ALLDATA."EVVEndTime", CVM."ConEVVStartTime" = ALLDATA."ConEVVStartTime", CVM."ConEVVEndTime" = ALLDATA."ConEVVEndTime", CVM."CaregiverID" = ALLDATA."CaregiverID", CVM."AppCaregiverID" = ALLDATA."AppCaregiverID", CVM."AideCode" = ALLDATA."AideCode", CVM."AideName" = ALLDATA."AideName", CVM."AideSSN" = ALLDATA."AideSSN", CVM."ConCaregiverID" = ALLDATA."ConCaregiverID", CVM."ConAppCaregiverID" = ALLDATA."ConAppCaregiverID", CVM."ConAideCode" = ALLDATA."ConAideCode", CVM."ConAideName" = ALLDATA."ConAideName", CVM."ConAideSSN" = ALLDATA."ConAideSSN", CVM."OfficeID" = ALLDATA."OfficeID", CVM."AppOfficeID" = ALLDATA."AppOfficeID", CVM."Office" = ALLDATA."Office", CVM."ConOfficeID" = ALLDATA."ConOfficeID", CVM."ConAppOfficeID" = ALLDATA."ConAppOfficeID", CVM."ConOffice" = ALLDATA."ConOffice", CVM."PatientID" = ALLDATA."PatientID", CVM."AppPatientID" = ALLDATA."AppPatientID", CVM."PAdmissionID" = ALLDATA."PAdmissionID", CVM."PName" = ALLDATA."PName", CVM."PAddressID" = ALLDATA."PAddressID", CVM."PAppAddressID" = ALLDATA."PAppAddressID", CVM."PAddressL1" = ALLDATA."PAddressL1", CVM."PAddressL2" = ALLDATA."PAddressL2", CVM."PCity" = ALLDATA."PCity", CVM."PAddressState" = ALLDATA."PAddressState", CVM."PZipCode" = ALLDATA."PZipCode", CVM."PCounty" = ALLDATA."PCounty", CVM."PLongitude" = ALLDATA."PLongitude", CVM."PLatitude" = ALLDATA."PLatitude", CVM."ConPatientID" = ALLDATA."ConPatientID", CVM."ConAppPatientID" = ALLDATA."ConAppPatientID", CVM."ConPAdmissionID" = ALLDATA."ConPAdmissionID", CVM."ConPName" = ALLDATA."ConPName", CVM."ConPAddressID" = ALLDATA."ConPAddressID", CVM."ConPAppAddressID" = ALLDATA."ConPAppAddressID", CVM."ConPAddressL1" = ALLDATA."ConPAddressL1", CVM."ConPAddressL2" = ALLDATA."ConPAddressL2", CVM."ConPCity" = ALLDATA."ConPCity", CVM."ConPAddressState" = ALLDATA."ConPAddressState", CVM."ConPZipCode" = ALLDATA."ConPZipCode", CVM."ConPCounty" = ALLDATA."ConPCounty", CVM."ConPLongitude" = ALLDATA."ConPLongitude", CVM."ConPLatitude" = ALLDATA."ConPLatitude", CVM."PayerID" = ALLDATA."PayerID", CVM."AppPayerID" = ALLDATA."AppPayerID", CVM."Contract" = ALLDATA."Contract", CVM."ConPayerID" = ALLDATA."ConPayerID", CVM."ConAppPayerID" = ALLDATA."ConAppPayerID", CVM."ConContract" = ALLDATA."ConContract", CVM."BilledDate" = ALLDATA."BilledDate", CVM."ConBilledDate" = ALLDATA."ConBilledDate", CVM."BilledHours" = ALLDATA."BilledHours", CVM."ConBilledHours" = ALLDATA."ConBilledHours", CVM."Billed" = ALLDATA."Billed", CVM."ConBilled" = ALLDATA."ConBilled", CVM."MinuteDiffBetweenSch" = ALLDATA."MinuteDiffBetweenSch", CVM."DistanceMilesFromLatLng" = ALLDATA."DistanceMilesFromLatLng", CVM."AverageMilesPerHour" = ALLDATA."AverageMilesPerHour", CVM."ETATravleMinutes" = ALLDATA."ETATravleMinutes", CVM."ServiceCodeID" = ALLDATA."ServiceCodeID", CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID", CVM."RateType" = ALLDATA."RateType", CVM."ServiceCode" = ALLDATA."ServiceCode", CVM."ConServiceCodeID" = ALLDATA."ConServiceCodeID", CVM."ConAppServiceCodeID" = ALLDATA."ConAppServiceCodeID", CVM."ConRateType" = ALLDATA."ConRateType", CVM."ConServiceCode" = ALLDATA."ConServiceCode", CVM."UpdateFlag" = NULL, CVM."UpdatedDate" = CURRENT_TIMESTAMP(), CVM."StatusFlag" = CASE WHEN CVM."StatusFlag" NOT IN (''W'', ''I'') THEN ''U'' ELSE CVM."StatusFlag" END, CVM."ResolveDate" = NULL, CVM."AideFName" = ALLDATA."AideFName", CVM."AideLName" = ALLDATA."AideLName", CVM."ConAideFName" = ALLDATA."ConAideFName", CVM."ConAideLName" = ALLDATA."ConAideLName", CVM."PFName" = ALLDATA."PFName", CVM."PLName" = ALLDATA."PLName", CVM."ConPFName" = ALLDATA."ConPFName", CVM."ConPLName" = ALLDATA."ConPLName", CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber", CVM."ConPMedicaidNumber" = ALLDATA."ConPMedicaidNumber", CVM."PayerState" = ALLDATA."PayerState", CVM."ConPayerState" = ALLDATA."ConPayerState", CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy", CVM."ConLastUpdatedBy" = ALLDATA."ConLastUpdatedBy", CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate", CVM."ConLastUpdatedDate" = ALLDATA."ConLastUpdatedDate", CVM."BilledRate" = ALLDATA."BilledRate", CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount", CVM."ConBilledRate" = ALLDATA."ConBilledRate", CVM."ConTotalBilledAmount" = ALLDATA."ConTotalBilledAmount", CVM."IsMissed" = ALLDATA."IsMissed", CVM."MissedVisitReason" = ALLDATA."MissedVisitReason", CVM."EVVType" = ALLDATA."EVVType", CVM."ConIsMissed" = ALLDATA."ConIsMissed", CVM."ConMissedVisitReason" = ALLDATA."ConMissedVisitReason", CVM."ConEVVType" = ALLDATA."ConEVVType", CVM."PStatus" = ALLDATA."PStatus", CVM."ConPStatus" = ALLDATA."ConPStatus", CVM."AideStatus" = ALLDATA."AideStatus", CVM."ConAideStatus" = ALLDATA."ConAideStatus", CVM."P_PatientID" = ALLDATA."P_PatientID", CVM."P_AppPatientID" = ALLDATA."P_AppPatientID", CVM."ConP_PatientID" = ALLDATA."ConP_PatientID", CVM."ConP_AppPatientID" = ALLDATA."ConP_AppPatientID", CVM."PA_PatientID" = ALLDATA."PA_PatientID", CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID", CVM."ConPA_PatientID" = ALLDATA."ConPA_PatientID", CVM."ConPA_AppPatientID" = ALLDATA."ConPA_AppPatientID", CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID", CVM."P_PName" = ALLDATA."P_PName", CVM."P_PAddressID" = ALLDATA."P_PAddressID", CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID", CVM."P_PAddressL1" = ALLDATA."P_PAddressL1", CVM."P_PAddressL2" = ALLDATA."P_PAddressL2", CVM."P_PCity" = ALLDATA."P_PCity", CVM."P_PAddressState" = ALLDATA."P_PAddressState", CVM."P_PZipCode" = ALLDATA."P_PZipCode", CVM."P_PCounty" = ALLDATA."P_PCounty", CVM."P_PFName" = ALLDATA."P_PFName", CVM."P_PLName" = ALLDATA."P_PLName", CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber", CVM."ConP_PAdmissionID" = ALLDATA."ConP_PAdmissionID", CVM."ConP_PName" = ALLDATA."ConP_PName", CVM."ConP_PAddressID" = ALLDATA."ConP_PAddressID", CVM."ConP_PAppAddressID" = ALLDATA."ConP_PAppAddressID", CVM."ConP_PAddressL1" = ALLDATA."ConP_PAddressL1", CVM."ConP_PAddressL2" = ALLDATA."ConP_PAddressL2", CVM."ConP_PCity" = ALLDATA."ConP_PCity", CVM."ConP_PAddressState" = ALLDATA."ConP_PAddressState", CVM."ConP_PZipCode" = ALLDATA."ConP_PZipCode", CVM."ConP_PCounty" = ALLDATA."ConP_PCounty", CVM."ConP_PFName" = ALLDATA."ConP_PFName", CVM."ConP_PLName" = ALLDATA."ConP_PLName", CVM."ConP_PMedicaidNumber" = ALLDATA."ConP_PMedicaidNumber", CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID", CVM."PA_PName" = ALLDATA."PA_PName", CVM."PA_PAddressID" = ALLDATA."PA_PAddressID", CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID", CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1", CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2", CVM."PA_PCity" = ALLDATA."PA_PCity", CVM."PA_PAddressState" = ALLDATA."PA_PAddressState", CVM."PA_PZipCode" = ALLDATA."PA_PZipCode", CVM."PA_PCounty" = ALLDATA."PA_PCounty", CVM."PA_PFName" = ALLDATA."PA_PFName", CVM."PA_PLName" = ALLDATA."PA_PLName", CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber", CVM."ConPA_PAdmissionID" = ALLDATA."ConPA_PAdmissionID", CVM."ConPA_PName" = ALLDATA."ConPA_PName", CVM."ConPA_PAddressID" = ALLDATA."ConPA_PAddressID", CVM."ConPA_PAppAddressID" = ALLDATA."ConPA_PAppAddressID", CVM."ConPA_PAddressL1" = ALLDATA."ConPA_PAddressL1", CVM."ConPA_PAddressL2" = ALLDATA."ConPA_PAddressL2", CVM."ConPA_PCity" = ALLDATA."ConPA_PCity", CVM."ConPA_PAddressState" = ALLDATA."ConPA_PAddressState", CVM."ConPA_PZipCode" = ALLDATA."ConPA_PZipCode", CVM."ConPA_PCounty" = ALLDATA."ConPA_PCounty", CVM."ConPA_PFName" = ALLDATA."ConPA_PFName", CVM."ConPA_PLName" = ALLDATA."ConPA_PLName", CVM."ConPA_PMedicaidNumber" = ALLDATA."ConPA_PMedicaidNumber", CVM."ContractType" = ALLDATA."ContractType", CVM."ConContractType" = ALLDATA."ConContractType", CVM."InServiceFlag" = CASE WHEN CVM."InServiceFlag" = ''N'' THEN ALLDATA."InServiceFlag" ELSE CVM."InServiceFlag" END, CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled", CVM."ConBillRateNonBilled" = ALLDATA."ConBillRateNonBilled", CVM."BillRateBoth" = ALLDATA."BillRateBoth", CVM."ConBillRateBoth" = ALLDATA."ConBillRateBoth", CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber", CVM."ConFederalTaxNumber" = ALLDATA."ConFederalTaxNumber", CVM."PTOStartDate" = ALLDATA."PTOStartDate", CVM."PTOEndDate" = ALLDATA."PTOEndDate", CVM."ConPTOStartDate" = ALLDATA."ConPTOStartDate", CVM."ConPTOEndDate" = ALLDATA."ConPTOEndDate"
    FROM (
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
            V1."PTOStartDate" AS "PTOStartDate",
			V1."PTOEndDate" AS "PTOEndDate",
            V2."PTOStartDate" AS "ConPTOStartDate",
			V2."PTOEndDate" AS "ConPTOEndDate",
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
            ''N'' AS "InServiceFlag",
            ''Y'' AS "PTOFlag",
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
		CAST(NULL AS STRING) "IsMissed", 
		CAST(NULL AS STRING) "EVVType",
		CAST(NULL AS NUMBER) "BilledRate", 
		CAST(NULL AS NUMBER) "TotalBilledAmount",
		DPR."Provider Id" as "ProviderID", 
		DPR."Application Provider Id" as "AppProviderID", 
		DPR."Provider Name" AS "ProviderName", 
		CAST(NULL AS STRING) "AgencyContact",
		DPR."Phone Number 1" AS "AgencyPhone", 
		DPR."Federal Tax Number" AS "FederalTaxNumber",
		MD5(CONCAT(''P'', CAST(FCA."Caregiver Vacation Id" AS VARCHAR))) AS "VisitID",
		CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "AppVisitID",
		CAST(FCA."Start Date" AS DATE) AS "VisitDate", 
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
		CAST(NULL AS STRING) "BilledDate",
		CAST(NULL AS NUMBER) "BilledHours",
		CAST(NULL AS STRING) "Billed",
		CAST(NULL AS TIMESTAMP) "InserviceStartDate",
		CAST(NULL AS TIMESTAMP) "InserviceEndDate",
		CAST(NULL AS STRING) "AppCaregiverInserviceID",
		CAST(FCA."Start Date" AS timestamp) AS "PTOStartDate",
		CAST(FCA."End Date" AS timestamp) AS "PTOEndDate",
		CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "PTOVacationID",
		CAST(NULL AS STRING) "ServiceCodeID",
		CAST(NULL AS STRING) "AppServiceCodeID",
		CAST(NULL AS STRING) "RateType",
		CAST(NULL AS STRING) "ServiceCode",
		CAST(NULL AS STRING) "LastUpdatedDate",
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
	   ANALYTICS.BI.FACTCAREGIVERABSENCE AS FCA
	   
	   INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR ON CAR."Caregiver Id" = FCA."Global Caregiver Id" AND TRIM(CAR."SSN") IS NOT NULL AND TRIM(CAR."SSN")!=''''

	   INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR ON DPR."Provider Id" = FCA."Provider Id" AND DPR."Is Active" = TRUE AND DPR."Is Demo" = FALSE
	   
	   LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF ON DOF."Office Id" = FCA."Office Id" AND DOF."Is Active" = TRUE

	   LEFT JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM1 ON CVM1."VisitID" = MD5(CONCAT(''P'', CAST(FCA."Caregiver Vacation Id" AS VARCHAR))) AND CVM1."CONFLICTID" IS NOT NULL
	   
	   WHERE CAST(FCA."Start Date" AS DATE) BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE())) AND 
       
       DPR."Provider Id" NOT IN(SELECT "ProviderID" AS PAID FROM CONFLICTREPORT."PUBLIC".EXCLUDED_AGENCY) AND NOT EXISTS (SELECT 1 FROM CONFLICTREPORT."PUBLIC".EXCLUDED_SSN AS SSN WHERE TRIM(CAR."SSN") = SSN.SSN)
	   ) AS V1
       INNER JOIN
       (
			SELECT DISTINCT CAST(NULL AS NUMBER) "CONFLICTID", CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") AS "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", CAST(NULL AS TIMESTAMP) "InserviceStartDate", CAST(NULL AS TIMESTAMP) "InserviceEndDate", CAST(NULL AS STRING) "AppCaregiverInserviceID", CAST(FCA."Start Date" AS timestamp) AS "PTOStartDate", CAST(FCA."End Date" AS timestamp) AS "PTOEndDate", CAST(FCA."Caregiver Vacation Id" AS VARCHAR) AS "PTOVacationID", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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
			LEFT JOIN ANALYTICS.BI.FACTCAREGIVERABSENCE AS FCA ON FCA."Global Caregiver Id" = CR1."Caregiver Id" AND FCA."Provider Id" = CR1."Provider Id" AND (CR1."Visit Start Time" IS NOT NULL AND CR1."Visit End Time" IS NOT NULL AND CAST(CR1."Visit Start Time" AS DATE) <= CAST(FCA."End Date" AS DATE) AND CAST(CR1."Visit End Time" AS DATE) >= CAST(FCA."Start Date" AS DATE))

			LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
			LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
			WHERE CR1."Is Missed" = FALSE AND CAST(FCA."Caregiver Vacation Id" AS VARCHAR) IS NULL AND DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
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
		AND (CAST(V2."VisitStartTime" AS DATE) <= CAST(V1."PTOEndDate" AS DATE) AND CAST(V2."VisitEndTime" AS DATE) >= CAST(V1."PTOStartDate" AS DATE))
		AND V2."ProviderID" IS NOT NULL
       			AND
				V1."ProviderID" != V2."ProviderID"
				AND
				V1."PTOVacationID" IS NOT NULL
				AND
				V2."PTOVacationID" IS NULL
       ) AS ALLDATA WHERE CVM."VisitID" = ALLDATA."VisitID" AND CVM."ConVisitID" = ALLDATA."ConVisitID" AND CVM."PTOStartDate" IS NOT NULL AND CVM."PTOEndDate" IS NOT NULL AND CVM."ConPTOStartDate" IS NULL AND CVM."ConPTOEndDate" IS NULL AND CVM."UpdateFlag" = 1`;

        		
  	
  	snowflake.execute({ sqlText: sql_query_reverse_pto_update });
  	snowflake.execute({ sqlText: sql_query_insert_pto_update });
  
    
    return "CONFLICTVISITMAPS table updated successfully.";
  } catch (err) {
	var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
    // If an error occurs, capture it and raise it with a custom message
  	throw "ERROR: " + err.message;  // Returns the error message to the caller
  }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.UPDATE_DATA_CONFLICTVISITMAPS_3()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  try {
      
		var sql_queryseconds = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
			SET CVM."UpdateFlag" = NULL, CVM."StatusFlag" = ''D'', CVM."ResolveDate" = COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP), CVM."ResolvedBy" = COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
			FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR AS DELETECR 
			WHERE CVM."ConVisitID" = DELETECR."Visit Id" AND CVM."StatusFlag"!=''D'' AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
		
		var sql_queryseconds1 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
			SET CF."StatusFlag" = ''D'', CF."ResolveDate" = COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP), CF."ResolvedBy" = COALESCE(CVM."AgencyContact", CVM."ProviderName")
			FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR DELETECR
			INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
			    ON CVM."VisitID" = DELETECR."Visit Id"
			WHERE CF."StatusFlag" != ''D'' AND CF."CONFLICTID" = CVM."CONFLICTID" AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
		
		var sql_queryseconds2 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
				SET 
					CF."StatusFlag" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN ''D''
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN ''R''
							ELSE CF."StatusFlag"
						END,
					CF."ResolveDate" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
							ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
						END,
					CF."ResolvedBy" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
							WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
							WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
							ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
						END
				FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
				WHERE CVM.CONFLICTID = CF.CONFLICTID AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
		
			var sql_queryseconds3 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
				SET 
				CVM."StatusFlag" = 
					CASE 
						WHEN CVM."StatusFlag" = ''D'' THEN ''D''
						WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN ''R''
						ELSE CVM."StatusFlag"
					END,
				CVM."ResolveDate" = 
					CASE 
						WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
						WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
						ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
					END,
				CVM."ResolvedBy" = 
					CASE 
						WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
						WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
						WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
						ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
					END WHERE DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
				
			var sql_queryseconds4 = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
			SET CVM."UpdateFlag" = NULL,
				CVM."StatusFlag" = CASE 
					WHEN CVM."StatusFlag" = ''D'' THEN ''D''
					WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN ''R''
					ELSE ''R''
				END,
				CVM."ResolveDate" = COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP),
				CVM."ResolvedBy" = 
					CASE 
						WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
						WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
						WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
						ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
					END
			WHERE CVM."UpdateFlag" = 1 AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;

			var sql_queryseconds4_AA = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
				SET 
					CF."UpdatedRFlag" = ''1''
				FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
				WHERE CVM.CONFLICTID = CF.CONFLICTID AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;

			var sql_queryseconds4_A = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
            SET CF."StatusFlag" = ''U'', CF."UpdatedRFlag" = NULL
            WHERE CF.CONFLICTID IN (
                SELECT CF.CONFLICTID 
                FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF 
                INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
                WHERE CF."StatusFlag" NOT IN (''D'', ''I'', ''W'', ''U'') AND CVM."StatusFlag" IN(''U'') AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
                GROUP BY CF.CONFLICTID
            )`;
			
      		var sql_queryseconds5 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
					SET 
						CVM."StatusFlag" = 
							CASE 
								WHEN CVM."StatusFlag" = ''D'' THEN ''D''
								WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN ''R''
								ELSE ''R''
							END,
						CVM."ResolveDate" = 
							CASE 
								WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
								WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
								ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
							END,
						CVM."ResolvedBy" = 
							CASE 
								WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
								WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
								WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
								ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
							END
					WHERE CVM.CONFLICTID IN (
						SELECT CF.CONFLICTID 
						FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF 
						INNER JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID 
						WHERE CF."StatusFlag" IN (''R'', ''D'') AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
						GROUP BY CF.CONFLICTID 
						HAVING COUNT(CVM.ID) = 1
					) AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
			
			var sql_queryseconds6 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
				SET 
					CF."StatusFlag" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN ''D''
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN ''R''
							ELSE ''R''
						END,
					CF."ResolveDate" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
							ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
						END,
					CF."ResolvedBy" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
							WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
							WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
							ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
						END
				FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
				WHERE CVM.CONFLICTID = CF.CONFLICTID AND CF.CONFLICTID IN(
					SELECT 
						DISTINCT CVM.CONFLICTID
					FROM 
						CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
					WHERE 
						CVM.CONFLICTID IN (
							SELECT DISTINCT CVM.CONFLICTID
							FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM WHERE CVM."StatusFlag" IN(''R'', ''D'') AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
						) AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
						GROUP BY CVM.CONFLICTID
						HAVING COUNT(CVM.ID) = 1
				) AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
			
			var sql_queryseconds7 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
				SET CVM."StatusFlag" = 
						CASE 
							WHEN CVM."StatusFlag" = ''D'' THEN ''D''
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN ''R''
							ELSE ''R''
						END,
					CVM."ResolveDate" = 
						CASE 
							WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
							ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
						END,
					CVM."ResolvedBy" = 
						CASE 
							WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
							WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
							WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
							ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
						END
				WHERE CVM.CONFLICTID IN (
				  SELECT CF.CONFLICTID
				  FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF
				  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
				  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID AND CVM1."StatusFlag" IN(''R'', ''D'') AND DATE(CVM1."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
				  WHERE CF."StatusFlag" IN(''R'', ''D'')
				  GROUP BY CF.CONFLICTID
				  HAVING COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID)
				) AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
			
			var sql_queryseconds8 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
				SET CVM."StatusFlag" = 
						CASE 
							WHEN CVM."StatusFlag" = ''D'' THEN ''D''
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN ''R''
							ELSE ''R''
						END,
					CVM."ResolveDate" = 
						CASE 
							WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
							ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
						END,
					CVM."ResolvedBy" = 
						CASE 
							WHEN CVM."StatusFlag" = ''D'' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
							WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
							WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
							ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
						END
				WHERE CVM.CONFLICTID IN (
				  SELECT CF.CONFLICTID
				  FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF
				  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
				  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID AND CVM1."StatusFlag" IN(''R'', ''D'') AND DATE(CVM1."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
				  WHERE CF."StatusFlag" IN(''R'', ''D'')
				  GROUP BY CF.CONFLICTID
				  HAVING COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID) OR (COUNT(DISTINCT CVM.ID)-1) = COUNT(DISTINCT CVM1.ID)
				) AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
			
			var sql_queryseconds9 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
				SET CF."StatusFlag" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN ''D''
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN ''R''
							ELSE ''R''
						END,
					CF."ResolveDate" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
							WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
							ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
						END,
					CF."ResolvedBy" = 
						CASE 
							WHEN CF."StatusFlag" = ''D'' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
							WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
							WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
							ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
						END
				FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
				WHERE CVM.CONFLICTID = CF.CONFLICTID AND CF.CONFLICTID IN (
				  SELECT CF.CONFLICTID
				  FROM CONFLICTREPORT.PUBLIC.CONFLICTS CF
				  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM ON CVM.CONFLICTID = CF.CONFLICTID AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
				  LEFT JOIN CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID AND CVM1."StatusFlag" IN(''R'', ''D'') AND DATE(CVM1."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
				  WHERE CF."StatusFlag" NOT IN(''R'', ''D'')
				  GROUP BY CF.CONFLICTID
				  HAVING COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID) AND COUNT(DISTINCT CVM.ID) > 0 AND COUNT(DISTINCT CVM1.ID) > 0
				) AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;			
			
			var sql_queryseconds10 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTS CF
				SET CF."StatusFlag" = 
						CASE 
							WHEN CF."NoResponseFlag" = ''Yes'' THEN ''N''
							ELSE CF."StatusFlag"
						END,
					CF."ResolveDate" = NULL,
					CF."ResolvedBy" = NULL
				FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM WHERE CVM.CONFLICTID = CF.CONFLICTID AND CF."StatusFlag" IN (''U'', ''N'', ''W'', ''I'') AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
			
			var sql_queryseconds11 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS CVM
				SET CVM."StatusFlag" = 
						CASE 
							WHEN CVM."ConNoResponseFlag" = ''Yes'' THEN ''N''
							ELSE CVM."StatusFlag"
						END,
					CVM."ResolveDate" = NULL,
					CVM."ResolvedBy" = NULL
				WHERE CVM."StatusFlag" IN (''U'', ''N'', ''W'', ''I'') AND DATE(CVM."VisitDate") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))`;
			
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


			var update_records_p = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.SSN = ALLDATA.SSN,
        CVM."ProviderID" = ALLDATA."ProviderID",
        CVM."AppProviderID" = ALLDATA."AppProviderID",
        CVM."ProviderName" = ALLDATA."ProviderName",
        CVM."VisitDate" = ALLDATA."VisitDate",
        CVM."SchStartTime" = ALLDATA."SchStartTime",
        CVM."SchEndTime" = ALLDATA."SchEndTime",
        CVM."VisitStartTime" = ALLDATA."VisitStartTime",
        CVM."VisitEndTime" = ALLDATA."VisitEndTime",
        CVM."EVVStartTime" = ALLDATA."EVVStartTime",
        CVM."EVVEndTime" = ALLDATA."EVVEndTime",
        CVM."CaregiverID" = ALLDATA."CaregiverID",
        CVM."AppCaregiverID" = ALLDATA."AppCaregiverID",
        CVM."AideCode" = ALLDATA."AideCode",
        CVM."AideName" = ALLDATA."AideName",
        CVM."AideSSN" = ALLDATA."AideSSN",
        CVM."OfficeID" = ALLDATA."OfficeID",
        CVM."AppOfficeID" = ALLDATA."AppOfficeID",
        CVM."Office" = ALLDATA."Office",
        CVM."PatientID" = ALLDATA."PatientID",
        CVM."AppPatientID" = ALLDATA."AppPatientID",
        CVM."PAdmissionID" = ALLDATA."PAdmissionID",
        CVM."PName" = ALLDATA."PName",
        CVM."PAddressID" = ALLDATA."PAddressID",
        CVM."PAppAddressID" = ALLDATA."PAppAddressID",
        CVM."PAddressL1" = ALLDATA."PAddressL1",
        CVM."PAddressL2" = ALLDATA."PAddressL2",
        CVM."PCity" = ALLDATA."PCity",
        CVM."PAddressState" = ALLDATA."PAddressState",
        CVM."PZipCode" = ALLDATA."PZipCode",
        CVM."PCounty" = ALLDATA."PCounty",
        CVM."PLongitude" = ALLDATA."PLongitude",
        CVM."PLatitude" = ALLDATA."PLatitude",
        CVM."PayerID" = ALLDATA."PayerID",
        CVM."AppPayerID" = ALLDATA."AppPayerID",
        CVM."BilledDate" = ALLDATA."BilledDate",
        CVM."BilledHours" = ALLDATA."BilledHours",
        CVM."Billed" = ALLDATA."Billed",
        CVM."ServiceCodeID" = ALLDATA."ServiceCodeID",
        CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID",
        CVM."RateType" = ALLDATA."RateType",
        CVM."ServiceCode" = ALLDATA."ServiceCode",
        CVM."AideFName" = ALLDATA."AideFName",
        CVM."AideLName" = ALLDATA."AideLName",
        CVM."PFName" = ALLDATA."PFName",
        CVM."PLName" = ALLDATA."PLName",
        CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber",
        CVM."PayerState" = ALLDATA."PayerState",
        CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy",
        CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate",
        CVM."BilledRate" = ALLDATA."BilledRate",
        CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount",
        CVM."IsMissed" = ALLDATA."IsMissed",
        CVM."MissedVisitReason" = ALLDATA."MissedVisitReason",
        CVM."EVVType" = ALLDATA."EVVType",
        CVM."PStatus" = ALLDATA."PStatus",
        CVM."AideStatus" = ALLDATA."AideStatus",
        CVM."P_PatientID" = ALLDATA."P_PatientID",
        CVM."P_AppPatientID" = ALLDATA."P_AppPatientID",
        CVM."PA_PatientID" = ALLDATA."PA_PatientID",
        CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID",
        CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID",
        CVM."P_PName" = ALLDATA."P_PName",
        CVM."P_PAddressID" = ALLDATA."P_PAddressID",
        CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID",
        CVM."P_PAddressL1" = ALLDATA."P_PAddressL1",
        CVM."P_PAddressL2" = ALLDATA."P_PAddressL2",
        CVM."P_PCity" = ALLDATA."P_PCity",
        CVM."P_PAddressState" = ALLDATA."P_PAddressState",
        CVM."P_PZipCode" = ALLDATA."P_PZipCode",
        CVM."P_PCounty" = ALLDATA."P_PCounty",
        CVM."P_PFName" = ALLDATA."P_PFName",
        CVM."P_PLName" = ALLDATA."P_PLName",
        CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber",
        CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID",
        CVM."PA_PName" = ALLDATA."PA_PName",
        CVM."PA_PAddressID" = ALLDATA."PA_PAddressID",
        CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID",
        CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1",
        CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2",
        CVM."PA_PCity" = ALLDATA."PA_PCity",
        CVM."PA_PAddressState" = ALLDATA."PA_PAddressState",
        CVM."PA_PZipCode" = ALLDATA."PA_PZipCode",
        CVM."PA_PCounty" = ALLDATA."PA_PCounty",
        CVM."PA_PFName" = ALLDATA."PA_PFName",
        CVM."PA_PLName" = ALLDATA."PA_PLName",
        CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber",
        CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled",
        CVM."BillRateBoth" = ALLDATA."BillRateBoth",
        CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber"
  	FROM (
  	        SELECT
            V1."SSN",
            V1."ProviderID" AS "ProviderID",    
            V1."AppProviderID" AS "AppProviderID",  
            V1."ProviderName" AS "ProviderName",
            V1."VisitID" AS "VisitID",
            V1."AppVisitID" AS "AppVisitID",
            V1."VisitDate" AS "VisitDate",
            V1."SchStartTime" AS "SchStartTime",
            V1."SchEndTime" AS "SchEndTime",            
            V1."VisitStartTime" AS "VisitStartTime",
            V1."VisitEndTime" AS "VisitEndTime",
            V1."EVVStartTime" AS "EVVStartTime",
            V1."EVVEndTime" AS "EVVEndTime",
            V1."CaregiverID" AS "CaregiverID",
            V1."AppCaregiverID" AS "AppCaregiverID",
            V1."AideCode" AS "AideCode",
            V1."AideName" AS "AideName",
            V1."AideSSN" AS "AideSSN",
            V1."OfficeID" AS "OfficeID",
            V1."AppOfficeID" AS "AppOfficeID",
            V1."Office" AS "Office",            
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
            V1."PayerID" AS "PayerID",
            V1."AppPayerID" AS "AppPayerID",
            V1."Contract" AS "Contract",
            V1."BilledDate" AS "BilledDate",
            V1."BilledHours" AS "BilledHours",
            V1."Billed" AS "Billed",
			V1."ServiceCodeID" AS "ServiceCodeID",
	        V1."AppServiceCodeID" AS "AppServiceCodeID",
	        V1."RateType" AS "RateType",
	        V1."ServiceCode" AS "ServiceCode",
            V1."AideFName" AS "AideFName",
            V1."AideLName" AS "AideLName",
            V1."PFName" AS "PFName",
            V1."PLName" AS "PLName",
			V1."PMedicaidNumber" AS "PMedicaidNumber",
			V1."PayerState" AS "PayerState",
			V1."AgencyContact" AS "AgencyContact",
			V1."AgencyPhone" AS "AgencyPhone",
			V1."LastUpdatedBy" AS "LastUpdatedBy",
			V1."LastUpdatedDate" AS "LastUpdatedDate",
			V1."BilledRate" AS "BilledRate",
			V1."TotalBilledAmount" AS "TotalBilledAmount",
			V1."IsMissed" AS "IsMissed",
			V1."MissedVisitReason" AS "MissedVisitReason",
			V1."EVVType" AS "EVVType",
			V1."PStatus" AS "PStatus",
			V1."AideStatus" AS "AideStatus",
			V1."P_PatientID" AS "P_PatientID",
			V1."P_AppPatientID" AS "P_AppPatientID",
			V1."PA_PatientID" AS "PA_PatientID",
			V1."PA_AppPatientID" AS "PA_AppPatientID",
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
			V1."ContractType" AS "ContractType",
			V1."P_PStatus" AS "P_PStatus",
			V1."PA_PStatus" AS "PA_PStatus",
			V1."BillRateNonBilled" AS "BillRateNonBilled",
			V1."BillRateBoth" AS "BillRateBoth",
			V1."FederalTaxNumber" AS "FederalTaxNumber"
       FROM
       (SELECT DISTINCT CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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
		LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
		LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
		INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM."VisitID" = CR1."Visit Id"
		INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTS AS C ON C.CONFLICTID = CVM.CONFLICTID AND C."StatusFlag" = ''R''
		WHERE DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))		
		) AS V1
  	) AS ALLDATA WHERE CVM."VisitID" = ALLDATA."VisitID"`;



var update_deleted_records_p = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.SSN = ALLDATA.SSN,
        CVM."ProviderID" = ALLDATA."ProviderID",
        CVM."AppProviderID" = ALLDATA."AppProviderID",
        CVM."ProviderName" = ALLDATA."ProviderName",
        CVM."VisitDate" = ALLDATA."VisitDate",
        CVM."SchStartTime" = ALLDATA."SchStartTime",
        CVM."SchEndTime" = ALLDATA."SchEndTime",
        CVM."VisitStartTime" = ALLDATA."VisitStartTime",
        CVM."VisitEndTime" = ALLDATA."VisitEndTime",
        CVM."EVVStartTime" = ALLDATA."EVVStartTime",
        CVM."EVVEndTime" = ALLDATA."EVVEndTime",
        CVM."CaregiverID" = ALLDATA."CaregiverID",
        CVM."AppCaregiverID" = ALLDATA."AppCaregiverID",
        CVM."AideCode" = ALLDATA."AideCode",
        CVM."AideName" = ALLDATA."AideName",
        CVM."AideSSN" = ALLDATA."AideSSN",
        CVM."OfficeID" = ALLDATA."OfficeID",
        CVM."AppOfficeID" = ALLDATA."AppOfficeID",
        CVM."Office" = ALLDATA."Office",
        CVM."PatientID" = ALLDATA."PatientID",
        CVM."AppPatientID" = ALLDATA."AppPatientID",
        CVM."PAdmissionID" = ALLDATA."PAdmissionID",
        CVM."PName" = ALLDATA."PName",
        CVM."PAddressID" = ALLDATA."PAddressID",
        CVM."PAppAddressID" = ALLDATA."PAppAddressID",
        CVM."PAddressL1" = ALLDATA."PAddressL1",
        CVM."PAddressL2" = ALLDATA."PAddressL2",
        CVM."PCity" = ALLDATA."PCity",
        CVM."PAddressState" = ALLDATA."PAddressState",
        CVM."PZipCode" = ALLDATA."PZipCode",
        CVM."PCounty" = ALLDATA."PCounty",
        CVM."PLongitude" = ALLDATA."PLongitude",
        CVM."PLatitude" = ALLDATA."PLatitude",
        CVM."PayerID" = ALLDATA."PayerID",
        CVM."AppPayerID" = ALLDATA."AppPayerID",
        CVM."BilledDate" = ALLDATA."BilledDate",
        CVM."BilledHours" = ALLDATA."BilledHours",
        CVM."Billed" = ALLDATA."Billed",
        CVM."ServiceCodeID" = ALLDATA."ServiceCodeID",
        CVM."AppServiceCodeID" = ALLDATA."AppServiceCodeID",
        CVM."RateType" = ALLDATA."RateType",
        CVM."ServiceCode" = ALLDATA."ServiceCode",
        CVM."AideFName" = ALLDATA."AideFName",
        CVM."AideLName" = ALLDATA."AideLName",
        CVM."PFName" = ALLDATA."PFName",
        CVM."PLName" = ALLDATA."PLName",
        CVM."PMedicaidNumber" = ALLDATA."PMedicaidNumber",
        CVM."PayerState" = ALLDATA."PayerState",
        CVM."LastUpdatedBy" = ALLDATA."LastUpdatedBy",
        CVM."LastUpdatedDate" = ALLDATA."LastUpdatedDate",
        CVM."BilledRate" = ALLDATA."BilledRate",
        CVM."TotalBilledAmount" = ALLDATA."TotalBilledAmount",
        CVM."IsMissed" = ALLDATA."IsMissed",
        CVM."MissedVisitReason" = ALLDATA."MissedVisitReason",
        CVM."EVVType" = ALLDATA."EVVType",
        CVM."PStatus" = ALLDATA."PStatus",
        CVM."AideStatus" = ALLDATA."AideStatus",
        CVM."P_PatientID" = ALLDATA."P_PatientID",
        CVM."P_AppPatientID" = ALLDATA."P_AppPatientID",
        CVM."PA_PatientID" = ALLDATA."PA_PatientID",
        CVM."PA_AppPatientID" = ALLDATA."PA_AppPatientID",
        CVM."P_PAdmissionID" = ALLDATA."P_PAdmissionID",
        CVM."P_PName" = ALLDATA."P_PName",
        CVM."P_PAddressID" = ALLDATA."P_PAddressID",
        CVM."P_PAppAddressID" = ALLDATA."P_PAppAddressID",
        CVM."P_PAddressL1" = ALLDATA."P_PAddressL1",
        CVM."P_PAddressL2" = ALLDATA."P_PAddressL2",
        CVM."P_PCity" = ALLDATA."P_PCity",
        CVM."P_PAddressState" = ALLDATA."P_PAddressState",
        CVM."P_PZipCode" = ALLDATA."P_PZipCode",
        CVM."P_PCounty" = ALLDATA."P_PCounty",
        CVM."P_PFName" = ALLDATA."P_PFName",
        CVM."P_PLName" = ALLDATA."P_PLName",
        CVM."P_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber",
        CVM."PA_PAdmissionID" = ALLDATA."PA_PAdmissionID",
        CVM."PA_PName" = ALLDATA."PA_PName",
        CVM."PA_PAddressID" = ALLDATA."PA_PAddressID",
        CVM."PA_PAppAddressID" = ALLDATA."PA_PAppAddressID",
        CVM."PA_PAddressL1" = ALLDATA."PA_PAddressL1",
        CVM."PA_PAddressL2" = ALLDATA."PA_PAddressL2",
        CVM."PA_PCity" = ALLDATA."PA_PCity",
        CVM."PA_PAddressState" = ALLDATA."PA_PAddressState",
        CVM."PA_PZipCode" = ALLDATA."PA_PZipCode",
        CVM."PA_PCounty" = ALLDATA."PA_PCounty",
        CVM."PA_PFName" = ALLDATA."PA_PFName",
        CVM."PA_PLName" = ALLDATA."PA_PLName",
        CVM."PA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber",
        CVM."BillRateNonBilled" = ALLDATA."BillRateNonBilled",
        CVM."BillRateBoth" = ALLDATA."BillRateBoth",
        CVM."FederalTaxNumber" = ALLDATA."FederalTaxNumber"
  	FROM (
  	        SELECT
            V1."SSN",
            V1."ProviderID" AS "ProviderID",    
            V1."AppProviderID" AS "AppProviderID",  
            V1."ProviderName" AS "ProviderName",
            V1."VisitID" AS "VisitID",
            V1."AppVisitID" AS "AppVisitID",
            V1."VisitDate" AS "VisitDate",
            V1."SchStartTime" AS "SchStartTime",
            V1."SchEndTime" AS "SchEndTime",            
            V1."VisitStartTime" AS "VisitStartTime",
            V1."VisitEndTime" AS "VisitEndTime",
            V1."EVVStartTime" AS "EVVStartTime",
            V1."EVVEndTime" AS "EVVEndTime",
            V1."CaregiverID" AS "CaregiverID",
            V1."AppCaregiverID" AS "AppCaregiverID",
            V1."AideCode" AS "AideCode",
            V1."AideName" AS "AideName",
            V1."AideSSN" AS "AideSSN",
            V1."OfficeID" AS "OfficeID",
            V1."AppOfficeID" AS "AppOfficeID",
            V1."Office" AS "Office",            
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
            V1."PayerID" AS "PayerID",
            V1."AppPayerID" AS "AppPayerID",
            V1."Contract" AS "Contract",
            V1."BilledDate" AS "BilledDate",
            V1."BilledHours" AS "BilledHours",
            V1."Billed" AS "Billed",
			V1."ServiceCodeID" AS "ServiceCodeID",
	        V1."AppServiceCodeID" AS "AppServiceCodeID",
	        V1."RateType" AS "RateType",
	        V1."ServiceCode" AS "ServiceCode",
            V1."AideFName" AS "AideFName",
            V1."AideLName" AS "AideLName",
            V1."PFName" AS "PFName",
            V1."PLName" AS "PLName",
			V1."PMedicaidNumber" AS "PMedicaidNumber",
			V1."PayerState" AS "PayerState",
			V1."AgencyContact" AS "AgencyContact",
			V1."AgencyPhone" AS "AgencyPhone",
			V1."LastUpdatedBy" AS "LastUpdatedBy",
			V1."LastUpdatedDate" AS "LastUpdatedDate",
			V1."BilledRate" AS "BilledRate",
			V1."TotalBilledAmount" AS "TotalBilledAmount",
			V1."IsMissed" AS "IsMissed",
			V1."MissedVisitReason" AS "MissedVisitReason",
			V1."EVVType" AS "EVVType",
			V1."PStatus" AS "PStatus",
			V1."AideStatus" AS "AideStatus",
			V1."P_PatientID" AS "P_PatientID",
			V1."P_AppPatientID" AS "P_AppPatientID",
			V1."PA_PatientID" AS "PA_PatientID",
			V1."PA_AppPatientID" AS "PA_AppPatientID",
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
			V1."ContractType" AS "ContractType",
			V1."P_PStatus" AS "P_PStatus",
			V1."PA_PStatus" AS "PA_PStatus",
			V1."BillRateNonBilled" AS "BillRateNonBilled",
			V1."BillRateBoth" AS "BillRateBoth",
			V1."FederalTaxNumber" AS "FederalTaxNumber"
       FROM
       (SELECT DISTINCT CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM 
       	ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR AS CR1
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
		LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
		LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
		INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM."VisitID" = CR1."Visit Id"
		INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTS AS C ON C.CONFLICTID = CVM.CONFLICTID AND C."StatusFlag" = ''D''
		WHERE DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))		
		) AS V1
  	) AS ALLDATA WHERE CVM."VisitID" = ALLDATA."VisitID"`;

var update_records_c = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.SSN = ALLDATA.SSN,
        CVM."ConProviderID" = ALLDATA."ProviderID",
        CVM."ConAppProviderID" = ALLDATA."AppProviderID",
        CVM."ConProviderName" = ALLDATA."ProviderName",
        CVM."VisitDate" = ALLDATA."VisitDate",
        CVM."ConSchStartTime" = ALLDATA."SchStartTime",
        CVM."ConSchEndTime" = ALLDATA."SchEndTime",
        CVM."ConVisitStartTime" = ALLDATA."VisitStartTime",
        CVM."ConVisitEndTime" = ALLDATA."VisitEndTime",
        CVM."ConEVVStartTime" = ALLDATA."EVVStartTime",
        CVM."ConEVVEndTime" = ALLDATA."EVVEndTime",
        CVM."ConCaregiverID" = ALLDATA."CaregiverID",
        CVM."ConAppCaregiverID" = ALLDATA."AppCaregiverID",
        CVM."ConAideCode" = ALLDATA."AideCode",
        CVM."ConAideName" = ALLDATA."AideName",
        CVM."ConAideSSN" = ALLDATA."AideSSN",
        CVM."ConOfficeID" = ALLDATA."OfficeID",
        CVM."ConAppOfficeID" = ALLDATA."AppOfficeID",
        CVM."ConOffice" = ALLDATA."Office",
        CVM."ConPatientID" = ALLDATA."PatientID",
        CVM."ConAppPatientID" = ALLDATA."AppPatientID",
        CVM."ConPAdmissionID" = ALLDATA."PAdmissionID",
        CVM."ConPName" = ALLDATA."PName",
        CVM."ConPAddressID" = ALLDATA."PAddressID",
        CVM."ConPAppAddressID" = ALLDATA."PAppAddressID",
        CVM."ConPAddressL1" = ALLDATA."PAddressL1",
        CVM."ConPAddressL2" = ALLDATA."PAddressL2",
        CVM."ConPCity" = ALLDATA."PCity",
        CVM."ConPAddressState" = ALLDATA."PAddressState",
        CVM."ConPZipCode" = ALLDATA."PZipCode",
        CVM."ConPCounty" = ALLDATA."PCounty",
        CVM."ConPLongitude" = ALLDATA."PLongitude",
        CVM."ConPLatitude" = ALLDATA."PLatitude",
        CVM."ConPayerID" = ALLDATA."PayerID",
        CVM."ConAppPayerID" = ALLDATA."AppPayerID",
        CVM."ConBilledDate" = ALLDATA."BilledDate",
        CVM."ConBilledHours" = ALLDATA."BilledHours",
        CVM."ConBilled" = ALLDATA."Billed",
        CVM."ConServiceCodeID" = ALLDATA."ServiceCodeID",
        CVM."ConAppServiceCodeID" = ALLDATA."AppServiceCodeID",
        CVM."ConRateType" = ALLDATA."RateType",
        CVM."ConServiceCode" = ALLDATA."ServiceCode",
        CVM."ConAideFName" = ALLDATA."AideFName",
        CVM."ConAideLName" = ALLDATA."AideLName",
        CVM."ConPFName" = ALLDATA."PFName",
        CVM."ConPLName" = ALLDATA."PLName",
        CVM."ConPMedicaidNumber" = ALLDATA."PMedicaidNumber",
        CVM."ConPayerState" = ALLDATA."PayerState",
        CVM."ConLastUpdatedBy" = ALLDATA."LastUpdatedBy",
        CVM."ConLastUpdatedDate" = ALLDATA."LastUpdatedDate",
        CVM."ConBilledRate" = ALLDATA."BilledRate",
        CVM."ConTotalBilledAmount" = ALLDATA."TotalBilledAmount",
        CVM."ConIsMissed" = ALLDATA."IsMissed",
        CVM."ConMissedVisitReason" = ALLDATA."MissedVisitReason",
        CVM."ConEVVType" = ALLDATA."EVVType",
        CVM."ConPStatus" = ALLDATA."PStatus",
        CVM."ConAideStatus" = ALLDATA."AideStatus",
        CVM."ConP_PatientID" = ALLDATA."P_PatientID",
        CVM."ConP_AppPatientID" = ALLDATA."P_AppPatientID",
        CVM."ConPA_PatientID" = ALLDATA."PA_PatientID",
        CVM."ConPA_AppPatientID" = ALLDATA."PA_AppPatientID",
        CVM."ConP_PAdmissionID" = ALLDATA."P_PAdmissionID",
        CVM."ConP_PName" = ALLDATA."P_PName",
        CVM."ConP_PAddressID" = ALLDATA."P_PAddressID",
        CVM."ConP_PAppAddressID" = ALLDATA."P_PAppAddressID",
        CVM."ConP_PAddressL1" = ALLDATA."P_PAddressL1",
        CVM."ConP_PAddressL2" = ALLDATA."P_PAddressL2",
        CVM."ConP_PCity" = ALLDATA."P_PCity",
        CVM."ConP_PAddressState" = ALLDATA."P_PAddressState",
        CVM."ConP_PZipCode" = ALLDATA."P_PZipCode",
        CVM."ConP_PCounty" = ALLDATA."P_PCounty",
        CVM."ConP_PFName" = ALLDATA."P_PFName",
        CVM."ConP_PLName" = ALLDATA."P_PLName",
        CVM."ConP_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber",
        CVM."ConPA_PAdmissionID" = ALLDATA."PA_PAdmissionID",
        CVM."ConPA_PName" = ALLDATA."PA_PName",
        CVM."ConPA_PAddressID" = ALLDATA."PA_PAddressID",
        CVM."ConPA_PAppAddressID" = ALLDATA."PA_PAppAddressID",
        CVM."ConPA_PAddressL1" = ALLDATA."PA_PAddressL1",
        CVM."ConPA_PAddressL2" = ALLDATA."PA_PAddressL2",
        CVM."ConPA_PCity" = ALLDATA."PA_PCity",
        CVM."ConPA_PAddressState" = ALLDATA."PA_PAddressState",
        CVM."ConPA_PZipCode" = ALLDATA."PA_PZipCode",
        CVM."ConPA_PCounty" = ALLDATA."PA_PCounty",
        CVM."ConPA_PFName" = ALLDATA."PA_PFName",
        CVM."ConPA_PLName" = ALLDATA."PA_PLName",
        CVM."ConPA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber",
        CVM."ConBillRateNonBilled" = ALLDATA."BillRateNonBilled",
        CVM."ConBillRateBoth" = ALLDATA."BillRateBoth",
        CVM."ConFederalTaxNumber" = ALLDATA."FederalTaxNumber"
  	FROM (
  	        SELECT
            V1."SSN",
            V1."ProviderID" AS "ProviderID",    
            V1."AppProviderID" AS "AppProviderID",  
            V1."ProviderName" AS "ProviderName",
            V1."VisitID" AS "VisitID",
            V1."AppVisitID" AS "AppVisitID",
            V1."VisitDate" AS "VisitDate",
            V1."SchStartTime" AS "SchStartTime",
            V1."SchEndTime" AS "SchEndTime",            
            V1."VisitStartTime" AS "VisitStartTime",
            V1."VisitEndTime" AS "VisitEndTime",
            V1."EVVStartTime" AS "EVVStartTime",
            V1."EVVEndTime" AS "EVVEndTime",
            V1."CaregiverID" AS "CaregiverID",
            V1."AppCaregiverID" AS "AppCaregiverID",
            V1."AideCode" AS "AideCode",
            V1."AideName" AS "AideName",
            V1."AideSSN" AS "AideSSN",
            V1."OfficeID" AS "OfficeID",
            V1."AppOfficeID" AS "AppOfficeID",
            V1."Office" AS "Office",            
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
            V1."PayerID" AS "PayerID",
            V1."AppPayerID" AS "AppPayerID",
            V1."Contract" AS "Contract",
            V1."BilledDate" AS "BilledDate",
            V1."BilledHours" AS "BilledHours",
            V1."Billed" AS "Billed",
			V1."ServiceCodeID" AS "ServiceCodeID",
	        V1."AppServiceCodeID" AS "AppServiceCodeID",
	        V1."RateType" AS "RateType",
	        V1."ServiceCode" AS "ServiceCode",
            V1."AideFName" AS "AideFName",
            V1."AideLName" AS "AideLName",
            V1."PFName" AS "PFName",
            V1."PLName" AS "PLName",
			V1."PMedicaidNumber" AS "PMedicaidNumber",
			V1."PayerState" AS "PayerState",
			V1."AgencyContact" AS "AgencyContact",
			V1."AgencyPhone" AS "AgencyPhone",
			V1."LastUpdatedBy" AS "LastUpdatedBy",
			V1."LastUpdatedDate" AS "LastUpdatedDate",
			V1."BilledRate" AS "BilledRate",
			V1."TotalBilledAmount" AS "TotalBilledAmount",
			V1."IsMissed" AS "IsMissed",
			V1."MissedVisitReason" AS "MissedVisitReason",
			V1."EVVType" AS "EVVType",
			V1."PStatus" AS "PStatus",
			V1."AideStatus" AS "AideStatus",
			V1."P_PatientID" AS "P_PatientID",
			V1."P_AppPatientID" AS "P_AppPatientID",
			V1."PA_PatientID" AS "PA_PatientID",
			V1."PA_AppPatientID" AS "PA_AppPatientID",
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
			V1."ContractType" AS "ContractType",
			V1."P_PStatus" AS "P_PStatus",
			V1."PA_PStatus" AS "PA_PStatus",
			V1."BillRateNonBilled" AS "BillRateNonBilled",
			V1."BillRateBoth" AS "BillRateBoth",
			V1."FederalTaxNumber" AS "FederalTaxNumber"
       FROM
       (SELECT DISTINCT CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
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
		LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
		LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
		INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM."ConVisitID" = CR1."Visit Id" AND CVM."StatusFlag" = ''R''
		WHERE DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))		
		) AS V1
  	) AS ALLDATA WHERE CVM."ConVisitID" = ALLDATA."VisitID"`;


    var update_deleted_records_c = `UPDATE CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM
  	SET CVM.SSN = ALLDATA.SSN,
        CVM."ConProviderID" = ALLDATA."ProviderID",
        CVM."ConAppProviderID" = ALLDATA."AppProviderID",
        CVM."ConProviderName" = ALLDATA."ProviderName",
        CVM."VisitDate" = ALLDATA."VisitDate",
        CVM."ConSchStartTime" = ALLDATA."SchStartTime",
        CVM."ConSchEndTime" = ALLDATA."SchEndTime",
        CVM."ConVisitStartTime" = ALLDATA."VisitStartTime",
        CVM."ConVisitEndTime" = ALLDATA."VisitEndTime",
        CVM."ConEVVStartTime" = ALLDATA."EVVStartTime",
        CVM."ConEVVEndTime" = ALLDATA."EVVEndTime",
        CVM."ConCaregiverID" = ALLDATA."CaregiverID",
        CVM."ConAppCaregiverID" = ALLDATA."AppCaregiverID",
        CVM."ConAideCode" = ALLDATA."AideCode",
        CVM."ConAideName" = ALLDATA."AideName",
        CVM."ConAideSSN" = ALLDATA."AideSSN",
        CVM."ConOfficeID" = ALLDATA."OfficeID",
        CVM."ConAppOfficeID" = ALLDATA."AppOfficeID",
        CVM."ConOffice" = ALLDATA."Office",
        CVM."ConPatientID" = ALLDATA."PatientID",
        CVM."ConAppPatientID" = ALLDATA."AppPatientID",
        CVM."ConPAdmissionID" = ALLDATA."PAdmissionID",
        CVM."ConPName" = ALLDATA."PName",
        CVM."ConPAddressID" = ALLDATA."PAddressID",
        CVM."ConPAppAddressID" = ALLDATA."PAppAddressID",
        CVM."ConPAddressL1" = ALLDATA."PAddressL1",
        CVM."ConPAddressL2" = ALLDATA."PAddressL2",
        CVM."ConPCity" = ALLDATA."PCity",
        CVM."ConPAddressState" = ALLDATA."PAddressState",
        CVM."ConPZipCode" = ALLDATA."PZipCode",
        CVM."ConPCounty" = ALLDATA."PCounty",
        CVM."ConPLongitude" = ALLDATA."PLongitude",
        CVM."ConPLatitude" = ALLDATA."PLatitude",
        CVM."ConPayerID" = ALLDATA."PayerID",
        CVM."ConAppPayerID" = ALLDATA."AppPayerID",
        CVM."ConBilledDate" = ALLDATA."BilledDate",
        CVM."ConBilledHours" = ALLDATA."BilledHours",
        CVM."ConBilled" = ALLDATA."Billed",
        CVM."ConServiceCodeID" = ALLDATA."ServiceCodeID",
        CVM."ConAppServiceCodeID" = ALLDATA."AppServiceCodeID",
        CVM."ConRateType" = ALLDATA."RateType",
        CVM."ConServiceCode" = ALLDATA."ServiceCode",
        CVM."ConAideFName" = ALLDATA."AideFName",
        CVM."ConAideLName" = ALLDATA."AideLName",
        CVM."ConPFName" = ALLDATA."PFName",
        CVM."ConPLName" = ALLDATA."PLName",
        CVM."ConPMedicaidNumber" = ALLDATA."PMedicaidNumber",
        CVM."ConPayerState" = ALLDATA."PayerState",
        CVM."ConLastUpdatedBy" = ALLDATA."LastUpdatedBy",
        CVM."ConLastUpdatedDate" = ALLDATA."LastUpdatedDate",
        CVM."ConBilledRate" = ALLDATA."BilledRate",
        CVM."ConTotalBilledAmount" = ALLDATA."TotalBilledAmount",
        CVM."ConIsMissed" = ALLDATA."IsMissed",
        CVM."ConMissedVisitReason" = ALLDATA."MissedVisitReason",
        CVM."ConEVVType" = ALLDATA."EVVType",
        CVM."ConPStatus" = ALLDATA."PStatus",
        CVM."ConAideStatus" = ALLDATA."AideStatus",
        CVM."ConP_PatientID" = ALLDATA."P_PatientID",
        CVM."ConP_AppPatientID" = ALLDATA."P_AppPatientID",
        CVM."ConPA_PatientID" = ALLDATA."PA_PatientID",
        CVM."ConPA_AppPatientID" = ALLDATA."PA_AppPatientID",
        CVM."ConP_PAdmissionID" = ALLDATA."P_PAdmissionID",
        CVM."ConP_PName" = ALLDATA."P_PName",
        CVM."ConP_PAddressID" = ALLDATA."P_PAddressID",
        CVM."ConP_PAppAddressID" = ALLDATA."P_PAppAddressID",
        CVM."ConP_PAddressL1" = ALLDATA."P_PAddressL1",
        CVM."ConP_PAddressL2" = ALLDATA."P_PAddressL2",
        CVM."ConP_PCity" = ALLDATA."P_PCity",
        CVM."ConP_PAddressState" = ALLDATA."P_PAddressState",
        CVM."ConP_PZipCode" = ALLDATA."P_PZipCode",
        CVM."ConP_PCounty" = ALLDATA."P_PCounty",
        CVM."ConP_PFName" = ALLDATA."P_PFName",
        CVM."ConP_PLName" = ALLDATA."P_PLName",
        CVM."ConP_PMedicaidNumber" = ALLDATA."P_PMedicaidNumber",
        CVM."ConPA_PAdmissionID" = ALLDATA."PA_PAdmissionID",
        CVM."ConPA_PName" = ALLDATA."PA_PName",
        CVM."ConPA_PAddressID" = ALLDATA."PA_PAddressID",
        CVM."ConPA_PAppAddressID" = ALLDATA."PA_PAppAddressID",
        CVM."ConPA_PAddressL1" = ALLDATA."PA_PAddressL1",
        CVM."ConPA_PAddressL2" = ALLDATA."PA_PAddressL2",
        CVM."ConPA_PCity" = ALLDATA."PA_PCity",
        CVM."ConPA_PAddressState" = ALLDATA."PA_PAddressState",
        CVM."ConPA_PZipCode" = ALLDATA."PA_PZipCode",
        CVM."ConPA_PCounty" = ALLDATA."PA_PCounty",
        CVM."ConPA_PFName" = ALLDATA."PA_PFName",
        CVM."ConPA_PLName" = ALLDATA."PA_PLName",
        CVM."ConPA_PMedicaidNumber" = ALLDATA."PA_PMedicaidNumber",
        CVM."ConBillRateNonBilled" = ALLDATA."BillRateNonBilled",
        CVM."ConBillRateBoth" = ALLDATA."BillRateBoth",
        CVM."ConFederalTaxNumber" = ALLDATA."FederalTaxNumber"
  	FROM (
  	        SELECT
            V1."SSN",
            V1."ProviderID" AS "ProviderID",    
            V1."AppProviderID" AS "AppProviderID",  
            V1."ProviderName" AS "ProviderName",
            V1."VisitID" AS "VisitID",
            V1."AppVisitID" AS "AppVisitID",
            V1."VisitDate" AS "VisitDate",
            V1."SchStartTime" AS "SchStartTime",
            V1."SchEndTime" AS "SchEndTime",            
            V1."VisitStartTime" AS "VisitStartTime",
            V1."VisitEndTime" AS "VisitEndTime",
            V1."EVVStartTime" AS "EVVStartTime",
            V1."EVVEndTime" AS "EVVEndTime",
            V1."CaregiverID" AS "CaregiverID",
            V1."AppCaregiverID" AS "AppCaregiverID",
            V1."AideCode" AS "AideCode",
            V1."AideName" AS "AideName",
            V1."AideSSN" AS "AideSSN",
            V1."OfficeID" AS "OfficeID",
            V1."AppOfficeID" AS "AppOfficeID",
            V1."Office" AS "Office",            
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
            V1."PayerID" AS "PayerID",
            V1."AppPayerID" AS "AppPayerID",
            V1."Contract" AS "Contract",
            V1."BilledDate" AS "BilledDate",
            V1."BilledHours" AS "BilledHours",
            V1."Billed" AS "Billed",
			V1."ServiceCodeID" AS "ServiceCodeID",
	        V1."AppServiceCodeID" AS "AppServiceCodeID",
	        V1."RateType" AS "RateType",
	        V1."ServiceCode" AS "ServiceCode",
            V1."AideFName" AS "AideFName",
            V1."AideLName" AS "AideLName",
            V1."PFName" AS "PFName",
            V1."PLName" AS "PLName",
			V1."PMedicaidNumber" AS "PMedicaidNumber",
			V1."PayerState" AS "PayerState",
			V1."AgencyContact" AS "AgencyContact",
			V1."AgencyPhone" AS "AgencyPhone",
			V1."LastUpdatedBy" AS "LastUpdatedBy",
			V1."LastUpdatedDate" AS "LastUpdatedDate",
			V1."BilledRate" AS "BilledRate",
			V1."TotalBilledAmount" AS "TotalBilledAmount",
			V1."IsMissed" AS "IsMissed",
			V1."MissedVisitReason" AS "MissedVisitReason",
			V1."EVVType" AS "EVVType",
			V1."PStatus" AS "PStatus",
			V1."AideStatus" AS "AideStatus",
			V1."P_PatientID" AS "P_PatientID",
			V1."P_AppPatientID" AS "P_AppPatientID",
			V1."PA_PatientID" AS "PA_PatientID",
			V1."PA_AppPatientID" AS "PA_AppPatientID",
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
			V1."ContractType" AS "ContractType",
			V1."P_PStatus" AS "P_PStatus",
			V1."PA_PStatus" AS "PA_PStatus",
			V1."BillRateNonBilled" AS "BillRateNonBilled",
			V1."BillRateBoth" AS "BillRateBoth",
			V1."FederalTaxNumber" AS "FederalTaxNumber"
       FROM
       (SELECT DISTINCT CR1."Bill Rate Non-Billed" AS "BillRateNonBilled", CASE WHEN CR1."Billed" = ''yes'' THEN CR1."Billed Rate" ELSE CR1."Bill Rate Non-Billed" END AS "BillRateBoth", TRIM(CAR."SSN") as "SSN", CAST(NULL AS STRING) "PStatus", CAR."Status" AS "AideStatus", CR1."Missed Visit Reason" AS "MissedVisitReason", CR1."Is Missed" AS "IsMissed", CR1."Call Out Device Type" AS "EVVType", CR1."Billed Rate" AS "BilledRate", CR1."Total Billed Amount" AS "TotalBilledAmount", CR1."Provider Id" as "ProviderID", CR1."Application Provider Id" as "AppProviderID", DPR."Provider Name" AS "ProviderName", CAST(NULL AS STRING) "AgencyContact", DPR."Phone Number 1" AS "AgencyPhone", DPR."Federal Tax Number" AS "FederalTaxNumber", CR1."Visit Id" as "VisitID", CR1."Application Visit Id" as "AppVisitID", DATE(CR1."Visit Date") AS "VisitDate", CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime", CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime", CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime", CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime", CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime", CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime", CR1."Caregiver Id" as "CaregiverID", CR1."Application Caregiver Id" as "AppCaregiverID", CAR."Caregiver Code" as "AideCode", CAR."Caregiver Fullname" as "AideName", CAR."Caregiver Firstname" as "AideFName", CAR."Caregiver Lastname" as "AideLName", TRIM(CAR."SSN") as "AideSSN", CR1."Office Id" as "OfficeID", CR1."Application Office Id" as "AppOfficeID", DOF."Office Name" as "Office", CR1."Payer Patient Id" as "PA_PatientID", CR1."Application Payer Patient Id" as "PA_AppPatientID", CR1."Provider Patient Id" as "P_PatientID", CR1."Application Provider Patient Id" as "P_AppPatientID", CR1."Patient Id" as "PatientID", CR1."Application Patient Id" as "AppPatientID", CAST(NULL AS STRING) "PAdmissionID", CAST(NULL AS STRING) "PName", CAST(NULL AS STRING) "PFName", CAST(NULL AS STRING) "PLName", CAST(NULL AS STRING) "PMedicaidNumber", CAST(NULL AS STRING) "PAddressID", CAST(NULL AS STRING) "PAppAddressID", CAST(NULL AS STRING) "PAddressL1", CAST(NULL AS STRING) "PAddressL2", CAST(NULL AS STRING) "PCity", CAST(NULL AS STRING) "PAddressState", CAST(NULL AS STRING) "PZipCode", CAST(NULL AS STRING) "PCounty", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[1], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Longitude" END AS "Longitude", CASE WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call Out GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != '','' THEN REPLACE(SPLIT(CR1."Call In GPS Coordinates", '','')[0], ''"'', CAST(NULL AS NUMBER)) ELSE DPAD_P."Provider_Latitude" END AS "Latitude", CR1."Payer Id" as "PayerID", CR1."Application Payer Id" as "AppPayerID", COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract", SPA."Payer State" AS "PayerState", CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate", CR1."Billed Hours" AS "BilledHours", CR1."Billed" AS "Billed", DSC."Service Code Id" AS "ServiceCodeID", DSC."Application Service Code Id" AS "AppServiceCodeID", CR1."Bill Type" as "RateType", DSC."Service Code" as "ServiceCode", CAST(CR1."Visit Updated Timestamp" AS timestamp) as "LastUpdatedDate", DUSR."User Fullname" AS "LastUpdatedBy", DPA_P."Admission Id" as "P_PAdmissionID", DPA_P."Patient Name" as "P_PName", DPA_P."Patient Firstname" as "P_PFName", DPA_P."Patient Lastname" as "P_PLName", DPA_P."Medicaid Number" as "P_PMedicaidNumber", DPA_P."Status" AS "P_PStatus", DPAD_P."Patient Address Id" as "P_PAddressID", DPAD_P."Application Patient Address Id" as "P_PAppAddressID", DPAD_P."Address Line 1" as "P_PAddressL1", DPAD_P."Address Line 2" as "P_PAddressL2", DPAD_P."City" as "P_PCity", DPAD_P."Address State" as "P_PAddressState", DPAD_P."Zip Code" as "P_PZipCode", DPAD_P."County" as "P_PCounty", DPA_PA."Admission Id" as "PA_PAdmissionID", DPA_PA."Patient Name" as "PA_PName", DPA_PA."Patient Firstname" as "PA_PFName", DPA_PA."Patient Lastname" as "PA_PLName", DPA_PA."Medicaid Number" as "PA_PMedicaidNumber", DPA_PA."Status" AS "PA_PStatus", DPAD_PA."Patient Address Id" as "PA_PAddressID", DPAD_PA."Application Patient Address Id" as "PA_PAppAddressID", DPAD_PA."Address Line 1" as "PA_PAddressL1", DPAD_PA."Address Line 2" as "PA_PAddressL2", DPAD_PA."City" as "PA_PCity", DPAD_PA."Address State" as "PA_PAddressState", DPAD_PA."Zip Code" as "PA_PZipCode", DPAD_PA."County" as "PA_PCounty", CASE WHEN (CR1."Application Payer Id" = ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''Internal'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" != ''0'') THEN ''UPR'' WHEN (CR1."Application Payer Id" != ''0'' AND CR1."Application Contract Id" = ''0'') THEN ''Payer'' END AS "ContractType" FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR AS CR1
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
		LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC ON DSC."Service Code Id" = CR1."Service Code Id"
		LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR ON DUSR."User Id"=CR1."Visit Updated User Id"
		INNER JOIN CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS AS CVM ON CVM."ConVisitID" = CR1."Visit Id" AND CVM."StatusFlag" = ''D''
		WHERE DATE(CR1."Visit Date") BETWEEN DATE(DATEADD(year, -2, GETDATE())) AND DATE(DATEADD(day, 45, GETDATE()))
		) AS V1
  	) AS ALLDATA WHERE CVM."ConVisitID" = ALLDATA."VisitID"`;

	//var updateflag = `UPDATE CONFLICTREPORT.PUBLIC.SETTINGS SET "UpdateCronFlag" = 1`;
	
		
  	snowflake.execute({ sqlText: sql_queryseconds });
  	snowflake.execute({ sqlText: sql_queryseconds1 });
  	snowflake.execute({ sqlText: sql_queryseconds2 });
  	snowflake.execute({ sqlText: sql_queryseconds3 });
  	snowflake.execute({ sqlText: sql_queryseconds4 });
  	snowflake.execute({ sqlText: sql_queryseconds4_AA });
  	snowflake.execute({ sqlText: sql_queryseconds4_A });
  	snowflake.execute({ sqlText: sql_queryseconds5 });
  	snowflake.execute({ sqlText: sql_queryseconds6 });
  	snowflake.execute({ sqlText: sql_queryseconds7 });
  	snowflake.execute({ sqlText: sql_queryseconds8 });
  	snowflake.execute({ sqlText: sql_queryseconds9 });
  	snowflake.execute({ sqlText: sql_queryseconds10 });
  	snowflake.execute({ sqlText: sql_queryseconds11 });
  	snowflake.execute({ sqlText: updatequery });
  	snowflake.execute({ sqlText: updatequerya });
  	snowflake.execute({ sqlText: update_records_p });
  	snowflake.execute({ sqlText: update_deleted_records_p });
  	snowflake.execute({ sqlText: update_records_c });
  	snowflake.execute({ sqlText: update_deleted_records_c });
	//snowflake.execute({ sqlText: updateflag });
  
    
    return "CONFLICTVISITMAPS table updated successfully.";
  } catch (err) {
		var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
    // If an error occurs, capture it and raise it with a custom message
  	throw "ERROR: " + err.message;  // Returns the error message to the caller
  }
';

CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.UPDATE_PHONE_CONTACT()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    // SQL Query 1: Update AgencyContact if NULL or empty
    var q1 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CV
              SET CV."AgencyContact" = CM."CONTACT_NAME", 
                  CV."AgencyPhone" = CM."PHONE"
              FROM CONFLICTREPORT.PUBLIC.CONTACT_MAINTENANCE AS CM
              WHERE CV."ProviderID" = CM."ProviderID" 
                AND CV."ProviderID" = CM."PID"
                AND (CV."AgencyContact" IS NULL OR CV."AgencyContact" = '''')`;

    // SQL Query 2: Update ConAgencyContact if NULL or empty
    var q2 = `UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CV
              SET CV."ConAgencyContact" = CM."CONTACT_NAME", 
                  CV."ConAgencyPhone" = CM."PHONE"
              FROM CONFLICTREPORT.PUBLIC.CONTACT_MAINTENANCE AS CM
              WHERE CV."ProviderID" = CM."ProviderID"
                AND CV."ConProviderID" = CM."PID"
                AND (CV."ConAgencyContact" IS NULL OR CV."ConAgencyContact" = '''')`;

    // Execute the first query
    var stmt1 = snowflake.createStatement({sqlText: q1});
    stmt1.execute();

    // Execute the second query
    var stmt2 = snowflake.createStatement({sqlText: q2});
    stmt2.execute();

    return ''Updates successful'';
} catch (err) {
	var updatesetting = `UPDATE CONFLICTREPORT."PUBLIC".SETTINGS SET "InProgressFlag" = 2`;
		snowflake.execute({ sqlText: updatesetting });
    // Capture error message and return it
    throw ''ERROR: '' + err.message;
}
';