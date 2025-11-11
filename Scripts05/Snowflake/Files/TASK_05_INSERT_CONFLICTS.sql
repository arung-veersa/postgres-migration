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