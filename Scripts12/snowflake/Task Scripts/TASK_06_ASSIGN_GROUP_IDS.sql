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