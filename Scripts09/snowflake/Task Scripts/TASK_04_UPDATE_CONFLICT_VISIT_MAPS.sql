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