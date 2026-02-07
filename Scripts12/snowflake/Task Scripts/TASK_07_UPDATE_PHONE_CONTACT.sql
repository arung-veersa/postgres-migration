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