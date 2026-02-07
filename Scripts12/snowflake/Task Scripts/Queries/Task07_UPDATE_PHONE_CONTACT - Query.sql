-- SQL Query 1: Update AgencyContact if NULL or empty
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CV 
SET 
  CV."AgencyContact" = CM."CONTACT_NAME", 
  CV."AgencyPhone" = CM."PHONE" 
FROM 
  CONFLICTREPORT.PUBLIC.CONTACT_MAINTENANCE AS CM 
WHERE 
  CV."ProviderID" = CM."ProviderID" 
  AND CV."ProviderID" = CM."PID" 
  AND (
    CV."AgencyContact" IS NULL 
    OR CV."AgencyContact" = ''
  );

-- SQL Query 2: Update ConAgencyContact if NULL or empty
UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS CV 
SET 
  CV."ConAgencyContact" = CM."CONTACT_NAME", 
  CV."ConAgencyPhone" = CM."PHONE" 
FROM 
  CONFLICTREPORT.PUBLIC.CONTACT_MAINTENANCE AS CM 
WHERE 
  CV."ProviderID" = CM."ProviderID" 
  AND CV."ConProviderID" = CM."PID" 
  AND (
    CV."ConAgencyContact" IS NULL 
    OR CV."ConAgencyContact" = ''
  );

-- error case
UPDATE 
  CONFLICTREPORT."PUBLIC".SETTINGS 
SET 
  "InProgressFlag" = 2;