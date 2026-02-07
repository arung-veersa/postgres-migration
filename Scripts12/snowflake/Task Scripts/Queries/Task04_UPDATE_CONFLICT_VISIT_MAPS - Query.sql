UPDATE 
  CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS AS target 
SET 
  target."CONFLICTID" = source."CONFLICTID" 
FROM 
  (
    SELECT 
      "VisitID", 
      "AppVisitID", 
      ROW_NUMBER() OVER (
        ORDER BY 
          "VisitID", 
          "AppVisitID"
      ) + COALESCE(
        (
          SELECT 
            MAX(CONFLICTID) 
          FROM 
            CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
        ), 
        0
      ) AS "CONFLICTID" 
    FROM 
      CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS 
    WHERE 
      "CONFLICTID" IS NULL 
    GROUP BY 
      "VisitID", 
      "AppVisitID"
  ) AS source 
WHERE 
  target."VisitID" = source."VisitID" 
  AND target."AppVisitID" = source."AppVisitID";

UPDATE 
  CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS cv 
SET 
  CRDATEUNIQUE = (
    SELECT 
      MIN(v."CreatedDate") 
    FROM 
      CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS v 
    WHERE 
      v.CONFLICTID = cv.CONFLICTID
  ) 
WHERE 
  CRDATEUNIQUE IS NULL;

-- error case
UPDATE 
  CONFLICTREPORT."PUBLIC".SETTINGS 
SET 
  "InProgressFlag" = 2;
