-- ======================================================================
-- TASK 04 - STEP 3A: Set UpdatedRFlag on CONFLICTS
-- Original: sql_queryseconds4_AA
-- 
-- Supports chunked processing via {start_date} and {end_date} placeholders
-- ======================================================================

UPDATE {conflict_schema}.conflicts AS CF
SET "UpdatedRFlag" = '1'
FROM {conflict_schema}.conflictvisitmaps AS CVM
WHERE CVM."CONFLICTID" = CF."CONFLICTID"
  AND CVM."VisitDate" >= '{start_date}'::timestamp
  AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day');
