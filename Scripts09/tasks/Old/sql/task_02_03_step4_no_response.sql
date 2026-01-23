-- ======================================================================
-- TASK 04 - STEP 4: No-Response Handling
-- Converted from Snowflake UPDATE_DATA_CONFLICTVISITMAPS_3 procedure
-- 
-- This step handles cases where there was no response to the conflict.
-- Sets StatusFlag to 'N' (No Response) and clears resolution fields.
--
-- Queries:
-- 1. sql_queryseconds10 - Update CONFLICTS for NoResponseFlag = 'Yes'
-- 2. sql_queryseconds11 - Update CONFLICTVISITMAPS for ConNoResponseFlag = 'Yes'
-- ======================================================================

-- Query 1: Set CONFLICTS StatusFlag to 'N' when NoResponseFlag = 'Yes'
-- Clears ResolveDate and ResolvedBy since conflict is not actually resolved
UPDATE {conflict_schema}.conflicts AS CF
SET 
    "StatusFlag" = 
        CASE 
            WHEN CF."NoResponseFlag" = 'Yes' THEN 'N'
            ELSE CF."StatusFlag"
        END,
    "ResolveDate" = NULL,
    "ResolvedBy" = NULL
FROM {conflict_schema}.conflictvisitmaps AS CVM
WHERE CVM."CONFLICTID" = CF."CONFLICTID"
  AND CF."StatusFlag" IN ('U', 'N', 'W', 'I')
  AND CVM."VisitDate" >= '{start_date}'::timestamp
  AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day');


-- Query 2: Set CONFLICTVISITMAPS StatusFlag to 'N' when ConNoResponseFlag = 'Yes'
-- Clears ResolveDate and ResolvedBy since conflict is not actually resolved
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "StatusFlag" = 
        CASE 
            WHEN CVM."ConNoResponseFlag" = 'Yes' THEN 'N'
            ELSE CVM."StatusFlag"
        END,
    "ResolveDate" = NULL,
    "ResolvedBy" = NULL
WHERE CVM."StatusFlag" IN ('U', 'N', 'W', 'I')
  AND CVM."VisitDate" >= '{start_date}'::timestamp
  AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day');
