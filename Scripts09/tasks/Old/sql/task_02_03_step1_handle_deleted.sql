-- ======================================================================
-- TASK 04 - STEP 1: Handle Deleted Visits
-- Converted from Snowflake UPDATE_DATA_CONFLICTVISITMAPS_3 procedure
-- 
-- This step marks visits as deleted when they exist in the deleted visits table.
-- Also propagates delete status to the parent CONFLICTS table.
--
-- Queries:
-- 1. sql_queryseconds - Mark CONFLICTVISITMAPS as deleted (via ConVisitID)
-- 2. sql_queryseconds1 - Mark CONFLICTS as deleted (via VisitID)
-- ======================================================================

-- Query 1: Mark CONFLICTVISITMAPS as deleted when ConVisit is in deleted table
-- Sets StatusFlag = 'D', UpdateFlag = NULL, sets ResolveDate and ResolvedBy
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "UpdateFlag" = NULL,
    "StatusFlag" = 'D',
    "ResolveDate" = COALESCE(CVM."ResolveDate", NOW()),
    "ResolvedBy" = COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
FROM {analytics_schema}.factvisitcallperformance_deleted_cr AS DELETECR
WHERE CVM."ConVisitID" = DELETECR."Visit Id"
  AND CVM."StatusFlag" != 'D'
  AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                 AND (NOW() + INTERVAL '45 days')::date;


-- Query 2: Mark CONFLICTS as deleted when Visit is in deleted table
-- Propagates delete status from visit level to conflict level
UPDATE {conflict_schema}.conflicts AS CF
SET 
    "StatusFlag" = 'D',
    "ResolveDate" = COALESCE(CF."ResolveDate", NOW()),
    "ResolvedBy" = COALESCE(CVM."AgencyContact", CVM."ProviderName")
FROM {analytics_schema}.factvisitcallperformance_deleted_cr AS DELETECR
INNER JOIN {conflict_schema}.conflictvisitmaps AS CVM
    ON CVM."VisitID" = DELETECR."Visit Id"
WHERE CF."StatusFlag" != 'D'
  AND CF."CONFLICTID" = CVM."CONFLICTID"
  AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                 AND (NOW() + INTERVAL '45 days')::date;
