-- ======================================================================
-- TASK 04 - STEP 3C: Resolve Single-Visit CONFLICTVISITMAPS
-- Original: sql_queryseconds5
-- 
-- For conflicts with StatusFlag R or D and only 1 CVM
-- Supports chunked processing via {start_date} and {end_date} placeholders
-- 
-- NOTE: Inner subquery uses FULL 2-year range for correct single-visit detection
--       Outer WHERE uses chunk range to limit which CVMs are updated per chunk
-- ======================================================================

UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "StatusFlag" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE 'R'
        END,
    "ResolveDate" = COALESCE(CVM."ResolveDate", NOW()),
    "ResolvedBy" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
        END
WHERE CVM."CONFLICTID" IN (
    -- Inner subquery: Uses FULL 2-year range to correctly count CVMs per conflict
    SELECT CF."CONFLICTID"
    FROM {conflict_schema}.conflicts AS CF
    INNER JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
        ON CVM2."CONFLICTID" = CF."CONFLICTID"
    WHERE CF."StatusFlag" IN ('R', 'D')
      AND CVM2."VisitDate" >= (NOW() - INTERVAL '2 years')
      AND CVM2."VisitDate" < (NOW() + INTERVAL '45 days')
    GROUP BY CF."CONFLICTID"
    HAVING COUNT(CVM2."ID") = 1
)
-- Outer filter: Uses chunk range to limit updates per execution
AND CVM."VisitDate" >= '{start_date}'::timestamp
AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day');
