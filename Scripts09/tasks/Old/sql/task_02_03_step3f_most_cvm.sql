-- ======================================================================
-- TASK 04 - STEP 3F: Resolve All-or-Most CONFLICTVISITMAPS
-- Original: sql_queryseconds8
-- 
-- For CVMs where ALL or (COUNT-1) CVMs are resolved
-- Supports chunked processing via {start_date} and {end_date} placeholders
-- 
-- NOTE: Inner subquery uses FULL 2-year range for correct resolution detection
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
    -- Inner subquery: Uses FULL 2-year range to correctly count resolved vs total CVMs
    SELECT CF."CONFLICTID"
    FROM {conflict_schema}.conflicts AS CF
    LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
        ON CVM2."CONFLICTID" = CF."CONFLICTID"
        AND CVM2."VisitDate" >= (NOW() - INTERVAL '2 years')
        AND CVM2."VisitDate" < (NOW() + INTERVAL '45 days')
    LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM3 
        ON CVM3."CONFLICTID" = CF."CONFLICTID"
        AND CVM3."StatusFlag" IN ('R', 'D')
        AND CVM3."VisitDate" >= (NOW() - INTERVAL '2 years')
        AND CVM3."VisitDate" < (NOW() + INTERVAL '45 days')
    WHERE CF."StatusFlag" IN ('R', 'D')
    GROUP BY CF."CONFLICTID"
    HAVING COUNT(DISTINCT CVM2."ID") = COUNT(DISTINCT CVM3."ID")
        OR (COUNT(DISTINCT CVM2."ID") - 1) = COUNT(DISTINCT CVM3."ID")
)
-- Outer filter: Uses chunk range to limit updates per execution
AND CVM."VisitDate" >= '{start_date}'::timestamp
AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day');
