-- ======================================================================
-- TASK 04 - STEP 3G: Resolve All-Resolved CONFLICTS
-- Original: sql_queryseconds9
-- 
-- For CONFLICTS where ALL CVMs are resolved but conflict is NOT yet R or D
-- Supports chunked processing via {start_date} and {end_date} placeholders
-- 
-- NOTE: Inner subquery uses FULL 2-year range for correct all-resolved detection
--       Outer WHERE uses chunk range to limit which records are updated per chunk
-- ======================================================================

UPDATE {conflict_schema}.conflicts AS CF
SET 
    "StatusFlag" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE 'R'
        END,
    "ResolveDate" = COALESCE(CF."ResolveDate", NOW()),
    "ResolvedBy" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
        END
FROM {conflict_schema}.conflictvisitmaps AS CVM
WHERE CVM."CONFLICTID" = CF."CONFLICTID"
  AND CF."CONFLICTID" IN (
      -- Inner subquery: Uses FULL 2-year range to correctly count resolved vs total CVMs
      SELECT CF2."CONFLICTID"
      FROM {conflict_schema}.conflicts AS CF2
      LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
          ON CVM2."CONFLICTID" = CF2."CONFLICTID"
          AND CVM2."VisitDate" >= (NOW() - INTERVAL '2 years')
          AND CVM2."VisitDate" < (NOW() + INTERVAL '45 days')
      LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM3 
          ON CVM3."CONFLICTID" = CF2."CONFLICTID"
          AND CVM3."StatusFlag" IN ('R', 'D')
          AND CVM3."VisitDate" >= (NOW() - INTERVAL '2 years')
          AND CVM3."VisitDate" < (NOW() + INTERVAL '45 days')
      WHERE CF2."StatusFlag" NOT IN ('R', 'D')
      GROUP BY CF2."CONFLICTID"
      HAVING COUNT(DISTINCT CVM2."ID") = COUNT(DISTINCT CVM3."ID")
         AND COUNT(DISTINCT CVM2."ID") > 0
         AND COUNT(DISTINCT CVM3."ID") > 0
  )
  -- Outer filter: Uses chunk range to limit updates per execution
  AND CVM."VisitDate" >= '{start_date}'::timestamp
  AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day');
