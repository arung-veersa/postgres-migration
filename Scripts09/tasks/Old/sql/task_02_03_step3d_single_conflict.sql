-- ======================================================================
-- TASK 04 - STEP 3D: Resolve Single-Visit CONFLICTS
-- Original: sql_queryseconds6
-- 
-- For conflicts with StatusFlag R or D and only 1 CVM
-- Supports chunked processing via {start_date} and {end_date} placeholders
-- 
-- NOTE: Inner subquery uses FULL 2-year range for correct single-visit detection
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
      -- Inner subquery: Uses FULL 2-year range to correctly identify single-visit conflicts
      SELECT DISTINCT CVM2."CONFLICTID"
      FROM {conflict_schema}.conflictvisitmaps AS CVM2
      WHERE CVM2."CONFLICTID" IN (
          SELECT DISTINCT CVM3."CONFLICTID"
          FROM {conflict_schema}.conflictvisitmaps AS CVM3
          WHERE CVM3."StatusFlag" IN ('R', 'D')
            AND CVM3."VisitDate" >= (NOW() - INTERVAL '2 years')
            AND CVM3."VisitDate" < (NOW() + INTERVAL '45 days')
      )
      AND CVM2."VisitDate" >= (NOW() - INTERVAL '2 years')
      AND CVM2."VisitDate" < (NOW() + INTERVAL '45 days')
      GROUP BY CVM2."CONFLICTID"
      HAVING COUNT(CVM2."ID") = 1
  )
  -- Outer filter: Uses chunk range to limit updates per execution
  AND CVM."VisitDate" >= '{start_date}'::timestamp
  AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day');
