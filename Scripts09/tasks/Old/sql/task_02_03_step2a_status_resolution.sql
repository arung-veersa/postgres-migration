-- ======================================================================
-- TASK 04 - STEP 2A: Status Resolution - Update CONFLICTS table
-- Converted from Snowflake UPDATE_DATA_CONFLICTVISITMAPS_3 procedure
-- 
-- This sub-step updates CONFLICTS based on IsMissed flags in CONFLICTVISITMAPS.
-- Sets StatusFlag to 'R' (Resolved) when IsMissed or ConIsMissed is true.
--
-- Original Query: sql_queryseconds2
-- ======================================================================

-- Update CONFLICTS based on IsMissed flags in CONFLICTVISITMAPS
-- Sets StatusFlag to 'R' when either IsMissed or ConIsMissed is true
UPDATE {conflict_schema}.conflicts AS CF
SET 
    "StatusFlag" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE CF."StatusFlag"
        END,
    "ResolveDate" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN COALESCE(CF."ResolveDate", NOW())
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", NOW())
            ELSE COALESCE(CF."ResolveDate", NOW())
        END,
    "ResolvedBy" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
        END
FROM {conflict_schema}.conflictvisitmaps AS CVM
WHERE CVM."CONFLICTID" = CF."CONFLICTID"
  AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                 AND (NOW() + INTERVAL '45 days')::date;
