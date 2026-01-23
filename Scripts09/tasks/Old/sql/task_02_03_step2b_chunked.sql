-- ======================================================================
-- TASK 03 - STEP 2B: Status Resolution (Chunked by Date Range)
-- OPTIMIZED: Only update rows that actually need changes (skip no-ops)
--
-- This file is executed with parameterized date range placeholders:
--   {start_date} and {end_date} are replaced at runtime
--
-- Part 1: Update CONFLICTVISITMAPS based on IsMissed flags
-- Part 2: Clear UpdateFlag and finalize resolution status
-- ======================================================================

-- ======================================================================
-- PART 1: Update CONFLICTVISITMAPS based on IsMissed flags
-- OPTIMIZED: Only update rows where IsMissed/ConIsMissed is TRUE
--            AND StatusFlag is not already 'D' or 'R' (skip no-ops)
-- ======================================================================
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "StatusFlag" = 'R',
    "ResolveDate" = COALESCE(CVM."ResolveDate", NOW()),
    "ResolvedBy" = 
        CASE 
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            ELSE COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        END
WHERE CVM."VisitDate" >= '{start_date}'::timestamp 
  AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day')
  AND (CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE)
  AND CVM."StatusFlag" NOT IN ('D', 'R');


-- ======================================================================
-- PART 2: Clear UpdateFlag and finalize resolution status
-- Only processes rows that were marked for update (UpdateFlag = 1)
-- ======================================================================
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "UpdateFlag" = NULL,
    "StatusFlag" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE 'R'
        END,
    "ResolveDate" = COALESCE(CVM."ResolveDate", NOW()),
    "ResolvedBy" = 
        CASE 
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
        END
WHERE CVM."UpdateFlag" = 1
  AND CVM."VisitDate" >= '{start_date}'::timestamp 
  AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day');
