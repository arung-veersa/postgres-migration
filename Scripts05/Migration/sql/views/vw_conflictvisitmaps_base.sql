-- =====================================================
-- View: vw_conflictvisitmaps_base
-- Purpose: Base view for TASK_02 - filters visits that need updating
-- 
-- This view encapsulates the filtering logic for which visits
-- should be updated with fresh Analytics data.
-- =====================================================

CREATE OR REPLACE VIEW public.vw_conflictvisitmaps_base AS
SELECT 
    "ID",
    "CONFLICTID",
    "VisitID",
    "AppVisitID",
    "SSN",
    "VisitDate",
    "ProviderID",
    "AppProviderID"
FROM public."conflictvisitmaps"
WHERE 
    "CONFLICTID" IS NOT NULL
    AND "UpdateFlag" = 1
    AND "InserviceStartDate" IS NULL 
    AND "InserviceEndDate" IS NULL 
    AND "PTOStartDate" IS NULL 
    AND "PTOEndDate" IS NULL
    AND "ConInserviceStartDate" IS NULL 
    AND "ConInserviceEndDate" IS NULL 
    AND "ConPTOStartDate" IS NULL 
    AND "ConPTOEndDate" IS NULL
;

COMMENT ON VIEW public.vw_conflictvisitmaps_base IS 
    'Base view for TASK_02: Returns visits that need updating with fresh Analytics data';

