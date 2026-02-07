-- ============================================================================
-- TASK_01: Prepare Temp Data for Conflict Processing
-- ============================================================================
-- Purpose: 
--   1. Sync payer-provider reminders from Analytics
--   2. Copy conflict visit maps data to temp table (last 2 years + 45 days)
--   3. Set processing flag
--
-- Execution: Run directly in DBeaver
-- 
-- Variables (set these before running):
--   :conflict_schema   - Conflict data schema (e.g., 'conflict_dev')
--   :analytics_schema  - Analytics data schema (e.g., 'analytics_dev')
--
-- Example:
--   -- Set variables in DBeaver
--   -- @set conflict_schema = 'conflict_dev'
--   -- @set analytics_schema = 'analytics_dev'
-- ============================================================================

-- ============================================================================
-- STEP 1: INSERT new payer-provider reminders
-- ============================================================================
-- Sync new payer-provider relationships from Analytics

INSERT INTO :conflict_schema.payer_provider_reminders (
    "PayerID", 
    "AppPayerID", 
    "Contract", 
    "ProviderID", 
    "AppProviderID", 
    "ProviderName", 
    "CreatedDateTime", 
    "NumberOfDays"
)
SELECT DISTINCT 
    DPP."Payer Id"::uuid AS "PayerID", 
    DPP."Application Payer Id" AS "AppPayerID", 
    DPA."Payer Name" AS "Contract", 
    DPP."Provider Id"::uuid AS "ProviderID", 
    DPP."Application Provider Id" AS "AppProviderID", 
    DP."Provider Name" AS "ProviderName", 
    CURRENT_TIMESTAMP AS "CreatedDateTime", 
    CAST(NULL AS INTEGER) AS "NumberOfDays"
FROM :analytics_schema.dimprovider AS DP
INNER JOIN :analytics_schema.dimpayerprovider AS DPP 
    ON DPP."Provider Id"::varchar = DP."Provider Id"::varchar
INNER JOIN :analytics_schema.dimpayer AS DPA 
    ON DPA."Payer Id"::varchar = DPP."Payer Id"::varchar
WHERE NOT EXISTS (
    SELECT 1 
    FROM :conflict_schema.payer_provider_reminders AS PPR_N 
    WHERE PPR_N."PayerID" = DPP."Payer Id"::uuid
    AND PPR_N."ProviderID" = DPP."Provider Id"::uuid
);

-- ============================================================================
-- STEP 2: UPDATE existing payer-provider reminders
-- ============================================================================
-- Update names for existing payer-provider relationships

UPDATE :conflict_schema.payer_provider_reminders AS PPR
SET 
    "Contract" = DPA."Payer Name",
    "ProviderName" = DP."Provider Name"
FROM :analytics_schema.dimprovider AS DP
INNER JOIN :analytics_schema.dimpayerprovider AS DPP 
    ON DPP."Provider Id"::varchar = DP."Provider Id"::varchar
INNER JOIN :analytics_schema.dimpayer AS DPA 
    ON DPA."Payer Id"::varchar = DPP."Payer Id"::varchar
WHERE 
    PPR."PayerID" = DPP."Payer Id"::uuid
    AND PPR."ProviderID" = DPP."Provider Id"::uuid;

-- ============================================================================
-- STEP 3: TRUNCATE temp table
-- ============================================================================
-- Clear temp table before copying new data

TRUNCATE TABLE :conflict_schema.conflictvisitmaps_temp;

-- ============================================================================
-- STEP 4: Copy data to CONFLICTVISITMAPS_TEMP
-- ============================================================================
-- Copy conflict visit maps for date range: (TODAY - 2 years) to (TODAY + 45 days)

-- If temp table = main table structure exactly
INSERT INTO :conflict_schema.conflictvisitmaps_temp
SELECT * 
FROM :conflict_schema.conflictvisitmaps
WHERE "VisitDate" BETWEEN 
    (CURRENT_DATE - INTERVAL '2 years') 
    AND (CURRENT_DATE + INTERVAL '45 days');

-- ============================================================================
-- STEP 5: Set processing flag
-- ============================================================================
-- Mark that processing has started

UPDATE :conflict_schema.settings 
SET "InProgressFlag" = 1;

-- ============================================================================
-- TASK_01 Complete
-- ============================================================================
-- Expected outcome:
--   - Payer-provider reminders synced
--   - Temp table populated with ~8M rows (2 years + 45 days)
--   - Processing flag set to 1
--
-- Next: Run TASK_02_03 (conflict detection MERGE)
-- ============================================================================
