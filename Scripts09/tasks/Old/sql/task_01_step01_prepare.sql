-- ============================================================================
-- TASK_01 - STEP 01: Prepare Steps (Steps 1-3)
-- ============================================================================
-- Purpose:
--   1. Sync PAYER_PROVIDER_REMINDERS from Analytics (insert new, update existing)
--   2. Truncate CONFLICTVISITMAPS_TEMP
--
-- Schema Placeholders:
--   {conflict_schema}  - Conflict data schema (e.g., conflict_dev)
--   {analytics_schema} - Analytics data schema (e.g., analytics_dev)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Step 1: INSERT new payer-provider reminders that don't exist
-- ----------------------------------------------------------------------------
INSERT INTO {conflict_schema}.payer_provider_reminders (
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
    CAST(NULL AS NUMERIC) AS "NumberOfDays"
FROM {analytics_schema}.dimprovider AS DP
INNER JOIN {analytics_schema}.dimpayerprovider AS DPP 
    ON DPP."Provider Id"::varchar = DP."Provider Id"::varchar
INNER JOIN {analytics_schema}.dimpayer AS DPA 
    ON DPA."Payer Id"::varchar = DPP."Payer Id"::varchar
WHERE NOT EXISTS (
    SELECT 1 
    FROM {conflict_schema}.payer_provider_reminders AS PPR_N 
    WHERE PPR_N."PayerID" = DPP."Payer Id"::uuid
    AND PPR_N."ProviderID" = DPP."Provider Id"::uuid
);

-- ----------------------------------------------------------------------------
-- Step 2: UPDATE existing payer-provider reminders with latest names
-- ----------------------------------------------------------------------------
UPDATE {conflict_schema}.payer_provider_reminders AS PPR
SET 
    "Contract" = DPA."Payer Name",
    "ProviderName" = DP."Provider Name"
FROM {analytics_schema}.dimprovider AS DP
INNER JOIN {analytics_schema}.dimpayerprovider AS DPP 
    ON DPP."Provider Id"::varchar = DP."Provider Id"::varchar
INNER JOIN {analytics_schema}.dimpayer AS DPA 
    ON DPA."Payer Id"::varchar = DPP."Payer Id"::varchar
WHERE 
    PPR."PayerID" = DPP."Payer Id"::uuid
    AND PPR."ProviderID" = DPP."Provider Id"::uuid;

-- ----------------------------------------------------------------------------
-- Step 3: TRUNCATE temp table
-- ----------------------------------------------------------------------------
TRUNCATE TABLE {conflict_schema}.conflictvisitmaps_temp;
