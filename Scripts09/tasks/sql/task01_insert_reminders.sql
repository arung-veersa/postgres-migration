-- ============================================================================
-- TASK_01 - Step 1: INSERT new payer-provider reminders
-- ============================================================================
-- Purpose: Sync PAYER_PROVIDER_REMINDERS from Analytics (insert new records)
-- 
-- Schema Placeholders:
--   {conflict_schema}  - Conflict data schema (e.g., conflict_dev)
--   {analytics_schema} - Analytics data schema (e.g., analytics_dev)
-- ============================================================================

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
    CAST(NULL AS INTEGER) AS "NumberOfDays"
FROM {analytics_schema}.dimprovider AS DP
INNER JOIN {analytics_schema}.dimpayerprovider AS DPP 
    ON DPP."Provider Id" = DP."Provider Id"
INNER JOIN {analytics_schema}.dimpayer AS DPA 
    ON DPA."Payer Id" = DPP."Payer Id"
WHERE NOT EXISTS (
    SELECT 1 
    FROM {conflict_schema}.payer_provider_reminders AS PPR_N 
    WHERE PPR_N."PayerID" = DPP."Payer Id"::uuid
    AND PPR_N."ProviderID" = DPP."Provider Id"::uuid
);
