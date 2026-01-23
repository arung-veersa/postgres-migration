-- ============================================================================
-- TASK_01 - Step 2: UPDATE existing payer-provider reminders
-- ============================================================================
-- Purpose: Update existing PAYER_PROVIDER_REMINDERS with latest names from Analytics
-- Note: Joins use UUIDs, so no explicit BIGINT casting is required here.
-- 
-- Schema Placeholders:
--   {conflict_schema}  - Conflict data schema (e.g., conflict_dev)
--   {analytics_schema} - Analytics data schema (e.g., analytics_dev)
-- ============================================================================

UPDATE {conflict_schema}.payer_provider_reminders AS PPR
SET 
    "Contract" = DPA."Payer Name",
    "ProviderName" = DP."Provider Name"
FROM {analytics_schema}.dimprovider AS DP
INNER JOIN {analytics_schema}.dimpayerprovider AS DPP 
    ON DPP."Provider Id" = DP."Provider Id"
INNER JOIN {analytics_schema}.dimpayer AS DPA 
    ON DPA."Payer Id" = DPP."Payer Id"
WHERE 
    PPR."PayerID" = DPP."Payer Id"::uuid
    AND PPR."ProviderID" = DPP."Provider Id"::uuid;
