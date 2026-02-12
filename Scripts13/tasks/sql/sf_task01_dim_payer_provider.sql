-- Task 01 Step 1: Fetch payer-provider dimension data from Snowflake
-- Joins DIMPROVIDER, DIMPAYERPROVIDER, DIMPAYER to get all distinct
-- payer-provider relationships with names for payer_provider_reminders sync.
--
-- Source: TASK_01_COPY_DATA_FROM_CONFLICTVISITMAPS_TO_TEMP.sql (table_command2)
-- Parameters: {sf_database}, {sf_schema}

SELECT DISTINCT
    DPP."Payer Id" AS "PayerID",
    DPP."Application Payer Id" AS "AppPayerID",
    DPA."Payer Name" AS "Contract",
    DPP."Provider Id" AS "ProviderID",
    DPP."Application Provider Id" AS "AppProviderID",
    DP."Provider Name" AS "ProviderName"
FROM {sf_database}.{sf_schema}.DIMPROVIDER AS DP
INNER JOIN {sf_database}.{sf_schema}.DIMPAYERPROVIDER AS DPP
    ON DPP."Provider Id" = DP."Provider Id"
INNER JOIN {sf_database}.{sf_schema}.DIMPAYER AS DPA
    ON DPA."Payer Id" = DPP."Payer Id"
