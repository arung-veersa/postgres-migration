-- Quick test: Compare V5_CORRECTED logic vs Validation logic
-- Run this to see which records differ

WITH 
StateMapping AS (
    SELECT 'MISSISSIPPI' AS full_name, 'MS' AS abbr
    UNION ALL SELECT 'NEW YORK', 'NY'
    UNION ALL SELECT 'PENNSYLVANIA', 'PA'
    UNION ALL SELECT 'LONG ISLAND', 'LI'
),

ProviderAddresses AS (
    SELECT DISTINCT
        prov."Provider Id",
        UPPER(TRIM(prov."Address State")) AS ProviderAddressState
    FROM ANALYTICS.BI.DIMPROVIDER prov
),

ConflictData AS (
    SELECT 
        cvm."CONFLICTID",
        cvm."P_PAddressState" AS P_Raw,
        cvm."PA_PAddressState" AS PA_Raw,
        cvm."ConP_PAddressState" AS ConP_Raw,
        cvm."ConPA_PAddressState" AS ConPA_Raw,
        
        NULLIF(TRIM(COALESCE(sm1.abbr, UPPER(TRIM(cvm."P_PAddressState")))), '') AS P_Clean,
        NULLIF(TRIM(COALESCE(sm2.abbr, UPPER(TRIM(cvm."PA_PAddressState")))), '') AS PA_Clean,
        NULLIF(TRIM(COALESCE(sm3.abbr, UPPER(TRIM(cvm."ConP_PAddressState")))), '') AS ConP_Clean,
        NULLIF(TRIM(COALESCE(sm4.abbr, UPPER(TRIM(cvm."ConPA_PAddressState")))), '') AS ConPA_Clean,
        
        NULLIF(TRIM(pa1.ProviderAddressState), '') AS ProviderState,
        NULLIF(TRIM(pa2.ProviderAddressState), '') AS ConProviderState
        
    FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS cvm
    LEFT JOIN StateMapping sm1 ON UPPER(TRIM(cvm."P_PAddressState")) = sm1.full_name
    LEFT JOIN StateMapping sm2 ON UPPER(TRIM(cvm."PA_PAddressState")) = sm2.full_name
    LEFT JOIN StateMapping sm3 ON UPPER(TRIM(cvm."ConP_PAddressState")) = sm3.full_name
    LEFT JOIN StateMapping sm4 ON UPPER(TRIM(cvm."ConPA_PAddressState")) = sm4.full_name
    LEFT JOIN ProviderAddresses pa1 ON cvm."ProviderID" = pa1."Provider Id"
    LEFT JOIN ProviderAddresses pa2 ON cvm."ConProviderID" = pa2."Provider Id"
),

TwoApproaches AS (
    SELECT 
        "CONFLICTID",
        P_Clean, PA_Clean, ConP_Clean, ConPA_Clean,
        ProviderState, ConProviderState,
        
        -- V5_CORRECTED Logic
        CASE WHEN P_Clean IS NULL AND PA_Clean IS NULL THEN ProviderState ELSE P_Clean END AS P_Corrected,
        CASE WHEN P_Clean IS NULL AND PA_Clean IS NULL THEN ProviderState ELSE PA_Clean END AS PA_Corrected,
        CASE WHEN ConP_Clean IS NULL AND ConPA_Clean IS NULL THEN ConProviderState ELSE ConP_Clean END AS ConP_Corrected,
        CASE WHEN ConP_Clean IS NULL AND ConPA_Clean IS NULL THEN ConProviderState ELSE ConPA_Clean END AS ConPA_Corrected,
        
        -- Validation Logic (COALESCE)
        COALESCE(P_Clean, CASE WHEN PA_Clean IS NULL THEN ProviderState ELSE NULL END) AS P_Validation,
        COALESCE(PA_Clean, CASE WHEN P_Clean IS NULL THEN ProviderState ELSE NULL END) AS PA_Validation,
        COALESCE(ConP_Clean, CASE WHEN ConPA_Clean IS NULL THEN ConProviderState ELSE NULL END) AS ConP_Validation,
        COALESCE(ConPA_Clean, CASE WHEN ConP_Clean IS NULL THEN ConProviderState ELSE NULL END) AS ConPA_Validation
        
    FROM ConflictData
),

Classifications AS (
    SELECT 
        "CONFLICTID",
        
        -- V5_CORRECTED Decision
        CASE
            WHEN (P_Corrected IS NULL AND PA_Corrected IS NULL) OR (ConP_Corrected IS NULL AND ConPA_Corrected IS NULL)
                THEN 'KEEP'
            WHEN (P_Corrected = ConP_Corrected) OR (P_Corrected = ConPA_Corrected) 
                 OR (PA_Corrected = ConP_Corrected) OR (PA_Corrected = ConPA_Corrected)
                THEN 'KEEP'
            ELSE 'REMOVE'
        END AS Corrected_Decision,
        
        -- Validation Decision
        CASE
            WHEN (P_Validation IS NULL AND PA_Validation IS NULL) OR (ConP_Validation IS NULL AND ConPA_Validation IS NULL)
                THEN 'KEEP'
            WHEN (P_Validation = ConP_Validation) OR (P_Validation = ConPA_Validation) 
                 OR (PA_Validation = ConP_Validation) OR (PA_Validation = ConPA_Validation)
                THEN 'KEEP'
            ELSE 'REMOVE'
        END AS Validation_Decision,
        
        -- Show values for debugging
        P_Corrected, PA_Corrected, ConP_Corrected, ConPA_Corrected,
        P_Validation, PA_Validation, ConP_Validation, ConPA_Validation
        
    FROM TwoApproaches
)

-- Show counts
SELECT 
    'V5_CORRECTED says REMOVE' AS Method,
    COUNT(*) AS Count
FROM Classifications
WHERE Corrected_Decision = 'REMOVE'

UNION ALL

SELECT 
    'Validation says REMOVE',
    COUNT(*)
FROM Classifications
WHERE Validation_Decision = 'REMOVE'

UNION ALL

SELECT 
    'Decisions DIFFER',
    COUNT(*)
FROM Classifications
WHERE Corrected_Decision != Validation_Decision;


-- Show sample where they differ
SELECT TOP 10
    "CONFLICTID",
    Corrected_Decision,
    Validation_Decision,
    P_Corrected, PA_Corrected, ConP_Corrected, ConPA_Corrected,
    P_Validation, PA_Validation, ConP_Validation, ConPA_Validation
FROM Classifications
WHERE Corrected_Decision != Validation_Decision;
