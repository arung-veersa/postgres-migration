-- ================================================================================
-- FUNCTION: IS_LEGITIMATE_CONFLICT
-- ================================================================================
-- Purpose: Determine if a conflict between two visits is legitimate (same-state)
--          or should be filtered out (cross-state)
-- 
-- Returns: TRUE = Legitimate conflict (keep/generate)
--          FALSE = Cross-state conflict (skip/don't generate)
-- ================================================================================

CREATE OR REPLACE FUNCTION CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
    -- Visit 1 (Patient side) addresses
    V1_P_PAddressState VARCHAR,
    V1_PA_PAddressState VARCHAR,
    V1_ProviderID VARCHAR,
    
    -- Visit 2 (Conflicting side) addresses  
    V2_P_PAddressState VARCHAR,
    V2_PA_PAddressState VARCHAR,
    V2_ProviderID VARCHAR
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    WITH
    -- Step 1: State name standardization (full name to abbreviation)
    StateMapping AS (
        SELECT 'MISSISSIPPI' AS full_name, 'MS' AS abbr
        UNION ALL SELECT 'NEW YORK', 'NY'
        UNION ALL SELECT 'PENNSYLVANIA', 'PA'
        UNION ALL SELECT 'LONG ISLAND', 'LI'
    ),
    
    -- Step 2: Get Provider addresses for fallback
    ProviderAddresses AS (
        SELECT 
            V1_ProviderID AS ProviderID,
            (SELECT UPPER(TRIM("Address State")) 
             FROM ANALYTICS.BI.DIMPROVIDER 
             WHERE "Provider Id" = V1_ProviderID 
             LIMIT 1) AS ProviderState
        
        UNION ALL
        
        SELECT 
            V2_ProviderID AS ProviderID,
            (SELECT UPPER(TRIM("Address State")) 
             FROM ANALYTICS.BI.DIMPROVIDER 
             WHERE "Provider Id" = V2_ProviderID 
             LIMIT 1) AS ProviderState
    ),
    
    -- Step 3: Clean and normalize address states
    CleanedStates AS (
        SELECT
            -- V1 (Patient) cleaned states
            NULLIF(
                TRIM(
                    COALESCE(
                        (SELECT abbr FROM StateMapping WHERE full_name = UPPER(TRIM(V1_P_PAddressState))),
                        UPPER(TRIM(V1_P_PAddressState))
                    )
                ), 
                ''
            ) AS V1_P_Clean,
            
            NULLIF(
                TRIM(
                    COALESCE(
                        (SELECT abbr FROM StateMapping WHERE full_name = UPPER(TRIM(V1_PA_PAddressState))),
                        UPPER(TRIM(V1_PA_PAddressState))
                    )
                ), 
                ''
            ) AS V1_PA_Clean,
            
            -- V2 (Conflicting) cleaned states
            NULLIF(
                TRIM(
                    COALESCE(
                        (SELECT abbr FROM StateMapping WHERE full_name = UPPER(TRIM(V2_P_PAddressState))),
                        UPPER(TRIM(V2_P_PAddressState))
                    )
                ), 
                ''
            ) AS V2_P_Clean,
            
            NULLIF(
                TRIM(
                    COALESCE(
                        (SELECT abbr FROM StateMapping WHERE full_name = UPPER(TRIM(V2_PA_PAddressState))),
                        UPPER(TRIM(V2_PA_PAddressState))
                    )
                ), 
                ''
            ) AS V2_PA_Clean,
            
            -- Provider fallback states
            NULLIF(TRIM((SELECT ProviderState FROM ProviderAddresses WHERE ProviderID = V1_ProviderID)), '') AS V1_ProviderState,
            NULLIF(TRIM((SELECT ProviderState FROM ProviderAddresses WHERE ProviderID = V2_ProviderID)), '') AS V2_ProviderState
    ),
    
    -- Step 4: Apply fallback logic
    ResolvedStates AS (
        SELECT
            -- V1 (Patient) final states after fallback
            CASE
                WHEN V1_P_Clean IS NULL AND V1_PA_Clean IS NULL THEN V1_ProviderState
                ELSE V1_P_Clean
            END AS V1_P_Final,
            
            CASE
                WHEN V1_P_Clean IS NULL AND V1_PA_Clean IS NULL THEN V1_ProviderState
                ELSE V1_PA_Clean
            END AS V1_PA_Final,
            
            -- V2 (Conflicting) final states after fallback
            CASE
                WHEN V2_P_Clean IS NULL AND V2_PA_Clean IS NULL THEN V2_ProviderState
                ELSE V2_P_Clean
            END AS V2_P_Final,
            
            CASE
                WHEN V2_P_Clean IS NULL AND V2_PA_Clean IS NULL THEN V2_ProviderState
                ELSE V2_PA_Clean
            END AS V2_PA_Final
        
        FROM CleanedStates
    )
    
    -- Step 5: Determine if conflict is legitimate (ANY-to-ANY matching)
    SELECT 
        CASE
            -- If all addresses are NULL on either side, cannot determine → KEEP (TRUE)
            WHEN (V1_P_Final IS NULL AND V1_PA_Final IS NULL) 
                 OR (V2_P_Final IS NULL AND V2_PA_Final IS NULL)
                THEN TRUE
            
            -- Check all 4 possible combinations (2x2 matrix)
            -- If ANY match found → Same state → KEEP (TRUE)
            WHEN (V1_P_Final = V2_P_Final)
                 OR (V1_P_Final = V2_PA_Final)
                 OR (V1_PA_Final = V2_P_Final)
                 OR (V1_PA_Final = V2_PA_Final)
                THEN TRUE
            
            -- No match found → Cross-state → SKIP (FALSE)
            ELSE FALSE
        END AS Is_Legitimate
    
    FROM ResolvedStates
$$;


-- ================================================================================
-- USAGE EXAMPLES
-- ================================================================================

-- Example 1: Same state (NY-NY) → Should return TRUE
SELECT CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
    'NY', 'NY',  -- V1 addresses
    'provider-1-id',
    'NY', 'NY',  -- V2 addresses
    'provider-2-id'
);
-- Expected: TRUE (legitimate conflict)


-- Example 2: Cross-state (NY-PA) → Should return FALSE
SELECT CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
    'NY', 'NY',  -- V1 addresses in NY
    'provider-1-id',
    'PA', 'PA',  -- V2 addresses in PA
    'provider-2-id'
);
-- Expected: FALSE (cross-state, don't generate)


-- Example 3: Mixed states with match (NY-NY vs NY-PA) → Should return TRUE
SELECT CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
    'NY', 'NY',  -- V1 addresses (both NY)
    'provider-1-id',
    'NY', 'PA',  -- V2 addresses (one NY, one PA)
    'provider-2-id'
);
-- Expected: TRUE (V1_P='NY' matches V2_P='NY')


-- Example 4: Both NULL, use fallback (same provider state) → Should return TRUE
SELECT CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
    NULL, NULL,  -- V1 addresses NULL
    'provider-ny-id',  -- Provider in NY
    NULL, NULL,  -- V2 addresses NULL
    'provider-ny-id'   -- Same provider in NY
);
-- Expected: TRUE (both fall back to same provider)


-- Example 5: Both NULL, use fallback (different provider states) → Should return FALSE
SELECT CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
    NULL, NULL,  -- V1 addresses NULL
    'provider-ny-id',  -- Provider in NY
    NULL, NULL,  -- V2 addresses NULL
    'provider-pa-id'   -- Provider in PA
);
-- Expected: FALSE (fallback shows NY vs PA = cross-state)


-- Example 6: Indeterminate (all NULL, no provider address) → Should return TRUE
SELECT CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
    NULL, NULL,  -- V1 addresses NULL
    'provider-no-address-id',  -- Provider has no address
    NULL, NULL,  -- V2 addresses NULL
    'provider-no-address-id'
);
-- Expected: TRUE (indeterminate, keep as legitimate)


-- ================================================================================
-- TESTING: Validate against existing CONFLICTVISITMAPS
-- ================================================================================

-- Test 1: How many current conflicts would be filtered?
SELECT 
    COUNT(*) AS Total_Conflicts,
    SUM(CASE 
        WHEN CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
            "P_PAddressState", 
            "PA_PAddressState", 
            "ProviderID",
            "ConP_PAddressState", 
            "ConPA_PAddressState", 
            "ConProviderID"
        ) = FALSE 
        THEN 1 ELSE 0 
    END) AS Would_Be_Filtered,
    SUM(CASE 
        WHEN CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
            "P_PAddressState", 
            "PA_PAddressState", 
            "ProviderID",
            "ConP_PAddressState", 
            "ConPA_PAddressState", 
            "ConProviderID"
        ) = TRUE 
        THEN 1 ELSE 0 
    END) AS Would_Be_Generated
FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS;
-- Expected: Would_Be_Filtered should be ~52,577


-- Test 2: Compare function output with V5_CORRECTED classification
SELECT 
    cvm."ID",
    cvm."CONFLICTID",
    
    -- Function result
    CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT(
        cvm."P_PAddressState", 
        cvm."PA_PAddressState", 
        cvm."ProviderID",
        cvm."ConP_PAddressState", 
        cvm."ConPA_PAddressState", 
        cvm."ConProviderID"
    ) AS Function_Says_Legitimate,
    
    -- Should match V5_CORRECTED logic
    -- (This would be a complex CTE to compare)
    
FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS cvm
LIMIT 100;
