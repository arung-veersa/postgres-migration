-- ================================================================================
-- DELETE CROSS-STATE CONFLICTS - V5 CORRECTED
-- ================================================================================
-- Order of operations:
--   1. Delete from CONFLICTS (parent) - Only if ALL CONFLICTVISITMAPS rows are cross-state
--   2. Delete from CONFLICTVISITMAPS (child) - Delete all cross-state rows
-- ================================================================================

-- Step 1: Delete from CONFLICTS (Parent)
-- Logic: Delete only if ALL CONFLICTVISITMAPS rows for that CONFLICTID are cross-state
DELETE FROM CONFLICTREPORT."PUBLIC".CONFLICTS
WHERE "CONFLICTID" IN (
    WITH 
    -- State name standardization (full name to abbreviation)
    StateMapping AS (
        SELECT 'MISSISSIPPI' AS full_name, 'MS' AS abbr
        UNION ALL SELECT 'NEW YORK', 'NY'
        UNION ALL SELECT 'PENNSYLVANIA', 'PA'
        UNION ALL SELECT 'LONG ISLAND', 'LI'
    ),

    -- Get Provider addresses for fallback
    ProviderAddresses AS (
        SELECT DISTINCT
            prov."Provider Id",
            UPPER(TRIM(prov."Address State")) AS ProviderAddressState
        FROM ANALYTICS.BI.DIMPROVIDER prov
    ),

    -- Normalize and prepare all address states with fallback
    ConflictData AS (
        SELECT 
            cvm."ID",
            cvm."CONFLICTID",
            cvm."ProviderID",
            cvm."ConProviderID",
            
            -- Normalize states (convert full names to abbreviations, handle NULL/empty)
            NULLIF(TRIM(COALESCE(sm1.abbr, UPPER(TRIM(cvm."P_PAddressState")))), '') AS P_State_Clean,
            NULLIF(TRIM(COALESCE(sm2.abbr, UPPER(TRIM(cvm."PA_PAddressState")))), '') AS PA_State_Clean,
            NULLIF(TRIM(COALESCE(sm3.abbr, UPPER(TRIM(cvm."ConP_PAddressState")))), '') AS ConP_State_Clean,
            NULLIF(TRIM(COALESCE(sm4.abbr, UPPER(TRIM(cvm."ConPA_PAddressState")))), '') AS ConPA_State_Clean,
            
            -- Provider addresses for fallback
            NULLIF(TRIM(pa1.ProviderAddressState), '') AS ProviderState,
            NULLIF(TRIM(pa2.ProviderAddressState), '') AS ConProviderState
            
        FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS cvm
        
        -- Join state mappings
        LEFT JOIN StateMapping sm1 ON UPPER(TRIM(cvm."P_PAddressState")) = sm1.full_name
        LEFT JOIN StateMapping sm2 ON UPPER(TRIM(cvm."PA_PAddressState")) = sm2.full_name
        LEFT JOIN StateMapping sm3 ON UPPER(TRIM(cvm."ConP_PAddressState")) = sm3.full_name
        LEFT JOIN StateMapping sm4 ON UPPER(TRIM(cvm."ConPA_PAddressState")) = sm4.full_name
        
        -- Join provider addresses for fallback
        LEFT JOIN ProviderAddresses pa1 ON cvm."ProviderID" = pa1."Provider Id"
        LEFT JOIN ProviderAddresses pa2 ON cvm."ConProviderID" = pa2."Provider Id"
    ),

    -- Apply fallback logic: Use provider address if both patient addresses are NULL
    ResolvedStates AS (
        SELECT 
            "ID",
            "CONFLICTID",
            
            -- LeftSide: Patient addresses with fallback
            CASE
                WHEN P_State_Clean IS NULL AND PA_State_Clean IS NULL THEN ProviderState
                ELSE P_State_Clean
            END AS P_Final,
            
            CASE
                WHEN P_State_Clean IS NULL AND PA_State_Clean IS NULL THEN ProviderState
                ELSE PA_State_Clean
            END AS PA_Final,
            
            -- RightSide: Conflicting patient addresses with fallback
            CASE
                WHEN ConP_State_Clean IS NULL AND ConPA_State_Clean IS NULL THEN ConProviderState
                ELSE ConP_State_Clean
            END AS ConP_Final,
            
            CASE
                WHEN ConP_State_Clean IS NULL AND ConPA_State_Clean IS NULL THEN ConProviderState
                ELSE ConPA_State_Clean
            END AS ConPA_Final
            
        FROM ConflictData
    ),

    -- Determine which conflicts match (ANY-to-ANY comparison)
    ConflictClassification AS (
        SELECT 
            "ID",
            "CONFLICTID",
            
            -- ANY-to-ANY matching: If any LeftSide matches any RightSide, keep the conflict
            CASE
                -- If all addresses are NULL on either side, cannot determine (keep as legitimate)
                WHEN (P_Final IS NULL AND PA_Final IS NULL) 
                     OR (ConP_Final IS NULL AND ConPA_Final IS NULL)
                    THEN TRUE
                
                -- Check all 4 possible combinations (2x2 matrix)
                WHEN (P_Final = ConP_Final)
                     OR (P_Final = ConPA_Final)
                     OR (PA_Final = ConP_Final)
                     OR (PA_Final = ConPA_Final)
                    THEN TRUE
                
                -- No match found = cross-state conflict
                ELSE FALSE
            END AS Is_Legitimate_Conflict
            
        FROM ResolvedStates
    ),

    -- Aggregate by CONFLICTID to determine which ones have ALL rows being deleted
    ConflictAggregation AS (
        SELECT 
            "CONFLICTID",
            SUM(CASE WHEN Is_Legitimate_Conflict = TRUE THEN 1 ELSE 0 END) AS Rows_To_Keep
        FROM ConflictClassification
        GROUP BY "CONFLICTID"
    )

    -- Select CONFLICTIDs where ALL rows are being deleted (Rows_To_Keep = 0)
    SELECT "CONFLICTID"
    FROM ConflictAggregation
    WHERE Rows_To_Keep = 0
);


-- Step 2: Delete from CONFLICTVISITMAPS (Child)
-- Logic: Delete all cross-state rows (regardless of whether other rows for same CONFLICTID are kept)
DELETE FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS
WHERE "ID" IN (
    WITH 
    -- State name standardization (full name to abbreviation)
    StateMapping AS (
        SELECT 'MISSISSIPPI' AS full_name, 'MS' AS abbr
        UNION ALL SELECT 'NEW YORK', 'NY'
        UNION ALL SELECT 'PENNSYLVANIA', 'PA'
        UNION ALL SELECT 'LONG ISLAND', 'LI'
    ),

    -- Get Provider addresses for fallback
    ProviderAddresses AS (
        SELECT DISTINCT
            prov."Provider Id",
            UPPER(TRIM(prov."Address State")) AS ProviderAddressState
        FROM ANALYTICS.BI.DIMPROVIDER prov
    ),

    -- Normalize and prepare all address states with fallback
    ConflictData AS (
        SELECT 
            cvm."ID",
            cvm."CONFLICTID",
            cvm."ProviderID",
            cvm."ConProviderID",
            
            -- Normalize states (convert full names to abbreviations, handle NULL/empty)
            NULLIF(TRIM(COALESCE(sm1.abbr, UPPER(TRIM(cvm."P_PAddressState")))), '') AS P_State_Clean,
            NULLIF(TRIM(COALESCE(sm2.abbr, UPPER(TRIM(cvm."PA_PAddressState")))), '') AS PA_State_Clean,
            NULLIF(TRIM(COALESCE(sm3.abbr, UPPER(TRIM(cvm."ConP_PAddressState")))), '') AS ConP_State_Clean,
            NULLIF(TRIM(COALESCE(sm4.abbr, UPPER(TRIM(cvm."ConPA_PAddressState")))), '') AS ConPA_State_Clean,
            
            -- Provider addresses for fallback
            NULLIF(TRIM(pa1.ProviderAddressState), '') AS ProviderState,
            NULLIF(TRIM(pa2.ProviderAddressState), '') AS ConProviderState
            
        FROM CONFLICTREPORT."PUBLIC".CONFLICTVISITMAPS cvm
        
        -- Join state mappings
        LEFT JOIN StateMapping sm1 ON UPPER(TRIM(cvm."P_PAddressState")) = sm1.full_name
        LEFT JOIN StateMapping sm2 ON UPPER(TRIM(cvm."PA_PAddressState")) = sm2.full_name
        LEFT JOIN StateMapping sm3 ON UPPER(TRIM(cvm."ConP_PAddressState")) = sm3.full_name
        LEFT JOIN StateMapping sm4 ON UPPER(TRIM(cvm."ConPA_PAddressState")) = sm4.full_name
        
        -- Join provider addresses for fallback
        LEFT JOIN ProviderAddresses pa1 ON cvm."ProviderID" = pa1."Provider Id"
        LEFT JOIN ProviderAddresses pa2 ON cvm."ConProviderID" = pa2."Provider Id"
    ),

    -- Apply fallback logic: Use provider address if both patient addresses are NULL
    ResolvedStates AS (
        SELECT 
            "ID",
            "CONFLICTID",
            
            -- LeftSide: Patient addresses with fallback
            CASE
                WHEN P_State_Clean IS NULL AND PA_State_Clean IS NULL THEN ProviderState
                ELSE P_State_Clean
            END AS P_Final,
            
            CASE
                WHEN P_State_Clean IS NULL AND PA_State_Clean IS NULL THEN ProviderState
                ELSE PA_State_Clean
            END AS PA_Final,
            
            -- RightSide: Conflicting patient addresses with fallback
            CASE
                WHEN ConP_State_Clean IS NULL AND ConPA_State_Clean IS NULL THEN ConProviderState
                ELSE ConP_State_Clean
            END AS ConP_Final,
            
            CASE
                WHEN ConP_State_Clean IS NULL AND ConPA_State_Clean IS NULL THEN ConProviderState
                ELSE ConPA_State_Clean
            END AS ConPA_Final
            
        FROM ConflictData
    ),

    -- Determine which conflicts match (ANY-to-ANY comparison)
    ConflictClassification AS (
        SELECT 
            "ID",
            "CONFLICTID",
            
            -- ANY-to-ANY matching: If any LeftSide matches any RightSide, keep the conflict
            CASE
                -- If all addresses are NULL on either side, cannot determine (keep as legitimate)
                WHEN (P_Final IS NULL AND PA_Final IS NULL) 
                     OR (ConP_Final IS NULL AND ConPA_Final IS NULL)
                    THEN TRUE
                
                -- Check all 4 possible combinations (2x2 matrix)
                WHEN (P_Final = ConP_Final)
                     OR (P_Final = ConPA_Final)
                     OR (PA_Final = ConP_Final)
                     OR (PA_Final = ConPA_Final)
                    THEN TRUE
                
                -- No match found = cross-state conflict
                ELSE FALSE
            END AS Is_Legitimate_Conflict
            
        FROM ResolvedStates
    )

    -- Select IDs where conflict is NOT legitimate (cross-state)
    SELECT "ID"
    FROM ConflictClassification
    WHERE Is_Legitimate_Conflict = FALSE
);
