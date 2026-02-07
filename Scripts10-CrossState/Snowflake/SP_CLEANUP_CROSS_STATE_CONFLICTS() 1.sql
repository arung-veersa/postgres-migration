CREATE OR REPLACE PROCEDURE CONFLICTREPORT_SANDBOX.PUBLIC.SP_CLEANUP_CROSS_STATE_CONFLICTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN

    /* =========================================================
       STEP 1: Delete from CONFLICTS where Rows_To_Keep = 0    
       Logic: Delete CONFLICTS only if ALL CONFLICTVISITMAPS rows 
              for that CONFLICTID are cross-state (Rows_To_Keep = 0)
       ========================================================= */
    DELETE FROM CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTS c
    WHERE c."CONFLICTID" IN (
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
                
            FROM CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTVISITMAPS cvm
            
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
                COUNT(*) AS Total_Rows,
                SUM(CASE WHEN Is_Legitimate_Conflict = FALSE THEN 1 ELSE 0 END) AS Rows_To_Delete,
                SUM(CASE WHEN Is_Legitimate_Conflict = TRUE THEN 1 ELSE 0 END) AS Rows_To_Keep
            FROM ConflictClassification
            GROUP BY "CONFLICTID"
        ),

        -- Identify CONFLICTIDs where ALL rows are being deleted (Rows_To_Keep = 0)
        ConflictsToDelete AS (
            SELECT "CONFLICTID"
            FROM ConflictAggregation
            WHERE Rows_To_Keep = 0
        )

        SELECT "CONFLICTID" FROM ConflictsToDelete
    );

    /* =========================================================
       STEP 2: Delete cross-state rows from CONFLICTVISITMAPS
       
       Logic: Delete rows where NO LeftSide state matches ANY RightSide state
              (after applying state normalization and provider address fallback)
       ========================================================= */
    DELETE FROM CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTVISITMAPS cvm
    USING (
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
                cvm_inner."ID",
                cvm_inner."CONFLICTID",
                cvm_inner."ProviderID",
                cvm_inner."ConProviderID",
                
                -- Normalize states (convert full names to abbreviations, handle NULL/empty)
                NULLIF(TRIM(COALESCE(sm1.abbr, UPPER(TRIM(cvm_inner."P_PAddressState")))), '') AS P_State_Clean,
                NULLIF(TRIM(COALESCE(sm2.abbr, UPPER(TRIM(cvm_inner."PA_PAddressState")))), '') AS PA_State_Clean,
                NULLIF(TRIM(COALESCE(sm3.abbr, UPPER(TRIM(cvm_inner."ConP_PAddressState")))), '') AS ConP_State_Clean,
                NULLIF(TRIM(COALESCE(sm4.abbr, UPPER(TRIM(cvm_inner."ConPA_PAddressState")))), '') AS ConPA_State_Clean,
                
                -- Provider addresses for fallback
                NULLIF(TRIM(pa1.ProviderAddressState), '') AS ProviderState,
                NULLIF(TRIM(pa2.ProviderAddressState), '') AS ConProviderState
                
            FROM CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTVISITMAPS cvm_inner
            
            -- Join state mappings
            LEFT JOIN StateMapping sm1 ON UPPER(TRIM(cvm_inner."P_PAddressState")) = sm1.full_name
            LEFT JOIN StateMapping sm2 ON UPPER(TRIM(cvm_inner."PA_PAddressState")) = sm2.full_name
            LEFT JOIN StateMapping sm3 ON UPPER(TRIM(cvm_inner."ConP_PAddressState")) = sm3.full_name
            LEFT JOIN StateMapping sm4 ON UPPER(TRIM(cvm_inner."ConPA_PAddressState")) = sm4.full_name
            
            -- Join provider addresses for fallback
            LEFT JOIN ProviderAddresses pa1 ON cvm_inner."ProviderID" = pa1."Provider Id"
            LEFT JOIN ProviderAddresses pa2 ON cvm_inner."ConProviderID" = pa2."Provider Id"
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

        -- Select IDs to delete (cross-state conflicts)
        SELECT "ID"
        FROM ConflictClassification
        WHERE Is_Legitimate_Conflict = FALSE

    ) bad
    WHERE cvm."ID" = bad."ID";

    RETURN 'Success: Cross-state conflicts cleaned';

END;
$$;
