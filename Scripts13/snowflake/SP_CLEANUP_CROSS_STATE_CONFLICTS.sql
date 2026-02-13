CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.SP_CLEANUP_CROSS_STATE_CONFLICTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    BEGIN TRANSACTION;
 
    /* =========================================================
       STEP 0: Create temporary table with conflict classification
       
       This temp table contains: ID, CONFLICTID, Is_Legitimate_Conflict
       Used by both STEP 1 (CONFLICTS) and STEP 2 (CVM)
       ========================================================= */
    CREATE OR REPLACE TEMPORARY TABLE CROSS_STATE_CLASSIFICATION AS
    WITH
    -- State name standardization (full name to abbreviation)
    StateMapping AS (
        SELECT ''MISSISSIPPI'' AS full_name, ''MS'' AS abbr
        UNION ALL SELECT ''NEW YORK'', ''NY''
        UNION ALL SELECT ''PENNSYLVANIA'', ''PA''
        UNION ALL SELECT ''LONG ISLAND'', ''LI''
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
            NULLIF(TRIM(COALESCE(sm1.abbr, UPPER(TRIM(cvm."P_PAddressState")))), '''') AS P_State_Clean,
            NULLIF(TRIM(COALESCE(sm2.abbr, UPPER(TRIM(cvm."PA_PAddressState")))), '''') AS PA_State_Clean,
            NULLIF(TRIM(COALESCE(sm3.abbr, UPPER(TRIM(cvm."ConP_PAddressState")))), '''') AS ConP_State_Clean,
            NULLIF(TRIM(COALESCE(sm4.abbr, UPPER(TRIM(cvm."ConPA_PAddressState")))), '''') AS ConPA_State_Clean,
           
            -- Provider addresses for fallback
            NULLIF(TRIM(pa1.ProviderAddressState), '''') AS ProviderState,
            NULLIF(TRIM(pa2.ProviderAddressState), '''') AS ConProviderState
           
        FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS cvm
       
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
    )
 
    -- Final classification: Determine Is_Legitimate_Conflict for each row
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
       
    FROM ResolvedStates;
 
    /* =========================================================
       STEP 1: Delete from CONFLICTS where Rows_To_Keep = 0
       
       Logic: Delete CONFLICTS only if ALL CONFLICTVISITMAPS rows
              for that CONFLICTID are cross-state (Rows_To_Keep = 0)
       ========================================================= */
    DELETE FROM CONFLICTREPORT.PUBLIC.CONFLICTS c
    WHERE c."CONFLICTID" IN (
        SELECT "CONFLICTID"
        FROM CROSS_STATE_CLASSIFICATION
        GROUP BY "CONFLICTID"
        HAVING SUM(CASE WHEN Is_Legitimate_Conflict = TRUE THEN 1 ELSE 0 END) = 0
    );
 
    /* =========================================================
       STEP 2: Delete cross-state rows from CONFLICTVISITMAPS
       
       Logic: Delete rows where Is_Legitimate_Conflict = FALSE
              (NO LeftSide state matches ANY RightSide state)
       ========================================================= */
    DELETE FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS cvm
    WHERE cvm."ID" IN (
        SELECT "ID"
        FROM CROSS_STATE_CLASSIFICATION
        WHERE Is_Legitimate_Conflict = FALSE
    );
 
    /* =========================================================
       STEP 3: Cleanup - Drop temporary table
       ========================================================= */
    DROP TABLE IF EXISTS CROSS_STATE_CLASSIFICATION;
 
    /* =========================================================
       Commit transaction if all steps successful
       ========================================================= */
    COMMIT;
 
    RETURN ''Success: Cross-state conflicts cleaned'';
 
EXCEPTION
    WHEN OTHER THEN
        /* =========================================================
           Rollback on any error to maintain data consistency
           ========================================================= */
        ROLLBACK;
        RETURN ''Error: '' || SQLERRM || '' - Transaction rolled back'';
END;
';