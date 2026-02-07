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
        
        -- Additional columns for output
        cvm."VisitDate",
        cvm."ProviderName",
        cvm."ConProviderName",
        cvm."PayerID",
        cvm."ConPayerID",
        
        -- Original address states (for display)
        cvm."P_PAddressState",
        cvm."PA_PAddressState",
        cvm."ConP_PAddressState",
        cvm."ConPA_PAddressState",
        
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
-- CORRECTED VERSION - Simplified ANY-to-ANY matching
ResolvedStates AS (
    SELECT 
        "ID",
        "CONFLICTID",
        "ProviderID",
        "ConProviderID",
        
        -- Pass through for output
        "VisitDate",
        "ProviderName",
        "ConProviderName",
        "PayerID",
        "ConPayerID",
        
        -- Original states (for display)
        "P_PAddressState",
        "PA_PAddressState",
        "ConP_PAddressState",
        "ConPA_PAddressState",
        
        -- Provider states (for display when fallback is used)
        ProviderState,
        ConProviderState,
        
        -- LeftSide: Patient addresses with fallback
        -- If BOTH P and PA are NULL → use ProviderState (fallback)
        -- Otherwise → keep P and PA as-is (even if one is NULL)
        CASE
            WHEN P_State_Clean IS NULL AND PA_State_Clean IS NULL THEN ProviderState
            ELSE P_State_Clean
        END AS P_Final,
        
        CASE
            WHEN P_State_Clean IS NULL AND PA_State_Clean IS NULL THEN ProviderState
            ELSE PA_State_Clean
        END AS PA_Final,
        
        -- RightSide: Conflicting patient addresses with fallback
        -- If BOTH ConP and ConPA are NULL → use ConProviderState (fallback)
        -- Otherwise → keep ConP and ConPA as-is (even if one is NULL)
        CASE
            WHEN ConP_State_Clean IS NULL AND ConPA_State_Clean IS NULL THEN ConProviderState
            ELSE ConP_State_Clean
        END AS ConP_Final,
        
        CASE
            WHEN ConP_State_Clean IS NULL AND ConPA_State_Clean IS NULL THEN ConProviderState
            ELSE ConPA_State_Clean
        END AS ConPA_Final,
        
        -- Track if fallback was used
        CASE 
            WHEN P_State_Clean IS NULL AND PA_State_Clean IS NULL AND ProviderState IS NOT NULL 
                THEN TRUE 
            ELSE FALSE 
        END AS Patient_Used_Fallback,
        
        CASE 
            WHEN ConP_State_Clean IS NULL AND ConPA_State_Clean IS NULL AND ConProviderState IS NOT NULL 
                THEN TRUE 
            ELSE FALSE 
        END AS ConPatient_Used_Fallback
        
    FROM ConflictData
),

-- Determine which conflicts match (ANY-to-ANY comparison)
-- Business Logic: 
--   LeftSide = {P_Final, PA_Final} (after fallback if both NULL)
--   RightSide = {ConP_Final, ConPA_Final} (after fallback if both NULL)
--   If ANY LeftSide matches ANY RightSide → NOT cross-state (KEEP)
--   Otherwise → cross-state (REMOVE)
ConflictClassification AS (
    SELECT 
        *,
        
        -- ANY-to-ANY matching: If any LeftSide matches any RightSide, keep the conflict
        -- Check if ANY patient address matches ANY conflicting patient address
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
        END AS Is_Legitimate_Conflict,
        
        -- Detailed classification for reporting
        CASE
            WHEN (P_Final IS NULL AND PA_Final IS NULL) 
                 OR (ConP_Final IS NULL AND ConPA_Final IS NULL)
                THEN 'Kept - Indeterminate (NULL addresses)'
            
            WHEN (P_Final = ConP_Final)
                 OR (P_Final = ConPA_Final)
                 OR (PA_Final = ConP_Final)
                 OR (PA_Final = ConPA_Final)
                THEN 'Kept - Same State (Match Found)'
            
            ELSE 'Remove - Cross State (No Match)'
        END AS Conflict_Classification,
        
        -- Show which specific matches were found (for transparency)
        CONCAT_WS(', ',
            CASE WHEN P_Final = ConP_Final THEN 'P=ConP' END,
            CASE WHEN P_Final = ConPA_Final THEN 'P=ConPA' END,
            CASE WHEN PA_Final = ConP_Final THEN 'PA=ConP' END,
            CASE WHEN PA_Final = ConPA_Final THEN 'PA=ConPA' END
        ) AS Matching_Combinations
        
    FROM ResolvedStates
)

-- Final output: Show ONLY cross-state conflicts that should be deleted
SELECT 
    cc."ID",
    cc."CONFLICTID",
    cc."VisitDate",
    pay."Payer Name" AS "PayerName",
    cc."ProviderName",
    conpay."Payer Name" AS "ConPayerName",
    cc."ConProviderName",
    cc."P_PAddressState" AS "Raw_P_State",
    cc."PA_PAddressState" AS "Raw_PA_State",
    cc.Patient_Used_Fallback AS "PATIENT_USED_FALLBACK",
    cc.ProviderState AS "PATIENT_FALLBACK_STATE",
    cc."ConP_PAddressState" AS "Raw_ConP_State",
    cc."ConPA_PAddressState" AS "Raw_ConPA_State",
    cc.ConPatient_Used_Fallback AS "CONPATIENT_USED_FALLBACK",
    cc.ConProviderState AS "CONPATIENT_FALLBACK_STATE",
    cc.Is_Legitimate_Conflict AS "IS_LEGITIMATE_CONFLICT",
    cc.Conflict_Classification AS "CONFLICT_CLASSIFICATION",
    cc.Matching_Combinations AS "MATCHING_COMBINATIONS"
    
FROM ConflictClassification cc
LEFT JOIN ANALYTICS.BI.DIMPAYER pay 
    ON pay."Payer Id" = cc."PayerID"
LEFT JOIN ANALYTICS.BI.DIMPAYER conpay 
    ON conpay."Payer Id" = cc."ConPayerID"

WHERE cc.Is_Legitimate_Conflict = FALSE
  AND cc.Conflict_Classification = 'Remove - Cross State (No Match)'

ORDER BY 
    cc."ID",
    cc."CONFLICTID", 
    cc."VisitDate" DESC;
