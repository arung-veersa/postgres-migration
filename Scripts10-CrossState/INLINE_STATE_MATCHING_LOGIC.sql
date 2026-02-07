-- ================================================================================
-- INLINE STATE MATCHING LOGIC - For use in TASK_03 generation scripts
-- ================================================================================
-- Purpose: Add this CTE pattern to filter out cross-state conflicts during generation
-- Performance: Inline logic is faster than function calls for large datasets
-- Maintainability: Copy this exact pattern to all 3 scripts
-- ================================================================================

-- Add this CTE AFTER V1 and V2 are defined, BEFORE the final INSERT:

StateFiltered AS (
    SELECT 
        V1.*,
        V2.* AS Con_,  -- Prefix V2 columns to avoid collision
        
        -- V1 (Patient) states - normalized and cleaned
        NULLIF(
            TRIM(
                CASE 
                    WHEN UPPER(TRIM(V1."P_PAddressState")) = 'MISSISSIPPI' THEN 'MS'
                    WHEN UPPER(TRIM(V1."P_PAddressState")) = 'NEW YORK' THEN 'NY'
                    WHEN UPPER(TRIM(V1."P_PAddressState")) = 'PENNSYLVANIA' THEN 'PA'
                    WHEN UPPER(TRIM(V1."P_PAddressState")) = 'LONG ISLAND' THEN 'LI'
                    ELSE UPPER(TRIM(V1."P_PAddressState"))
                END
            ), 
            ''
        ) AS V1_P_Clean,
        
        NULLIF(
            TRIM(
                CASE 
                    WHEN UPPER(TRIM(V1."PA_PAddressState")) = 'MISSISSIPPI' THEN 'MS'
                    WHEN UPPER(TRIM(V1."PA_PAddressState")) = 'NEW YORK' THEN 'NY'
                    WHEN UPPER(TRIM(V1."PA_PAddressState")) = 'PENNSYLVANIA' THEN 'PA'
                    WHEN UPPER(TRIM(V1."PA_PAddressState")) = 'LONG ISLAND' THEN 'LI'
                    ELSE UPPER(TRIM(V1."PA_PAddressState"))
                END
            ), 
            ''
        ) AS V1_PA_Clean,
        
        -- V2 (Conflicting) states - normalized and cleaned
        NULLIF(
            TRIM(
                CASE 
                    WHEN UPPER(TRIM(V2."P_PAddressState")) = 'MISSISSIPPI' THEN 'MS'
                    WHEN UPPER(TRIM(V2."P_PAddressState")) = 'NEW YORK' THEN 'NY'
                    WHEN UPPER(TRIM(V2."P_PAddressState")) = 'PENNSYLVANIA' THEN 'PA'
                    WHEN UPPER(TRIM(V2."P_PAddressState")) = 'LONG ISLAND' THEN 'LI'
                    ELSE UPPER(TRIM(V2."P_PAddressState"))
                END
            ), 
            ''
        ) AS V2_P_Clean,
        
        NULLIF(
            TRIM(
                CASE 
                    WHEN UPPER(TRIM(V2."PA_PAddressState")) = 'MISSISSIPPI' THEN 'MS'
                    WHEN UPPER(TRIM(V2."PA_PAddressState")) = 'NEW YORK' THEN 'NY'
                    WHEN UPPER(TRIM(V2."PA_PAddressState")) = 'PENNSYLVANIA' THEN 'PA'
                    WHEN UPPER(TRIM(V2."PA_PAddressState")) = 'LONG ISLAND' THEN 'LI'
                    ELSE UPPER(TRIM(V2."PA_PAddressState"))
                END
            ), 
            ''
        ) AS V2_PA_Clean,
        
        -- Provider fallback states (lookup from DIMPROVIDER)
        (SELECT NULLIF(TRIM(UPPER("Address State")), '') 
         FROM ANALYTICS.BI.DIMPROVIDER 
         WHERE "Provider Id" = V1."ProviderID" 
         LIMIT 1) AS V1_ProviderState,
        
        (SELECT NULLIF(TRIM(UPPER("Address State")), '') 
         FROM ANALYTICS.BI.DIMPROVIDER 
         WHERE "Provider Id" = V2."ProviderID" 
         LIMIT 1) AS V2_ProviderState
        
    FROM V1
    INNER JOIN V2 
        ON V1."SSN" = V2."SSN"
        AND V1."VisitDate" = V2."VisitDate"
        AND [... other existing join conditions ...]
    
    WHERE [... existing WHERE conditions ...]
),

-- Apply fallback and check ANY-to-ANY matching
LegitimateConflicts AS (
    SELECT 
        *,
        
        -- V1 final states (with fallback)
        CASE 
            WHEN V1_P_Clean IS NULL AND V1_PA_Clean IS NULL THEN V1_ProviderState
            ELSE V1_P_Clean
        END AS V1_P_Final,
        
        CASE 
            WHEN V1_P_Clean IS NULL AND V1_PA_Clean IS NULL THEN V1_ProviderState
            ELSE V1_PA_Clean
        END AS V1_PA_Final,
        
        -- V2 final states (with fallback)
        CASE 
            WHEN V2_P_Clean IS NULL AND V2_PA_Clean IS NULL THEN V2_ProviderState
            ELSE V2_P_Clean
        END AS V2_P_Final,
        
        CASE 
            WHEN V2_P_Clean IS NULL AND V2_PA_Clean IS NULL THEN V2_ProviderState
            ELSE V2_PA_Clean
        END AS V2_PA_Final
        
    FROM StateFiltered
)

-- Final SELECT: Only include legitimate conflicts
SELECT 
    [... all your INSERT columns ...]
FROM LegitimateConflicts
WHERE 
    -- Keep if indeterminate (all NULL on either side)
    (V1_P_Final IS NULL AND V1_PA_Final IS NULL) 
    OR (V2_P_Final IS NULL AND V2_PA_Final IS NULL)
    -- Keep if ANY match found (ANY-to-ANY)
    OR (V1_P_Final = V2_P_Final)
    OR (V1_P_Final = V2_PA_Final)
    OR (V1_PA_Final = V2_P_Final)
    OR (V1_PA_Final = V2_PA_Final);


-- ================================================================================
-- SIMPLIFIED VERSION: If V1/V2 already have NormalizedState column
-- ================================================================================
-- If your V1 and V2 already compute NormalizedState (like I saw in the scripts),
-- you can simplify significantly:

SimplifiedFiltered AS (
    SELECT 
        V1.*,
        V2.*,
        
        -- Just need provider fallback
        (SELECT NULLIF(TRIM(UPPER("Address State")), '') 
         FROM ANALYTICS.BI.DIMPROVIDER 
         WHERE "Provider Id" = V1."ProviderID" 
         LIMIT 1) AS V1_ProviderState,
        
        (SELECT NULLIF(TRIM(UPPER("Address State")), '') 
         FROM ANALYTICS.BI.DIMPROVIDER 
         WHERE "Provider Id" = V2."ProviderID" 
         LIMIT 1) AS V2_ProviderState,
        
        -- V1 already has NormalizedState (from your existing logic)
        -- V2 already has NormalizedState
        
        -- Apply fallback: Use NormalizedState if not NULL, else use ProviderState
        COALESCE(
            NULLIF(V1."NormalizedState", ''),
            (SELECT NULLIF(TRIM(UPPER("Address State")), '') 
             FROM ANALYTICS.BI.DIMPROVIDER 
             WHERE "Provider Id" = V1."ProviderID" 
             LIMIT 1)
        ) AS V1_FinalState,
        
        COALESCE(
            NULLIF(V2."NormalizedState", ''),
            (SELECT NULLIF(TRIM(UPPER("Address State")), '') 
             FROM ANALYTICS.BI.DIMPROVIDER 
             WHERE "Provider Id" = V2."ProviderID" 
             LIMIT 1)
        ) AS V2_FinalState
        
    FROM V1
    INNER JOIN V2 ON [... existing conditions ...]
    WHERE [... existing conditions ...]
      -- Add state filter directly here:
      AND (
          -- Keep if both NULL (indeterminate)
          (V1_FinalState IS NULL AND V2_FinalState IS NULL)
          -- Keep if states match
          OR (V1_FinalState = V2_FinalState)
      )
)


-- ================================================================================
-- ULTRA-SIMPLIFIED: Direct WHERE clause (if NormalizedState exists)
-- ================================================================================
-- Add directly to your existing WHERE clause:

WHERE [... existing conditions ...]
  AND (
      -- Both indeterminate
      (V1."NormalizedState" IS NULL AND V2."NormalizedState" IS NULL)
      -- OR states match
      OR (V1."NormalizedState" = V2."NormalizedState")
  )

-- NOTE: This simplified version assumes:
-- 1. NormalizedState already combines P and PA addresses (with COALESCE)
-- 2. You're okay with not using provider fallback (simpler, but less accurate)
-- 3. You accept some false negatives (missed conflicts when one side is NULL)


-- ================================================================================
-- RECOMMENDATION
-- ================================================================================
-- 
-- Use the FULL INLINE VERSION (StateFiltered + LegitimateConflicts CTEs)
-- 
-- Why:
-- ✅ Matches V5_CORRECTED logic exactly
-- ✅ Handles ALL edge cases (NULL, empty, fallback, ANY-to-ANY)
-- ✅ No function call overhead
-- ✅ Query optimizer can optimize better
-- ✅ Clear and maintainable
-- 
-- Trade-off:
-- ⚠️ More code to maintain (copy to 3 scripts)
-- ⚠️ Changes must be synchronized
-- 
-- Mitigation:
-- - Document clearly that all 3 scripts must be kept in sync
-- - Add comment with version number: /* V5_CORRECTED Logic - Version 1.0 */
-- - Consider generating scripts from template to ensure consistency
-- ================================================================================
