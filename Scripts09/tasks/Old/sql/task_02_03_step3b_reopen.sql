-- ======================================================================
-- TASK 04 - STEP 3B: Reopen CONFLICTS to 'U' (Unresolved)
-- Original: sql_queryseconds4_A
-- 
-- For conflicts where CVMs have unresolved status but conflict is not D/I/W/U
-- Supports chunked processing via {start_date} and {end_date} placeholders
-- ======================================================================

UPDATE {conflict_schema}.conflicts AS CF
SET 
    "StatusFlag" = 'U',
    "UpdatedRFlag" = NULL
WHERE CF."CONFLICTID" IN (
    SELECT CF2."CONFLICTID"
    FROM {conflict_schema}.conflicts AS CF2
    INNER JOIN {conflict_schema}.conflictvisitmaps AS CVM 
        ON CVM."CONFLICTID" = CF2."CONFLICTID"
    WHERE CF2."StatusFlag" NOT IN ('D', 'I', 'W', 'U')
      AND CVM."StatusFlag" IN ('U')
      AND CVM."VisitDate" >= '{start_date}'::timestamp
      AND CVM."VisitDate" < ('{end_date}'::date + INTERVAL '1 day')
    GROUP BY CF2."CONFLICTID"
);
