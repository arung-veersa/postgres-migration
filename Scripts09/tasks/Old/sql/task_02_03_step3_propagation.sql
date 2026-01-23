-- ======================================================================
-- TASK 04 - STEP 3: Status Propagation
-- Converted from Snowflake UPDATE_DATA_CONFLICTVISITMAPS_3 procedure
-- 
-- This step propagates status between CONFLICTVISITMAPS and CONFLICTS.
-- Handles various resolution scenarios including single-visit conflicts,
-- all-visits-resolved conflicts, and flag synchronization.
--
-- Queries:
-- 1. sql_queryseconds4_AA - Set UpdatedRFlag on CONFLICTS
-- 2. sql_queryseconds4_A - Set CONFLICTS StatusFlag to 'U' for unresolved
-- 3. sql_queryseconds5 - Resolve single-visit CONFLICTVISITMAPS
-- 4. sql_queryseconds6 - Resolve single-visit CONFLICTS
-- 5. sql_queryseconds7 - Resolve all-visits-resolved CONFLICTVISITMAPS
-- 6. sql_queryseconds8 - Resolve all-or-most-visits-resolved CONFLICTVISITMAPS
-- 7. sql_queryseconds9 - Resolve all-visits-resolved CONFLICTS
-- ======================================================================

-- Query 1: Set UpdatedRFlag = '1' on CONFLICTS that have CONFLICTVISITMAPS in date range
UPDATE {conflict_schema}.conflicts AS CF
SET "UpdatedRFlag" = '1'
FROM {conflict_schema}.conflictvisitmaps AS CVM
WHERE CVM."CONFLICTID" = CF."CONFLICTID"
  AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                 AND (NOW() + INTERVAL '45 days')::date;


-- Query 2: Set CONFLICTS StatusFlag to 'U' and clear UpdatedRFlag
-- For conflicts where CVMs have unresolved status but conflict is not D/I/W/U
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
      AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                     AND (NOW() + INTERVAL '45 days')::date
    GROUP BY CF2."CONFLICTID"
);


-- Query 3: Resolve CONFLICTVISITMAPS for conflicts with StatusFlag R or D and only 1 CVM
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "StatusFlag" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE 'R'
        END,
    "ResolveDate" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ResolveDate", NOW())
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", NOW())
            ELSE COALESCE(CVM."ResolveDate", NOW())
        END,
    "ResolvedBy" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
        END
WHERE CVM."CONFLICTID" IN (
    SELECT CF."CONFLICTID"
    FROM {conflict_schema}.conflicts AS CF
    INNER JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
        ON CVM2."CONFLICTID" = CF."CONFLICTID"
    WHERE CF."StatusFlag" IN ('R', 'D')
      AND CVM2."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                      AND (NOW() + INTERVAL '45 days')::date
    GROUP BY CF."CONFLICTID"
    HAVING COUNT(CVM2."ID") = 1
)
AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                               AND (NOW() + INTERVAL '45 days')::date;


-- Query 4: Resolve CONFLICTS for conflicts with StatusFlag R or D and only 1 CVM
UPDATE {conflict_schema}.conflicts AS CF
SET 
    "StatusFlag" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE 'R'
        END,
    "ResolveDate" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN COALESCE(CF."ResolveDate", NOW())
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", NOW())
            ELSE COALESCE(CF."ResolveDate", NOW())
        END,
    "ResolvedBy" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
        END
FROM {conflict_schema}.conflictvisitmaps AS CVM
WHERE CVM."CONFLICTID" = CF."CONFLICTID"
  AND CF."CONFLICTID" IN (
      SELECT DISTINCT CVM2."CONFLICTID"
      FROM {conflict_schema}.conflictvisitmaps AS CVM2
      WHERE CVM2."CONFLICTID" IN (
          SELECT DISTINCT CVM3."CONFLICTID"
          FROM {conflict_schema}.conflictvisitmaps AS CVM3
          WHERE CVM3."StatusFlag" IN ('R', 'D')
            AND CVM3."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                            AND (NOW() + INTERVAL '45 days')::date
      )
      AND CVM2."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                      AND (NOW() + INTERVAL '45 days')::date
      GROUP BY CVM2."CONFLICTID"
      HAVING COUNT(CVM2."ID") = 1
  )
  AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                 AND (NOW() + INTERVAL '45 days')::date;


-- Query 5: Resolve CONFLICTVISITMAPS where ALL CVMs are resolved (count match)
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "StatusFlag" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE 'R'
        END,
    "ResolveDate" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ResolveDate", NOW())
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", NOW())
            ELSE COALESCE(CVM."ResolveDate", NOW())
        END,
    "ResolvedBy" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
        END
WHERE CVM."CONFLICTID" IN (
    SELECT CF."CONFLICTID"
    FROM {conflict_schema}.conflicts AS CF
    LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
        ON CVM2."CONFLICTID" = CF."CONFLICTID"
        AND CVM2."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                        AND (NOW() + INTERVAL '45 days')::date
    LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM3 
        ON CVM3."CONFLICTID" = CF."CONFLICTID"
        AND CVM3."StatusFlag" IN ('R', 'D')
        AND CVM3."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                        AND (NOW() + INTERVAL '45 days')::date
    WHERE CF."StatusFlag" IN ('R', 'D')
    GROUP BY CF."CONFLICTID"
    HAVING COUNT(DISTINCT CVM2."ID") = COUNT(DISTINCT CVM3."ID")
)
AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                               AND (NOW() + INTERVAL '45 days')::date;


-- Query 6: Resolve CONFLICTVISITMAPS where ALL or (COUNT-1) CVMs are resolved
UPDATE {conflict_schema}.conflictvisitmaps AS CVM
SET 
    "StatusFlag" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE 'R'
        END,
    "ResolveDate" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ResolveDate", NOW())
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", NOW())
            ELSE COALESCE(CVM."ResolveDate", NOW())
        END,
    "ResolvedBy" = 
        CASE 
            WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
        END
WHERE CVM."CONFLICTID" IN (
    SELECT CF."CONFLICTID"
    FROM {conflict_schema}.conflicts AS CF
    LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
        ON CVM2."CONFLICTID" = CF."CONFLICTID"
        AND CVM2."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                        AND (NOW() + INTERVAL '45 days')::date
    LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM3 
        ON CVM3."CONFLICTID" = CF."CONFLICTID"
        AND CVM3."StatusFlag" IN ('R', 'D')
        AND CVM3."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                        AND (NOW() + INTERVAL '45 days')::date
    WHERE CF."StatusFlag" IN ('R', 'D')
    GROUP BY CF."CONFLICTID"
    HAVING COUNT(DISTINCT CVM2."ID") = COUNT(DISTINCT CVM3."ID")
        OR (COUNT(DISTINCT CVM2."ID") - 1) = COUNT(DISTINCT CVM3."ID")
)
AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                               AND (NOW() + INTERVAL '45 days')::date;


-- Query 7: Resolve CONFLICTS where ALL CVMs are resolved but conflict is NOT yet R or D
UPDATE {conflict_schema}.conflicts AS CF
SET 
    "StatusFlag" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN 'D'
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
            ELSE 'R'
        END,
    "ResolveDate" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN COALESCE(CF."ResolveDate", NOW())
            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", NOW())
            ELSE COALESCE(CF."ResolveDate", NOW())
        END,
    "ResolvedBy" = 
        CASE 
            WHEN CF."StatusFlag" = 'D' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
            ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
        END
FROM {conflict_schema}.conflictvisitmaps AS CVM
WHERE CVM."CONFLICTID" = CF."CONFLICTID"
  AND CF."CONFLICTID" IN (
      SELECT CF2."CONFLICTID"
      FROM {conflict_schema}.conflicts AS CF2
      LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM2 
          ON CVM2."CONFLICTID" = CF2."CONFLICTID"
          AND CVM2."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                          AND (NOW() + INTERVAL '45 days')::date
      LEFT JOIN {conflict_schema}.conflictvisitmaps AS CVM3 
          ON CVM3."CONFLICTID" = CF2."CONFLICTID"
          AND CVM3."StatusFlag" IN ('R', 'D')
          AND CVM3."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                          AND (NOW() + INTERVAL '45 days')::date
      WHERE CF2."StatusFlag" NOT IN ('R', 'D')
      GROUP BY CF2."CONFLICTID"
      HAVING COUNT(DISTINCT CVM2."ID") = COUNT(DISTINCT CVM3."ID")
         AND COUNT(DISTINCT CVM2."ID") > 0
         AND COUNT(DISTINCT CVM3."ID") > 0
  )
  AND CVM."VisitDate"::date BETWEEN (NOW() - INTERVAL '2 years')::date 
                                 AND (NOW() + INTERVAL '45 days')::date;
