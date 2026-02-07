-- ============================================================================
-- TASK_02_03b: Status Flag Lifecycle Management
-- ============================================================================
-- Purpose:
--   Handle all status flag transitions and lifecycle operations that were
--   part of TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_3 in Snowflake.
--
--   This script runs AFTER task02_03_conflict_detection_merge.sql to:
--   - Mark deleted visits as resolved (StatusFlag = 'D')
--   - Auto-resolve missed visits (StatusFlag = 'R')
--   - Cleanup orphaned conflicts
--   - Cascade status flags between CONFLICTVISITMAPS ↔ CONFLICTS tables
--   - Calculate billed rates per minute
--   - Update resolved conflict data from source
--
-- Execution: Run directly in DBeaver (AFTER task02_03 MERGE completes)
--
-- Variables (set these before running):
--   :conflict_schema   - Conflict data schema (e.g., 'conflict_dev')
--   :analytics_schema  - Analytics data schema (e.g., 'analytics_dev')
--
-- Runtime: ~5-10 minutes (estimated)
--
-- NOTE: This script contains 15+ UPDATE operations. They should be run
--       sequentially as each operation depends on the previous state.
-- ============================================================================

-- ============================================================================
-- SECTION 1: Mark Deleted Visits (StatusFlag = 'D')
-- ============================================================================

-- Operation 1a: Mark CONFLICTVISITMAPS as deleted when ConVisitID is deleted
UPDATE :conflict_schema.conflictvisitmaps AS CVM
SET 
    "UpdateFlag" = NULL,
    "StatusFlag" = 'D',
    "ResolveDate" = COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP),
    "ResolvedBy" = COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
FROM :analytics_schema.factvisitcallperformance_deleted_cr AS DELETECR
WHERE 
    CVM."ConVisitID" = DELETECR."Visit Id"::uuid
    AND CVM."StatusFlag" != 'D'
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- Operation 1b: Cascade deleted status to CONFLICTS table
UPDATE :conflict_schema.conflicts CF
SET 
    "StatusFlag" = 'D',
    "ResolveDate" = COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP),
    "ResolvedBy" = COALESCE(CVM."AgencyContact", CVM."ProviderName")
FROM :analytics_schema.factvisitcallperformance_deleted_cr DELETECR
INNER JOIN :conflict_schema.conflictvisitmaps CVM
    ON CVM."VisitID" = DELETECR."Visit Id"::uuid
WHERE 
    CF."StatusFlag" != 'D'
    AND CF."CONFLICTID" = CVM."CONFLICTID"
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- ============================================================================
-- SECTION 2: Auto-Resolve Missed Visits (StatusFlag = 'R')
-- ============================================================================

-- Operation 2a: Update CONFLICTS for missed visits
UPDATE :conflict_schema.conflicts CF
SET 
    "StatusFlag" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN 'D'
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
        ELSE CF."StatusFlag"
    END,
    "ResolveDate" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
        ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
    END,
    "ResolvedBy" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
    END
FROM :conflict_schema.conflictvisitmaps CVM
WHERE 
    CVM.CONFLICTID = CF.CONFLICTID
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- Operation 2b: Update CONFLICTVISITMAPS for missed visits
UPDATE :conflict_schema.conflictvisitmaps CVM
SET 
    "StatusFlag" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN 'D'
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
        ELSE CVM."StatusFlag"
    END,
    "ResolveDate" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
        ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
    END,
    "ResolvedBy" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
    END
WHERE 
    CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- ============================================================================
-- SECTION 3: Cleanup Orphaned Conflicts (UpdateFlag = 1)
-- ============================================================================

-- Operation 3: Mark orphaned conflicts as resolved
UPDATE :conflict_schema.conflictvisitmaps AS CVM
SET 
    "UpdateFlag" = NULL,
    "StatusFlag" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN 'D'
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
        ELSE 'R'
    END,
    "ResolveDate" = COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP),
    "ResolvedBy" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
    END
WHERE 
    CVM."UpdateFlag" = 1
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- ============================================================================
-- SECTION 4: Cascade Status Flags to CONFLICTS Table
-- ============================================================================

-- Operation 4a: Set UpdatedRFlag for conflicts
UPDATE :conflict_schema.conflicts CF
SET "UpdatedRFlag" = '1'
FROM :conflict_schema.conflictvisitmaps CVM
WHERE 
    CVM.CONFLICTID = CF.CONFLICTID
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- Operation 4b: Update CONFLICTS to 'U' status when visits are updated
UPDATE :conflict_schema.conflicts CF
SET 
    "StatusFlag" = 'U',
    "UpdatedRFlag" = NULL
WHERE CF.CONFLICTID IN (
    SELECT CF.CONFLICTID 
    FROM :conflict_schema.conflicts CF 
    INNER JOIN :conflict_schema.conflictvisitmaps CVM ON CVM.CONFLICTID = CF.CONFLICTID 
    WHERE 
        CF."StatusFlag" NOT IN ('D', 'I', 'W', 'U')
        AND CVM."StatusFlag" IN ('U')
        AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days')
    GROUP BY CF.CONFLICTID
);

-- Operation 4c: Resolve single-visit conflicts
UPDATE :conflict_schema.conflictvisitmaps CVM
SET 
    "StatusFlag" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN 'D'
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
        ELSE 'R'
    END,
    "ResolveDate" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
        ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
    END,
    "ResolvedBy" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
    END
WHERE CVM.CONFLICTID IN (
    SELECT CF.CONFLICTID
    FROM :conflict_schema.conflicts CF
    INNER JOIN :conflict_schema.conflictvisitmaps CVM ON CVM.CONFLICTID = CF.CONFLICTID
    WHERE 
        CF."StatusFlag" IN ('R', 'D')
        AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days')
    GROUP BY CF.CONFLICTID
    HAVING COUNT(CVM.ID) = 1
)
AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- Operation 4d: Cascade resolved status to CONFLICTS table (single visit)
UPDATE :conflict_schema.conflicts CF
SET 
    "StatusFlag" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN 'D'
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
        ELSE 'R'
    END,
    "ResolveDate" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
        ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
    END,
    "ResolvedBy" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
    END
FROM :conflict_schema.conflictvisitmaps CVM
WHERE 
    CVM.CONFLICTID = CF.CONFLICTID
    AND CF.CONFLICTID IN (
        SELECT CF.CONFLICTID
        FROM :conflict_schema.conflicts CF
        LEFT JOIN :conflict_schema.conflictvisitmaps CVM ON CVM.CONFLICTID = CF.CONFLICTID 
            AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days')
        LEFT JOIN :conflict_schema.conflictvisitmaps CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID
            AND CVM1."StatusFlag" IN ('R', 'D')
            AND CVM1."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days')
        WHERE CF."StatusFlag" IN ('R', 'D')
        GROUP BY CF.CONFLICTID
        HAVING COUNT(DISTINCT CVM.ID) = 1
    )
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- Operation 4e: Resolve CONFLICTVISITMAPS when all visits resolved/deleted
UPDATE :conflict_schema.conflictvisitmaps CVM
SET 
    "StatusFlag" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN 'D'
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
        ELSE 'R'
    END,
    "ResolveDate" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
        ELSE COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP)
    END,
    "ResolvedBy" = CASE 
        WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
    END
WHERE CVM.CONFLICTID IN (
    SELECT CF.CONFLICTID
    FROM :conflict_schema.conflicts CF
    LEFT JOIN :conflict_schema.conflictvisitmaps CVM ON CVM.CONFLICTID = CF.CONFLICTID
        AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days')
    LEFT JOIN :conflict_schema.conflictvisitmaps CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID
        AND CVM1."StatusFlag" IN ('R', 'D')
        AND CVM1."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days')
    WHERE CF."StatusFlag" IN ('R', 'D')
    GROUP BY CF.CONFLICTID
    HAVING COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID)
)
AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- Operation 4f: Resolve CONFLICTS when all visits resolved/deleted
UPDATE :conflict_schema.conflicts CF
SET 
    "StatusFlag" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN 'D'
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
        ELSE 'R'
    END,
    "ResolveDate" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
        WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
        ELSE COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP)
    END,
    "ResolvedBy" = CASE 
        WHEN CF."StatusFlag" = 'D' THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
        WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
        ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
    END
FROM :conflict_schema.conflictvisitmaps CVM
WHERE 
    CVM.CONFLICTID = CF.CONFLICTID
    AND CF.CONFLICTID IN (
        SELECT CF.CONFLICTID
        FROM :conflict_schema.conflicts CF
        LEFT JOIN :conflict_schema.conflictvisitmaps CVM ON CVM.CONFLICTID = CF.CONFLICTID
            AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days')
        LEFT JOIN :conflict_schema.conflictvisitmaps CVM1 ON CVM1.CONFLICTID = CF.CONFLICTID
            AND CVM1."StatusFlag" IN ('R', 'D')
            AND CVM1."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days')
        WHERE CF."StatusFlag" NOT IN ('R', 'D')
        GROUP BY CF.CONFLICTID
        HAVING COUNT(DISTINCT CVM.ID) = COUNT(DISTINCT CVM1.ID)
           AND COUNT(DISTINCT CVM.ID) > 0
           AND COUNT(DISTINCT CVM1.ID) > 0
    )
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- ============================================================================
-- SECTION 5: Handle NoResponse Flag
-- ============================================================================

-- Operation 5a: Update CONFLICTS with NoResponse flag
UPDATE :conflict_schema.conflicts CF
SET 
    "StatusFlag" = CASE 
        WHEN CF."NoResponseFlag" = 'Yes' THEN 'N'
        ELSE CF."StatusFlag"
    END,
    "ResolveDate" = NULL,
    "ResolvedBy" = NULL
FROM :conflict_schema.conflictvisitmaps CVM
WHERE 
    CVM.CONFLICTID = CF.CONFLICTID
    AND CF."StatusFlag" IN ('U', 'N', 'W', 'I')
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- Operation 5b: Update CONFLICTVISITMAPS with ConNoResponse flag
UPDATE :conflict_schema.conflictvisitmaps CVM
SET 
    "StatusFlag" = CASE 
        WHEN CVM."ConNoResponseFlag" = 'Yes' THEN 'N'
        ELSE CVM."StatusFlag"
    END,
    "ResolveDate" = NULL,
    "ResolvedBy" = NULL
WHERE 
    CVM."StatusFlag" IN ('U', 'N', 'W', 'I')
    AND CVM."VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- ============================================================================
-- SECTION 6: Calculate Coalesced Time Fields
-- ============================================================================

-- Operation 6: Set ShVTSTTime/ShVTENTime coalesced fields
UPDATE :conflict_schema.conflictvisitmaps
SET 
    "ShVTSTTime" = COALESCE("VisitStartTime", "SchStartTime", "InserviceStartDate"),
    "ShVTENTime" = COALESCE("VisitEndTime", "SchEndTime", "InserviceEndDate"),
    "CShVTSTTime" = COALESCE("ConVisitStartTime", "ConSchStartTime", "ConInserviceStartDate"),
    "CShVTENTime" = COALESCE("ConVisitEndTime", "ConSchEndTime", "ConInserviceEndDate")
WHERE 
    "VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- ============================================================================
-- SECTION 7: Calculate Billed Rate Per Minute
-- ============================================================================

-- Operation 7: Calculate BilledRateMinute and ConBilledRateMinute
UPDATE :conflict_schema.conflictvisitmaps
SET 
    "BilledRateMinute" = CASE 
        WHEN "Billed" = 'yes' AND "RateType" = 'Hourly' AND "BillRateBoth" > 0 
            THEN "BillRateBoth" / 60
        WHEN "Billed" = 'yes' AND "RateType" = 'Daily' AND "BillRateBoth" > 0 AND "BilledHours" > 0 
            THEN ("BillRateBoth" / "BilledHours") / 60
        WHEN "Billed" = 'yes' AND "RateType" = 'Visit' AND "BillRateBoth" > 0 AND "BilledHours" > 0 
            THEN ("BillRateBoth" / "BilledHours") / 60
        WHEN "Billed" != 'yes' AND "RateType" = 'Hourly' AND "BillRateBoth" > 0 
            THEN "BillRateBoth" / 60
        WHEN "Billed" != 'yes' AND "RateType" = 'Daily' AND "BillRateBoth" > 0 
             AND "SchStartTime" IS NOT NULL AND "SchEndTime" IS NOT NULL 
             AND "SchStartTime" != "SchEndTime" 
            THEN ("BillRateBoth" / (EXTRACT(EPOCH FROM ("SchEndTime" - "SchStartTime")) / 3600)) / 60
        WHEN "Billed" != 'yes' AND "RateType" = 'Visit' AND "BillRateBoth" > 0 
             AND "SchStartTime" IS NOT NULL AND "SchEndTime" IS NOT NULL 
             AND "SchStartTime" != "SchEndTime" 
            THEN ("BillRateBoth" / (EXTRACT(EPOCH FROM ("SchEndTime" - "SchStartTime")) / 3600)) / 60
        ELSE 0
    END,
    "ConBilledRateMinute" = CASE 
        WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Hourly' AND "ConBillRateBoth" > 0 
            THEN "ConBillRateBoth" / 60
        WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Daily' AND "ConBillRateBoth" > 0 AND "ConBilledHours" > 0 
            THEN ("ConBillRateBoth" / "ConBilledHours") / 60
        WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Visit' AND "ConBillRateBoth" > 0 AND "ConBilledHours" > 0 
            THEN ("ConBillRateBoth" / "ConBilledHours") / 60
        WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Hourly' AND "ConBillRateBoth" > 0 
            THEN "ConBillRateBoth" / 60
        WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Daily' AND "ConBillRateBoth" > 0 
             AND "ConSchStartTime" IS NOT NULL AND "ConSchEndTime" IS NOT NULL 
             AND "ConSchStartTime" != "ConSchEndTime" 
            THEN ("ConBillRateBoth" / (EXTRACT(EPOCH FROM ("ConSchEndTime" - "ConSchStartTime")) / 3600)) / 60
        WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Visit' AND "ConBillRateBoth" > 0 
             AND "ConSchStartTime" IS NOT NULL AND "ConSchEndTime" IS NOT NULL 
             AND "ConSchStartTime" != "ConSchEndTime" 
            THEN ("ConBillRateBoth" / (EXTRACT(EPOCH FROM ("ConSchEndTime" - "ConSchStartTime")) / 3600)) / 60
        ELSE 0
    END
WHERE 
    "VisitDate"::date BETWEEN (CURRENT_DATE - INTERVAL '2 years') AND (CURRENT_DATE + INTERVAL '45 days');

-- ============================================================================
-- SECTION 8: Update Resolved Conflict Data from Source
-- ============================================================================
-- NOTE: The Snowflake script has 4 complex UPDATE operations that refresh
--       PRIMARY and CONFLICTING visit data from FACTVISITCALLPERFORMANCE_CR
--       based on StatusFlag = 'R' (Resolved) or 'D' (Deleted).
--
--       These operations are EXPENSIVE (large joins with 58M row table) and
--       should be tested carefully. They are OPTIONAL for initial testing.
--
--       If needed, they can be ported as separate operations after validating
--       the core status flag logic above.
-- ============================================================================

-- ============================================================================
-- TASK_02_03b Complete
-- ============================================================================
-- Expected outcome:
--   - Deleted visits marked with StatusFlag = 'D'
--   - Missed visits auto-resolved (StatusFlag = 'R')
--   - Orphaned conflicts cleaned up
--   - Status flags cascaded between CONFLICTVISITMAPS ↔ CONFLICTS
--   - Billing rates calculated
--   - NoResponse flags handled
--
-- Next: Run TASK_04 (assign CONFLICTID)
-- ============================================================================
