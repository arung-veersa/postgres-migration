"""
Task 03 - Status Management & Computed Columns

Consolidates all post-conflict-creation processing from the original Snowflake
stored procedures:
  - TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_3.sql  (status cascade, aggregation)
  - TASK_03_INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_2.sql  (computed columns)

Three groups of operations, all run sequentially (14 SQL steps + 1 fetch = 15 total):

  Group B (Snowflake-dependent, delta via "Visit Updated Timestamp"):
    Step 0:  Fetch recently-deleted Visit IDs from Snowflake (delta=lookback_hours)
    Step 1:  Mark CVM as 'D' when VisitID or ConVisitID is a deleted visit
             (UNION CTE for hash-join-friendly equi-joins on both columns)
    Step 2:  Cascade 'D' to parent conflicts when VisitID is a deleted visit

  Group A (pure PostgreSQL status cascade, 9 steps):
    Step 3:  IsMissed cascade -> conflicts
    Step 4:  IsMissed cascade -> conflictvisitmaps
    Step 5:  UpdateFlag orphan cleanup -> conflictvisitmaps
    Step 6:  Aggregation: mark UpdatedRFlag='1' on conflicts
    Step 7:  Aggregation: propagate StatusFlag='U' to conflicts
    Step 8:  Cascade-resolve CVM under R/D conflicts (combined: singleton +
             near-all-resolved + all-resolved, single CTE-driven scan)
    Step 9:  Cascade-resolve CF where all CVM are R/D (combined: singleton_cf +
             all_cvm_resolved_cf, single CTE-driven scan)
    Step 10: NoResponseFlag -> conflicts (reset to 'N')
    Step 11: NoResponseFlag -> conflictvisitmaps (reset to 'N', forced seq scan)

  Group C (computed columns, 3 steps):
    Step 12: ShVTSTTime / CShVTSTTime (COALESCE of visit/sch/inservice times)
    Step 13: BilledRateMinute / ConBilledRateMinute (rate-per-minute, epsilon check)
    Step 14: ReverseUUID (canonical pair key, WHERE IS NULL only)

Reference SPs:
  - Scripts13/snowflake/Task Scripts/TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_3.sql
  - Scripts13/snowflake/Task Scripts/TASK_03_INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_2.sql
"""

import os
import time
from typing import Optional, Callable, Dict, Any, List

import psycopg2
import psycopg2.extras

from config.settings import Settings
from lib.connections import ConnectionFactory
from lib.utils import get_logger, format_duration

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer for env var {name}={value!r}, using default={default}")
        return default


# ---------------------------------------------------------------------------
# Date window helper
# ---------------------------------------------------------------------------

_DATE_WINDOW_SQL = """
    "VisitDate" >= CURRENT_DATE - INTERVAL '{lookback_years} years'
                          AND "VisitDate" < CURRENT_DATE + INTERVAL '{lookforward_days} days' + INTERVAL '1 day'
"""


def _dw(lookback_years: int, lookforward_days: int) -> str:
    """Return the date window SQL fragment."""
    return _DATE_WINDOW_SQL.format(
        lookback_years=lookback_years,
        lookforward_days=lookforward_days,
    )


# ---------------------------------------------------------------------------
# Billed-rate CASE expressions (reused in SET + WHERE epsilon check)
# ::real cast ensures values are stored in the column's native float4 type.
# WHERE uses ABS() > 0.0001 epsilon to avoid float precision false positives.
# ---------------------------------------------------------------------------

_BRM_CASE = """(CASE
    WHEN "Billed" = 'yes' AND "RateType" = 'Hourly' AND "BillRateBoth" > 0
        THEN "BillRateBoth" / 60.0
    WHEN "Billed" = 'yes' AND "RateType" = 'Daily' AND "BillRateBoth" > 0 AND "BilledHours" > 0
        THEN ("BillRateBoth" / "BilledHours") / 60.0
    WHEN "Billed" = 'yes' AND "RateType" = 'Visit' AND "BillRateBoth" > 0 AND "BilledHours" > 0
        THEN ("BillRateBoth" / "BilledHours") / 60.0
    WHEN "Billed" != 'yes' AND "RateType" = 'Hourly' AND "BillRateBoth" > 0
        THEN "BillRateBoth" / 60.0
    WHEN "Billed" != 'yes' AND "RateType" = 'Daily' AND "BillRateBoth" > 0
         AND "SchStartTime" IS NOT NULL AND "SchEndTime" IS NOT NULL
         AND "SchStartTime" != "SchEndTime"
        THEN ("BillRateBoth" / (EXTRACT(EPOCH FROM ("SchEndTime" - "SchStartTime")) / 3600.0)) / 60.0
    WHEN "Billed" != 'yes' AND "RateType" = 'Visit' AND "BillRateBoth" > 0
         AND "SchStartTime" IS NOT NULL AND "SchEndTime" IS NOT NULL
         AND "SchStartTime" != "SchEndTime"
        THEN ("BillRateBoth" / (EXTRACT(EPOCH FROM ("SchEndTime" - "SchStartTime")) / 3600.0)) / 60.0
    ELSE 0
END)::real"""

_CBRM_CASE = """(CASE
    WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Hourly' AND "ConBillRateBoth" > 0
        THEN "ConBillRateBoth" / 60.0
    WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Daily' AND "ConBillRateBoth" > 0 AND "ConBilledHours" > 0
        THEN ("ConBillRateBoth" / "ConBilledHours") / 60.0
    WHEN "ConBilled" = 'yes' AND "ConRateType" = 'Visit' AND "ConBillRateBoth" > 0 AND "ConBilledHours" > 0
        THEN ("ConBillRateBoth" / "ConBilledHours") / 60.0
    WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Hourly' AND "ConBillRateBoth" > 0
        THEN "ConBillRateBoth" / 60.0
    WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Daily' AND "ConBillRateBoth" > 0
         AND "ConSchStartTime" IS NOT NULL AND "ConSchEndTime" IS NOT NULL
         AND "ConSchStartTime" != "ConSchEndTime"
        THEN ("ConBillRateBoth" / (EXTRACT(EPOCH FROM ("ConSchEndTime" - "ConSchStartTime")) / 3600.0)) / 60.0
    WHEN "ConBilled" != 'yes' AND "ConRateType" = 'Visit' AND "ConBillRateBoth" > 0
         AND "ConSchStartTime" IS NOT NULL AND "ConSchEndTime" IS NOT NULL
         AND "ConSchStartTime" != "ConSchEndTime"
        THEN ("ConBillRateBoth" / (EXTRACT(EPOCH FROM ("ConSchEndTime" - "ConSchStartTime")) / 3600.0)) / 60.0
    ELSE 0
END)::real"""


# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------

def _build_steps(schema: str, lookback_years: int, lookforward_days: int) -> List[Dict[str, Any]]:
    """
    Build the ordered list of SQL steps.

    Each step is a dict with:
      - name: identifier for logging
      - description: human-readable description
      - group: 'A', 'B', or 'C'
      - sql: the SQL to execute (or None for custom-handler steps)
    """
    dw_cvm = _dw(lookback_years, lookforward_days)
    # For JOINs involving CVM, we need the date window on CVM."VisitDate"
    dw_cvm_alias = dw_cvm.replace('"VisitDate"', 'CVM."VisitDate"')

    steps = [
        # ------------------------------------------------------------------
        # GROUP B: Deleted visit handling (steps 1-2)
        # Step 0 (fetch deleted IDs) is handled by custom code before these.
        # ------------------------------------------------------------------
        {
            'name': 'deleted_visits_cvm',
            'description': 'Mark CVM StatusFlag=D when VisitID or ConVisitID is a deleted visit',
            'group': 'B',
            # UNION CTE splits the OR into two equi-joins so PostgreSQL can
            # use hash joins instead of a catastrophic nested-loop cross-join.
            'sql': f"""
                WITH del_matches AS (
                    SELECT CVM."ID"
                    FROM {schema}.conflictvisitmaps AS CVM
                    INNER JOIN _tmp_deleted_visits AS DEL
                        ON CVM."ConVisitID"::text = DEL."VisitID"
                    WHERE CVM."StatusFlag" != 'D'
                      AND {dw_cvm_alias}
                    UNION
                    SELECT CVM."ID"
                    FROM {schema}.conflictvisitmaps AS CVM
                    INNER JOIN _tmp_deleted_visits AS DEL
                        ON CVM."VisitID"::text = DEL."VisitID"
                    WHERE CVM."StatusFlag" != 'D'
                      AND {dw_cvm_alias}
                )
                UPDATE {schema}.conflictvisitmaps AS CVM
                SET "UpdateFlag" = NULL,
                    "StatusFlag" = 'D',
                    "ResolveDate" = COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP),
                    "ResolvedBy" = COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
                FROM del_matches AS dm
                WHERE CVM."ID" = dm."ID"
            """,
        },
        {
            'name': 'deleted_visits_cf',
            'description': 'Cascade StatusFlag=D to conflicts when VisitID is deleted',
            'group': 'B',
            'sql': f"""
                UPDATE {schema}.conflicts AS CF
                SET "StatusFlag" = 'D',
                    "ResolveDate" = COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP),
                    "ResolvedBy" = COALESCE(CVM."AgencyContact", CVM."ProviderName")
                FROM _tmp_deleted_visits AS DEL
                INNER JOIN {schema}.conflictvisitmaps AS CVM
                    ON CVM."VisitID"::text = DEL."VisitID"
                WHERE CF."StatusFlag" != 'D'
                  AND CF."CONFLICTID" = CVM."CONFLICTID"
                  AND {dw_cvm_alias}
            """,
        },

        # ------------------------------------------------------------------
        # GROUP A: Pure PostgreSQL status cascade (steps 3-14)
        # ------------------------------------------------------------------

        # Step 3: IsMissed cascade -> conflicts
        # PG optimisation: only touch CF where IsMissed/ConIsMissed is TRUE
        # and status would actually change (not already R/D)
        {
            'name': 'ismissed_cascade_cf',
            'description': 'IsMissed cascade: set CF StatusFlag=R when CVM.IsMissed/ConIsMissed',
            'group': 'A',
            'sql': f"""
                UPDATE {schema}.conflicts AS CF
                SET "StatusFlag" = 'R',
                    "ResolveDate" = COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP),
                    "ResolvedBy" =
                        CASE
                            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
                            ELSE COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
                        END
                FROM {schema}.conflictvisitmaps AS CVM
                WHERE CVM."CONFLICTID" = CF."CONFLICTID"
                  AND (CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE)
                  AND CF."StatusFlag" NOT IN ('R', 'D')
                  AND {dw_cvm_alias}
            """,
        },

        # Step 4: IsMissed cascade -> conflictvisitmaps
        # PG optimisation: only touch CVM where IsMissed/ConIsMissed is TRUE
        # and status would actually change (not already R/D)
        {
            'name': 'ismissed_cascade_cvm',
            'description': 'IsMissed cascade: set CVM StatusFlag=R when IsMissed/ConIsMissed',
            'group': 'A',
            'sql': f"""
                UPDATE {schema}.conflictvisitmaps AS CVM
                SET "StatusFlag" = 'R',
                    "ResolveDate" = COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP),
                    "ResolvedBy" =
                        CASE
                            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
                            ELSE COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
                        END
                WHERE (CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE)
                  AND CVM."StatusFlag" NOT IN ('R', 'D')
                  AND {dw_cvm}
            """,
        },

        # Step 5: UpdateFlag orphan cleanup
        {
            'name': 'updateflag_orphan_cleanup',
            'description': 'Resolve CVM with UpdateFlag=1 (orphaned re-detection markers)',
            'group': 'A',
            'sql': f"""
                UPDATE {schema}.conflictvisitmaps AS CVM
                SET "UpdateFlag" = NULL,
                    "StatusFlag" =
                        CASE
                            WHEN CVM."StatusFlag" = 'D' THEN 'D'
                            WHEN CVM."IsMissed" = TRUE OR CVM."ConIsMissed" = TRUE THEN 'R'
                            ELSE 'R'
                        END,
                    "ResolveDate" = COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP),
                    "ResolvedBy" =
                        CASE
                            WHEN CVM."StatusFlag" = 'D' THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
                            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
                            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
                            ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
                        END
                WHERE CVM."UpdateFlag" = 1
                  AND {dw_cvm}
            """,
        },

        # Step 6: Aggregation mark - set UpdatedRFlag='1' on conflicts with CVM in window
        # PG optimisation: EXISTS avoids massive CVM×CF join; only checks 1 CVM per CF
        {
            'name': 'aggregation_mark_updatedrflag',
            'description': 'Set UpdatedRFlag=1 on conflicts with CVM in date window',
            'group': 'A',
            'sql': f"""
                UPDATE {schema}.conflicts AS CF
                SET "UpdatedRFlag" = '1'
                WHERE (CF."UpdatedRFlag" IS NULL OR CF."UpdatedRFlag" != '1')
                  AND EXISTS (
                    SELECT 1 FROM {schema}.conflictvisitmaps AS CVM
                    WHERE CVM."CONFLICTID" = CF."CONFLICTID"
                      AND CVM."VisitDate" >= CURRENT_DATE - INTERVAL '{lookback_years} years'
                      AND CVM."VisitDate" < CURRENT_DATE + INTERVAL '{lookforward_days} days' + INTERVAL '1 day'
                  )
            """,
        },

        # Step 7: Aggregation - propagate StatusFlag='U' from CVM to conflicts
        # CTE-driven: start from selective CVM StatusFlag='U', join to CF
        {
            'name': 'aggregation_status_u_propagation',
            'description': 'Propagate StatusFlag=U from CVM to parent conflicts',
            'group': 'A',
            'sql': f"""
                WITH cf_with_u_cvm AS (
                    SELECT DISTINCT CVM."CONFLICTID"
                    FROM {schema}.conflictvisitmaps AS CVM
                    WHERE CVM."StatusFlag" = 'U'
                      AND CVM."VisitDate" >= CURRENT_DATE - INTERVAL '{lookback_years} years'
                      AND CVM."VisitDate" < CURRENT_DATE + INTERVAL '{lookforward_days} days' + INTERVAL '1 day'
                )
                UPDATE {schema}.conflicts AS CF
                SET "StatusFlag" = 'U',
                    "UpdatedRFlag" = NULL
                FROM cf_with_u_cvm AS u
                WHERE CF."CONFLICTID" = u."CONFLICTID"
                  AND CF."StatusFlag" NOT IN ('D', 'I', 'W', 'U')
            """,
        },

        # Step 8: Combined CVM cascade - single scan replaces 3 separate steps
        # Resolves non-R/D CVM where parent CF is R/D and at most 1 non-R/D CVM remains
        # Covers: singleton (1 total CVM), near-all-resolved (1 non-R/D left),
        #         and all-resolved (safety net: 0 non-R/D left, no-op by construction)
        {
            'name': 'cascade_resolve_cvm',
            'description': 'Cascade-resolve CVM under R/D conflicts (singleton + near-all-resolved)',
            'group': 'A',
            'sql': f"""
                WITH active_cvm AS (
                    SELECT DISTINCT CVM."CONFLICTID"
                    FROM {schema}.conflictvisitmaps AS CVM
                    INNER JOIN {schema}.conflicts AS CF
                        ON CF."CONFLICTID" = CVM."CONFLICTID"
                    WHERE CVM."StatusFlag" NOT IN ('R', 'D')
                      AND CF."StatusFlag" IN ('R', 'D')
                      AND {dw_cvm_alias}
                ),
                resolve_ids AS (
                    SELECT a."CONFLICTID"
                    FROM active_cvm a
                    INNER JOIN {schema}.conflictvisitmaps AS CVM2
                        ON CVM2."CONFLICTID" = a."CONFLICTID"
                        AND CVM2."VisitDate" >= CURRENT_DATE - INTERVAL '{lookback_years} years'
                        AND CVM2."VisitDate" < CURRENT_DATE + INTERVAL '{lookforward_days} days' + INTERVAL '1 day'
                    GROUP BY a."CONFLICTID"
                    HAVING COUNT(CVM2."ID") - COUNT(CASE WHEN CVM2."StatusFlag" IN ('R','D') THEN 1 END) <= 1
                )
                UPDATE {schema}.conflictvisitmaps AS CVM
                SET "StatusFlag" = 'R',
                    "ResolveDate" = COALESCE(CVM."ResolveDate", CURRENT_TIMESTAMP),
                    "ResolvedBy" =
                        CASE
                            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
                            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
                            ELSE COALESCE(CVM."ResolvedBy", CVM."ConAgencyContact", CVM."ConProviderName")
                        END
                FROM resolve_ids AS r
                WHERE CVM."CONFLICTID" = r."CONFLICTID"
                  AND CVM."StatusFlag" NOT IN ('R', 'D')
                  AND {dw_cvm}
            """,
        },

        # Step 9: Combined CF cascade - single scan replaces 2 separate steps
        # Resolves non-R/D CF where all CVM in date window are R/D
        # Covers: singleton_cf (1 CVM total, that CVM is R/D) and
        #         all_cvm_resolved_cf (all CVM are R/D but CF is not)
        {
            'name': 'cascade_resolve_cf',
            'description': 'Cascade-resolve conflicts where all CVM are R/D',
            'group': 'A',
            'sql': f"""
                WITH active_cf AS (
                    SELECT CF."CONFLICTID"
                    FROM {schema}.conflicts AS CF
                    WHERE CF."StatusFlag" NOT IN ('R', 'D')
                ),
                all_cvm_rd AS (
                    SELECT a."CONFLICTID"
                    FROM active_cf a
                    INNER JOIN {schema}.conflictvisitmaps AS CVM
                        ON CVM."CONFLICTID" = a."CONFLICTID"
                        AND CVM."VisitDate" >= CURRENT_DATE - INTERVAL '{lookback_years} years'
                        AND CVM."VisitDate" < CURRENT_DATE + INTERVAL '{lookforward_days} days' + INTERVAL '1 day'
                    GROUP BY a."CONFLICTID"
                    HAVING COUNT(CVM."ID") > 0
                       AND COUNT(CVM."ID") = COUNT(CASE WHEN CVM."StatusFlag" IN ('R','D') THEN 1 END)
                )
                UPDATE {schema}.conflicts AS CF
                SET "StatusFlag" = 'R',
                    "ResolveDate" = COALESCE(CF."ResolveDate", CURRENT_TIMESTAMP),
                    "ResolvedBy" =
                        CASE
                            WHEN CVM."IsMissed" = TRUE THEN COALESCE(CVM."AgencyContact", CVM."ProviderName")
                            WHEN CVM."ConIsMissed" = TRUE THEN COALESCE(CVM."ConAgencyContact", CVM."ConProviderName")
                            ELSE COALESCE(CF."ResolvedBy", CVM."AgencyContact", CVM."ProviderName")
                        END
                FROM all_cvm_rd AS r
                INNER JOIN {schema}.conflictvisitmaps AS CVM
                    ON CVM."CONFLICTID" = r."CONFLICTID"
                    AND CVM."StatusFlag" IN ('R', 'D')
                WHERE CF."CONFLICTID" = r."CONFLICTID"
                  AND CF."StatusFlag" NOT IN ('R', 'D')
                  AND {dw_cvm_alias}
            """,
        },

        # Step 10: NoResponseFlag -> conflicts
        # PG optimisation: EXISTS avoids full CVM join; only ~241 CF rows need the check
        {
            'name': 'noresponse_flag_cf',
            'description': 'Reset CF to N when NoResponseFlag=Yes (active statuses only)',
            'group': 'A',
            'sql': f"""
                UPDATE {schema}.conflicts AS CF
                SET "StatusFlag" = 'N',
                    "ResolveDate" = NULL,
                    "ResolvedBy" = NULL
                WHERE CF."NoResponseFlag" = 'Yes'
                  AND CF."StatusFlag" IN ('U', 'W', 'I')
                  AND EXISTS (
                    SELECT 1 FROM {schema}.conflictvisitmaps AS CVM
                    WHERE CVM."CONFLICTID" = CF."CONFLICTID"
                      AND CVM."VisitDate" >= CURRENT_DATE - INTERVAL '{lookback_years} years'
                      AND CVM."VisitDate" < CURRENT_DATE + INTERVAL '{lookforward_days} days' + INTERVAL '1 day'
                  )
            """,
        },

        # Step 11: NoResponseFlag -> conflictvisitmaps
        # PG optimisation: force seq scan -- partial index on StatusFlag causes
        # catastrophic random-I/O plan (CONFLICTID is leading column but not filtered).
        # Also skip no-op updates where row is already in target state.
        {
            'name': 'noresponse_flag_cvm',
            'description': 'Reset CVM to N when ConNoResponseFlag=Yes (active statuses only)',
            'group': 'A',
            'sql': f"""
                SET LOCAL enable_indexscan = OFF;
                SET LOCAL enable_bitmapscan = OFF;
                UPDATE {schema}.conflictvisitmaps AS CVM
                SET "StatusFlag" = 'N',
                    "ResolveDate" = NULL,
                    "ResolvedBy" = NULL
                WHERE CVM."ConNoResponseFlag" = 'Yes'
                  AND CVM."StatusFlag" NOT IN ('R', 'D')
                  AND (CVM."StatusFlag" != 'N'
                       OR CVM."ResolveDate" IS NOT NULL
                       OR CVM."ResolvedBy" IS NOT NULL)
                  AND {dw_cvm}
            """,
        },

        # ------------------------------------------------------------------
        # GROUP C: Computed columns (steps 12-14)
        # ------------------------------------------------------------------

        # Step 12: ShVTSTTime / CShVTSTTime / ShVTENTime / CShVTENTime
        # PG optimisation: only update rows where computed value differs from current
        {
            'name': 'computed_time_columns',
            'description': 'Compute ShVTSTTime/ShVTENTime/CShVTSTTime/CShVTENTime (COALESCE)',
            'group': 'C',
            'sql': f"""
                UPDATE {schema}.conflictvisitmaps
                SET "ShVTSTTime"  = COALESCE("VisitStartTime", "SchStartTime", "InserviceStartDate"),
                    "ShVTENTime"  = COALESCE("VisitEndTime", "SchEndTime", "InserviceEndDate"),
                    "CShVTSTTime" = COALESCE("ConVisitStartTime", "ConSchStartTime", "ConInserviceStartDate"),
                    "CShVTENTime" = COALESCE("ConVisitEndTime", "ConSchEndTime", "ConInserviceEndDate")
                WHERE {dw_cvm}
                  AND (
                    "ShVTSTTime"  IS DISTINCT FROM COALESCE("VisitStartTime", "SchStartTime", "InserviceStartDate")
                    OR "ShVTENTime"  IS DISTINCT FROM COALESCE("VisitEndTime", "SchEndTime", "InserviceEndDate")
                    OR "CShVTSTTime" IS DISTINCT FROM COALESCE("ConVisitStartTime", "ConSchStartTime", "ConInserviceStartDate")
                    OR "CShVTENTime" IS DISTINCT FROM COALESCE("ConVisitEndTime", "ConSchEndTime", "ConInserviceEndDate")
                  )
            """,
        },

        # Step 13: BilledRateMinute / ConBilledRateMinute (rate-per-minute)
        # SET uses ::real to store values in the column's native type.
        # WHERE uses epsilon (0.0001) instead of IS DISTINCT FROM to avoid
        # false positives from float precision drift (float8 CASE vs real column).
        # COALESCE(..., 1) handles NULLs: if column is NULL, treat as needing update.
        # Also force seq scan to avoid partial-index random I/O trap.
        {
            'name': 'computed_billed_rate',
            'description': 'Compute BilledRateMinute and ConBilledRateMinute',
            'group': 'C',
            'sql': f"""
                SET LOCAL enable_indexscan = OFF;
                SET LOCAL enable_bitmapscan = OFF;
                UPDATE {schema}.conflictvisitmaps
                SET "BilledRateMinute" = {_BRM_CASE},
                    "ConBilledRateMinute" = {_CBRM_CASE}
                WHERE {dw_cvm}
                  AND (COALESCE(ABS("BilledRateMinute" - {_BRM_CASE}), 1) > 0.0001
                       OR COALESCE(ABS("ConBilledRateMinute" - {_CBRM_CASE}), 1) > 0.0001)
            """,
        },

        # Step 14: ReverseUUID (only for new rows where IS NULL)
        {
            'name': 'computed_reverse_uuid',
            'description': 'Compute ReverseUUID for new rows (WHERE IS NULL)',
            'group': 'C',
            'sql': f"""
                UPDATE {schema}.conflictvisitmaps
                SET "ReverseUUID" = LEAST(
                        CONCAT("VisitID"::text, '~', "AppVisitID"),
                        CONCAT("ConVisitID"::text, '~', "ConAppVisitID")
                    ) || '_' || GREATEST(
                        CONCAT("VisitID"::text, '~', "AppVisitID"),
                        CONCAT("ConVisitID"::text, '~', "ConAppVisitID")
                    )
                WHERE "ReverseUUID" IS NULL
                  AND "VisitID" IS NOT NULL
                  AND "AppVisitID" IS NOT NULL
                  AND "ConVisitID" IS NOT NULL
                  AND "ConAppVisitID" IS NOT NULL
                  AND {dw_cvm}
            """,
        },
    ]

    return steps


# ---------------------------------------------------------------------------
# Snowflake: fetch deleted Visit IDs
# ---------------------------------------------------------------------------

_SF_DELETED_VISITS_QUERY = """
    SELECT DISTINCT "Visit Id"
    FROM {sf_database}.{sf_schema}.FACTVISITCALLPERFORMANCE_DELETED_CR
    WHERE DATE("Visit Date") BETWEEN DATEADD(year, -{lookback_years}, GETDATE())
                                 AND DATEADD(day, {lookforward_days}, GETDATE())
      AND "Visit Updated Timestamp" >= DATEADD(HOUR, -{lookback_hours}, GETDATE())
"""


def _fetch_deleted_visit_ids(
    sf_manager,
    db_names: Dict[str, str],
    lookback_years: int,
    lookforward_days: int,
    lookback_hours: int,
) -> List[str]:
    """Query Snowflake for deleted Visit IDs updated within lookback_hours."""
    query = _SF_DELETED_VISITS_QUERY.format(
        sf_database=db_names['sf_database'],
        sf_schema=db_names['sf_schema'],
        lookback_years=lookback_years,
        lookforward_days=lookforward_days,
        lookback_hours=lookback_hours,
    )
    rows = sf_manager.execute_query(query)
    return [str(r[0]) for r in rows if r[0]]


def _load_deleted_ids_to_pg(
    pg_conn,
    deleted_ids: List[str],
) -> int:
    """Create temp table and bulk-insert deleted Visit IDs into PostgreSQL."""
    cursor = pg_conn.cursor()
    try:
        cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _tmp_deleted_visits (
                "VisitID" TEXT NOT NULL
            ) ON COMMIT PRESERVE ROWS
        """)
        cursor.execute("TRUNCATE _tmp_deleted_visits")

        if deleted_ids:
            psycopg2.extras.execute_batch(
                cursor,
                'INSERT INTO _tmp_deleted_visits ("VisitID") VALUES (%s)',
                [(vid,) for vid in deleted_ids],
                page_size=1000,
            )
            # Index speeds up hash/merge joins against CVM VisitID/ConVisitID
            cursor.execute('CREATE INDEX ON _tmp_deleted_visits ("VisitID")')

        pg_conn.commit()
        return len(deleted_ids)
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Step executor
# ---------------------------------------------------------------------------

def _execute_step(
    pg_conn,
    step: Dict[str, Any],
    step_num: int,
    total_steps: int,
) -> Dict[str, Any]:
    """Execute a single SQL step, commit, and return timing + rowcount."""
    start = time.time()
    label = f"[{step_num}/{total_steps}]"

    cursor = pg_conn.cursor()
    try:
        cursor.execute(step['sql'])
        rowcount = cursor.rowcount
        pg_conn.commit()
        duration = time.time() - start

        logger.info(
            f"  {label} {step['name']}: {rowcount:,} rows ({duration:.1f}s) "
            f"-- {step['description']}"
        )
        return {
            'name': step['name'],
            'group': step['group'],
            'rows_affected': rowcount,
            'duration_seconds': round(duration, 2),
            'status': 'ok',
        }
    except Exception as e:
        pg_conn.rollback()
        duration = time.time() - start
        logger.error(f"  {label} {step['name']}: FAILED ({duration:.1f}s) -- {e}")
        return {
            'name': step['name'],
            'group': step['group'],
            'rows_affected': 0,
            'duration_seconds': round(duration, 2),
            'status': 'error',
            'error': str(e),
        }
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Main action
# ---------------------------------------------------------------------------

def run_task03_status_management(
    settings: Settings,
    shutdown_check: Optional[Callable[[], bool]] = None,
) -> dict:
    """
    Execute post-conflict-creation status management and computed columns.

    Runs steps sequentially:
      - Group B: Deleted visit handling (Snowflake delta + PG) -- uses
                 "Visit Updated Timestamp" delta filter (lookback_hours) to
                 scan only recently-changed rows from 25.5M-row deleted table.
                 Disabled by default; enable via enable_phase_b config or
                 ENABLE_PHASE_B=1 env var.
      - Group A: Status cascade (pure PG, 9 steps)
      - Group C: Computed columns (pure PG, 3 steps)
    """
    sf_config = settings.get_snowflake_config()
    pg_config = settings.get_postgres_config()
    task_params = settings.get_task03_parameters()
    db_names = settings.get_database_names()

    lookback_years = _get_env_int('LOOKBACK_YEARS', task_params.get('lookback_years', 2))
    lookforward_days = _get_env_int('LOOKFORWARD_DAYS', task_params.get('lookforward_days', 45))
    lookback_hours = _get_env_int('LOOKBACK_HOURS', task_params.get('lookback_hours', 36))
    schema = db_names['pg_schema']

    # Phase B (deleted visit handling) queries Snowflake
    # FACTVISITCALLPERFORMANCE_DELETED_CR using a delta filter on
    # "Visit Updated Timestamp" to avoid scanning all 25.5M rows.
    # Controlled via config.json task03_parameters.enable_phase_b (default: false)
    # or env var ENABLE_PHASE_B=1 (env var overrides config).
    enable_phase_b_env = os.environ.get('ENABLE_PHASE_B')
    if enable_phase_b_env is not None:
        enable_phase_b = enable_phase_b_env == '1'
    else:
        enable_phase_b = task_params.get('enable_phase_b', False)

    logger.info("=" * 60)
    logger.info("TASK 03 - Status Management & Computed Columns")
    logger.info("=" * 60)
    logger.info("Configuration:")
    logger.info(f"  Date window: -{lookback_years}Y to +{lookforward_days}D")
    logger.info(f"  Delta lookback: {lookback_hours}h")
    logger.info(f"  Schema: {schema}")
    logger.info(f"  Phase B (deleted visits): {'ENABLED' if enable_phase_b else 'DISABLED'}")

    conn_factory = ConnectionFactory(sf_config, pg_config)
    step_results: List[Dict[str, Any]] = []
    total_errors = 0

    try:
        pg_manager = conn_factory.get_postgres_manager()
        pg_conn = pg_manager.get_connection(database=db_names['pg_database'])

        # Build all steps
        steps = _build_steps(schema, lookback_years, lookforward_days)

        # Optional: run only specific steps (comma-separated names).
        # Config: task03_parameters.only_steps   Env override: ONLY_STEPS
        # Useful for targeted testing, e.g.  "only_steps": "computed_billed_rate"
        only_steps_raw = os.environ.get('ONLY_STEPS') or task_params.get('only_steps', '')
        only_steps_raw = (only_steps_raw or '').strip()
        if only_steps_raw:
            only_steps = {s.strip() for s in only_steps_raw.split(',') if s.strip()}
            logger.info(f"  ONLY_STEPS filter: {sorted(only_steps)}")
            steps = [s for s in steps if s['name'] in only_steps]

        # Count only the steps we'll actually run
        active_steps = [s for s in steps if s['group'] != 'B' or enable_phase_b]
        total_steps = len(active_steps) + (1 if enable_phase_b else 0)  # +1 for SF fetch
        step_num = 0

        # ==================================================================
        # PHASE B: Fetch deleted Visit IDs from Snowflake
        # ==================================================================
        logger.info("")
        logger.info("-" * 60)
        logger.info("PHASE B: Deleted visit handling")
        logger.info("-" * 60)

        phase_b_start = time.time()

        if not enable_phase_b:
            logger.info("  SKIPPED -- Phase B disabled (set enable_phase_b=true in config or ENABLE_PHASE_B=1 env var)")
            step_results.append({
                'name': 'phase_b_skipped',
                'group': 'B',
                'rows_affected': 0,
                'duration_seconds': 0,
                'status': 'skipped',
            })
        else:
            step_num += 1
            try:
                sf_manager = conn_factory.get_snowflake_manager()

                logger.info(f"  [{step_num}/{total_steps}] Querying Snowflake for deleted Visit IDs "
                            f"(FACTVISITCALLPERFORMANCE_DELETED_CR, delta={lookback_hours}h, "
                            f"window=-{lookback_years}Y to +{lookforward_days}D)...")
                fetch_start = time.time()
                deleted_ids = _fetch_deleted_visit_ids(
                    sf_manager, db_names, lookback_years, lookforward_days, lookback_hours,
                )
                fetch_dur = time.time() - fetch_start
                logger.info(f"  [{step_num}/{total_steps}] Fetched {len(deleted_ids):,} deleted Visit IDs "
                            f"from Snowflake ({fetch_dur:.1f}s)")

                load_start = time.time()
                loaded = _load_deleted_ids_to_pg(pg_conn, deleted_ids)
                load_dur = time.time() - load_start
                logger.info(f"  Loaded {loaded:,} IDs into _tmp_deleted_visits ({load_dur:.1f}s)")

                step_results.append({
                    'name': 'fetch_deleted_visit_ids',
                    'group': 'B',
                    'rows_affected': len(deleted_ids),
                    'duration_seconds': round(fetch_dur + load_dur, 2),
                    'status': 'ok',
                })

                # Close Snowflake connection early -- we're done with it
                if conn_factory.snowflake_manager:
                    conn_factory.snowflake_manager.close()
                    conn_factory.snowflake_manager = None

            except Exception as e:
                logger.error(f"  Failed to fetch deleted visits from Snowflake: {e}")
                logger.warning("  Continuing with empty deleted visits list (steps 1-2 will be no-ops)")
                # Create empty temp table so steps 1-2 can still execute
                _load_deleted_ids_to_pg(pg_conn, [])
                step_results.append({
                    'name': 'fetch_deleted_visit_ids',
                    'group': 'B',
                    'rows_affected': 0,
                    'duration_seconds': 0,
                    'status': 'error',
                    'error': str(e),
                })
                total_errors += 1

            # Execute steps 1-2 (deleted visit UPDATEs)
            for step in steps:
                if step['group'] != 'B':
                    continue
                step_num += 1
                if shutdown_check and shutdown_check():
                    logger.warning("Shutdown requested -- stopping status management")
                    break
                result = _execute_step(pg_conn, step, step_num, total_steps)
                step_results.append(result)
                if result['status'] == 'error':
                    total_errors += 1

        phase_b_dur = time.time() - phase_b_start
        logger.info(f"  Phase B complete ({phase_b_dur:.1f}s)")

        # ==================================================================
        # PHASE A: Status cascade (pure PostgreSQL)
        # ==================================================================
        logger.info("")
        logger.info("-" * 60)
        logger.info("PHASE A: Status cascade")
        logger.info("-" * 60)

        phase_a_start = time.time()
        for step in steps:
            if step['group'] != 'A':
                continue
            step_num += 1
            if shutdown_check and shutdown_check():
                logger.warning("Shutdown requested -- stopping status management")
                break
            result = _execute_step(pg_conn, step, step_num, total_steps)
            step_results.append(result)
            if result['status'] == 'error':
                total_errors += 1

        phase_a_dur = time.time() - phase_a_start
        logger.info(f"  Phase A complete ({phase_a_dur:.1f}s)")

        # ==================================================================
        # PHASE C: Computed columns
        # ==================================================================
        logger.info("")
        logger.info("-" * 60)
        logger.info("PHASE C: Computed columns")
        logger.info("-" * 60)

        phase_c_start = time.time()
        for step in steps:
            if step['group'] != 'C':
                continue
            step_num += 1
            if shutdown_check and shutdown_check():
                logger.warning("Shutdown requested -- stopping status management")
                break
            result = _execute_step(pg_conn, step, step_num, total_steps)
            step_results.append(result)
            if result['status'] == 'error':
                total_errors += 1

        phase_c_dur = time.time() - phase_c_start
        logger.info(f"  Phase C complete ({phase_c_dur:.1f}s)")

        # ==================================================================
        # Clean up temp table
        # ==================================================================
        try:
            cursor = pg_conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS _tmp_deleted_visits")
            pg_conn.commit()
            cursor.close()
        except Exception:
            pass  # temp table auto-drops on disconnect anyway

        # ==================================================================
        # Summary
        # ==================================================================
        _log_summary(step_results, phase_b_dur, phase_a_dur, phase_c_dur, total_errors)

        status = 'completed' if total_errors == 0 else 'partial'
        return {
            'status': status,
            'statistics': {
                'steps_total': len(step_results),
                'steps_ok': sum(1 for r in step_results if r['status'] == 'ok'),
                'steps_skipped': sum(1 for r in step_results if r['status'] == 'skipped'),
                'steps_error': total_errors,
                'total_rows_affected': sum(r.get('rows_affected', 0) for r in step_results),
                'phase_b_duration': round(phase_b_dur, 2),
                'phase_a_duration': round(phase_a_dur, 2),
                'phase_c_duration': round(phase_c_dur, 2),
            },
            'step_results': step_results,
            'parameters': {
                'lookback_years': lookback_years,
                'lookforward_days': lookforward_days,
                'lookback_hours': lookback_hours,
                'enable_phase_b': enable_phase_b,
            },
        }

    finally:
        conn_factory.close_all()


# ---------------------------------------------------------------------------
# Summary logging
# ---------------------------------------------------------------------------

def _log_summary(
    step_results: List[Dict[str, Any]],
    phase_b_dur: float,
    phase_a_dur: float,
    phase_c_dur: float,
    total_errors: int,
) -> None:
    """Log a human-readable summary of the status management run."""
    total_dur = phase_b_dur + phase_a_dur + phase_c_dur

    logger.info("")
    logger.info("=" * 60)
    logger.info("STATUS MANAGEMENT SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Phase B (deleted visits):    {phase_b_dur:.1f}s")
    logger.info(f"  Phase A (status cascade):    {phase_a_dur:.1f}s")
    logger.info(f"  Phase C (computed columns):  {phase_c_dur:.1f}s")
    logger.info(f"  Total:                       {total_dur:.1f}s")
    logger.info("")

    for r in step_results:
        if r['status'] == 'skipped':
            status_marker = "SKIP"
        elif r['status'] == 'ok':
            status_marker = "OK"
        else:
            status_marker = "ERR"
        logger.info(
            f"  [{status_marker:>4}] {r['name']}: {r.get('rows_affected', 0):,} rows "
            f"({r.get('duration_seconds', 0):.1f}s)"
        )

    logger.info("")
    total_rows = sum(r.get('rows_affected', 0) for r in step_results)
    logger.info(f"  Total rows affected: {total_rows:,}")
    logger.info(f"  Errors: {total_errors}")
    logger.info("=" * 60)
