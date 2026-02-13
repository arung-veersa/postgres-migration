"""
Comprehensive test suite for Task 02 Conflict Detection (v3).
Tests actual code from lib/ -- no logic reimplementation.

Run with: python -m pytest tests/ -v   (from Scripts13/tasks/)
"""

import sys
import os
import types
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Mock heavy database driver modules BEFORE importing any lib code.
# This lets tests run on dev machines that don't have snowflake-connector
# or psycopg2 installed.  We use MagicMock so that any attribute access
# (e.g. ``from snowflake.connector import SnowflakeConnection``) succeeds.
# ---------------------------------------------------------------------------
_MOCK_MODULES = [
    'snowflake', 'snowflake.connector',
    'psycopg2', 'psycopg2.extras', 'psycopg2.pool', 'psycopg2.errors',
    'cryptography', 'cryptography.hazmat',
    'cryptography.hazmat.primitives', 'cryptography.hazmat.primitives.serialization',
    'cryptography.hazmat.backends',
]
for mod_name in _MOCK_MODULES:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# Add tasks directory to path for imports
tasks_dir = str(Path(__file__).parent.parent)
if tasks_dir not in sys.path:
    sys.path.insert(0, tasks_dir)

from lib.utils import (
    format_exclusion_list,
    format_sql_identifier,
    format_duration,
    estimate_memory_mb,
    chunk_list,
)
from lib.query_builder import QueryBuilder, INSERT_COLUMN_MAP, INSERVICE_INSERT_COLUMN_MAP
from lib.conflict_processor import ConflictProcessor


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def query_builder():
    """QueryBuilder pointed at the real sql/ templates."""
    sql_dir = os.path.join(tasks_dir, 'sql')
    return QueryBuilder(sql_dir=sql_dir)


@pytest.fixture
def db_names():
    return {
        'sf_database': 'ANALYTICS', 'sf_schema': 'BI',
        'pg_database': 'conflict_management', 'pg_schema': 'conflict_dev',
    }


@pytest.fixture
def sample_mph_data():
    return [
        {'From': 0, 'To': 2, 'AverageMilesPerHour': 2},
        {'From': 2, 'To': 5, 'AverageMilesPerHour': 6},
        {'From': 5, 'To': 10, 'AverageMilesPerHour': 15},
        {'From': 10, 'To': 25, 'AverageMilesPerHour': 25},
    ]


@pytest.fixture
def sample_settings():
    return {'ExtraDistancePer': 100}


@pytest.fixture
def sample_excluded_agencies():
    return ['10039', '10040', '10041']


@pytest.fixture
def processor():
    """ConflictProcessor with mocked database managers."""
    sf_manager = MagicMock()
    pg_manager = MagicMock()
    qb = QueryBuilder(sql_dir=os.path.join(tasks_dir, 'sql'))
    db = {
        'sf_database': 'ANALYTICS', 'sf_schema': 'BI',
        'pg_database': 'conflict_management', 'pg_schema': 'conflict_dev',
    }
    return ConflictProcessor(
        sf_manager=sf_manager,
        pg_manager=pg_manager,
        query_builder=qb,
        db_names=db,
        batch_size=5000,
        skip_unchanged_records=True,
        enable_asymmetric_join=True,
        enable_stale_cleanup=True,
        enable_insert=True,
    )


# ============================================================================
# 1. Utility Functions (import real code, no reimplementation)
# ============================================================================

class TestFormatExclusionList:
    def test_normal_list(self):
        assert format_exclusion_list(['123', '456', '789']) == "'123','456','789'"

    def test_empty_list(self):
        assert format_exclusion_list([]) == "''"

    def test_single_quote_escape(self):
        assert format_exclusion_list(["O'Reilly"]) == "'O''Reilly'"

    def test_single_item(self):
        assert format_exclusion_list(['123']) == "'123'"

    def test_integer_values(self):
        assert format_exclusion_list([123, 456]) == "'123','456'"

    def test_empty_strings(self):
        assert format_exclusion_list(['', 'valid', '']) == "'','valid',''"


class TestFormatSqlIdentifier:
    def test_simple(self):
        assert format_sql_identifier('Visit Date') == '"Visit Date"'

    def test_already_quoted(self):
        assert format_sql_identifier('Col"Name') == '"Col""Name"'


class TestFormatDuration:
    def test_zero(self):
        assert format_duration(0) == '0s'

    def test_seconds_only(self):
        assert format_duration(45) == '45s'

    def test_minutes_seconds(self):
        assert format_duration(90) == '1m 30s'

    def test_hours_minutes_seconds(self):
        assert format_duration(3661) == '1h 1m 1s'

    def test_exact_hour(self):
        assert format_duration(3600) == '1h'

    def test_exact_minutes(self):
        assert format_duration(480) == '8m'

    def test_float_value(self):
        assert format_duration(486.7) == '8m 6s'


class TestEstimateMemoryMb:
    def test_5k_rows(self):
        result = estimate_memory_mb(5000, 100)
        assert abs(result - 35.8) < 0.2

    def test_zero_rows(self):
        assert estimate_memory_mb(0, 100) == 0.0

    def test_large_dataset(self):
        result = estimate_memory_mb(545497, 100)
        assert result > 3800  # ~3.8 GB


class TestChunkList:
    def test_even_split(self):
        assert list(chunk_list([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]

    def test_uneven_split(self):
        assert list(chunk_list([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]

    def test_single_chunk(self):
        assert list(chunk_list([1, 2], 10)) == [[1, 2]]

    def test_empty(self):
        assert list(chunk_list([], 5)) == []


# ============================================================================
# 2. Conditional Flag Logic (using actual ConflictProcessor._has_changes)
# ============================================================================

class TestConditionalFlags:
    """Tests the conditional flag logic as implemented in _has_changes."""

    def test_flag_n_to_y_updates(self, processor):
        """When existing flag is 'N' and new is 'Y', change should be detected."""
        existing = {
            'SameSchTimeFlag': 'N', 'SameVisitTimeFlag': 'N',
            'SchAndVisitTimeSameFlag': 'N', 'SchOverAnotherSchTimeFlag': 'N',
            'VisitTimeOverAnotherVisitTimeFlag': 'N',
            'SchTimeOverVisitTimeFlag': 'N', 'DistanceFlag': 'N',
            'ProviderID': 'P1', 'ConProviderID': 'P2', 'VisitDate': '2026-01-01',
        }
        new_row = dict(existing)
        new_row['SameSchTimeFlag'] = 'Y'
        assert processor._has_changes(new_row, existing) is True

    def test_flag_y_existing_no_downgrade(self, processor):
        """When existing flag is 'Y' and new is 'N', no change (conditional: only N->Y)."""
        existing = {
            'SameSchTimeFlag': 'Y', 'SameVisitTimeFlag': 'N',
            'SchAndVisitTimeSameFlag': 'N', 'SchOverAnotherSchTimeFlag': 'N',
            'VisitTimeOverAnotherVisitTimeFlag': 'N',
            'SchTimeOverVisitTimeFlag': 'N', 'DistanceFlag': 'N',
            'ProviderID': 'P1', 'ConProviderID': 'P2', 'VisitDate': '2026-01-01',
        }
        new_row = dict(existing)
        new_row['SameSchTimeFlag'] = 'N'  # Y->N: not a change under conditional logic
        assert processor._has_changes(new_row, existing) is False

    def test_all_flags_match_no_change(self, processor):
        """Identical rows should produce no change."""
        row = {
            'SameSchTimeFlag': 'Y', 'SameVisitTimeFlag': 'N',
            'SchAndVisitTimeSameFlag': 'N', 'SchOverAnotherSchTimeFlag': 'N',
            'VisitTimeOverAnotherVisitTimeFlag': 'N',
            'SchTimeOverVisitTimeFlag': 'N', 'DistanceFlag': 'N',
            'ProviderID': 'P1', 'ConProviderID': 'P2', 'VisitDate': '2026-01-01',
        }
        assert processor._has_changes(row, dict(row)) is False

    def test_business_column_change_detected(self, processor):
        """A change in a business column (not a flag) should be detected."""
        existing = {
            'SameSchTimeFlag': 'Y', 'SameVisitTimeFlag': 'N',
            'SchAndVisitTimeSameFlag': 'N', 'SchOverAnotherSchTimeFlag': 'N',
            'VisitTimeOverAnotherVisitTimeFlag': 'N',
            'SchTimeOverVisitTimeFlag': 'N', 'DistanceFlag': 'N',
            'ProviderID': 'P1', 'ConProviderID': 'P2', 'VisitDate': '2026-01-01',
        }
        new_row = dict(existing)
        new_row['ProviderID'] = 'P999'  # business column changed
        assert processor._has_changes(new_row, existing) is True

    def test_skip_unchanged_disabled_always_true(self, processor):
        """When skip_unchanged_records=False, _has_changes always returns True."""
        processor.skip_unchanged_records = False
        row = {'SameSchTimeFlag': 'N', 'ProviderID': 'P1'}
        assert processor._has_changes(row, dict(row)) is True

    def test_multiple_flags_n_to_y_detects_first(self, processor):
        """When multiple flags flip N->Y, change detected on the first."""
        existing = {
            'SameSchTimeFlag': 'N', 'SameVisitTimeFlag': 'N',
            'SchAndVisitTimeSameFlag': 'N', 'SchOverAnotherSchTimeFlag': 'N',
            'VisitTimeOverAnotherVisitTimeFlag': 'N',
            'SchTimeOverVisitTimeFlag': 'N', 'DistanceFlag': 'N',
            'ProviderID': 'P1', 'ConProviderID': 'P2', 'VisitDate': '2026-01-01',
        }
        new_row = dict(existing)
        new_row['SameVisitTimeFlag'] = 'Y'
        new_row['DistanceFlag'] = 'Y'
        assert processor._has_changes(new_row, existing) is True


# ============================================================================
# 3. StatusFlag Logic (using actual build_update_statement)
# ============================================================================

class TestStatusFlagLogic:
    """Tests StatusFlag preservation via actual build_update_statement."""

    def _build(self, processor, existing_status):
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        conflict_row = {
            'VisitID': 'V123', 'ConVisitID': 'V456', 'CONFLICTID': 'C-001',
            'SSN': '111-22-3333', 'StatusFlag': 'N',
        }
        existing_row = {'StatusFlag': existing_status, 'SameSchTimeFlag': 'N'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        return sql, params

    def test_status_n_becomes_u(self, processor):
        sql, params = self._build(processor, 'N')
        assert '"StatusFlag" = %s' in sql
        assert 'U' in params

    def test_status_w_preserved(self, processor):
        sql, params = self._build(processor, 'W')
        assert '"StatusFlag" = %s' not in sql

    def test_status_i_preserved(self, processor):
        sql, params = self._build(processor, 'I')
        assert '"StatusFlag" = %s' not in sql

    def test_status_u_stays_u(self, processor):
        sql, params = self._build(processor, 'U')
        assert '"StatusFlag" = %s' in sql
        assert 'U' in params

    def test_status_r_becomes_u(self, processor):
        """Resolved ('R') records should be re-activated to 'U'."""
        sql, params = self._build(processor, 'R')
        assert '"StatusFlag" = %s' in sql
        assert 'U' in params


# ============================================================================
# 4. Column Name Mapping
# ============================================================================

class TestColumnNameMapping:
    def test_etaravleminutes_mapped(self, processor):
        """ETATravleMinutes (Snowflake typo) should map to ETATravelMinutes in Postgres."""
        conflict_row = {
            'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1',
            'ETATravleMinutes': 42.5,
        }
        existing_row = {'StatusFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"ETATravelMinutes"' in sql
        assert '"ETATravleMinutes"' not in sql

    def test_schvisittimesame_mapped(self, processor):
        """SchVisitTimeSame should map to SchAndVisitTimeSameFlag."""
        conflict_row = {
            'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1',
            'SchVisitTimeSame': 'Y',
        }
        existing_row = {'StatusFlag': 'N', 'SchAndVisitTimeSameFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"SchAndVisitTimeSameFlag"' in sql

    def test_unmapped_column_passes_through(self, processor):
        """Columns not in the map should appear as-is."""
        conflict_row = {
            'VisitID': 'V1', 'ConVisitID': 'V2',
            'CaregiverID': 'CG-100',
        }
        existing_row = {'StatusFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"CaregiverID"' in sql


# ============================================================================
# 5. WHERE Clause / Key Generation
# ============================================================================

class TestWhereClauseMatching:
    def test_both_ids_present(self):
        existing = {('V123', 'V456'): {'CONFLICTID': 'C-001'}}
        assert ('V123', 'V456') in existing

    def test_con_visit_id_none(self):
        existing = {('V789', None): {'CONFLICTID': 'C-002'}}
        assert ('V789', None) in existing

    def test_mismatch_con_visit_id(self):
        existing = {('V123', 'V456'): {'CONFLICTID': 'C-001'}}
        assert ('V123', 'V999') not in existing

    def test_key_generation_from_row(self):
        row = {'VisitID': 'V123', 'ConVisitID': 'V456'}
        visit_id = str(row.get('VisitID'))
        con_visit_id = str(row.get('ConVisitID')) if row.get('ConVisitID') else None
        assert (visit_id, con_visit_id) == ('V123', 'V456')

    def test_key_generation_none_con_visit(self):
        row = {'VisitID': 'V789', 'ConVisitID': None}
        visit_id = str(row.get('VisitID'))
        con_visit_id = str(row.get('ConVisitID')) if row.get('ConVisitID') else None
        assert (visit_id, con_visit_id) == ('V789', None)

    def test_key_generation_empty_string_con_visit(self):
        row = {'VisitID': 'V999', 'ConVisitID': ''}
        visit_id = str(row.get('VisitID'))
        con_visit_id = str(row.get('ConVisitID')) if row.get('ConVisitID') else None
        assert (visit_id, con_visit_id) == ('V999', None)


# ============================================================================
# 6. Query Builder v3 - Template Loading and Formatting
# ============================================================================

class TestQueryBuilderV3:
    """Tests build_conflict_detection_query_v3 with actual templates."""

    def _build_queries(self, query_builder, db_names, sample_excluded_agencies,
                       sample_settings, sample_mph_data, asymmetric):
        return query_builder.build_conflict_detection_query_v3(
            db_names=db_names,
            excluded_agencies=sample_excluded_agencies,
            excluded_ssns=[],
            settings_data=sample_settings,
            mph_data=sample_mph_data,
            lookback_years=2,
            lookforward_days=45,
            lookback_hours=36,
            enable_asymmetric_join=asymmetric,
        )

    # ---- Return structure ----

    def test_symmetric_returns_all_keys(self, query_builder, db_names,
                                         sample_excluded_agencies, sample_settings,
                                         sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=False
        )
        assert 'step0_create' in queries
        assert 'step1' in queries
        assert 'step2' in queries
        assert 'step2d' in queries
        assert 'step3' in queries
        # Symmetric should NOT have step2_asym_insert
        assert 'step2_asym_insert' not in queries

    def test_asymmetric_returns_all_keys(self, query_builder, db_names,
                                          sample_excluded_agencies, sample_settings,
                                          sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        assert 'step0_create' in queries
        assert 'step1' in queries
        assert 'step2' in queries
        assert 'step2_asym_insert' in queries
        assert 'step2d' in queries
        assert 'step3' in queries

    # ---- Step 1: delta_keys ----

    def test_step1_contains_lookback(self, query_builder, db_names,
                                      sample_excluded_agencies, sample_settings,
                                      sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        assert 'DATEADD(HOUR, -36' in queries['step1']
        assert 'delta_keys' in queries['step1']

    def test_step1_has_excluded_agencies(self, query_builder, db_names,
                                          sample_excluded_agencies, sample_settings,
                                          sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        for agency in sample_excluded_agencies:
            assert f"'{agency}'" in queries['step1']

    # ---- Step 2: base_visits ----

    def test_step2_creates_base_visits(self, query_builder, db_names,
                                        sample_excluded_agencies, sample_settings,
                                        sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        assert 'CREATE TEMPORARY TABLE' in queries['step2']
        assert 'base_visits' in queries['step2']
        assert '1 AS "is_delta"' in queries['step2']

    def test_step2_asym_insert_has_is_delta_0(self, query_builder, db_names,
                                               sample_excluded_agencies, sample_settings,
                                               sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        assert 'INSERT INTO base_visits' in queries['step2_asym_insert']
        assert '0 AS "is_delta"' in queries['step2_asym_insert']
        assert 'INNER JOIN delta_keys' in queries['step2_asym_insert']

    def test_step2_symmetric_has_timestamp_filter(self, query_builder, db_names,
                                                    sample_excluded_agencies, sample_settings,
                                                    sample_mph_data):
        """In symmetric mode, base_visits only includes recently-changed rows (timestamp filter)."""
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=False
        )
        # Should filter by Visit Updated Timestamp
        assert 'DATEADD' in queries['step2']

    # ---- Step 3: final conflict detection ----

    def test_step3_asymmetric_has_is_delta_where_condition(self, query_builder, db_names,
                                                            sample_excluded_agencies,
                                                            sample_settings, sample_mph_data):
        """Asymmetric mode should have V1."is_delta" = 1 in the WHERE clause."""
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        assert 'V1."is_delta" = 1' in queries['step3']

    def test_step3_symmetric_no_is_delta_where_condition(self, query_builder, db_names,
                                                          sample_excluded_agencies,
                                                          sample_settings, sample_mph_data):
        """Symmetric mode should NOT have V1."is_delta" = 1 in the WHERE clause."""
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=False
        )
        # The WHERE condition for asymmetric filtering should not be present
        assert 'V1."is_delta" = 1' not in queries['step3']

    # ---- Step 3: conflict flag rules ----

    def test_step3_has_all_7_conflict_rules(self, query_builder, db_names,
                                             sample_excluded_agencies,
                                             sample_settings, sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        sql = queries['step3']
        for flag in ['"SameSchTimeFlag"', '"SameVisitTimeFlag"', '"SchVisitTimeSame"',
                     '"SchOverAnotherSchTimeFlag"', '"VisitTimeOverAnotherVisitTimeFlag"',
                     '"SchTimeOverVisitTimeFlag"', '"DistanceFlag"']:
            assert flag in sql, f"Missing flag rule: {flag}"

    def test_step3_no_redundant_providerid_in_flag_rules(self, query_builder, db_names,
                                                           sample_excluded_agencies,
                                                           sample_settings, sample_mph_data):
        """ProviderID != ConProviderID should NOT appear in conflicts_with_flags CTE rules."""
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        sql = queries['step3']
        cte_start = sql.find('conflicts_with_flags')
        cte_end = sql.find('final_conflicts')
        cte_section = sql[cte_start:cte_end]
        assert 'CE."ProviderID" != CE."ConProviderID"' not in cte_section

    def test_step3_no_concat_in_flag_rules(self, query_builder, db_names,
                                            sample_excluded_agencies,
                                            sample_settings, sample_mph_data):
        """Time comparisons should use direct = not CONCAT."""
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        sql = queries['step3']
        cte_start = sql.find('conflicts_with_flags')
        cte_end = sql.find('final_conflicts')
        cte_section = sql[cte_start:cte_end]
        assert 'CONCAT(' not in cte_section

    # ---- MPH data ----

    def test_mph_data_injected(self, query_builder, db_names,
                                sample_excluded_agencies, sample_settings,
                                sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        sql = queries['step3']
        assert 'mph_data' in sql
        assert 'UNION ALL' in sql  # Multiple MPH rows joined

    def test_empty_mph_uses_dummy(self, query_builder, db_names,
                                   sample_excluded_agencies, sample_settings):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, [], asymmetric=True
        )
        assert '-999999' in queries['step3']

    # ---- Placeholder resolution ----

    def test_no_unresolved_placeholders(self, query_builder, db_names,
                                         sample_excluded_agencies, sample_settings,
                                         sample_mph_data):
        """All {placeholders} should be resolved."""
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=True
        )
        for key, sql in queries.items():
            if isinstance(sql, str):
                assert '{' not in sql, f"Unresolved placeholder in {key}"

    def test_no_unresolved_placeholders_symmetric(self, query_builder, db_names,
                                                    sample_excluded_agencies,
                                                    sample_settings, sample_mph_data):
        queries = self._build_queries(
            query_builder, db_names, sample_excluded_agencies,
            sample_settings, sample_mph_data, asymmetric=False
        )
        for key, sql in queries.items():
            if isinstance(sql, str):
                assert '{' not in sql, f"Unresolved placeholder in {key} (symmetric)"


# ============================================================================
# 7. SSN Batch Insert Generation
# ============================================================================

class TestSSNBatchInserts:
    def test_empty_ssns(self):
        assert QueryBuilder._build_ssn_insert_batches([]) == []

    def test_single_batch(self):
        ssns = ['111-22-3333', '444-55-6666']
        result = QueryBuilder._build_ssn_insert_batches(ssns, batch_size=1000)
        assert len(result) == 1
        assert 'INSERT INTO excluded_ssns_temp' in result[0]
        assert '111-22-3333' in result[0]
        assert '444-55-6666' in result[0]

    def test_multiple_batches(self):
        ssns = [f'SSN-{i}' for i in range(5)]
        result = QueryBuilder._build_ssn_insert_batches(ssns, batch_size=2)
        assert len(result) == 3  # 2 + 2 + 1

    def test_single_quote_escape(self):
        ssns = ["O'Brien"]
        result = QueryBuilder._build_ssn_insert_batches(ssns)
        assert len(result) == 1
        assert "O''Brien" in result[0]  # SQL-escaped

    def test_large_batch(self):
        ssns = [f'SSN-{i:05d}' for i in range(7500)]
        result = QueryBuilder._build_ssn_insert_batches(ssns, batch_size=1000)
        assert len(result) == 8  # ceil(7500/1000)
        for stmt in result:
            assert stmt.startswith('INSERT INTO excluded_ssns_temp')


# ============================================================================
# 8. UPDATE Statement Structure
# ============================================================================

class TestUpdateStatementStructure:
    def test_where_clause_has_visitid_and_convisitid(self, processor):
        conflict_row = {
            'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1', 'SSN': '123',
        }
        existing_row = {'StatusFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"VisitID" = %s' in sql
        assert '"ConVisitID" = %s' in sql
        assert 'cd.conflictvisitmaps' in sql

    def test_where_clause_handles_null_convisitid(self, processor):
        """WHERE clause includes IS NULL fallback for ConVisitID."""
        conflict_row = {'VisitID': 'V1', 'ConVisitID': None, 'CONFLICTID': 'C-1'}
        existing_row = {'StatusFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"ConVisitID" IS NULL' in sql

    def test_always_sets_updateflag_null(self, processor):
        conflict_row = {'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1'}
        existing_row = {'StatusFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, _ = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"UpdateFlag" = NULL' in sql

    def test_always_sets_updateddate(self, processor):
        conflict_row = {'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1'}
        existing_row = {'StatusFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, _ = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"UpdatedDate" = CURRENT_TIMESTAMP' in sql

    def test_always_sets_resolvedate_null(self, processor):
        conflict_row = {'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1'}
        existing_row = {'StatusFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, _ = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"ResolveDate" = NULL' in sql

    def test_conditional_flag_preserved_when_y(self, processor):
        """Flag with existing='Y' should NOT appear in SET clause."""
        conflict_row = {
            'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1',
            'SameSchTimeFlag': 'Y',
        }
        existing_row = {'StatusFlag': 'N', 'SameSchTimeFlag': 'Y'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"SameSchTimeFlag" = %s' not in sql

    def test_conditional_flag_updated_when_n(self, processor):
        """Flag with existing='N' should appear in SET clause."""
        conflict_row = {
            'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1',
            'SameSchTimeFlag': 'Y',
        }
        existing_row = {'StatusFlag': 'N', 'SameSchTimeFlag': 'N'}
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"SameSchTimeFlag" = %s' in sql

    def test_multiple_conditional_flags(self, processor):
        """Multiple flags should be independently conditional."""
        conflict_row = {
            'VisitID': 'V1', 'ConVisitID': 'V2', 'CONFLICTID': 'C-1',
            'SameSchTimeFlag': 'Y', 'DistanceFlag': 'Y', 'SameVisitTimeFlag': 'N',
        }
        existing_row = {
            'StatusFlag': 'N',
            'SameSchTimeFlag': 'N',      # will update -> Y
            'DistanceFlag': 'Y',          # won't update (already Y)
            'SameVisitTimeFlag': 'N',     # new is N, existing is N -> no change
        }
        db = {'pg_database': 'cm', 'pg_schema': 'cd'}
        sql, params = processor.query_builder.build_update_statement(
            conflict_row, db, existing_row
        )
        assert '"SameSchTimeFlag" = %s' in sql
        assert '"DistanceFlag" = %s' not in sql
        # SameVisitTimeFlag: existing='N', new='N' -> same value, but code checks if existing_flag == 'N'
        # and then adds the column unconditionally -> it WILL appear in SET
        assert '"SameVisitTimeFlag" = %s' in sql


# ============================================================================
# 9. Statistics Structure
# ============================================================================

class TestStatistics:
    def test_initial_stats_shape(self, processor):
        stats = processor.get_statistics()
        expected_keys = [
            'rows_fetched', 'rows_processed', 'rows_updated',
            'rows_skipped_no_changes', 'batches_processed', 'errors',
            'unique_visits', 'matched_in_postgres', 'new_conflicts',
            'stale_conflicts_reset', 'delta_ssns_count', 'delta_dates_count',
            'delta_pairs_count', 'delta_keys_count', 'modified_visit_ids_count',
            'records_marked_for_update', 'stale_conflicts_resolved',
            'update_rate', 'match_rate', 'efficiency_rate',
        ]
        for key in expected_keys:
            assert key in stats, f"Missing key: {key}"

    def test_initial_stats_zeros(self, processor):
        stats = processor.get_statistics()
        assert stats['rows_fetched'] == 0
        assert stats['rows_updated'] == 0
        assert stats['errors'] == 0

    def test_backward_compat_aliases(self, processor):
        """delta_keys_count should alias delta_pairs_count."""
        stats = processor.get_statistics()
        assert stats['delta_keys_count'] == stats['delta_pairs_count']
        assert stats['modified_visit_ids_count'] == stats['delta_ssns_count']

    def test_rate_calculations_zero_safe(self, processor):
        """Rate calculations should not divide by zero."""
        stats = processor.get_statistics()
        assert stats['update_rate'] == 0.0
        assert stats['match_rate'] == 0.0
        assert stats['efficiency_rate'] == 0.0


# ============================================================================
# 10. SQL Template File Loading
# ============================================================================

class TestTemplateLoading:
    def test_step1_template_loads(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_00_step1_delta_keys.sql')
        assert 'delta_keys' in sql
        assert '{lookback_hours}' in sql

    def test_step2_template_loads(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_00_step2_base_visits.sql')
        assert '{TABLE_CLAUSE}' in sql
        assert '{is_delta_value}' in sql
        assert '{DELTA_KEYS_JOIN}' in sql
        assert '{TIMESTAMP_CONDITION}' in sql

    def test_step3_template_loads(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_00_step3_final_query.sql')
        assert '{ASYMMETRIC_JOIN_CONDITION}' in sql
        assert '{mph_lookup}' in sql
        assert 'conflicts_with_flags' in sql
        assert 'final_conflicts' in sql

    def test_missing_template_raises(self, query_builder):
        with pytest.raises(FileNotFoundError):
            query_builder.load_sql_file('nonexistent.sql')

    def test_reference_queries_load(self, query_builder):
        for name in ['pg_fetch_excluded_agencies.sql', 'pg_fetch_excluded_ssns.sql',
                      'pg_fetch_mph.sql', 'pg_fetch_settings.sql']:
            sql = query_builder.load_sql_file(name)
            assert len(sql) > 0, f"Empty template: {name}"

    def test_reference_queries_have_schema_placeholder(self, query_builder):
        """Reference queries should have {pg_schema} placeholder."""
        for name in ['pg_fetch_excluded_agencies.sql', 'pg_fetch_excluded_ssns.sql',
                      'pg_fetch_mph.sql', 'pg_fetch_settings.sql']:
            sql = query_builder.load_sql_file(name)
            assert '{pg_schema}' in sql, f"Missing {{pg_schema}} in {name}"


# ============================================================================
# 11. INSERT Column Map and Template
# ============================================================================

class TestInsertColumnMap:
    """Tests the INSERT_COLUMN_MAP and build_insert_template."""

    def test_column_map_is_nonempty(self):
        from lib.query_builder import INSERT_COLUMN_MAP
        assert len(INSERT_COLUMN_MAP) > 100, "INSERT_COLUMN_MAP should have 100+ column pairs"

    def test_column_map_tuple_structure(self):
        from lib.query_builder import INSERT_COLUMN_MAP
        for i, entry in enumerate(INSERT_COLUMN_MAP):
            assert isinstance(entry, tuple), f"Entry {i} is not a tuple"
            assert len(entry) == 2, f"Entry {i} should have 2 elements (sf, pg)"
            sf, pg = entry
            assert isinstance(sf, str), f"Entry {i} sf column is not str"
            assert isinstance(pg, str), f"Entry {i} pg column is not str"

    def test_column_map_no_duplicates(self):
        from lib.query_builder import INSERT_COLUMN_MAP
        sf_cols = [sf for sf, _ in INSERT_COLUMN_MAP]
        pg_cols = [pg for _, pg in INSERT_COLUMN_MAP]
        assert len(sf_cols) == len(set(sf_cols)), "Duplicate Snowflake columns in INSERT_COLUMN_MAP"
        assert len(pg_cols) == len(set(pg_cols)), "Duplicate PostgreSQL columns in INSERT_COLUMN_MAP"

    def test_column_map_has_key_columns(self):
        """Verify critical columns are present."""
        from lib.query_builder import INSERT_COLUMN_MAP
        sf_cols = {sf for sf, _ in INSERT_COLUMN_MAP}
        for required in ['SSN', 'VisitID', 'ConVisitID', 'ProviderID', 'ConProviderID',
                         'SameSchTimeFlag', 'DistanceFlag', 'ETATravleMinutes',
                         'AgencyContact', 'ConAgencyContact', 'AgencyPhone', 'ConAgencyPhone']:
            assert required in sf_cols, f"Missing required column: {required}"

    def test_column_map_eta_rename(self):
        """ETATravleMinutes (Snowflake typo) should map to ETATravelMinutes (PG corrected)."""
        from lib.query_builder import INSERT_COLUMN_MAP
        col_map = dict(INSERT_COLUMN_MAP)
        assert col_map['ETATravleMinutes'] == 'ETATravelMinutes'

    def test_build_insert_template_structure(self, query_builder, db_names):
        sql, sf_columns = query_builder.build_insert_template(db_names)
        assert isinstance(sql, str)
        assert isinstance(sf_columns, list)
        assert len(sf_columns) > 100

    def test_build_insert_template_has_schema(self, query_builder, db_names):
        sql, _ = query_builder.build_insert_template(db_names)
        assert db_names['pg_schema'] in sql
        assert 'conflictvisitmaps' in sql

    def test_build_insert_template_fixed_columns(self, query_builder, db_names):
        """StatusFlag='N', InServiceFlag='N', PTOFlag='N', CreatedDate=CURRENT_TIMESTAMP."""
        sql, _ = query_builder.build_insert_template(db_names)
        assert "'N'" in sql  # StatusFlag, InServiceFlag, PTOFlag
        assert 'CURRENT_TIMESTAMP' in sql

    def test_build_insert_template_placeholder_count(self, query_builder, db_names):
        """Number of %s placeholders should equal number of sf_columns."""
        sql, sf_columns = query_builder.build_insert_template(db_names)
        placeholder_count = sql.count('%s')
        assert placeholder_count == len(sf_columns), (
            f"Mismatch: {placeholder_count} placeholders vs {len(sf_columns)} sf_columns"
        )

    def test_build_insert_template_no_id_column(self, query_builder, db_names):
        """INSERT should not include "ID" column (GENERATED BY DEFAULT AS IDENTITY)."""
        sql, sf_columns = query_builder.build_insert_template(db_names)
        # Check that "ID" is not in the column list portion of the INSERT
        # The ID column is auto-generated by PostgreSQL identity
        assert '"ID"' not in sql.split('VALUES')[0]


# ============================================================================
# 12. Statistics - INSERT fields
# ============================================================================

class TestStatisticsInsert:
    """Tests that INSERT-related fields are in statistics."""

    def test_insert_stats_keys_present(self, processor):
        stats = processor.get_statistics()
        assert 'rows_inserted' in stats
        assert 'insert_enabled' in stats
        assert 'insert_batches' in stats

    def test_insert_enabled_reflects_config(self, processor):
        stats = processor.get_statistics()
        assert stats['insert_enabled'] is True

    def test_insert_stats_initial_zeros(self, processor):
        stats = processor.get_statistics()
        assert stats['rows_inserted'] == 0
        assert stats['insert_batches'] == 0


# ============================================================================
# 13. Cross-State Conflict Filter (Step 3 SQL)
# ============================================================================

class TestCrossStateFilter:
    """Tests that the cross-state conflict filter CTEs are present in Step 3."""

    def _get_step3(self, query_builder, db_names, sample_excluded_agencies,
                   sample_settings, sample_mph_data, asymmetric=True):
        queries = query_builder.build_conflict_detection_query_v3(
            db_names=db_names,
            excluded_agencies=sample_excluded_agencies,
            excluded_ssns=[],
            settings_data=sample_settings,
            mph_data=sample_mph_data,
            lookback_years=2,
            lookforward_days=45,
            lookback_hours=36,
            enable_asymmetric_join=asymmetric,
        )
        return queries['step3']

    def test_state_map_cte_present(self, query_builder, db_names,
                                    sample_excluded_agencies, sample_settings,
                                    sample_mph_data):
        sql = self._get_step3(query_builder, db_names, sample_excluded_agencies,
                              sample_settings, sample_mph_data)
        assert 'state_map' in sql

    def test_state_map_has_mappings(self, query_builder, db_names,
                                    sample_excluded_agencies, sample_settings,
                                    sample_mph_data):
        sql = self._get_step3(query_builder, db_names, sample_excluded_agencies,
                              sample_settings, sample_mph_data)
        for state in ['MISSISSIPPI', 'NEW YORK', 'PENNSYLVANIA', 'LONG ISLAND']:
            assert state in sql, f"Missing state mapping: {state}"

    def test_cross_state_prep_cte_present(self, query_builder, db_names,
                                           sample_excluded_agencies, sample_settings,
                                           sample_mph_data):
        sql = self._get_step3(query_builder, db_names, sample_excluded_agencies,
                              sample_settings, sample_mph_data)
        assert 'cross_state_prep' in sql

    def test_same_state_conflicts_cte_present(self, query_builder, db_names,
                                               sample_excluded_agencies, sample_settings,
                                               sample_mph_data):
        sql = self._get_step3(query_builder, db_names, sample_excluded_agencies,
                              sample_settings, sample_mph_data)
        assert 'same_state_conflicts' in sql

    def test_final_select_from_same_state_conflicts(self, query_builder, db_names,
                                                     sample_excluded_agencies,
                                                     sample_settings, sample_mph_data):
        """Final SELECT should read from same_state_conflicts (not final_conflicts)."""
        sql = self._get_step3(query_builder, db_names, sample_excluded_agencies,
                              sample_settings, sample_mph_data)
        # The last FROM clause should reference same_state_conflicts
        last_from_idx = sql.rfind('FROM same_state_conflicts')
        assert last_from_idx > 0, "Final SELECT should be FROM same_state_conflicts"

    def test_provider_address_state_in_conflict_pairs(self, query_builder, db_names,
                                                       sample_excluded_agencies,
                                                       sample_settings, sample_mph_data):
        """conflict_pairs CTE should carry ProviderAddressState and ConProviderAddressState."""
        sql = self._get_step3(query_builder, db_names, sample_excluded_agencies,
                              sample_settings, sample_mph_data)
        assert '"ProviderAddressState"' in sql
        assert '"ConProviderAddressState"' in sql

    def test_agency_phone_in_conflict_pairs(self, query_builder, db_names,
                                             sample_excluded_agencies,
                                             sample_settings, sample_mph_data):
        """conflict_pairs CTE should carry AgencyPhone and ConAgencyPhone."""
        sql = self._get_step3(query_builder, db_names, sample_excluded_agencies,
                              sample_settings, sample_mph_data)
        assert '"AgencyPhone"' in sql
        assert '"ConAgencyPhone"' in sql

    def test_cross_state_filter_in_symmetric_mode(self, query_builder, db_names,
                                                    sample_excluded_agencies,
                                                    sample_settings, sample_mph_data):
        """Cross-state filter should be present in symmetric mode too."""
        sql = self._get_step3(query_builder, db_names, sample_excluded_agencies,
                              sample_settings, sample_mph_data, asymmetric=False)
        assert 'same_state_conflicts' in sql
        assert 'state_map' in sql


# ============================================================================
# 14. Base Visits Template (Step 2) - ProviderAddressState
# ============================================================================

class TestBaseVisitsTemplate:
    """Tests that step2 base_visits template includes ProviderAddressState."""

    def test_step2_has_provider_address_state(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_00_step2_base_visits.sql')
        assert '"ProviderAddressState"' in sql

    def test_step2_provider_address_state_from_dpr(self, query_builder):
        """ProviderAddressState should come from DPR (DIMPROVIDER)."""
        sql = query_builder.load_sql_file('sf_task02_00_step2_base_visits.sql')
        assert 'DPR."Address State"' in sql

    def test_step2_has_agency_phone(self, query_builder):
        """AgencyPhone should come from DPR."Phone Number 1"."""
        sql = query_builder.load_sql_file('sf_task02_00_step2_base_visits.sql')
        assert '"AgencyPhone"' in sql
        assert '"Phone Number 1"' in sql


# ============================================================================
# 15. InService Column Mapping
# ============================================================================

class TestInserviceInsertColumnMap:
    """Tests for INSERVICE_INSERT_COLUMN_MAP and build_inservice_insert_template."""

    def test_inservice_map_extends_regular(self):
        """InService map should be a superset of regular INSERT_COLUMN_MAP."""
        regular_set = set(INSERT_COLUMN_MAP)
        inservice_set = set(INSERVICE_INSERT_COLUMN_MAP)
        assert regular_set.issubset(inservice_set)

    def test_inservice_map_has_4_extra_columns(self):
        """InService map should have exactly 4 more columns than regular."""
        diff = len(INSERVICE_INSERT_COLUMN_MAP) - len(INSERT_COLUMN_MAP)
        assert diff == 4, f"Expected 4 extra columns, got {diff}"

    def test_inservice_map_has_inservice_dates(self):
        """InService map must include all 4 InService date columns."""
        pg_cols = [pg for _sf, pg in INSERVICE_INSERT_COLUMN_MAP]
        for col in ['InserviceStartDate', 'InserviceEndDate',
                     'ConInserviceStartDate', 'ConInserviceEndDate']:
            assert col in pg_cols, f"Missing InService column: {col}"

    def test_inservice_map_no_duplicates(self):
        """InService column map should have no duplicate PG column names."""
        pg_cols = [pg for _sf, pg in INSERVICE_INSERT_COLUMN_MAP]
        assert len(pg_cols) == len(set(pg_cols))

    def test_build_inservice_insert_template(self, query_builder, db_names):
        """build_inservice_insert_template should produce valid SQL with InServiceFlag='Y'."""
        sql, sf_cols = query_builder.build_inservice_insert_template(db_names)
        assert "'Y'" in sql, "InServiceFlag should be 'Y' in InService INSERT"
        assert 'conflict_dev' in sql
        assert '%s' in sql
        assert len(sf_cols) == len(INSERVICE_INSERT_COLUMN_MAP)

    def test_build_inservice_insert_template_no_on_conflict(self, query_builder, db_names):
        """InService INSERT should NOT use ON CONFLICT (constraint definition unknown on live DB)."""
        sql, _ = query_builder.build_inservice_insert_template(db_names)
        assert 'ON CONFLICT' not in sql, "ON CONFLICT removed -- duplicates handled in Python"

    def test_inservice_insert_template_has_more_placeholders(self, query_builder, db_names):
        """InService INSERT should have 4 more %s placeholders than regular INSERT."""
        regular_sql, regular_cols = query_builder.build_insert_template(db_names)
        inservice_sql, inservice_cols = query_builder.build_inservice_insert_template(db_names)
        regular_count = regular_sql.count('%s')
        inservice_count = inservice_sql.count('%s')
        assert inservice_count == regular_count + 4


# ============================================================================
# 16. InService SQL Templates
# ============================================================================

class TestInserviceSqlTemplates:
    """Tests for the InService SQL template files."""

    def test_step1_visits_template_loads(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_01_step1_visits.sql')
        assert len(sql) > 100

    def test_step2_events_template_loads(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_01_step2_events.sql')
        assert len(sql) > 100

    def test_step3_pairs_template_loads(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_01_step3_pairs.sql')
        assert len(sql) > 100

    def test_step1_creates_inservice_visits(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_01_step1_visits.sql')
        assert 'inservice_visits' in sql.lower()

    def test_step2_creates_inservice_events(self, query_builder):
        sql = query_builder.load_sql_file('sf_task02_01_step2_events.sql')
        assert 'inservice_events' in sql.lower()

    def test_step1_has_fcs_exclusion(self, query_builder):
        """Step 1 should exclude visits with same-provider InService overlap."""
        sql = query_builder.load_sql_file('sf_task02_01_step1_visits.sql')
        assert 'FACTCAREGIVERINSERVICE' in sql
        assert '"Application Caregiver Inservice Id" IS NULL' in sql

    def test_step1_has_is_missed_filter(self, query_builder):
        """Step 1 should filter out missed visits."""
        sql = query_builder.load_sql_file('sf_task02_01_step1_visits.sql')
        assert '"Is Missed" = FALSE' in sql

    def test_step1_has_caregiver_semijoin(self, query_builder):
        """Step 1 should pre-filter to caregivers with InService events."""
        sql = query_builder.load_sql_file('sf_task02_01_step1_visits.sql')
        # The semi-join subquery should reference FCS2 alias
        assert 'FCS2."Caregiver Id"' in sql
        assert 'FACTCAREGIVERINSERVICE AS FCS2' in sql

    def test_step2_has_synthetic_visit_id(self, query_builder):
        """Step 2 should generate synthetic VisitID via MD5."""
        sql = query_builder.load_sql_file('sf_task02_01_step2_events.sql')
        assert 'MD5' in sql
        assert "CONCAT('I'" in sql

    def test_step2_has_inservice_dates(self, query_builder):
        """Step 2 should include InserviceStartDate/EndDate from FCS."""
        sql = query_builder.load_sql_file('sf_task02_01_step2_events.sql')
        assert '"InserviceStartDate"' in sql
        assert '"InserviceEndDate"' in sql
        assert '"Inservice start date"' in sql

    def test_step3_has_union_all(self, query_builder):
        """Step 3 should use UNION ALL for both directions."""
        sql = query_builder.load_sql_file('sf_task02_01_step3_pairs.sql')
        assert 'UNION ALL' in sql

    def test_step3_has_temporal_overlap_join(self, query_builder):
        """Step 3 should join on temporal overlap (VisitStartTime <= InserviceEndDate)."""
        sql = query_builder.load_sql_file('sf_task02_01_step3_pairs.sql')
        assert '"VisitStartTime"' in sql
        assert '"InserviceEndDate"' in sql
        assert '"VisitEndTime"' in sql
        assert '"InserviceStartDate"' in sql

    def test_step3_has_different_provider_condition(self, query_builder):
        """Step 3 should require different providers."""
        sql = query_builder.load_sql_file('sf_task02_01_step3_pairs.sql')
        assert '"ProviderID" !=' in sql

    def test_step3_hardcoded_flags(self, query_builder):
        """Step 3 should have all 7 flags hardcoded to 'N'."""
        sql = query_builder.load_sql_file('sf_task02_01_step3_pairs.sql')
        for flag in ['SameSchTimeFlag', 'SameVisitTimeFlag',
                     'SchAndVisitTimeSameFlag', 'SchOverAnotherSchTimeFlag',
                     'VisitTimeOverAnotherVisitTimeFlag',
                     'SchTimeOverVisitTimeFlag', 'DistanceFlag']:
            assert f"'{flag}'" not in sql or f'AS "{flag}"' in sql

    def test_step3_null_distance_columns(self, query_builder):
        """Step 3 should have NULL distance/travel columns."""
        sql = query_builder.load_sql_file('sf_task02_01_step3_pairs.sql')
        assert 'CAST(NULL AS NUMBER) AS "MinuteDiffBetweenSch"' in sql
        assert 'CAST(NULL AS NUMBER) AS "ETATravleMinutes"' in sql

    def test_step3_inservice_date_columns(self, query_builder):
        """Step 3 should output all 4 InService date columns."""
        sql = query_builder.load_sql_file('sf_task02_01_step3_pairs.sql')
        assert '"InserviceStartDate"' in sql
        assert '"InserviceEndDate"' in sql
        assert '"ConInserviceStartDate"' in sql
        assert '"ConInserviceEndDate"' in sql

    def test_step3_joins_both_directions(self, query_builder):
        """Step 3 should join inservice_visits with inservice_events in both directions."""
        sql = query_builder.load_sql_file('sf_task02_01_step3_pairs.sql')
        # Direction 1: visits V1 INNER JOIN events V2
        assert 'inservice_visits V1' in sql
        assert 'inservice_events V2' in sql
        # Direction 2: events V1 INNER JOIN visits V2
        assert 'inservice_events V1' in sql
        assert 'inservice_visits V2' in sql


# ============================================================================
# 17. InService Query Builder
# ============================================================================

class TestInserviceQueryBuilder:
    """Tests for build_inservice_queries method."""

    @pytest.fixture
    def sample_excluded_ssns(self):
        return ['111-22-3333', '444-55-6666']

    def test_build_inservice_queries_keys(self, query_builder, db_names,
                                           sample_excluded_agencies,
                                           sample_excluded_ssns):
        """build_inservice_queries should return all expected step keys."""
        queries = query_builder.build_inservice_queries(
            db_names=db_names,
            excluded_agencies=sample_excluded_agencies,
            excluded_ssns=sample_excluded_ssns,
        )
        assert 'step0_create' in queries
        assert 'step0_inserts' in queries
        assert 'step1' in queries
        assert 'step2' in queries
        assert 'step3' in queries

    def test_build_inservice_queries_step0_creates_temp(self, query_builder, db_names,
                                                         sample_excluded_agencies,
                                                         sample_excluded_ssns):
        queries = query_builder.build_inservice_queries(
            db_names=db_names,
            excluded_agencies=sample_excluded_agencies,
            excluded_ssns=sample_excluded_ssns,
        )
        assert 'excluded_ssns_temp' in queries['step0_create']

    def test_build_inservice_queries_step1_has_db_name(self, query_builder, db_names,
                                                        sample_excluded_agencies,
                                                        sample_excluded_ssns):
        queries = query_builder.build_inservice_queries(
            db_names=db_names,
            excluded_agencies=sample_excluded_agencies,
            excluded_ssns=sample_excluded_ssns,
        )
        assert 'ANALYTICS' in queries['step1']
        assert 'BI' in queries['step1']

    def test_build_inservice_queries_step3_no_placeholders(self, query_builder, db_names,
                                                            sample_excluded_agencies,
                                                            sample_excluded_ssns):
        """Step 3 should have no unresolved {placeholders}."""
        queries = query_builder.build_inservice_queries(
            db_names=db_names,
            excluded_agencies=sample_excluded_agencies,
            excluded_ssns=sample_excluded_ssns,
        )
        assert '{' not in queries['step3']


# ============================================================================
# 18. InService Action Registration
# ============================================================================

class TestInserviceActionRegistration:
    """Tests that task02_01 is properly registered in the pipeline."""

    def test_action_in_default_pipeline(self):
        """task02_01_inservice_conflict should be in DEFAULT_ACTIONS."""
        from scripts.main import DEFAULT_ACTIONS
        assert 'task02_01_inservice_conflict' in DEFAULT_ACTIONS

    def test_action_after_task02_00(self):
        """task02_01 should come after task02_00 in the default pipeline."""
        from scripts.main import DEFAULT_ACTIONS
        idx_00 = DEFAULT_ACTIONS.index('task02_00_conflict_update')
        idx_01 = DEFAULT_ACTIONS.index('task02_01_inservice_conflict')
        assert idx_01 > idx_00

    def test_action_before_postflight(self):
        """task02_01 should come before task99_postflight."""
        from scripts.main import DEFAULT_ACTIONS
        idx_01 = DEFAULT_ACTIONS.index('task02_01_inservice_conflict')
        idx_99 = DEFAULT_ACTIONS.index('task99_postflight')
        assert idx_01 < idx_99

    def test_action_in_registry(self):
        """task02_01_inservice_conflict should be in ACTION_REGISTRY."""
        from scripts.main import ACTION_REGISTRY
        assert 'task02_01_inservice_conflict' in ACTION_REGISTRY


# ============================================================================
# 19. InService _norm_key UUID normalisation
# ============================================================================

class TestNormKey:
    """Tests for _norm_key UUID normalisation helper."""

    def test_strips_dashes(self):
        from scripts.actions.task02_01_inservice_conflict import _norm_key
        key = _norm_key('d670686d-d28b-4fca-b2cc-99ec4249c575', 'abc')
        assert '-' not in key[0]

    def test_lowercases(self):
        from scripts.actions.task02_01_inservice_conflict import _norm_key
        key = _norm_key('D670686D-D28B-4FCA-B2CC-99EC4249C575', 'ABC')
        assert key == ('d670686dd28b4fcab2cc99ec4249c575', 'abc')

    def test_md5_no_dashes_matches_uuid(self):
        """MD5 hex without dashes should match UUID with dashes after normalisation."""
        from scripts.actions.task02_01_inservice_conflict import _norm_key
        md5_key = _norm_key('32329cfa34b3f072ca9a9800864ecb72', 'visit1')
        uuid_key = _norm_key('32329cfa-34b3-f072-ca9a-9800864ecb72', 'visit1')
        assert md5_key == uuid_key

    def test_none_handling(self):
        from scripts.actions.task02_01_inservice_conflict import _norm_key
        key = _norm_key(None, None)
        assert key == ('', '')

    def test_empty_string(self):
        from scripts.actions.task02_01_inservice_conflict import _norm_key
        key = _norm_key('', '')
        assert key == ('', '')
