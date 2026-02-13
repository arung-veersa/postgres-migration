"""
Query Builder for Task 02 Conflict Detection
Loads SQL templates and injects parameters dynamically
"""

import os
from typing import Dict, List, Any, Tuple
from .utils import get_logger, format_exclusion_list

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# INSERT column mapping: (snowflake_column, pg_column)
#
# Maps Snowflake Step 3 output column names (as received in the Python row
# dict) to PostgreSQL conflictvisitmaps column names.  Most are 1:1; the
# only name translation is ETATravleMinutes -> ETATravelMinutes (the Step 3
# SQL already aliases SchVisitTimeSame -> SchAndVisitTimeSameFlag).
#
# Columns NOT in this list (e.g. GroupID, ShVTSTTime, InserviceStartDate)
# are omitted from the INSERT and receive their PostgreSQL default (NULL).
# ---------------------------------------------------------------------------
INSERT_COLUMN_MAP: List[Tuple[str, str]] = [
    # --- identifiers ---
    ('CONFLICTID', 'CONFLICTID'),
    ('SSN', 'SSN'),
    # --- visit 1 provider ---
    ('ProviderID', 'ProviderID'),
    ('AppProviderID', 'AppProviderID'),
    ('ProviderName', 'ProviderName'),
    ('VisitID', 'VisitID'),
    ('AppVisitID', 'AppVisitID'),
    # --- visit 2 (conflict) provider ---
    ('ConProviderID', 'ConProviderID'),
    ('ConAppProviderID', 'ConAppProviderID'),
    ('ConProviderName', 'ConProviderName'),
    ('ConVisitID', 'ConVisitID'),
    ('ConAppVisitID', 'ConAppVisitID'),
    # --- dates / times ---
    ('VisitDate', 'VisitDate'),
    ('SchStartTime', 'SchStartTime'),
    ('SchEndTime', 'SchEndTime'),
    ('ConSchStartTime', 'ConSchStartTime'),
    ('ConSchEndTime', 'ConSchEndTime'),
    ('VisitStartTime', 'VisitStartTime'),
    ('VisitEndTime', 'VisitEndTime'),
    ('ConVisitStartTime', 'ConVisitStartTime'),
    ('ConVisitEndTime', 'ConVisitEndTime'),
    ('EVVStartTime', 'EVVStartTime'),
    ('EVVEndTime', 'EVVEndTime'),
    ('ConEVVStartTime', 'ConEVVStartTime'),
    ('ConEVVEndTime', 'ConEVVEndTime'),
    # --- caregiver ---
    ('CaregiverID', 'CaregiverID'),
    ('AppCaregiverID', 'AppCaregiverID'),
    ('AideCode', 'AideCode'),
    ('AideName', 'AideName'),
    ('AideFName', 'AideFName'),
    ('AideLName', 'AideLName'),
    ('AideSSN', 'AideSSN'),
    ('AideStatus', 'AideStatus'),
    ('ConCaregiverID', 'ConCaregiverID'),
    ('ConAppCaregiverID', 'ConAppCaregiverID'),
    ('ConAideCode', 'ConAideCode'),
    ('ConAideName', 'ConAideName'),
    ('ConAideFName', 'ConAideFName'),
    ('ConAideLName', 'ConAideLName'),
    ('ConAideSSN', 'ConAideSSN'),
    ('ConAideStatus', 'ConAideStatus'),
    # --- office ---
    ('OfficeID', 'OfficeID'),
    ('AppOfficeID', 'AppOfficeID'),
    ('Office', 'Office'),
    ('ConOfficeID', 'ConOfficeID'),
    ('ConAppOfficeID', 'ConAppOfficeID'),
    ('ConOffice', 'ConOffice'),
    # --- patient ---
    ('PatientID', 'PatientID'),
    ('AppPatientID', 'AppPatientID'),
    ('PAdmissionID', 'PAdmissionID'),
    ('PName', 'PName'),
    ('PFName', 'PFName'),
    ('PLName', 'PLName'),
    ('PMedicaidNumber', 'PMedicaidNumber'),
    ('PStatus', 'PStatus'),
    ('PAddressID', 'PAddressID'),
    ('PAppAddressID', 'PAppAddressID'),
    ('PAddressL1', 'PAddressL1'),
    ('PAddressL2', 'PAddressL2'),
    ('PCity', 'PCity'),
    ('PAddressState', 'PAddressState'),
    ('PZipCode', 'PZipCode'),
    ('PCounty', 'PCounty'),
    ('PLongitude', 'PLongitude'),
    ('PLatitude', 'PLatitude'),
    # --- conflict patient ---
    ('ConPatientID', 'ConPatientID'),
    ('ConAppPatientID', 'ConAppPatientID'),
    ('ConPAdmissionID', 'ConPAdmissionID'),
    ('ConPName', 'ConPName'),
    ('ConPFName', 'ConPFName'),
    ('ConPLName', 'ConPLName'),
    ('ConPMedicaidNumber', 'ConPMedicaidNumber'),
    ('ConPStatus', 'ConPStatus'),
    ('ConPAddressID', 'ConPAddressID'),
    ('ConPAppAddressID', 'ConPAppAddressID'),
    ('ConPAddressL1', 'ConPAddressL1'),
    ('ConPAddressL2', 'ConPAddressL2'),
    ('ConPCity', 'ConPCity'),
    ('ConPAddressState', 'ConPAddressState'),
    ('ConPZipCode', 'ConPZipCode'),
    ('ConPCounty', 'ConPCounty'),
    ('ConPLongitude', 'ConPLongitude'),
    ('ConPLatitude', 'ConPLatitude'),
    # --- payer ---
    ('PayerID', 'PayerID'),
    ('AppPayerID', 'AppPayerID'),
    ('Contract', 'Contract'),
    ('PayerState', 'PayerState'),
    ('ConPayerID', 'ConPayerID'),
    ('ConAppPayerID', 'ConAppPayerID'),
    ('ConContract', 'ConContract'),
    ('ConPayerState', 'ConPayerState'),
    # --- billing ---
    ('BilledDate', 'BilledDate'),
    ('ConBilledDate', 'ConBilledDate'),
    ('BilledHours', 'BilledHours'),
    ('ConBilledHours', 'ConBilledHours'),
    ('Billed', 'Billed'),
    ('ConBilled', 'ConBilled'),
    ('BilledRate', 'BilledRate'),
    ('TotalBilledAmount', 'TotalBilledAmount'),
    ('BillRateNonBilled', 'BillRateNonBilled'),
    ('BillRateBoth', 'BillRateBoth'),
    ('ConBilledRate', 'ConBilledRate'),
    ('ConTotalBilledAmount', 'ConTotalBilledAmount'),
    ('ConBillRateNonBilled', 'ConBillRateNonBilled'),
    ('ConBillRateBoth', 'ConBillRateBoth'),
    # --- distance / travel ---
    ('MinuteDiffBetweenSch', 'MinuteDiffBetweenSch'),
    ('DistanceMilesFromLatLng', 'DistanceMilesFromLatLng'),
    ('AverageMilesPerHour', 'AverageMilesPerHour'),
    ('ETATravleMinutes', 'ETATravelMinutes'),  # Snowflake typo -> PG corrected
    # --- service code ---
    ('ServiceCodeID', 'ServiceCodeID'),
    ('AppServiceCodeID', 'AppServiceCodeID'),
    ('RateType', 'RateType'),
    ('ServiceCode', 'ServiceCode'),
    ('ConServiceCodeID', 'ConServiceCodeID'),
    ('ConAppServiceCodeID', 'ConAppServiceCodeID'),
    ('ConRateType', 'ConRateType'),
    ('ConServiceCode', 'ConServiceCode'),
    # --- conflict flags ---
    ('SameSchTimeFlag', 'SameSchTimeFlag'),
    ('SameVisitTimeFlag', 'SameVisitTimeFlag'),
    ('SchAndVisitTimeSameFlag', 'SchAndVisitTimeSameFlag'),
    ('SchOverAnotherSchTimeFlag', 'SchOverAnotherSchTimeFlag'),
    ('VisitTimeOverAnotherVisitTimeFlag', 'VisitTimeOverAnotherVisitTimeFlag'),
    ('SchTimeOverVisitTimeFlag', 'SchTimeOverVisitTimeFlag'),
    ('DistanceFlag', 'DistanceFlag'),
    # --- missed / EVV ---
    ('IsMissed', 'IsMissed'),
    ('MissedVisitReason', 'MissedVisitReason'),
    ('EVVType', 'EVVType'),
    ('ConIsMissed', 'ConIsMissed'),
    ('ConMissedVisitReason', 'ConMissedVisitReason'),
    ('ConEVVType', 'ConEVVType'),
    # --- agency contact ---
    ('AgencyContact', 'AgencyContact'),
    ('ConAgencyContact', 'ConAgencyContact'),
    ('AgencyPhone', 'AgencyPhone'),
    ('ConAgencyPhone', 'ConAgencyPhone'),
    # --- last updated ---
    ('LastUpdatedBy', 'LastUpdatedBy'),
    ('ConLastUpdatedBy', 'ConLastUpdatedBy'),
    ('LastUpdatedDate', 'LastUpdatedDate'),
    ('ConLastUpdatedDate', 'ConLastUpdatedDate'),
    # --- contract type ---
    ('ContractType', 'ContractType'),
    ('ConContractType', 'ConContractType'),
    # --- federal tax ---
    ('FederalTaxNumber', 'FederalTaxNumber'),
    ('ConFederalTaxNumber', 'ConFederalTaxNumber'),
    # --- provider patient ---
    ('P_PatientID', 'P_PatientID'),
    ('P_AppPatientID', 'P_AppPatientID'),
    ('P_PAdmissionID', 'P_PAdmissionID'),
    ('P_PName', 'P_PName'),
    ('P_PFName', 'P_PFName'),
    ('P_PLName', 'P_PLName'),
    ('P_PMedicaidNumber', 'P_PMedicaidNumber'),
    ('P_PStatus', 'P_PStatus'),
    ('P_PAddressID', 'P_PAddressID'),
    ('P_PAppAddressID', 'P_PAppAddressID'),
    ('P_PAddressL1', 'P_PAddressL1'),
    ('P_PAddressL2', 'P_PAddressL2'),
    ('P_PCity', 'P_PCity'),
    ('P_PAddressState', 'P_PAddressState'),
    ('P_PZipCode', 'P_PZipCode'),
    ('P_PCounty', 'P_PCounty'),
    # --- conflict provider patient ---
    ('ConP_PatientID', 'ConP_PatientID'),
    ('ConP_AppPatientID', 'ConP_AppPatientID'),
    ('ConP_PAdmissionID', 'ConP_PAdmissionID'),
    ('ConP_PName', 'ConP_PName'),
    ('ConP_PFName', 'ConP_PFName'),
    ('ConP_PLName', 'ConP_PLName'),
    ('ConP_PMedicaidNumber', 'ConP_PMedicaidNumber'),
    ('ConP_PStatus', 'ConP_PStatus'),
    ('ConP_PAddressID', 'ConP_PAddressID'),
    ('ConP_PAppAddressID', 'ConP_PAppAddressID'),
    ('ConP_PAddressL1', 'ConP_PAddressL1'),
    ('ConP_PAddressL2', 'ConP_PAddressL2'),
    ('ConP_PCity', 'ConP_PCity'),
    ('ConP_PAddressState', 'ConP_PAddressState'),
    ('ConP_PZipCode', 'ConP_PZipCode'),
    ('ConP_PCounty', 'ConP_PCounty'),
    # --- payer patient ---
    ('PA_PatientID', 'PA_PatientID'),
    ('PA_AppPatientID', 'PA_AppPatientID'),
    ('PA_PAdmissionID', 'PA_PAdmissionID'),
    ('PA_PName', 'PA_PName'),
    ('PA_PFName', 'PA_PFName'),
    ('PA_PLName', 'PA_PLName'),
    ('PA_PMedicaidNumber', 'PA_PMedicaidNumber'),
    ('PA_PStatus', 'PA_PStatus'),
    ('PA_PAddressID', 'PA_PAddressID'),
    ('PA_PAppAddressID', 'PA_PAppAddressID'),
    ('PA_PAddressL1', 'PA_PAddressL1'),
    ('PA_PAddressL2', 'PA_PAddressL2'),
    ('PA_PCity', 'PA_PCity'),
    ('PA_PAddressState', 'PA_PAddressState'),
    ('PA_PZipCode', 'PA_PZipCode'),
    ('PA_PCounty', 'PA_PCounty'),
    # --- conflict payer patient ---
    ('ConPA_PatientID', 'ConPA_PatientID'),
    ('ConPA_AppPatientID', 'ConPA_AppPatientID'),
    ('ConPA_PAdmissionID', 'ConPA_PAdmissionID'),
    ('ConPA_PName', 'ConPA_PName'),
    ('ConPA_PFName', 'ConPA_PFName'),
    ('ConPA_PLName', 'ConPA_PLName'),
    ('ConPA_PMedicaidNumber', 'ConPA_PMedicaidNumber'),
    ('ConPA_PStatus', 'ConPA_PStatus'),
    ('ConPA_PAddressID', 'ConPA_PAddressID'),
    ('ConPA_PAppAddressID', 'ConPA_PAppAddressID'),
    ('ConPA_PAddressL1', 'ConPA_PAddressL1'),
    ('ConPA_PAddressL2', 'ConPA_PAddressL2'),
    ('ConPA_PCity', 'ConPA_PCity'),
    ('ConPA_PAddressState', 'ConPA_PAddressState'),
    ('ConPA_PZipCode', 'ConPA_PZipCode'),
    ('ConPA_PCounty', 'ConPA_PCounty'),
]

# ---------------------------------------------------------------------------
# InService INSERT column mapping: extends INSERT_COLUMN_MAP with 4 InService
# date columns that are NULL for regular conflicts but populated for InService.
# ---------------------------------------------------------------------------
INSERVICE_INSERT_COLUMN_MAP: List[Tuple[str, str]] = INSERT_COLUMN_MAP + [
    # --- InService dates ---
    ('InserviceStartDate', 'InserviceStartDate'),
    ('InserviceEndDate', 'InserviceEndDate'),
    ('ConInserviceStartDate', 'ConInserviceStartDate'),
    ('ConInserviceEndDate', 'ConInserviceEndDate'),
]


class QueryBuilder:
    """Builds parameterized SQL queries for conflict detection"""
    
    def __init__(self, sql_dir: str = 'sql'):
        self.sql_dir = self._resolve_path(sql_dir)
        logger.info(f"QueryBuilder initialized with SQL directory: {self.sql_dir}")
    
    def _resolve_path(self, path: str) -> str:
        """Resolve relative path from script location"""
        if os.path.isabs(path):
            return path
        
        # Get the directory of this file
        lib_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to tasks/ directory
        tasks_dir = os.path.dirname(lib_dir)
        
        return os.path.join(tasks_dir, path)
    
    def load_sql_file(self, filename: str) -> str:
        """
        Load SQL file content
        
        Args:
            filename: SQL file name
        
        Returns:
            SQL content as string
        """
        filepath = os.path.join(self.sql_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"SQL file not found: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    
    def build_conflict_detection_query_v3(
        self,
        db_names: Dict[str, str],
        excluded_agencies: List[str],
        excluded_ssns: List[str],
        settings_data: Dict[str, Any],
        mph_data: List[Dict[str, Any]],
        lookback_years: int = 2,
        lookforward_days: int = 45,
        lookback_hours: int = 36,
        enable_asymmetric_join: bool = False
    ) -> Dict[str, str]:
        """
        Build the v3 conflict detection query using temp tables (multi-step execution)
        
        V3 Approach:
        - Step 1 (asymmetric only): Create delta_keys temp table
        - Step 2: Create base_visits temp table (with conditional filtering)
        - Step 3: Final self-join query on base_visits temp table
        
        Args:
            db_names: Dict with keys: sf_database, sf_schema
            excluded_agencies: List of provider IDs to exclude
            excluded_ssns: List of SSNs to exclude
            settings_data: Dict with ExtraDistancePer value
            mph_data: List of dicts with From, To, AverageMilesPerHour
            lookback_years: Years in past for visit date filter
            lookforward_days: Days in future for visit date filter
            lookback_hours: Hours for updated timestamp filter
            enable_asymmetric_join: If True, uses asymmetric join for comprehensive detection
        
        Returns:
            Dict with keys 'step1', 'step2', 'step3' containing SQL statements
        """
        logger.info("Building v3 conflict detection query (temp table approach)...")
        logger.info(f"  Database params: {db_names}")
        logger.info(f"  Excluded agencies: {len(excluded_agencies)} items")
        logger.info(f"  Excluded SSNs: {len(excluded_ssns)} items (loaded into temp table)")
        logger.info(f"  Date params: -{lookback_years}Y to +{lookforward_days}D, updates in last {lookback_hours}H")
        logger.info(f"  Settings: ExtraDistancePer={settings_data.get('ExtraDistancePer', 100)}")
        logger.info(f"  MPH lookup: {len(mph_data)} ranges")
        logger.info(f"  Query mode: {'ASYMMETRIC' if enable_asymmetric_join else 'SYMMETRIC'}")
        
        # Format exclusion lists for SQL IN clauses
        agencies_csv = format_exclusion_list(excluded_agencies) if excluded_agencies else "''"
        
        # Format MPH data as inline SELECT statements
        mph_selects = []
        for i, row in enumerate(mph_data):
            from_val = row.get('From') if row.get('From') is not None else 0
            to_val = row.get('To') if row.get('To') is not None else 0
            mph_val = row.get('AverageMilesPerHour') if row.get('AverageMilesPerHour') is not None else 1
            
            if from_val is None or to_val is None or mph_val is None:
                logger.warning(f"Skipping invalid MPH row {i}: {row}")
                continue
                
            mph_selects.append(
                f"  SELECT {from_val} AS \"From\", {to_val} AS \"To\", "
                f"{mph_val} AS \"AverageMilesPerHour\""
            )
        
        if not mph_selects:
            logger.warning("No valid MPH data found, using dummy row")
            mph_lookup_sql = "  SELECT -999999 AS \"From\", -999999 AS \"To\", 1 AS \"AverageMilesPerHour\""
        else:
            mph_lookup_sql = "\n  UNION ALL\n".join(mph_selects)
            logger.info(f"Generated MPH lookup SQL with {len(mph_selects)} rows")
        
        queries = {}
        
        # STEP 0: Build excluded_ssns temp table (populated via batch INSERT)
        # With 7000+ SSNs, a temp table is far more efficient than an IN clause
        logger.info(f"  Step 0: Building excluded_ssns temp table ({len(excluded_ssns)} SSNs)")
        queries['step0_create'] = 'CREATE TEMPORARY TABLE IF NOT EXISTS excluded_ssns_temp (ssn VARCHAR)'
        queries['step0_inserts'] = self._build_ssn_insert_batches(excluded_ssns)
        
        # STEP 1: Delta keys temp table
        # Always create delta_keys now - needed for both asymmetric join AND stale cleanup scoping
        logger.info("  Step 1: Creating delta_keys temp table")
        step1_template = self.load_sql_file('sf_task02_00_step1_delta_keys.sql')
        queries['step1'] = step1_template.format(
            sf_database=db_names['sf_database'],
            sf_schema=db_names['sf_schema'],
            lookback_hours=lookback_hours,
            lookback_years=lookback_years,
            lookforward_days=lookforward_days,
            excluded_agencies=agencies_csv
        )
        if not enable_asymmetric_join:
            logger.info("    (symmetric mode: delta_keys used for stale cleanup scoping only)")
        
        # STEP 2: Base visits temp table
        # Uses a reusable template with placeholders for CREATE vs INSERT,
        # is_delta value, optional delta_keys JOIN, and timestamp condition.
        logger.info("  Step 2: Creating base_visits temp table")
        step2_template = self.load_sql_file('sf_task02_00_step2_base_visits.sql')
        
        # Common format args shared by both Part A and Part B
        common_args = dict(
            sf_database=db_names['sf_database'],
            sf_schema=db_names['sf_schema'],
            lookback_years=lookback_years,
            lookforward_days=lookforward_days,
            excluded_agencies=agencies_csv,
        )
        
        # Part A (always): CREATE base_visits with delta rows only (~70K, fast)
        # Uses partition pruning on timestamp >= (recent window)
        queries['step2'] = step2_template.format(
            **common_args,
            TABLE_CLAUSE='CREATE TEMPORARY TABLE IF NOT EXISTS base_visits AS',
            is_delta_value=1,
            DELTA_KEYS_JOIN='',
            TIMESTAMP_CONDITION=f'CR1."Visit Updated Timestamp" >= DATEADD(HOUR, -{lookback_hours}, GETDATE())',
        )
        
        if enable_asymmetric_join:
            # Part B (asymmetric only): INSERT related non-delta rows via INNER JOIN with delta_keys
            # Uses explicit JOIN (not IN subquery) and excludes delta rows to avoid duplicates
            logger.info("  Step 2 (Part B): INSERT related non-delta visits via delta_keys JOIN")
            delta_keys_join = """INNER JOIN delta_keys DK
    ON DATE(CR1."Visit Date") = DK.visit_date
    AND TRIM(CAR."SSN") = DK.ssn"""
            queries['step2_asym_insert'] = step2_template.format(
                **common_args,
                TABLE_CLAUSE='INSERT INTO base_visits',
                is_delta_value=0,
                DELTA_KEYS_JOIN=delta_keys_join,
                TIMESTAMP_CONDITION=f'CR1."Visit Updated Timestamp" < DATEADD(HOUR, -{lookback_hours}, GETDATE())',
            )
        
        # STEP 2d: Query to collect actual (visit_date, ssn) pairs from delta_keys
        # These are the exact scope that Snowflake scanned for conflict detection.
        # Using actual pairs instead of separate DISTINCT SSN + DISTINCT date lists
        # avoids the cross-product problem (507K SSNs x 597 dates = 302M combos)
        # that caused 3.9M false stale records. The actual pairs (~12M) are streamed
        # to Postgres via chunked COPY for precise stale scoping.
        logger.info("  Step 2d: Actual (visit_date, ssn) pairs for precise stale cleanup scope")
        queries['step2d'] = """SELECT visit_date, ssn FROM delta_keys"""
        
        # STEP 3: Final conflict detection query (self-join on base_visits)
        logger.info("  Step 3: Final conflict detection query on base_visits")
        step3_template = self.load_sql_file('sf_task02_00_step3_final_query.sql')
        
        # In asymmetric mode, constrain the self-join so at least one side (V1) is a delta visit.
        # This avoids the expensive all-vs-all self-join on ~9.6M rows.
        # In symmetric mode, all rows are delta visits anyway, so no condition needed.
        if enable_asymmetric_join:
            asymmetric_join_condition = 'AND V1."is_delta" = 1'
        else:
            asymmetric_join_condition = ""  # Symmetric mode: all rows are delta
        
        queries['step3'] = step3_template.format(
            mph_lookup=mph_lookup_sql,
            extra_distance_per=settings_data.get('ExtraDistancePer', 100),
            ASYMMETRIC_JOIN_CONDITION=asymmetric_join_condition
        )
        
        logger.info("✓ V3 conflict detection queries built successfully")
        return queries
    
    @staticmethod
    def _build_ssn_insert_batches(excluded_ssns: List[str], batch_size: int = 1000) -> List[str]:
        """
        Build batch INSERT statements for excluded SSNs temp table.
        
        With 7000+ SSNs, we can't use a single IN clause. Instead we populate
        a Snowflake temp table and reference it via subquery in step1/step2.
        
        Args:
            excluded_ssns: List of SSN strings to exclude
            batch_size: Number of SSNs per INSERT statement
        
        Returns:
            List of INSERT SQL statements
        """
        if not excluded_ssns:
            return []
        
        inserts = []
        for i in range(0, len(excluded_ssns), batch_size):
            batch = excluded_ssns[i:i + batch_size]
            # Escape single quotes in SSN values for safety
            values = ','.join([f"('{ssn.replace(chr(39), chr(39)+chr(39))}')" for ssn in batch])
            inserts.append(f"INSERT INTO excluded_ssns_temp (ssn) VALUES {values}")
        
        return inserts
    
    def build_reference_query(self, query_name: str, db_names: Dict[str, str]) -> str:
        """
        Build reference data query (excluded agencies, SSNs, settings, mph)
        
        Args:
            query_name: Name of query file (e.g., 'pg_fetch_excluded_agencies.sql')
            db_names: Dict with pg_database, pg_schema keys
        
        Returns:
            Parameterized SQL query
        """
        query_template = self.load_sql_file(query_name)
        
        query = query_template.format(
            pg_database=db_names['pg_database'],
            pg_schema=db_names['pg_schema']
        )
        
        return query
    
    def build_update_statement(
        self,
        conflict_row: Dict[str, Any],
        db_names: Dict[str, str],
        existing_row: Dict[str, Any],
        update_columns: List[str] = None
    ) -> tuple:
        """
        Build UPDATE statement for a single conflict record with conditional logic
        
        Args:
            conflict_row: Dictionary of column values from Snowflake result
            db_names: Dict with pg_database, pg_schema keys
            existing_row: Dictionary of existing column values from Postgres
            update_columns: List of columns to update (if None, updates all)
        
        Returns:
            Tuple of (sql_statement, params_tuple)
        """
        schema = db_names['pg_schema']
        
        # Column name mapping: Snowflake typo -> Postgres corrected name
        column_name_map = {
            'ETATravleMinutes': 'ETATravelMinutes',  # Fix Snowflake typo
            'SchVisitTimeSame': 'SchAndVisitTimeSameFlag'  # Fix shortened name
        }
        
        # Define columns that require conditional updates
        conditional_flag_columns = [
            'SameSchTimeFlag',
            'SameVisitTimeFlag',
            'SchAndVisitTimeSameFlag',
            'SchOverAnotherSchTimeFlag',
            'VisitTimeOverAnotherVisitTimeFlag',
            'SchTimeOverVisitTimeFlag',
            'DistanceFlag'
        ]
        
        # If no specific columns provided, generate from conflict_row
        if update_columns is None:
            # Exclude keys used for WHERE clause
            update_columns = [col for col in conflict_row.keys() 
                            if col not in ('VisitID', 'ConVisitID')]
        
        # Build SET clause
        set_clauses = []
        params = []
        
        for col in update_columns:
            if col not in conflict_row:
                continue
            
            # Translate column name if needed (Snowflake typo -> Postgres corrected)
            postgres_col = column_name_map.get(col, col)
                
            # Handle StatusFlag with conditional logic
            # Original: CASE WHEN CVM."StatusFlag" NOT IN ('W', 'I') THEN 'U' ELSE CVM."StatusFlag" END
            if col == 'StatusFlag':
                existing_status = existing_row.get('StatusFlag', '')
                if existing_status not in ('W', 'I'):
                    set_clauses.append('"StatusFlag" = %s')
                    params.append('U')  # Set to 'U' (Updated)
                # else: preserve existing 'W' (Whitelist) or 'I' (Ignore)
                continue
            
            # Handle conflict rule flags with conditional logic
            # Original: CASE WHEN CVM."FlagName" = 'N' THEN ALLDATA."FlagName" ELSE CVM."FlagName" END
            if col in conditional_flag_columns:
                existing_flag = existing_row.get(postgres_col, 'N')  # Use postgres_col for lookup
                if existing_flag == 'N':
                    # Only update if currently 'N' (not confirmed)
                    set_clauses.append(f'"{postgres_col}" = %s')  # Use postgres_col in SQL
                    params.append(conflict_row[col])
                # else: preserve existing value (manually confirmed 'Y' or other)
                continue
            
            # All other columns: unconditional update
            set_clauses.append(f'"{postgres_col}" = %s')  # Use postgres_col in SQL
            params.append(conflict_row[col])
        
        # Add special columns with fixed values
        set_clauses.append('"UpdateFlag" = NULL')
        set_clauses.append('"UpdatedDate" = CURRENT_TIMESTAMP')
        set_clauses.append('"ResolveDate" = NULL')
        
        # Build SQL with WHERE clause matching original logic:
        # (VisitID = ? AND ConVisitID = ?) OR (VisitID = ? AND ConVisitID IS NULL AND ? IS NULL)
        sql = f"""
            UPDATE {schema}.conflictvisitmaps
            SET {', '.join(set_clauses)}
            WHERE (
                ("VisitID" = %s AND "ConVisitID" = %s)
                OR ("VisitID" = %s AND "ConVisitID" IS NULL AND %s IS NULL)
            )
        """
        
        # Add WHERE clause params
        visit_id = conflict_row['VisitID']
        con_visit_id = conflict_row.get('ConVisitID')
        
        # For the OR clause: VisitID twice, ConVisitID twice
        params.extend([visit_id, con_visit_id, visit_id, con_visit_id])
        
        return (sql, tuple(params))
    
    def build_insert_template(self, db_names: Dict[str, str]) -> Tuple[str, List[str]]:
        """
        Build a parameterised INSERT template for new conflict records.
        
        The template is executed via psycopg2 ``execute_batch`` with a list of
        param tuples (one per row).  Fixed-value columns (StatusFlag, InServiceFlag,
        PTOFlag, CreatedDate) are embedded as SQL literals so they don't need to
        appear in the param tuple.
        
        Returns:
            Tuple of:
              - sql: INSERT statement with %s placeholders for data columns
              - sf_columns: ordered list of Snowflake dict-key names whose values
                must be extracted (in order) to build each param tuple
        """
        schema = db_names['pg_schema']
        
        # Snowflake column keys (for param extraction) and PG column names
        sf_columns = [sf for sf, _pg in INSERT_COLUMN_MAP]
        pg_columns = [pg for _sf, pg in INSERT_COLUMN_MAP]
        
        # Build the column list: data columns + fixed-value columns
        all_pg_cols = pg_columns + [
            'StatusFlag', 'InServiceFlag', 'PTOFlag', 'CreatedDate',
        ]
        col_list = ', '.join(f'"{c}"' for c in all_pg_cols)
        
        # Build the VALUES clause: %s for each data column, literals for fixed
        data_placeholders = ', '.join(['%s'] * len(pg_columns))
        fixed_literals = "'N', 'N', 'N', CURRENT_TIMESTAMP"
        values_clause = f"{data_placeholders}, {fixed_literals}"
        
        sql = (
            f"INSERT INTO {schema}.conflictvisitmaps ({col_list}) "
            f"VALUES ({values_clause})"
        )
        
        logger.info(f"Built INSERT template: {len(pg_columns)} data columns + 4 fixed")
        return sql, sf_columns

    # ------------------------------------------------------------------
    # InService conflict queries
    # ------------------------------------------------------------------

    def build_inservice_queries(
        self,
        db_names: Dict[str, str],
        excluded_agencies: List[str],
        excluded_ssns: List[str],
        lookback_years: int = 2,
        lookforward_days: int = 45,
    ) -> Dict[str, Any]:
        """
        Build the multi-step InService conflict detection queries.

        Steps:
          0. Create + populate excluded_ssns_temp table
          1. Create inservice_visits temp table (eligible visits)
          2. Create inservice_events temp table (InService events with synthetic VisitID)
          3. Final UNION ALL query producing pairs in both directions

        Returns:
            Dict with keys 'step0_create', 'step0_inserts', 'step1', 'step2', 'step3'
        """
        logger.info("Building InService conflict detection queries...")
        logger.info(f"  Database params: {db_names}")
        logger.info(f"  Excluded agencies: {len(excluded_agencies)} items")
        logger.info(f"  Excluded SSNs: {len(excluded_ssns)} items")
        logger.info(f"  Date params: -{lookback_years}Y to +{lookforward_days}D")

        agencies_csv = format_exclusion_list(excluded_agencies) if excluded_agencies else "''"

        queries: Dict[str, Any] = {}

        # STEP 0: excluded_ssns temp table (reuse existing helper)
        queries['step0_create'] = (
            'CREATE TEMPORARY TABLE IF NOT EXISTS excluded_ssns_temp (ssn VARCHAR)'
        )
        queries['step0_inserts'] = self._build_ssn_insert_batches(excluded_ssns)

        common_args = dict(
            sf_database=db_names['sf_database'],
            sf_schema=db_names['sf_schema'],
            lookback_years=lookback_years,
            lookforward_days=lookforward_days,
            excluded_agencies=agencies_csv,
        )

        # STEP 1: Visits temp table
        logger.info("  Step 1: inservice_visits temp table")
        step1_template = self.load_sql_file('sf_task02_01_step1_visits.sql')
        queries['step1'] = step1_template.format(**common_args)

        # STEP 2: InService events temp table
        logger.info("  Step 2: inservice_events temp table")
        step2_template = self.load_sql_file('sf_task02_01_step2_events.sql')
        queries['step2'] = step2_template.format(**common_args)

        # STEP 3: Final pairs query (UNION ALL)
        logger.info("  Step 3: Final InService pairs query (UNION ALL)")
        queries['step3'] = self.load_sql_file('sf_task02_01_step3_pairs.sql')

        logger.info("✓ InService conflict detection queries built successfully")
        return queries

    def build_inservice_insert_template(
        self, db_names: Dict[str, str]
    ) -> Tuple[str, List[str]]:
        """
        Build a parameterised INSERT template for InService conflict records.

        Same structure as ``build_insert_template`` but:
          - Uses INSERVICE_INSERT_COLUMN_MAP (includes 4 InService date columns)
          - Sets InServiceFlag = 'Y' (instead of 'N')

        Returns:
            Tuple of (sql, sf_columns) -- see ``build_insert_template`` docstring.
        """
        schema = db_names['pg_schema']

        sf_columns = [sf for sf, _pg in INSERVICE_INSERT_COLUMN_MAP]
        pg_columns = [pg for _sf, pg in INSERVICE_INSERT_COLUMN_MAP]

        all_pg_cols = pg_columns + [
            'StatusFlag', 'InServiceFlag', 'PTOFlag', 'CreatedDate',
        ]
        col_list = ', '.join(f'"{c}"' for c in all_pg_cols)

        data_placeholders = ', '.join(['%s'] * len(pg_columns))
        # InServiceFlag = 'Y' (key difference from regular insert)
        fixed_literals = "'N', 'Y', 'N', CURRENT_TIMESTAMP"
        values_clause = f"{data_placeholders}, {fixed_literals}"

        sql = (
            f"INSERT INTO {schema}.conflictvisitmaps ({col_list}) "
            f"VALUES ({values_clause})"
        )

        logger.info(
            f"Built InService INSERT template: {len(pg_columns)} data columns + 4 fixed"
        )
        return sql, sf_columns
