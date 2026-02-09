"""
Query Builder for Task 02 Conflict Detection
Loads SQL templates and injects parameters dynamically
"""

import os
from typing import Dict, List, Any
from .utils import get_logger, format_exclusion_list

logger = get_logger(__name__)


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
    
    def build_conflict_detection_query(
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
    ) -> str:
        """
        Build the main conflict detection query with all parameters injected
        Uses v2 template with conditional asymmetric join support
        
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
            Fully parameterized SQL query ready for execution
        """
        logger.info("Building conflict detection query...")
        logger.info(f"  Database params: {db_names}")
        logger.info(f"  Excluded agencies: {len(excluded_agencies)} items")
        logger.info(f"  Excluded SSNs: SKIPPED ({len(excluded_ssns)} empty - performance test)")
        logger.info(f"  Date params: -{lookback_years}Y to +{lookforward_days}D, updates in last {lookback_hours}H")
        logger.info(f"  Settings: ExtraDistancePer={settings_data.get('ExtraDistancePer', 100)}")
        logger.info(f"  MPH lookup: {len(mph_data)} ranges")
        logger.info(f"  Query mode: {'ASYMMETRIC' if enable_asymmetric_join else 'SYMMETRIC'}")
        
        # Load v2 template with placeholders
        query_template = self.load_sql_file('sf_task02_conflict_detection_v2.sql')
        
        # Format exclusion lists for SQL IN clauses
        agencies_csv = format_exclusion_list(excluded_agencies) if excluded_agencies else "''"
        ssns_csv = format_exclusion_list(excluded_ssns) if excluded_ssns else "''"
        
        # Format MPH data as inline SELECT statements
        mph_selects = []
        for i, row in enumerate(mph_data):
            from_val = row.get('From') if row.get('From') is not None else 0
            to_val = row.get('To') if row.get('To') is not None else 0
            mph_val = row.get('AverageMilesPerHour') if row.get('AverageMilesPerHour') is not None else 1
            
            # Skip invalid rows
            if from_val is None or to_val is None or mph_val is None:
                logger.warning(f"Skipping invalid MPH row {i}: {row}")
                continue
                
            mph_selects.append(
                f"  SELECT {from_val} AS \"From\", {to_val} AS \"To\", "
                f"{mph_val} AS \"AverageMilesPerHour\""
            )
        
        # If no valid MPH data, provide a dummy row that won't match any distance
        if not mph_selects:
            logger.warning("No valid MPH data found, using dummy row")
            mph_lookup_sql = "  SELECT -999999 AS \"From\", -999999 AS \"To\", 1 AS \"AverageMilesPerHour\""
        else:
            mph_lookup_sql = "\n  UNION ALL\n".join(mph_selects)
            logger.info(f"Generated MPH lookup SQL with {len(mph_selects)} rows")
        
        # Build conditional blocks based on enable_asymmetric_join
        if enable_asymmetric_join:
            logger.info("  Building asymmetric join structure (delta + all_visits + UNION)")
            # Asymmetric mode: No timestamp filter in base_visits (need all data for all_visits)
            base_visits_timestamp_filter = ""
            # Use {{}} to escape braces so .format() doesn't try to replace them
            delta_visits_timestamp_filter = f'WHERE "LastUpdatedDate" >= DATEADD(HOUR, -{lookback_hours}, GETDATE())'
            
            asymmetric_delta_keys = """
-- CTE 2b: Extract unique (VisitDate, SSN) keys from delta visits for targeted fetching
delta_conflict_keys AS (
  SELECT DISTINCT 
    "VisitDate",
    "SSN"
  FROM delta_visits
),

"""
            asymmetric_all_visits = """
-- CTE 2c: All visits matching delta conflict keys (optimized - only fetch relevant records)
all_visits AS (
  SELECT * 
  FROM base_visits
  WHERE ("VisitDate", "SSN") IN (
    SELECT "VisitDate", "SSN" FROM delta_conflict_keys
  )
),

"""
            conflict_pairs_join = self._build_asymmetric_conflict_pairs()
        else:
            logger.info("  Building symmetric join structure (delta self-join)")
            # Symmetric mode: Apply timestamp filter directly in base_visits for efficiency
            # Use f-string to inject lookback_hours value immediately
            base_visits_timestamp_filter = f'AND CR1."Visit Updated Timestamp" >= DATEADD(HOUR, -{lookback_hours}, GETDATE())'
            delta_visits_timestamp_filter = ""  # No additional filter needed
            
            asymmetric_delta_keys = ""
            asymmetric_all_visits = ""
            conflict_pairs_join = self._build_symmetric_conflict_pairs()
        
        # #region agent log
        logger.info(f"DEBUG: Template loaded, length={len(query_template)}, starts_with={query_template[:50]}")
        # #endregion
        
        # Replace conditional placeholders
        query = query_template.replace('{ASYMMETRIC_DELTA_KEYS}', asymmetric_delta_keys)
        # #region agent log
        logger.info(f"DEBUG: After DELTA_KEYS replace, length={len(query)}, starts_with={query[:50]}")
        # #endregion
        
        query = query.replace('{ASYMMETRIC_ALL_VISITS}', asymmetric_all_visits)
        # #region agent log
        logger.info(f"DEBUG: After ALL_VISITS replace, length={len(query)}, starts_with={query[:50]}")
        # #endregion
        
        query = query.replace('{CONFLICT_PAIRS_JOIN}', conflict_pairs_join)
        # #region agent log
        logger.info(f"DEBUG: After CONFLICT_PAIRS replace, length={len(query)}, starts_with={query[:50]}")
        # #endregion
        
        query = query.replace('{base_visits_timestamp_filter}', base_visits_timestamp_filter)
        # #region agent log
        logger.info(f"DEBUG: After base_visits_filter replace, length={len(query)}, starts_with={query[:50]}")
        # #endregion
        
        query = query.replace('{delta_visits_timestamp_filter}', delta_visits_timestamp_filter)
        # #region agent log
        logger.info(f"DEBUG: After delta_visits_filter replace, length={len(query)}, starts_with={query[:50]}")
        logger.info(f"DEBUG: About to call .format(), checking for remaining braces...")
        logger.info(f"DEBUG: asymmetric_delta_keys content preview: {asymmetric_delta_keys[:200] if asymmetric_delta_keys else 'EMPTY'}")
        # #endregion
        
        # Inject all parameters
        query = query.format(
            sf_database=db_names['sf_database'],
            sf_schema=db_names['sf_schema'],
            excluded_agencies=agencies_csv,
            extra_distance_per=settings_data.get('ExtraDistancePer', 100),
            mph_lookup=mph_lookup_sql,
            lookback_years=lookback_years,
            lookforward_days=lookforward_days,
            lookback_hours=lookback_hours
        )
        
        # #region agent log
        logger.info(f"DEBUG: After .format(), length={len(query)}, starts_with={query[:100]}")
        
        # Find WHERE the WITH keyword is
        with_idx = query.find('WITH')
        logger.info(f"DEBUG: 'WITH' keyword found at position {with_idx}")
        if with_idx >= 0:
            logger.info(f"DEBUG: Context around WITH (pos {with_idx}): {repr(query[max(0,with_idx-50):with_idx+100])}")
        
        # Find where delta_conflict_keys appears
        delta_idx = query.find('delta_conflict_keys')
        logger.info(f"DEBUG: 'delta_conflict_keys' found at position {delta_idx}")
        if delta_idx >= 0:
            logger.info(f"DEBUG: Context around delta_conflict_keys (pos {delta_idx}): {repr(query[max(0,delta_idx-100):delta_idx+50])}")
        # #endregion
        
        logger.info("✓ Conflict detection query built successfully")
        
        return query
    
    def _build_symmetric_conflict_pairs(self) -> str:
        """Build symmetric self-join (delta_visits vs delta_visits)"""
        return """  -- Symmetric self-join: Delta vs Delta only
  SELECT 
    V1."CONFLICTID",
    V1."SSN",
    V1."ProviderID", V1."AppProviderID", V1."ProviderName", V1."VisitID", V1."AppVisitID",
    V2."ProviderID" AS "ConProviderID", V2."AppProviderID" AS "ConAppProviderID", V2."ProviderName" AS "ConProviderName",
    V2."VisitID" AS "ConVisitID", V2."AppVisitID" AS "ConAppVisitID",
    V1."VisitDate",
    V1."SchStartTime", V1."SchEndTime", V2."SchStartTime" AS "ConSchStartTime", V2."SchEndTime" AS "ConSchEndTime",
    V1."VisitStartTime", V1."VisitEndTime", V2."VisitStartTime" AS "ConVisitStartTime", V2."VisitEndTime" AS "ConVisitEndTime",
    V1."EVVStartTime", V1."EVVEndTime", V2."EVVStartTime" AS "ConEVVStartTime", V2."EVVEndTime" AS "ConEVVEndTime",
    V1."CaregiverID", V1."AppCaregiverID", V1."AideCode", V1."AideName", V1."AideSSN",
    V2."CaregiverID" AS "ConCaregiverID", V2."AppCaregiverID" AS "ConAppCaregiverID", 
    V2."AideCode" AS "ConAideCode", V2."AideName" AS "ConAideName", V2."AideSSN" AS "ConAideSSN",
    V1."OfficeID", V1."AppOfficeID", V1."Office", 
    V2."OfficeID" AS "ConOfficeID", V2."AppOfficeID" AS "ConAppOfficeID", V2."Office" AS "ConOffice",
    V1."PatientID", V1."AppPatientID", V1."PAdmissionID", V1."PName", V1."PAddressID", V1."PAppAddressID",
    V1."PAddressL1", V1."PAddressL2", V1."PCity", V1."PAddressState", V1."PZipCode", V1."PCounty",
    V1."Longitude", V1."Latitude",
    V2."PatientID" AS "ConPatientID", V2."AppPatientID" AS "ConAppPatientID", V2."PAdmissionID" AS "ConPAdmissionID",
    V2."PName" AS "ConPName", V2."PAddressID" AS "ConPAddressID", V2."PAppAddressID" AS "ConPAppAddressID",
    V2."PAddressL1" AS "ConPAddressL1", V2."PAddressL2" AS "ConPAddressL2", V2."PCity" AS "ConPCity",
    V2."PAddressState" AS "ConPAddressState", V2."PZipCode" AS "ConPZipCode", V2."PCounty" AS "ConPCounty",
    V2."Longitude" AS "ConLongitude", V2."Latitude" AS "ConLatitude",
    V1."PayerID", V1."AppPayerID", V1."Contract", V2."PayerID" AS "ConPayerID", V2."AppPayerID" AS "ConAppPayerID", V2."Contract" AS "ConContract",
    V1."BilledDate", V2."BilledDate" AS "ConBilledDate", V1."BilledHours", V2."BilledHours" AS "ConBilledHours",
    V1."Billed", V2."Billed" AS "ConBilled",
    V1."ServiceCodeID", V1."AppServiceCodeID", V1."RateType", V1."ServiceCode",
    V2."ServiceCodeID" AS "ConServiceCodeID", V2."AppServiceCodeID" AS "ConAppServiceCodeID", 
    V2."RateType" AS "ConRateType", V2."ServiceCode" AS "ConServiceCode",
    V1."AideFName", V1."AideLName", V2."AideFName" AS "ConAideFName", V2."AideLName" AS "ConAideLName",
    V1."PFName", V1."PLName", V2."PFName" AS "ConPFName", V2."PLName" AS "ConPLName",
    V1."PMedicaidNumber", V2."PMedicaidNumber" AS "ConPMedicaidNumber",
    V1."PayerState", V2."PayerState" AS "ConPayerState",
    V1."LastUpdatedBy", V2."LastUpdatedBy" AS "ConLastUpdatedBy",
    V1."LastUpdatedDate", V2."LastUpdatedDate" AS "ConLastUpdatedDate",
    V1."BilledRate", V1."TotalBilledAmount", V2."BilledRate" AS "ConBilledRate", V2."TotalBilledAmount" AS "ConTotalBilledAmount",
    V1."IsMissed", V1."MissedVisitReason", V1."EVVType",
    V2."IsMissed" AS "ConIsMissed", V2."MissedVisitReason" AS "ConMissedVisitReason", V2."EVVType" AS "ConEVVType",
    V1."PStatus", V2."PStatus" AS "ConPStatus", V1."AideStatus", V2."AideStatus" AS "ConAideStatus",
    V1."P_PatientID", V1."P_AppPatientID", V2."P_PatientID" AS "ConP_PatientID", V2."P_AppPatientID" AS "ConP_AppPatientID",
    V1."PA_PatientID", V1."PA_AppPatientID", V2."PA_PatientID" AS "ConPA_PatientID", V2."PA_AppPatientID" AS "ConPA_AppPatientID",
    V1."P_PAdmissionID", V1."P_PName", V1."P_PAddressID", V1."P_PAppAddressID", V1."P_PAddressL1", V1."P_PAddressL2",
    V1."P_PCity", V1."P_PAddressState", V1."P_PZipCode", V1."P_PCounty", V1."P_PFName", V1."P_PLName", V1."P_PMedicaidNumber",
    V2."P_PAdmissionID" AS "ConP_PAdmissionID", V2."P_PName" AS "ConP_PName", V2."P_PAddressID" AS "ConP_PAddressID",
    V2."P_PAppAddressID" AS "ConP_PAppAddressID", V2."P_PAddressL1" AS "ConP_PAddressL1", V2."P_PAddressL2" AS "ConP_PAddressL2",
    V2."P_PCity" AS "ConP_PCity", V2."P_PAddressState" AS "ConP_PAddressState", V2."P_PZipCode" AS "ConP_PZipCode",
    V2."P_PCounty" AS "ConP_PCounty", V2."P_PFName" AS "ConP_PFName", V2."P_PLName" AS "ConP_PLName", V2."P_PMedicaidNumber" AS "ConP_PMedicaidNumber",
    V1."PA_PAdmissionID", V1."PA_PName", V1."PA_PAddressID", V1."PA_PAppAddressID", V1."PA_PAddressL1", V1."PA_PAddressL2",
    V1."PA_PCity", V1."PA_PAddressState", V1."PA_PZipCode", V1."PA_PCounty", V1."PA_PFName", V1."PA_PLName", V1."PA_PMedicaidNumber",
    V2."PA_PAdmissionID" AS "ConPA_PAdmissionID", V2."PA_PName" AS "ConPA_PName", V2."PA_PAddressID" AS "ConPA_PAddressID",
    V2."PA_PAppAddressID" AS "ConPA_PAppAddressID", V2."PA_PAddressL1" AS "ConPA_PAddressL1", V2."PA_PAddressL2" AS "ConPA_PAddressL2",
    V2."PA_PCity" AS "ConPA_PCity", V2."PA_PAddressState" AS "ConPA_PAddressState", V2."PA_PZipCode" AS "ConPA_PZipCode",
    V2."PA_PCounty" AS "ConPA_PCounty", V2."PA_PFName" AS "ConPA_PFName", V2."PA_PLName" AS "ConPA_PLName", V2."PA_PMedicaidNumber" AS "ConPA_PMedicaidNumber",
    V1."ContractType", V2."ContractType" AS "ConContractType",
    V1."P_PStatus", V2."P_PStatus" AS "ConP_PStatus",
    V1."PA_PStatus", V2."PA_PStatus" AS "ConPA_PStatus",
    V1."BillRateNonBilled", V2."BillRateNonBilled" AS "ConBillRateNonBilled",
    V1."BillRateBoth", V2."BillRateBoth" AS "ConBillRateBoth",
    V1."FederalTaxNumber", V2."FederalTaxNumber" AS "ConFederalTaxNumber"
  FROM delta_visits V1
  INNER JOIN delta_visits V2 
    ON V1."VisitDate" = V2."VisitDate"
    AND V1."SSN" = V2."SSN"
    AND V1."ProviderID" != V2."ProviderID"
    AND V1."VisitID" != V2."VisitID"
"""
    
    def _build_asymmetric_conflict_pairs(self) -> str:
        """Build asymmetric UNION join (delta vs all, all vs delta)"""
        return """  -- Direction 1: Delta visits joined with all visits
  SELECT 
    V1."CONFLICTID",
    V1."SSN",
    V1."ProviderID", V1."AppProviderID", V1."ProviderName", V1."VisitID", V1."AppVisitID",
    V2."ProviderID" AS "ConProviderID", V2."AppProviderID" AS "ConAppProviderID", V2."ProviderName" AS "ConProviderName",
    V2."VisitID" AS "ConVisitID", V2."AppVisitID" AS "ConAppVisitID",
    V1."VisitDate",
    V1."SchStartTime", V1."SchEndTime", V2."SchStartTime" AS "ConSchStartTime", V2."SchEndTime" AS "ConSchEndTime",
    V1."VisitStartTime", V1."VisitEndTime", V2."VisitStartTime" AS "ConVisitStartTime", V2."VisitEndTime" AS "ConVisitEndTime",
    V1."EVVStartTime", V1."EVVEndTime", V2."EVVStartTime" AS "ConEVVStartTime", V2."EVVEndTime" AS "ConEVVEndTime",
    V1."CaregiverID", V1."AppCaregiverID", V1."AideCode", V1."AideName", V1."AideSSN",
    V2."CaregiverID" AS "ConCaregiverID", V2."AppCaregiverID" AS "ConAppCaregiverID", 
    V2."AideCode" AS "ConAideCode", V2."AideName" AS "ConAideName", V2."AideSSN" AS "ConAideSSN",
    V1."OfficeID", V1."AppOfficeID", V1."Office", 
    V2."OfficeID" AS "ConOfficeID", V2."AppOfficeID" AS "ConAppOfficeID", V2."Office" AS "ConOffice",
    V1."PatientID", V1."AppPatientID", V1."PAdmissionID", V1."PName", V1."PAddressID", V1."PAppAddressID",
    V1."PAddressL1", V1."PAddressL2", V1."PCity", V1."PAddressState", V1."PZipCode", V1."PCounty",
    V1."Longitude", V1."Latitude",
    V2."PatientID" AS "ConPatientID", V2."AppPatientID" AS "ConAppPatientID", V2."PAdmissionID" AS "ConPAdmissionID",
    V2."PName" AS "ConPName", V2."PAddressID" AS "ConPAddressID", V2."PAppAddressID" AS "ConPAppAddressID",
    V2."PAddressL1" AS "ConPAddressL1", V2."PAddressL2" AS "ConPAddressL2", V2."PCity" AS "ConPCity",
    V2."PAddressState" AS "ConPAddressState", V2."PZipCode" AS "ConPZipCode", V2."PCounty" AS "ConPCounty",
    V2."Longitude" AS "ConLongitude", V2."Latitude" AS "ConLatitude",
    V1."PayerID", V1."AppPayerID", V1."Contract", V2."PayerID" AS "ConPayerID", V2."AppPayerID" AS "ConAppPayerID", V2."Contract" AS "ConContract",
    V1."BilledDate", V2."BilledDate" AS "ConBilledDate", V1."BilledHours", V2."BilledHours" AS "ConBilledHours",
    V1."Billed", V2."Billed" AS "ConBilled",
    V1."ServiceCodeID", V1."AppServiceCodeID", V1."RateType", V1."ServiceCode",
    V2."ServiceCodeID" AS "ConServiceCodeID", V2."AppServiceCodeID" AS "ConAppServiceCodeID", 
    V2."RateType" AS "ConRateType", V2."ServiceCode" AS "ConServiceCode",
    V1."AideFName", V1."AideLName", V2."AideFName" AS "ConAideFName", V2."AideLName" AS "ConAideLName",
    V1."PFName", V1."PLName", V2."PFName" AS "ConPFName", V2."PLName" AS "ConPLName",
    V1."PMedicaidNumber", V2."PMedicaidNumber" AS "ConPMedicaidNumber",
    V1."PayerState", V2."PayerState" AS "ConPayerState",
    V1."LastUpdatedBy", V2."LastUpdatedBy" AS "ConLastUpdatedBy",
    V1."LastUpdatedDate", V2."LastUpdatedDate" AS "ConLastUpdatedDate",
    V1."BilledRate", V1."TotalBilledAmount", V2."BilledRate" AS "ConBilledRate", V2."TotalBilledAmount" AS "ConTotalBilledAmount",
    V1."IsMissed", V1."MissedVisitReason", V1."EVVType",
    V2."IsMissed" AS "ConIsMissed", V2."MissedVisitReason" AS "ConMissedVisitReason", V2."EVVType" AS "ConEVVType",
    V1."PStatus", V2."PStatus" AS "ConPStatus", V1."AideStatus", V2."AideStatus" AS "ConAideStatus",
    V1."P_PatientID", V1."P_AppPatientID", V2."P_PatientID" AS "ConP_PatientID", V2."P_AppPatientID" AS "ConP_AppPatientID",
    V1."PA_PatientID", V1."PA_AppPatientID", V2."PA_PatientID" AS "ConPA_PatientID", V2."PA_AppPatientID" AS "ConPA_AppPatientID",
    V1."P_PAdmissionID", V1."P_PName", V1."P_PAddressID", V1."P_PAppAddressID", V1."P_PAddressL1", V1."P_PAddressL2",
    V1."P_PCity", V1."P_PAddressState", V1."P_PZipCode", V1."P_PCounty", V1."P_PFName", V1."P_PLName", V1."P_PMedicaidNumber",
    V2."P_PAdmissionID" AS "ConP_PAdmissionID", V2."P_PName" AS "ConP_PName", V2."P_PAddressID" AS "ConP_PAddressID",
    V2."P_PAppAddressID" AS "ConP_PAppAddressID", V2."P_PAddressL1" AS "ConP_PAddressL1", V2."P_PAddressL2" AS "ConP_PAddressL2",
    V2."P_PCity" AS "ConP_PCity", V2."P_PAddressState" AS "ConP_PAddressState", V2."P_PZipCode" AS "ConP_PZipCode",
    V2."P_PCounty" AS "ConP_PCounty", V2."P_PFName" AS "ConP_PFName", V2."P_PLName" AS "ConP_PLName", V2."P_PMedicaidNumber" AS "ConP_PMedicaidNumber",
    V1."PA_PAdmissionID", V1."PA_PName", V1."PA_PAddressID", V1."PA_PAppAddressID", V1."PA_PAddressL1", V1."PA_PAddressL2",
    V1."PA_PCity", V1."PA_PAddressState", V1."PA_PZipCode", V1."PA_PCounty", V1."PA_PFName", V1."PA_PLName", V1."PA_PMedicaidNumber",
    V2."PA_PAdmissionID" AS "ConPA_PAdmissionID", V2."PA_PName" AS "ConPA_PName", V2."PA_PAddressID" AS "ConPA_PAddressID",
    V2."PA_PAppAddressID" AS "ConPA_PAppAddressID", V2."PA_PAddressL1" AS "ConPA_PAddressL1", V2."PA_PAddressL2" AS "ConPA_PAddressL2",
    V2."PA_PCity" AS "ConPA_PCity", V2."PA_PAddressState" AS "ConPA_PAddressState", V2."PA_PZipCode" AS "ConPA_PZipCode",
    V2."PA_PCounty" AS "ConPA_PCounty", V2."PA_PFName" AS "ConPA_PFName", V2."PA_PLName" AS "ConPA_PLName", V2."PA_PMedicaidNumber" AS "ConPA_PMedicaidNumber",
    V1."ContractType", V2."ContractType" AS "ConContractType",
    V1."P_PStatus", V2."P_PStatus" AS "ConP_PStatus",
    V1."PA_PStatus", V2."PA_PStatus" AS "ConPA_PStatus",
    V1."BillRateNonBilled", V2."BillRateNonBilled" AS "ConBillRateNonBilled",
    V1."BillRateBoth", V2."BillRateBoth" AS "ConBillRateBoth",
    V1."FederalTaxNumber", V2."FederalTaxNumber" AS "ConFederalTaxNumber"
  FROM delta_visits V1
  INNER JOIN all_visits V2 
    ON V1."VisitDate" = V2."VisitDate"
    AND V1."SSN" = V2."SSN"
    AND V1."ProviderID" != V2."ProviderID"
    AND V1."VisitID" != V2."VisitID"
  
  UNION
  
  -- Direction 2: All visits joined with delta visits (catches reverse conflicts)
  SELECT 
    V1."CONFLICTID",
    V1."SSN",
    V1."ProviderID", V1."AppProviderID", V1."ProviderName", V1."VisitID", V1."AppVisitID",
    V2."ProviderID" AS "ConProviderID", V2."AppProviderID" AS "ConAppProviderID", V2."ProviderName" AS "ConProviderName",
    V2."VisitID" AS "ConVisitID", V2."AppVisitID" AS "ConAppVisitID",
    V1."VisitDate",
    V1."SchStartTime", V1."SchEndTime", V2."SchStartTime" AS "ConSchStartTime", V2."SchEndTime" AS "ConSchEndTime",
    V1."VisitStartTime", V1."VisitEndTime", V2."VisitStartTime" AS "ConVisitStartTime", V2."VisitEndTime" AS "ConVisitEndTime",
    V1."EVVStartTime", V1."EVVEndTime", V2."EVVStartTime" AS "ConEVVStartTime", V2."EVVEndTime" AS "ConEVVEndTime",
    V1."CaregiverID", V1."AppCaregiverID", V1."AideCode", V1."AideName", V1."AideSSN",
    V2."CaregiverID" AS "ConCaregiverID", V2."AppCaregiverID" AS "ConAppCaregiverID", 
    V2."AideCode" AS "ConAideCode", V2."AideName" AS "ConAideName", V2."AideSSN" AS "ConAideSSN",
    V1."OfficeID", V1."AppOfficeID", V1."Office", 
    V2."OfficeID" AS "ConOfficeID", V2."AppOfficeID" AS "ConAppOfficeID", V2."Office" AS "ConOffice",
    V1."PatientID", V1."AppPatientID", V1."PAdmissionID", V1."PName", V1."PAddressID", V1."PAppAddressID",
    V1."PAddressL1", V1."PAddressL2", V1."PCity", V1."PAddressState", V1."PZipCode", V1."PCounty",
    V1."Longitude", V1."Latitude",
    V2."PatientID" AS "ConPatientID", V2."AppPatientID" AS "ConAppPatientID", V2."PAdmissionID" AS "ConPAdmissionID",
    V2."PName" AS "ConPName", V2."PAddressID" AS "ConPAddressID", V2."PAppAddressID" AS "ConPAppAddressID",
    V2."PAddressL1" AS "ConPAddressL1", V2."PAddressL2" AS "ConPAddressL2", V2."PCity" AS "ConPCity",
    V2."PAddressState" AS "ConPAddressState", V2."PZipCode" AS "ConPZipCode", V2."PCounty" AS "ConPCounty",
    V2."Longitude" AS "ConLongitude", V2."Latitude" AS "ConLatitude",
    V1."PayerID", V1."AppPayerID", V1."Contract", V2."PayerID" AS "ConPayerID", V2."AppPayerID" AS "ConAppPayerID", V2."Contract" AS "ConContract",
    V1."BilledDate", V2."BilledDate" AS "ConBilledDate", V1."BilledHours", V2."BilledHours" AS "ConBilledHours",
    V1."Billed", V2."Billed" AS "ConBilled",
    V1."ServiceCodeID", V1."AppServiceCodeID", V1."RateType", V1."ServiceCode",
    V2."ServiceCodeID" AS "ConServiceCodeID", V2."AppServiceCodeID" AS "ConAppServiceCodeID", 
    V2."RateType" AS "ConRateType", V2."ServiceCode" AS "ConServiceCode",
    V1."AideFName", V1."AideLName", V2."AideFName" AS "ConAideFName", V2."AideLName" AS "ConAideLName",
    V1."PFName", V1."PLName", V2."PFName" AS "ConPFName", V2."PLName" AS "ConPLName",
    V1."PMedicaidNumber", V2."PMedicaidNumber" AS "ConPMedicaidNumber",
    V1."PayerState", V2."PayerState" AS "ConPayerState",
    V1."LastUpdatedBy", V2."LastUpdatedBy" AS "ConLastUpdatedBy",
    V1."LastUpdatedDate", V2."LastUpdatedDate" AS "ConLastUpdatedDate",
    V1."BilledRate", V1."TotalBilledAmount", V2."BilledRate" AS "ConBilledRate", V2."TotalBilledAmount" AS "ConTotalBilledAmount",
    V1."IsMissed", V1."MissedVisitReason", V1."EVVType",
    V2."IsMissed" AS "ConIsMissed", V2."MissedVisitReason" AS "ConMissedVisitReason", V2."EVVType" AS "ConEVVType",
    V1."PStatus", V2."PStatus" AS "ConPStatus", V1."AideStatus", V2."AideStatus" AS "ConAideStatus",
    V1."P_PatientID", V1."P_AppPatientID", V2."P_PatientID" AS "ConP_PatientID", V2."P_AppPatientID" AS "ConP_AppPatientID",
    V1."PA_PatientID", V1."PA_AppPatientID", V2."PA_PatientID" AS "ConPA_PatientID", V2."PA_AppPatientID" AS "ConPA_AppPatientID",
    V1."P_PAdmissionID", V1."P_PName", V1."P_PAddressID", V1."P_PAppAddressID", V1."P_PAddressL1", V1."P_PAddressL2",
    V1."P_PCity", V1."P_PAddressState", V1."P_PZipCode", V1."P_PCounty", V1."P_PFName", V1."P_PLName", V1."P_PMedicaidNumber",
    V2."P_PAdmissionID" AS "ConP_PAdmissionID", V2."P_PName" AS "ConP_PName", V2."P_PAddressID" AS "ConP_PAddressID",
    V2."P_PAppAddressID" AS "ConP_PAppAddressID", V2."P_PAddressL1" AS "ConP_PAddressL1", V2."P_PAddressL2" AS "ConP_PAddressL2",
    V2."P_PCity" AS "ConP_PCity", V2."P_PAddressState" AS "ConP_PAddressState", V2."P_PZipCode" AS "ConP_PZipCode",
    V2."P_PCounty" AS "ConP_PCounty", V2."P_PFName" AS "ConP_PFName", V2."P_PLName" AS "ConP_PLName", V2."P_PMedicaidNumber" AS "ConP_PMedicaidNumber",
    V1."PA_PAdmissionID", V1."PA_PName", V1."PA_PAddressID", V1."PA_PAppAddressID", V1."PA_PAddressL1", V1."PA_PAddressL2",
    V1."PA_PCity", V1."PA_PAddressState", V1."PA_PZipCode", V1."PA_PCounty", V1."PA_PFName", V1."PA_PLName", V1."PA_PMedicaidNumber",
    V2."PA_PAdmissionID" AS "ConPA_PAdmissionID", V2."PA_PName" AS "ConPA_PName", V2."PA_PAddressID" AS "ConPA_PAddressID",
    V2."PA_PAppAddressID" AS "ConPA_PAppAddressID", V2."PA_PAddressL1" AS "ConPA_PAddressL1", V2."PA_PAddressL2" AS "ConPA_PAddressL2",
    V2."PA_PCity" AS "ConPA_PCity", V2."PA_PAddressState" AS "ConPA_PAddressState", V2."PA_PZipCode" AS "ConPA_PZipCode",
    V2."PA_PCounty" AS "ConPA_PCounty", V2."PA_PFName" AS "ConPA_PFName", V2."PA_PLName" AS "ConPA_PLName", V2."PA_PMedicaidNumber" AS "ConPA_PMedicaidNumber",
    V1."ContractType", V2."ContractType" AS "ConContractType",
    V1."P_PStatus", V2."P_PStatus" AS "ConP_PStatus",
    V1."PA_PStatus", V2."PA_PStatus" AS "ConPA_PStatus",
    V1."BillRateNonBilled", V2."BillRateNonBilled" AS "ConBillRateNonBilled",
    V1."BillRateBoth", V2."BillRateBoth" AS "ConBillRateBoth",
    V1."FederalTaxNumber", V2."FederalTaxNumber" AS "ConFederalTaxNumber"
  FROM all_visits V1
  INNER JOIN delta_visits V2 
    ON V1."VisitDate" = V2."VisitDate"
    AND V1."SSN" = V2."SSN"
    AND V1."ProviderID" != V2."ProviderID"
    AND V1."VisitID" != V2."VisitID"
"""
    
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
        step1_template = self.load_sql_file('sf_task02_v3_step1_delta_keys.sql')
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
        step2_template = self.load_sql_file('sf_task02_v3_step2_base_visits.sql')
        
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
        step3_template = self.load_sql_file('sf_task02_v3_step3_final_query.sql')
        
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
