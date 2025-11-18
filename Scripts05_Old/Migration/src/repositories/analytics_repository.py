from typing import Any, Dict, Optional
import hashlib
import time
import pandas as pd
from src.connectors.snowflake_connector import SnowflakeConnector
from src.utils.logger import get_logger


logger = get_logger(__name__)


class AnalyticsRepository:
    """
    Minimal repository for Analytics (Snowflake) reads with a simple in-memory cache.
    Designed to be extended later (e.g., TTLs, Redis backend, typed methods).
    """

    def __init__(self, snowflake_connector: SnowflakeConnector, cache_enabled: bool = True):
        self._sf = snowflake_connector
        self._cache_enabled = cache_enabled
        self._cache: Dict[str, pd.DataFrame] = {}

    def fetch_dataframe(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        cache_key: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch a DataFrame from Analytics with basic caching.

        Args:
            query: SQL to execute against Snowflake
            params: Optional parameters for the query
            cache_key: Optional explicit cache key. If not provided, a hash of the query+params is used.

        Returns:
            pandas DataFrame (copy of cached data when served from cache)
        """
        key = cache_key or self._build_key(query, params)

        if self._cache_enabled and key in self._cache:
            logger.debug(f"AnalyticsRepository cache HIT: {key}")
            return self._cache[key].copy(deep=True)

        logger.debug(f"AnalyticsRepository cache MISS: {key}")
        df = self._sf.fetch_dataframe(query, params)

        if self._cache_enabled:
            # Store a defensive copy to protect cached data from caller mutations
            self._cache[key] = df.copy(deep=True)

        return df

    @staticmethod
    def _build_key(query: str, params: Optional[Dict[str, Any]]) -> str:
        params_repr = "" if not params else repr(sorted(params.items()))
        digest = hashlib.sha256((query + "|" + params_repr).encode("utf-8")).hexdigest()
        return f"analytics:{digest}"

    # Domain-specific, intention-revealing methods
    
    def get_payer_provider_relationships(self) -> pd.DataFrame:
        """
        Return payer-provider relationships as a DataFrame.
        Uses a stable cache key to allow reuse across tasks within the process.
        """
        query = """
            SELECT DISTINCT 
                DPP."Payer Id" AS "PayerID",
                DPP."Application Payer Id" AS "AppPayerID",
                DPA."Payer Name" AS "Contract",
                DPP."Provider Id" AS "ProviderID",
                DPP."Application Provider Id" AS "AppProviderID",
                DP."Provider Name" AS "ProviderName"
            FROM ANALYTICS.BI.DIMPROVIDER AS DP
            INNER JOIN ANALYTICS.BI.DIMPAYERPROVIDER AS DPP 
                ON DPP."Provider Id" = DP."Provider Id"
            INNER JOIN ANALYTICS.BI.DIMPAYER AS DPA 
                ON DPA."Payer Id" = DPP."Payer Id"
        """
        return self.fetch_dataframe(query, params=None, cache_key='payer_provider_relationships')
    
    def fetch_visit_data(self, 
                        date_from, 
                        date_to, 
                        ssns: list,
                        excluded_agencies: list = None,
                        excluded_ssns: list = None) -> pd.DataFrame:
        """
        Fetch visit data from Snowflake Analytics (FACTVISITCALLPERFORMANCE_CR).
        
        This is the base query for V2 (all visits) that will be joined with
        Postgres CONFLICTVISITMAPS to create V1.
        
        Based on SQL lines 306-329 in TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_0.sql
        
        Args:
            date_from: Start date
            date_to: End date
            ssns: List of SSNs to fetch
            excluded_agencies: Optional list of Provider IDs to exclude
            excluded_ssns: Optional list of SSNs to exclude
            
        Returns:
            DataFrame with visit data and all dimension joins
        """
        if not ssns:
            logger.warning("No SSNs provided, returning empty DataFrame")
            return pd.DataFrame()
        
        # Build filter strings
        ssns_str = ','.join([f"'{s}'" for s in ssns])
        
        excluded_agencies_clause = ""
        if excluded_agencies:
            agencies_str = ','.join([f"'{a}'" for a in excluded_agencies])
            excluded_agencies_clause = f"AND CR1.\"Provider Id\" NOT IN ({agencies_str})"
        
        excluded_ssns_clause = ""
        if excluded_ssns:
            excluded_ssns_str = ','.join([f"'{s}'" for s in excluded_ssns])
            excluded_ssns_clause = f"AND TRIM(CAR.\"SSN\") NOT IN ({excluded_ssns_str})"
        
        # Convert dates to strings for SQL
        date_from_str = date_from.strftime('%Y-%m-%d')
        date_to_str = date_to.strftime('%Y-%m-%d')
        
        # Complex query with all dimension joins
        # Performance note: DISTINCT removed - not needed for this query
        query = f"""
            SELECT 
                CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
                CASE WHEN CR1."Billed" = 'yes' 
                    THEN CR1."Billed Rate" 
                    ELSE CR1."Bill Rate Non-Billed" 
                END AS "BillRateBoth",
                TRIM(CAR."SSN") AS "SSN",
                CAST(NULL AS STRING) AS "PStatus",
                CAR."Status" AS "AideStatus",
                CR1."Missed Visit Reason" AS "MissedVisitReason",
                CR1."Is Missed" AS "IsMissed",
                CR1."Call Out Device Type" AS "EVVType",
                CR1."Billed Rate" AS "BilledRate",
                CR1."Total Billed Amount" AS "TotalBilledAmount",
                CR1."Provider Id" AS "ProviderID",
                CR1."Application Provider Id" AS "AppProviderID",
                DPR."Provider Name" AS "ProviderName",
                CAST(NULL AS STRING) AS "AgencyContact",
                DPR."Phone Number 1" AS "AgencyPhone",
                DPR."Federal Tax Number" AS "FederalTaxNumber",
                CR1."Visit Id" AS "VisitID",
                CR1."Application Visit Id" AS "AppVisitID",
                DATE(CR1."Visit Date") AS "VisitDate",
                CAST(CR1."Scheduled Start Time" AS timestamp) AS "SchStartTime",
                CAST(CR1."Scheduled End Time" AS timestamp) AS "SchEndTime",
                CAST(CR1."Visit Start Time" AS timestamp) AS "VisitStartTime",
                CAST(CR1."Visit End Time" AS timestamp) AS "VisitEndTime",
                CAST(CR1."Call In Time" AS timestamp) AS "EVVStartTime",
                CAST(CR1."Call Out Time" AS timestamp) AS "EVVEndTime",
                CR1."Caregiver Id" AS "CaregiverID",
                CR1."Application Caregiver Id" AS "AppCaregiverID",
                CAR."Caregiver Code" AS "AideCode",
                CAR."Caregiver Fullname" AS "AideName",
                CAR."Caregiver Firstname" AS "AideFName",
                CAR."Caregiver Lastname" AS "AideLName",
                TRIM(CAR."SSN") AS "AideSSN",
                CR1."Office Id" AS "OfficeID",
                CR1."Application Office Id" AS "AppOfficeID",
                DOF."Office Name" AS "Office",
                CR1."Payer Patient Id" AS "PA_PatientID",
                CR1."Application Payer Patient Id" AS "PA_AppPatientID",
                CR1."Provider Patient Id" AS "P_PatientID",
                CR1."Application Provider Patient Id" AS "P_AppPatientID",
                CR1."Patient Id" AS "PatientID",
                CR1."Application Patient Id" AS "AppPatientID",
                CAST(NULL AS STRING) AS "PAdmissionID",
                CAST(NULL AS STRING) AS "PName",
                CAST(NULL AS STRING) AS "PFName",
                CAST(NULL AS STRING) AS "PLName",
                CAST(NULL AS STRING) AS "PMedicaidNumber",
                CAST(NULL AS STRING) AS "PAddressID",
                CAST(NULL AS STRING) AS "PAppAddressID",
                CAST(NULL AS STRING) AS "PAddressL1",
                CAST(NULL AS STRING) AS "PAddressL2",
                CAST(NULL AS STRING) AS "PCity",
                CAST(NULL AS STRING) AS "PAddressState",
                CAST(NULL AS STRING) AS "PZipCode",
                CAST(NULL AS STRING) AS "PCounty",
                CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != ',' 
                    THEN CAST(REPLACE(SPLIT(CR1."Call In GPS Coordinates", ',')[1], '"', '') AS NUMBER)
                    WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != ','
                    THEN CAST(REPLACE(SPLIT(CR1."Call Out GPS Coordinates", ',')[1], '"', '') AS NUMBER)
                    ELSE DPAD_P."Provider_Longitude" 
                END AS "PLongitude",
                CASE WHEN CR1."Call In GPS Coordinates" IS NOT NULL AND CR1."Call In GPS Coordinates" != ','
                    THEN CAST(REPLACE(SPLIT(CR1."Call In GPS Coordinates", ',')[0], '"', '') AS NUMBER)
                    WHEN CR1."Call Out GPS Coordinates" IS NOT NULL AND CR1."Call Out GPS Coordinates" != ','
                    THEN CAST(REPLACE(SPLIT(CR1."Call Out GPS Coordinates", ',')[0], '"', '') AS NUMBER)
                    ELSE DPAD_P."Provider_Latitude" 
                END AS "PLatitude",
                CR1."Payer Id" AS "PayerID",
                CR1."Application Payer Id" AS "AppPayerID",
                COALESCE(SPA."Payer Name", DCON."Contract Name") AS "Contract",
                SPA."Payer State" AS "PayerState",
                CAST(CR1."Invoice Date" AS timestamp) AS "BilledDate",
                CR1."Billed Hours" AS "BilledHours",
                CR1."Billed" AS "Billed",
                DSC."Service Code Id" AS "ServiceCodeID",
                DSC."Application Service Code Id" AS "AppServiceCodeID",
                CR1."Bill Type" AS "RateType",
                DSC."Service Code" AS "ServiceCode",
                CAST(CR1."Visit Updated Timestamp" AS timestamp) AS "LastUpdatedDate",
                DUSR."User Fullname" AS "LastUpdatedBy",
                DPA_P."Admission Id" AS "P_PAdmissionID",
                DPA_P."Patient Name" AS "P_PName",
                DPA_P."Patient Firstname" AS "P_PFName",
                DPA_P."Patient Lastname" AS "P_PLName",
                DPA_P."Medicaid Number" AS "P_PMedicaidNumber",
                DPA_P."Status" AS "P_PStatus",
                DPAD_P."Patient Address Id" AS "P_PAddressID",
                DPAD_P."Application Patient Address Id" AS "P_PAppAddressID",
                DPAD_P."Address Line 1" AS "P_PAddressL1",
                DPAD_P."Address Line 2" AS "P_PAddressL2",
                DPAD_P."City" AS "P_PCity",
                DPAD_P."Address State" AS "P_PAddressState",
                DPAD_P."Zip Code" AS "P_PZipCode",
                DPAD_P."County" AS "P_PCounty",
                DPA_PA."Admission Id" AS "PA_PAdmissionID",
                DPA_PA."Patient Name" AS "PA_PName",
                DPA_PA."Patient Firstname" AS "PA_PFName",
                DPA_PA."Patient Lastname" AS "PA_PLName",
                DPA_PA."Medicaid Number" AS "PA_PMedicaidNumber",
                DPA_PA."Status" AS "PA_PStatus",
                DPAD_PA."Patient Address Id" AS "PA_PAddressID",
                DPAD_PA."Application Patient Address Id" AS "PA_PAppAddressID",
                DPAD_PA."Address Line 1" AS "PA_PAddressL1",
                DPAD_PA."Address Line 2" AS "PA_PAddressL2",
                DPAD_PA."City" AS "PA_PCity",
                DPAD_PA."Address State" AS "PA_PAddressState",
                DPAD_PA."Zip Code" AS "PA_PZipCode",
                DPAD_PA."County" AS "PA_PCounty",
                CASE 
                    WHEN (CR1."Application Payer Id" = '0' AND CR1."Application Contract Id" != '0') THEN 'Internal'
                    WHEN (CR1."Application Payer Id" != '0' AND CR1."Application Contract Id" != '0') THEN 'UPR'
                    WHEN (CR1."Application Payer Id" != '0' AND CR1."Application Contract Id" = '0') THEN 'Payer'
                END AS "ContractType"
            FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
            INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR 
                ON CAR."Caregiver Id" = CR1."Caregiver Id"
                AND TRIM(CAR."SSN") IS NOT NULL 
                AND TRIM(CAR."SSN") != ''
            LEFT JOIN ANALYTICS.BI.DIMOFFICE AS DOF 
                ON DOF."Office Id" = CR1."Office Id" 
                AND DOF."Is Active" = TRUE
            LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_P 
                ON DPA_P."Patient Id" = CR1."Provider Patient Id"
            LEFT JOIN (
                SELECT 
                    DDD."Patient Address Id", 
                    DDD."Application Patient Address Id", 
                    DDD."Address Line 1", 
                    DDD."Address Line 2", 
                    DDD."City", 
                    DDD."Address State", 
                    DDD."Zip Code", 
                    DDD."County", 
                    DDD."Patient Id", 
                    DDD."Application Patient Id", 
                    DDD."Longitude" AS "Provider_Longitude", 
                    DDD."Latitude" AS "Provider_Latitude", 
                    ROW_NUMBER() OVER (
                        PARTITION BY DDD."Patient Id" 
                        ORDER BY DDD."Application Created UTC Timestamp" DESC
                    ) AS rn
                FROM ANALYTICS.BI.DIMPATIENTADDRESS AS DDD
                WHERE DDD."Primary Address" = TRUE 
                AND (DDD."Address Type" = 'GPS' OR DDD."Address Type" LIKE '%GPS%')
            ) AS DPAD_P 
                ON DPAD_P."Patient Id" = DPA_P."Patient Id" 
                AND DPAD_P."RN" = 1
            LEFT JOIN ANALYTICS.BI.DIMPATIENT AS DPA_PA 
                ON DPA_PA."Patient Id" = CR1."Payer Patient Id"
            LEFT JOIN (
                SELECT 
                    DDD."Patient Address Id", 
                    DDD."Application Patient Address Id", 
                    DDD."Address Line 1", 
                    DDD."Address Line 2", 
                    DDD."City", 
                    DDD."Address State", 
                    DDD."Zip Code", 
                    DDD."County", 
                    DDD."Patient Id", 
                    DDD."Application Patient Id", 
                    ROW_NUMBER() OVER (
                        PARTITION BY DDD."Patient Id" 
                        ORDER BY DDD."Application Created UTC Timestamp" DESC
                    ) AS rn
                FROM ANALYTICS.BI.DIMPATIENTADDRESS AS DDD
                WHERE DDD."Primary Address" = TRUE 
                AND (DDD."Address Type" = 'GPS' OR DDD."Address Type" LIKE '%GPS%')
            ) AS DPAD_PA 
                ON DPAD_PA."Patient Id" = DPA_PA."Patient Id" 
                AND DPAD_PA."RN" = 1
            LEFT JOIN ANALYTICS.BI.DIMPAYER AS SPA 
                ON SPA."Payer Id" = CR1."Payer Id" 
                AND SPA."Is Active" = TRUE 
                AND SPA."Is Demo" = FALSE
            LEFT JOIN ANALYTICS.BI.DIMCONTRACT AS DCON 
                ON DCON."Contract Id" = CR1."Contract Id" 
                AND DCON."Is Active" = TRUE
            INNER JOIN ANALYTICS.BI.DIMPROVIDER AS DPR 
                ON DPR."Provider Id" = CR1."Provider Id" 
                AND DPR."Is Active" = TRUE 
                AND DPR."Is Demo" = FALSE
            LEFT JOIN ANALYTICS.BI.DIMSERVICECODE AS DSC 
                ON DSC."Service Code Id" = CR1."Service Code Id"
            LEFT JOIN ANALYTICS.BI.DIMUSER AS DUSR 
                ON DUSR."User Id" = CR1."Visit Updated User Id"
            WHERE CR1."Visit Date" >= '{date_from_str}'
            AND CR1."Visit Date" <= '{date_to_str}'
            AND TRIM(CAR."SSN") IN ({ssns_str})
            {excluded_agencies_clause}
            {excluded_ssns_clause}
        """
        
        start_time = time.time()
        logger.info(f"Fetching visit data for {len(ssns)} SSNs from Analytics")
        
        df = self.fetch_dataframe(query, params=None)
        
        elapsed = time.time() - start_time
        rows_per_second = len(df) / elapsed if elapsed > 0 else 0
        logger.info(f"Fetched {len(df)} visits from Analytics in {elapsed:.2f} seconds ({rows_per_second:.0f} rows/sec)")
        
        return df


