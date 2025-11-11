from typing import Any, Dict, Optional
import hashlib
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

    # Domain-specific, intention-revealing method
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


