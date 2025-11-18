"""
Unit tests for AnalyticsRepository.
"""

import os
import sys
import pandas as pd
from unittest.mock import Mock

# Ensure project root is on sys.path for direct test runs
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.repositories.analytics_repository import AnalyticsRepository


def test_fetch_dataframe_caches_result():
    """Repository caches first fetch and serves copies on subsequent hits."""
    # Setup snowflake connector mock
    sf = Mock()
    df_src = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    sf.fetch_dataframe.return_value = df_src

    repo = AnalyticsRepository(sf, cache_enabled=True)

    q = "SELECT 1"
    # First call -> MISS, fetches from Snowflake
    df1 = repo.fetch_dataframe(q, params=None, cache_key="k1")
    # Second call -> HIT, returns copy
    df2 = repo.fetch_dataframe(q, params=None, cache_key="k1")

    # Underlying connector called only once
    sf.fetch_dataframe.assert_called_once()

    # Returned DataFrames equal in content, distinct objects (copy on return)
    assert df1.equals(df_src)
    assert df2.equals(df_src)
    assert df1 is not df2

    # Mutate returned df; cached value should remain unchanged
    df2.loc[0, 'a'] = 999
    df3 = repo.fetch_dataframe(q, params=None, cache_key="k1")
    assert df3.loc[0, 'a'] == 1


def test_get_payer_provider_relationships_uses_cache():
    """Named method uses stable cache key and caches results."""
    sf = Mock()
    df_src = pd.DataFrame({
        'PayerID': ['P1'],
        'AppPayerID': ['AP1'],
        'Contract': ['C1'],
        'ProviderID': ['PR1'],
        'AppProviderID': ['APR1'],
        'ProviderName': ['Provider A']
    })
    sf.fetch_dataframe.return_value = df_src

    repo = AnalyticsRepository(sf, cache_enabled=True)

    # First call -> MISS
    df1 = repo.get_payer_provider_relationships()
    # Second call -> HIT
    df2 = repo.get_payer_provider_relationships()

    # Underlying connector called once
    sf.fetch_dataframe.assert_called_once()

    assert df1.equals(df_src)
    assert df2.equals(df_src)
    assert df1 is not df2


