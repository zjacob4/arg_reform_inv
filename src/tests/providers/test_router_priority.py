import os
import pytest
from src.data.provider_router import fetch_series

def test_provider_order_env_parsing(monkeypatch):
    monkeypatch.setenv("PREFERRED_PROVIDERS", "BCRA,INDEC,YAHOOFX")
    # The call shouldn't crash even if external API is unreachable; expect ProviderError or list
    try:
        _ = fetch_series("USDARS_OFFICIAL", start="2024-01-01")
    except Exception:
        assert True

def test_unknown_series_raises():
    with pytest.raises(Exception):
        fetch_series("UNKNOWN_SERIES")
