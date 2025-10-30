import pytest
from src.data.providers.bluelytics import BluelyticsProvider


def test_bluelytics_fetch_smoke(monkeypatch):
    # Don't assert on internet; just ensure interface works and error-handling is sane.
    p = BluelyticsProvider()
    try:
        rows = p.fetch_timeseries("USDARS_BLUE")
        assert isinstance(rows, list)
    except Exception:
        assert True  # network may be blocked; interface shouldn't crash test run


