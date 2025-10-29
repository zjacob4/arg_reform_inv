"""Data collection and management."""

from .db import connect, create_schema, upsert_series_meta, upsert_timeseries
from .provider_router import fetch_series

__all__ = [
    "connect",
    "create_schema",
    "upsert_series_meta",
    "upsert_timeseries",
    "fetch_series",
]

