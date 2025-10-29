"""Configuration management."""

from .settings import Settings, settings
from .series_registry import SeriesSpec, REGISTRY, get_series_spec, list_all_series

__all__ = [
    "Settings",
    "settings",
    "SeriesSpec",
    "REGISTRY",
    "get_series_spec",
    "list_all_series",
]

