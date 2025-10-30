"""Data providers for external sources."""

from .base import SeriesProvider, ProviderError
from .bcra import BCRAProvider
from .indec import INDECProvider
from .yahoo_fx import YahooFXProvider
from .bluelytics import BluelyticsProvider
from .imf_cpi import IMFProviderCPI
from .fred_cpi import FREDCPIProvider

__all__ = [
    "SeriesProvider",
    "ProviderError",
    "BCRAProvider",
    "INDECProvider",
    "YahooFXProvider",
    "BluelyticsProvider",
    "IMFProviderCPI",
    "FREDCPIProvider",
]

