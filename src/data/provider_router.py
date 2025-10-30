"""Provider router for fetching data from multiple sources with fallback."""

import os
from typing import List, Tuple, Optional
from datetime import datetime

from .providers.bcra import BCRAProvider
from .providers.indec import INDECProvider
from .providers.yahoo_fx import YahooFXProvider
from .providers.bluelytics import BluelyticsProvider
from .providers.base import ProviderError

# Default provider order - prefer local official sources first
DEFAULT_ORDER = "BCRA,INDEC,BLUELYTICS,YAHOOFX,IMF,TE"

# Provider registry - prefer local official sources first
PROVIDERS = {
    "BCRA": BCRAProvider(),
    "INDEC": INDECProvider(),
    "YAHOOFX": YahooFXProvider(),
    "BLUELYTICS": BluelyticsProvider(),
    # Keep IMF/TE available but de-prioritized if still present
    # "IMF": IMFProvider(),
    # "TE": TradingEconomicsProvider(),
}


def fetch_series(
    series_code: str,
    start: Optional[str] = None,
    end: Optional[str] = None
) -> List[Tuple[datetime, float]]:
    """Fetch time series data from providers with fallback.
    
    Reads PREFERRED_PROVIDERS from environment variable to determine
    the order of providers to try. Falls back to next provider on
    ProviderError or empty result.
    
    Args:
        series_code: Series identifier (e.g., "USDARS_OFFICIAL", "CPI_HEADLINE")
        start: Start date in YYYY-MM-DD format (optional)
        end: End date in YYYY-MM-DD format (optional)
        
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ProviderError: If all providers fail or series_code not found
    """
    # Get preferred provider order from env, default to DEFAULT_ORDER
    preferred = os.getenv("PREFERRED_PROVIDERS", DEFAULT_ORDER)
    provider_names = [p.strip() for p in preferred.split(",")]
    
    last_error = None
    for provider_name in provider_names:
        # Skip if provider not in registry
        if provider_name not in PROVIDERS:
            continue
            
        provider = PROVIDERS[provider_name]
        try:
            result = provider.fetch_timeseries(series_code, start=start, end=end)
            # Check if result is empty
            if result:
                return result
            # Empty result, try next provider
            continue
        except ProviderError as e:
            last_error = e
            # Try next provider on error
            continue
        except Exception as e:
            # Unexpected error, try next provider
            last_error = ProviderError(f"Unexpected error from {provider_name}: {e}")
            continue
    
    # All providers failed
    if last_error:
        raise ProviderError(
            f"All providers failed for series_code={series_code}. "
            f"Last error: {last_error}"
        )
    else:
        raise ProviderError(
            f"No providers available for series_code={series_code}. "
            f"Tried: {', '.join(provider_names)}"
        )

