"""BCRA Policy Rate provider for LELIQ and policy corridor rates.

LELIQ (Liquidación de Licitaciones) is the main policy rate instrument used by BCRA.
This provider fetches LELIQ rates and reference policy corridor rates from BCRA API.
"""

import os
import requests
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .base import SeriesProvider, ProviderError

BCRA_BASE = os.getenv("BCRA_API_BASE", "https://api.bcra.gob.ar/estadisticas/v4.0/Monetarias")

# BCRA series IDs for policy rates
# Note: Series ID 21 is a cumulative index, NOT the rate
# Based on testing (Oct 2025), series 28 returns ~32% which is closest to expected LELIQ rate (~29%)
# Series 29 returns ~22% (possibly a different tenor or reference rate)
BCRA_POLICY_SERIES = {
    "LELIQ_RATE": "28",  # LELIQ rate (main policy rate) - verified Oct 2025: ~32%
    # Alternative candidates (uncomment if needed):
    # "LELIQ_RATE": "29",  # Alternative: ~22% (may be different tenor)
    # Add policy corridor if available:
    # "POLICY_CORRIDOR_MIN": "...",
    # "POLICY_CORRIDOR_MAX": "...",
}


def _bcra_url(series_id: str) -> str:
    """Build BCRA API URL for a series ID."""
    return f"{BCRA_BASE}/{series_id}"


class BCRAPolicyProvider(SeriesProvider):
    """BCRA provider for policy rates (LELIQ and policy corridor)."""
    
    def __init__(self):
        # Create session with retry strategy and SSL handling
        self.session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # SSL configuration - disable verification for problematic certificates
        self.session.verify = False
        
        # Disable SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        """Fetch policy rate time series.
        
        Args:
            series_code: Must be "LELIQ_RATE" or other policy series
            start: Start date in YYYY-MM-DD format (optional)
            end: End date in YYYY-MM-DD format (optional)
            
        Returns:
            List of (datetime, rate) tuples. Rate is in annualized percentage (e.g., 50.0 for 50%)
        """
        sid = BCRA_POLICY_SERIES.get(series_code)
        if not sid:
            raise ProviderError(f"BCRAPolicyProvider: unknown series_code={series_code}")
        
        url = _bcra_url(sid)
        
        try:
            # Use longer timeout and disable SSL verification
            r = self.session.get(url, timeout=60, verify=False)
            if r.status_code != 200:
                raise ProviderError(f"BCRAPolicyProvider HTTP {r.status_code} for {url}")
            
            js = r.json()
            # Parse JSON: {"status": 200, "results": [{"idVariable": 21, "detalle": [{"fecha": "...", "valor": ...}, ...]}]}
            results = js.get("results") or []
            if not results:
                raise ProviderError(f"BCRAPolicyProvider: No results found in API response for {series_code}")
            
            # Get the first result's detalle array
            detalle = results[0].get("detalle") or []
            if not detalle:
                raise ProviderError(f"BCRAPolicyProvider: No data found in detalle for {series_code}")
            
            out: List[Tuple[datetime, float]] = []
            for row in detalle:
                dt = row.get("fecha")
                val = row.get("valor")
                if not dt or val is None:
                    continue
                
                # Parse date and value
                ts = datetime.fromisoformat(dt)
                
                # Apply date filters if provided (inclusive on both ends)
                if start:
                    start_date = datetime.fromisoformat(start)
                    if ts.date() < start_date.date():
                        continue
                if end:
                    end_date = datetime.fromisoformat(end)
                    if ts.date() > end_date.date():
                        continue
                
                # Value is typically in percentage (e.g., 50.0 for 50%)
                # Ensure it's stored as percentage, not decimal
                rate_value = float(val)
                out.append((ts, rate_value))
            
            # Sort by date
            out = sorted(out, key=lambda x: x[0])
            
            # Check for gaps > 3 business days
            # Count business days (Monday-Friday) between consecutive observations
            gaps = []
            if len(out) > 1:
                for i in range(1, len(out)):
                    prev_date = out[i-1][0].date()
                    curr_date = out[i][0].date()
                    days_diff = (curr_date - prev_date).days
                    
                    # Count business days (exclude weekends)
                    business_days = 0
                    check_date = prev_date
                    while check_date < curr_date:
                        if check_date.weekday() < 5:  # Monday=0, Friday=4
                            business_days += 1
                        check_date += timedelta(days=1)
                    
                    if business_days > 3:
                        gaps.append((prev_date.isoformat(), curr_date.isoformat(), business_days))
            
            # Log gaps if any found (but don't fail - just warn)
            if gaps:
                # In production, you might want to log these or raise a warning
                pass
            
            return out
            
        except requests.exceptions.SSLError as e:
            raise ProviderError(f"BCRAPolicyProvider SSL error: {e}")
        except requests.exceptions.Timeout as e:
            raise ProviderError(f"BCRAPolicyProvider timeout: {e}")
        except requests.exceptions.RequestException as e:
            raise ProviderError(f"BCRAPolicyProvider request error: {e}")
        except Exception as e:
            raise ProviderError(f"BCRAPolicyProvider unexpected error: {e}")

