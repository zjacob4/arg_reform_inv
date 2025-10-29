"""BCRA (Central Bank of Argentina) data provider."""

import os
import requests
from datetime import datetime
from typing import List, Tuple, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .base import SeriesProvider, ProviderError

BCRA_BASE = os.getenv("BCRA_API_BASE", "https://api.bcra.gob.ar/estadisticas/v4.0/Monetarias")

# Map internal series codes to BCRA series IDs
BCRA_SERIES = {
    "USDARS_OFFICIAL": "5",     # Tipo de cambio mayorista de referencia
    "RESERVES_USD": "1",        # Reservas internacionales
    "USDARS_RETAIL": "4",       # Tipo de cambio minorista (for parallel rate)
    "BADLAR_RATE": "7",         # Tasa de interés BADLAR
    "TM20_RATE": "8",           # Tasa de interés TM20
}


def _bcra_url(series_id: str, start: Optional[str], end: Optional[str]) -> str:
    # This endpoint ignores date filters, returns full series JSON
    return f"{BCRA_BASE}/{series_id}"


class BCRAProvider(SeriesProvider):
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
        sid = BCRA_SERIES.get(series_code)
        if not sid:
            raise ProviderError(f"BCRAProvider: unknown series_code={series_code}")
        
        url = _bcra_url(sid, start, end)
        
        try:
            # Use longer timeout and disable SSL verification
            r = self.session.get(url, timeout=60, verify=False)
            if r.status_code != 200:
                raise ProviderError(f"BCRAProvider HTTP {r.status_code} for {url}")
            
            js = r.json()
            # Parse JSON: {"status": 200, "results": [{"idVariable": 5, "detalle": [{"fecha": "...", "valor": ...}, ...]}]}
            results = js.get("results") or []
            if not results:
                raise ProviderError(f"BCRAProvider: No results found in API response for {series_code}")
            
            # Get the first result's detalle array
            detalle = results[0].get("detalle") or []
            if not detalle:
                raise ProviderError(f"BCRAProvider: No data found in detalle for {series_code}")
            
            out: List[Tuple[datetime, float]] = []
            for row in detalle:
                dt = row.get("fecha")
                val = row.get("valor")
                if not dt or val is None:
                    continue
                out.append((datetime.fromisoformat(dt), float(val)))
            return sorted(out, key=lambda x: x[0])
            
        except requests.exceptions.SSLError as e:
            raise ProviderError(f"BCRAProvider SSL error: {e}")
        except requests.exceptions.Timeout as e:
            raise ProviderError(f"BCRAProvider timeout: {e}")
        except requests.exceptions.RequestException as e:
            raise ProviderError(f"BCRAProvider request error: {e}")
        except Exception as e:
            raise ProviderError(f"BCRAProvider unexpected error: {e}")

