import os, requests
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from .base import SeriesProvider, ProviderError
from dotenv import load_dotenv
load_dotenv()


EOD_BASE = os.getenv("EODHD_BASE", "https://eodhd.com/api")
EOD_TOKEN = os.getenv("EODHD_API_TOKEN", "")

class GBondError(RuntimeError): ...

def fetch_gbond_series(code: str, start: str) -> List[Tuple[datetime, float]]:
    """
    code like US10Y, DE10Y, US05Y, DE05Y (if available).
    Returns [(ts, yield_decimal)]
    """
    r = requests.get(f"{EOD_BASE}/eod/{code}.GBOND",
                     params={"api_token": EOD_TOKEN, "fmt":"json", "from": start},
                     timeout=30)
    if r.status_code != 200:
        raise GBondError(f"GBOND HTTP {r.status_code}: {r.text[:200]}")
    js = r.json()
    out=[]
    for it in js:
        d = it.get("date"); y=it.get("close") or it.get("price")
        if d and y not in (None,""):
            out.append((datetime.fromisoformat(d), float(y)/100.0))  # convert % to decimal
    return out


class GBondProvider(SeriesProvider):
    """EODHD government bond yield provider using GBOND endpoint.
    
    Supports series codes that map to GBOND codes (e.g., "US10Y", "DE10Y", "US05Y", "DE05Y").
    Series codes should be GBOND vendor codes or prefixed with "GBOND_".
    """
    
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        """Fetch government bond yield time series.
        
        Args:
            series_code: GBOND vendor code (e.g., "US10Y" for US 10Y Treasury)
                         or "GBOND_{CODE}" format
            start: Start date in YYYY-MM-DD format (required)
            end: End date in YYYY-MM-DD format (optional, not used by current implementation)
            
        Returns:
            List of (datetime, yield_decimal) tuples where yield is in decimal form
        """
        if not start:
            raise ProviderError("GBondProvider: start date is required")
        
        # Remove prefix if present
        if series_code.startswith("GBOND_"):
            code = series_code.replace("GBOND_", "")
        else:
            code = series_code
        
        try:
            return fetch_gbond_series(code, start)
        except GBondError as e:
            raise ProviderError(f"GBondProvider: {e}")
