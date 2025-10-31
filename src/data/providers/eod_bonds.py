import os, requests
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from .base import SeriesProvider, ProviderError
from dotenv import load_dotenv
load_dotenv()

EOD_BASE = os.getenv("EODHD_BASE", "https://eodhd.com/api")
EOD_TOKEN = os.getenv("EODHD_API_TOKEN", "")

class EODBondError(RuntimeError): ...

def _eod(url: str, params: dict) -> list:
    params = {**params, "api_token": EOD_TOKEN, "fmt": "json"}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        hint = ""
        if r.status_code == 403:
            hint = " (403 Forbidden: ensure EUBOND pricing is enabled for your key)"
        raise EODBondError(f"EOD HTTP {r.status_code}: {r.text[:200]}{hint}")
    js = r.json()
    if not isinstance(js, list):
        return []
    return js

def fetch_eubond_eod(code: str, start: Optional[str] = None, end: Optional[str] = None) -> List[Tuple[datetime, float, Optional[float]]]:
    """
    Returns [(ts, close_px, yield_or_none)] for vendor Code on EUBOND.
    """
    if not EOD_TOKEN:
        raise EODBondError("Missing EODHD_API_TOKEN")
    url = f"{EOD_BASE}/eod/{code}.EUBOND"
    params = {}
    if start: params["from"] = start
    if end:   params["to"]   = end
    js = _eod(url, params)
    out=[]
    for it in js:
        d = it.get("date")
        px = it.get("close") or it.get("adjusted_close") or it.get("price")
        y  = it.get("yield") or it.get("Yield")  # some feeds use 'Yield'
        if d and px is not None:
            out.append((datetime.fromisoformat(d), float(px), float(y) if y not in (None,"") else None))
    return out


class EODBondProvider(SeriesProvider):
    """EODHD bond price provider using EUBOND endpoint.
    
    Supports series codes that map to EUBOND vendor codes.
    Series codes should be in format: BOND_EUBOND_{CODE} or just EUBOND codes.
    """
    
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        """Fetch bond price time series.
        
        Args:
            series_code: EUBOND vendor code (e.g., "ARG713" for GD30) 
                         or "BOND_EUBOND_{CODE}" format
            start: Start date in YYYY-MM-DD format
            end: End date in YYYY-MM-DD format
            
        Returns:
            List of (datetime, price) tuples
        """
        # Remove prefix if present
        if series_code.startswith("BOND_EUBOND_"):
            code = series_code.replace("BOND_EUBOND_", "")
        else:
            code = series_code
        
        try:
            # fetch_eubond_eod returns (ts, price, yield) tuples, we just need price
            rows_with_yield = fetch_eubond_eod(code, start=start, end=end)
            # Extract just (datetime, price) pairs
            return [(ts, price) for ts, price, _ in rows_with_yield]
        except EODBondError as e:
            raise ProviderError(f"EODBondProvider: {e}")
