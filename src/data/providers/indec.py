"""INDEC (Argentine statistics agency) data provider."""

import os
import requests
from datetime import datetime
from typing import List, Tuple, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .base import SeriesProvider, ProviderError

INDEC_BASE = os.getenv("INDEC_API_BASE", "https://apis.datos.gob.ar/series/api/series")

# Example CPI series IDs (update as needed from INDEC catalog)
INDEC_SERIES = {
    "CPI_HEADLINE": "ipc_nivel_general_nacional",  # example name; replace with the exact id you use
    "CPI_CORE": "ipc_nucleo_nivel_general_nacional",  # if available; else compute core later
}


def _indec_url(series_id: str, start: Optional[str], end: Optional[str]) -> str:
    # /?ids=<series_id>&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&format=json
    params = [f"ids={series_id}", "format=json"]
    if start:
        params.append(f"start_date={start}")
    if end:
        params.append(f"end_date={end}")
    return f"{INDEC_BASE}/?{'&'.join(params)}"


class INDECProvider(SeriesProvider):
    def __init__(self):
        # Create session with retry strategy
        self.session = requests.Session()
        
        # Retry strategy with longer backoff for slow APIs
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,  # Longer backoff for slow APIs
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        sid = INDEC_SERIES.get(series_code)
        if not sid:
            raise ProviderError(f"INDECProvider: unknown series_code={series_code}")
        
        url = _indec_url(sid, start, end)
        
        try:
            # Use longer timeout for slow INDEC API
            r = self.session.get(url, timeout=120)  # 2 minutes timeout
            if r.status_code != 200:
                raise ProviderError(f"INDECProvider HTTP {r.status_code} for {url}")
            
            js = r.json()
            # Expect: {"data":[["2025-01-01", 123.4], ...]} or {"series":[{"data":[...] }]}
            data = None
            if isinstance(js, dict):
                if "data" in js:
                    data = js["data"]
                elif "series" in js and js["series"]:
                    data = js["series"][0].get("data", None)
            if not data:
                raise ProviderError("INDECProvider: unexpected schema")
            
            out: List[Tuple[datetime, float]] = []
            for row in data:
                # row like ["2025-01-01", 123.4] or {"date": "...", "value": ...}
                if isinstance(row, (list, tuple)) and len(row) >= 2:
                    ts = datetime.fromisoformat(str(row[0]))
                    out.append((ts, float(row[1])))
                elif isinstance(row, dict):
                    d = row.get("date")
                    v = row.get("value")
                    if d and v is not None:
                        out.append((datetime.fromisoformat(str(d)), float(v)))
            return sorted(out, key=lambda x: x[0])
            
        except requests.exceptions.Timeout as e:
            raise ProviderError(f"INDECProvider timeout: {e}")
        except requests.exceptions.RequestException as e:
            raise ProviderError(f"INDECProvider request error: {e}")
        except Exception as e:
            raise ProviderError(f"INDECProvider unexpected error: {e}")

