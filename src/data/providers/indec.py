import requests
from datetime import datetime
from typing import List, Tuple, Optional
from .base import SeriesProvider, ProviderError

BASE = "https://apis.datos.gob.ar/series/api"

# We will discover the National CPI (base 2016) series id dynamically using /search,
# then fetch values via /series. We also allow transform suffixes (e.g., :percent_change_a_month_ago).

CANDIDATE_QUERIES = [
    "ipc nivel general 2016 nacional",
    "índice de precios al consumidor nacional 2016",
    "ipc 2016 nivel general indec",
]

# Hard-known example from docs (used only as last-resort fallback if search fails):
DOCS_EXAMPLE_ID = "101.1_I2NG_2016_M_22"  # National CPI (Dec-2016=100) per API docs


def _search_first_id() -> Optional[str]:
    for q in CANDIDATE_QUERIES:
        r = requests.get(f"{BASE}/search", params={"q": q, "limit": 5}, timeout=30)
        r.raise_for_status()
        js = r.json()
        items = (js.get("data") or {}).get("results") or js.get("results") or []
        # Try to pick "nivel general", monthly, national base 2016
        for it in items:
            sid = it.get("id")
            title = (it.get("title") or "").lower()
            units = (it.get("units") or "").lower()
            # Favor monthly National CPI base 2016, nivel general
            if sid and "ipc" in title and "2016" in title and "nivel general" in title:
                return sid
    return None


def _resolve_series_id() -> str:
    sid = _search_first_id()
    return sid or DOCS_EXAMPLE_ID


def _fetch_series(series_id: str, start: Optional[str], end: Optional[str]) -> List[Tuple[datetime, float]]:
    params = {"ids": series_id, "format": "json"}
    if start:
        params["start_date"] = start
    if end:
        params["end_date"] = end
    r = requests.get(f"{BASE}/series/", params=params, timeout=30)
    if r.status_code == 404:
        raise ProviderError(f"INDEC/Series 404 for {series_id}")
    r.raise_for_status()
    js = r.json()
    data = (js.get("data") or js)  # API returns {data: {series:[...]}} or direct obj
    # Standard shape: {"data":[{"dates":[...],"values":[...]}]} or {"series":[...]} depending on version.
    # Normalize robustly:
    rows = []
    if isinstance(js, dict) and "series" in js:
        series = js["series"]
        for s in series:
            for t, v in zip(s.get("index", []), s.get("values", [])):
                if v is not None:
                    rows.append((datetime.fromisoformat(str(t)), float(v)))
    else:
        # alternative shape (older gateway)
        # try tabular "data" with two columns
        if "data" in js and isinstance(js["data"], list) and len(js["data"]) and isinstance(js["data"][0], list):
            for t, v in js["data"]:
                if v is not None:
                    rows.append((datetime.fromisoformat(str(t)), float(v)))
    return sorted(rows, key=lambda x: x[0])


class INDECProvider(SeriesProvider):
    """
    Exposes:
      - CPI_NATIONAL_INDEX  (level, Dec2016=100)
      - CPI_NATIONAL_YOY    (YoY pct change)
      - CPI_NATIONAL_MOM    (MoM pct change)
    """
    def fetch_timeseries(self, series_code: str, start: Optional[str]=None, end: Optional[str]=None):
        sid = _resolve_series_id()
        transform = None
        if series_code == "CPI_NATIONAL_INDEX":
            pass
        elif series_code == "CPI_NATIONAL_YOY":
            transform = "percent_change_a_year_ago"
        elif series_code == "CPI_NATIONAL_MOM":
            transform = "percent_change"
        else:
            raise ProviderError(f"INDECProvider: unknown series_code={series_code}")

        sid_full = f"{sid}:{transform}" if transform else sid
        return _fetch_series(sid_full, start, end)

