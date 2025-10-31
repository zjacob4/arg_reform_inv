import os, requests
import yaml
import pathlib
from datetime import datetime
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv
from src.quality.embi_checks import price_sanity, daily_jump_ok
from .base import SeriesProvider, ProviderError

load_dotenv()

EOD_BASE = os.getenv("EODHD_BASE", "https://eodhd.com/api")
EOD_TOKEN = os.getenv("EODHD_API_TOKEN", "")

class EODBondError(RuntimeError): ...

def _eod(url: str, params: dict) -> dict:
    params = {**params, "api_token": EOD_TOKEN, "fmt": "json"}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise EODBondError(f"EOD HTTP {r.status_code}: {r.text[:240]}")
    return r.json()

def fetch_bond_eod_by_isin(isin: str, start: Optional[str]=None, end: Optional[str]=None) -> List[Tuple[datetime, float]]:
    """
    Returns list of (date, clean_price) for the bond identified by ISIN.
    Uses EODHD /api/eod-bond/{ISIN} endpoint (EOD close).
    """
    if not EOD_TOKEN:
        raise EODBondError("Missing EODHD_API_TOKEN")
    # url = f"{EOD_BASE}/eod-bond/{isin}"
    url = f"{EOD_BASE}/eod/{isin}.BOND"
    params = {}
    if start: params["from"] = start
    if end:   params["to"]   = end
    js = _eod(url, params)
    rows: List[Tuple[datetime, float]] = []
    # Expect array of {date:"YYYY-MM-DD", close: <price>, ...}
    if isinstance(js, list):
        for it in js:
            d = it.get("date")
            px = it.get("close")
            if d and px is not None:
                rows.append((datetime.fromisoformat(d), float(px)))
    elif isinstance(js, dict) and "eod" in js:
        for it in js["eod"]:
            d = it.get("date")
            px = it.get("close")
            if d and px is not None:
                rows.append((datetime.fromisoformat(d), float(px)))
    rows.sort(key=lambda x: x[0])
    
    # Apply quality filters
    filtered = []
    prev = None
    for (ts, px) in rows:
        if not price_sanity(px):
            continue
        if not daily_jump_ok(prev, px):
            continue
        filtered.append((ts, px))
        prev = px
    rows = filtered
    
    return rows

def fetch_quotes_for_universe(bonds_meta: List[dict], start: Optional[str]=None, end: Optional[str]=None) -> Dict[str, List[Tuple[datetime, float]]]:
    data: Dict[str, List[Tuple[datetime, float]]] = {}
    for b in bonds_meta:
        isin = b.get("isin")
        tkr  = (b.get("ticker") or isin).upper()
        if not isin:
            continue
        try:
            rows = fetch_bond_eod_by_isin(isin, start=start, end=end)
            if rows:
                data[tkr] = rows
        except Exception as e:
            # skip silently; caller can decide how strict to be
            continue
    return data


class EODBondProvider(SeriesProvider):
    """EODHD bond price provider for Argentine USD bonds.
    
    Supports series codes like BOND_PRICE_{TICKER} (e.g., BOND_PRICE_GD30).
    Maps tickers to ISINs via arg_usd_bonds.yaml.
    """
    
    def __init__(self, bonds_yaml_path: str = "src/data/bonds/arg_usd_bonds.yaml"):
        self.bonds_yaml_path = bonds_yaml_path
        self._ticker_to_isin = self._load_ticker_map()
    
    def _load_ticker_map(self) -> Dict[str, str]:
        """Load ticker -> ISIN mapping from YAML."""
        try:
            with open(pathlib.Path(self.bonds_yaml_path), "r") as f:
                bonds = yaml.safe_load(f)
            return {b["ticker"].upper(): b["isin"] for b in bonds if "ticker" in b and "isin" in b}
        except Exception as e:
            return {}
    
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        """Fetch bond price time series.
        
        Args:
            series_code: Must be "BOND_PRICE_{TICKER}" format (e.g., "BOND_PRICE_GD30")
            start: Start date in YYYY-MM-DD format
            end: End date in YYYY-MM-DD format
            
        Returns:
            List of (datetime, price) tuples
        """
        if not series_code.startswith("BOND_PRICE_"):
            raise ProviderError(f"EODBondProvider: series_code must start with 'BOND_PRICE_' (got {series_code})")
        
        ticker = series_code.replace("BOND_PRICE_", "").upper()
        isin = self._ticker_to_isin.get(ticker)
        
        if not isin:
            raise ProviderError(f"EODBondProvider: unknown ticker {ticker} (not found in {self.bonds_yaml_path})")
        
        try:
            return fetch_bond_eod_by_isin(isin, start=start, end=end)
        except EODBondError as e:
            raise ProviderError(f"EODBondProvider: {e}")

