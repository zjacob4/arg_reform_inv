import os
import requests
from datetime import datetime
from typing import List, Tuple, Optional
from .base import SeriesProvider, ProviderError
from dotenv import load_dotenv


FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# FRED series used:
#  - ARGCPICOREAICPIndex: Argentina Core CPI, index (monthly)
#  - ARGCPITOTLZG: Argentina CPI YoY percent change (monthly)

def _series_map() -> dict:
    # Allow overriding via env if needed
    idx = os.getenv("CPI_NATIONAL_INDEX", "ARGCPALTT01IXNBM").strip()
    yoy = os.getenv("CPI_NATIONAL_YOY", "ARGCPALTT01GYM").strip()
    mom = os.getenv("CPI_NATIONAL_MOM", "ARGCPALTT01GPM").strip()
    return {
        "CPI_NATIONAL_INDEX": idx,
        "CPI_NATIONAL_YOY": yoy,
        "CPI_NATIONAL_MOM": mom,
    }


def _build_params(series_id: str, start: Optional[str], end: Optional[str]) -> dict:
    # Load environment variables from .env file
    load_dotenv()
    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        raise ProviderError("FRED API key missing. Set FRED_API_KEY in environment.")
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start or "2016-01-01",
    }
    if end:
        params["observation_end"] = end
    return params


class FREDCPIProvider(SeriesProvider):
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> List[Tuple[datetime, float]]:
        mapped = _series_map().get(series_code)
        if not mapped and series_code == "CPI_NATIONAL_MOM":
            # We'll compute MoM locally from the index series
            idx_rows = self.fetch_timeseries("CPI_NATIONAL_INDEX", start=start, end=end)
            return self._pct_change(idx_rows, k=1)
        if not mapped:
            raise ProviderError(f"FREDCPIProvider: unknown series_code={series_code}")

        params = _build_params(mapped, start, end)
        try:
            r = requests.get(FRED_BASE, params=params, timeout=45)
            if r.status_code != 200:
                try:
                    detail = r.json()
                except Exception:
                    detail = r.text
                raise ProviderError(f"FRED HTTP {r.status_code}: {detail}")
            js = r.json()
            observations = js.get("observations", [])
            out: List[Tuple[datetime, float]] = []
            for o in observations:
                d = o.get("date")
                v = o.get("value")
                if not d or v in (None, "."):
                    continue
                try:
                    ts = datetime.fromisoformat(d)
                except Exception:
                    # Monthly without day -> append "-01"
                    ts = datetime.fromisoformat(str(d) + "-01")
                out.append((ts, float(v)))
            out.sort(key=lambda x: x[0])
            # Ensure CPI_NATIONAL_MOM computed if requested directly
            if series_code == "CPI_NATIONAL_MOM":
                return self._pct_change(out, k=1)
            return out
        except requests.exceptions.RequestException as e:
            raise ProviderError(f"FRED request error: {e}")
        except Exception as e:
            raise ProviderError(f"FRED unexpected error: {e}")

    @staticmethod
    def _pct_change(seq: List[Tuple[datetime, float]], k: int) -> List[Tuple[datetime, float]]:
        out: List[Tuple[datetime, float]] = []
        for i in range(len(seq)):
            if i >= k and seq[i - k][1] != 0:
                out.append((seq[i][0], (seq[i][1] / seq[i - k][1] - 1) * 100))
        return out


