import requests
from datetime import datetime
from typing import List, Tuple, Optional
from .base import SeriesProvider, ProviderError

SDMX = "https://dataservices.imf.org/REST/SDMX_JSON.svc"
# IFS CPI (index) commonly referenced as PCPI_IX (All items CPI index)
# Dataset 'IFS' + key 'ARG.PCPI_IX'
# Use CompactData for performance: /CompactData/IFS/ARG.PCPI_IX?startPeriod=2016


def _url(start: Optional[str], end: Optional[str]) -> str:
    base = f"{SDMX}/CompactData/IFS/ARG.PCPI_IX"
    qs = []
    if start:
        qs.append(f"startPeriod={start}")
    if end:
        qs.append(f"endPeriod={end}")
    return base + ("?" + "&".join(qs) if qs else "")


class IMFProviderCPI(SeriesProvider):
    def fetch_timeseries(self, series_code: str, start: Optional[str] = None, end: Optional[str] = None) -> List[Tuple[datetime, float]]:
        if series_code not in ("CPI_NATIONAL_INDEX", "CPI_NATIONAL_YOY", "CPI_NATIONAL_MOM"):
            raise ProviderError("IMFProviderCPI supports CPI series only.")
        r = requests.get(_url(start, end), timeout=60)
        if r.status_code != 200:
            raise ProviderError(f"IMFProviderCPI HTTP {r.status_code}")
        js = r.json()
        # Path: CompactData -> DataSet -> Series -> Obs[]
        try:
            ser = js["CompactData"]["DataSet"]["Series"]["Obs"]
        except Exception:
            return []
        rows: List[Tuple[datetime, float]] = []
        for o in ser:
            t = o.get("@TIME_PERIOD")
            v = o.get("@OBS_VALUE")
            if t and v is not None:
                rows.append((datetime.fromisoformat(t + "-01"), float(v)))
        rows.sort(key=lambda x: x[0])
        if series_code == "CPI_NATIONAL_INDEX":
            return rows
        # Compute MoM/YoY locally if requested
        def pct_change(seq: List[Tuple[datetime, float]], k: int) -> List[Tuple[datetime, float]]:
            out: List[Tuple[datetime, float]] = []
            for i in range(len(seq)):
                if i >= k and seq[i - k][1] != 0:
                    out.append((seq[i][0], (seq[i][1] / seq[i - k][1] - 1) * 100))
            return out
        return pct_change(rows, 12) if series_code.endswith("YOY") else pct_change(rows, 1)


