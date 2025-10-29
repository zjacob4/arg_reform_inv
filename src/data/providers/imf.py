"""IMF data provider using SDMX API."""

import requests
from datetime import datetime
from typing import List, Tuple, Optional
from .base import SeriesProvider, ProviderError

# IMF SDMX endpoint notes:
# IFS example for CPI:  https://dataservices.imf.org/ODS/api/IFS/PCPI_IX?Country=ARG&startPeriod=2020&endPeriod=2025
# IFS example for Reserves (e.g., IRFCL or similar code) depends on series availability.
# We'll map logical names to IMF codes in a series map.

IMF_BASE = "https://dataservices.imf.org/ODS/api/IFS"

IMF_MAP = {
    "CPI_HEADLINE": {"code": "PCPI_IX", "country": "ARG"},     # CPI index (you'll treat into yoy or use MoM later)
    # Add more if desired
}


class IMFProvider(SeriesProvider):
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        if series_code not in IMF_MAP:
            raise ProviderError(f"IMFProvider: unknown series_code {series_code}")
        meta = IMF_MAP[series_code]
        params = []
        if start:
            params.append(f"startPeriod={start}")
        if end:
            params.append(f"endPeriod={end}")
        qp = ("&" + "&".join(params)) if params else ""
        url = f"{IMF_BASE}/{meta['code']}?Country={meta['country']}{qp}"
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            raise ProviderError(f"IMFProvider HTTP {r.status_code}")
        j = r.json()
        # SDMX-ish: j["CompactData"]["DataSet"]["Series"]["Obs"] -> [{"@TIME_PERIOD":"2024-05","@OBS_VALUE":"123.4"}, ...]
        try:
            series = j["CompactData"]["DataSet"]["Series"]["Obs"]
        except Exception as e:
            raise ProviderError(f"IMFProvider parse error: {e}")
        out: List[Tuple[datetime, float]] = []
        for obs in series:
            t = obs.get("@TIME_PERIOD")
            v = obs.get("@OBS_VALUE")
            if t is None or v in (None, ""):
                continue
            # Parse YYYY or YYYY-MM
            ts = datetime.strptime(t, "%Y-%m") if "-" in t else datetime.strptime(t, "%Y")
            out.append((ts, float(v)))
        return out

