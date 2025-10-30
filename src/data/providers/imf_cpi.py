import os
from datetime import datetime
from typing import List, Tuple, Optional
from .base import SeriesProvider, ProviderError

# Use the sdmx1 library to access IMF SDMX-REST services
# References:
# - IMF Data Services knowledge base: https://datasupport.imf.org/knowledge?id=knowledge_category&sys_kb_id=b849dc6b47294ad8805d07c4f16d4311&category_id=9959b2bc1b6391903dba646fbd4bcb6a
# - sdmx1 data sources: https://sdmx1.readthedocs.io/en/v2.22.0/sources.html
import sdmx


class IMFProviderCPI(SeriesProvider):
    """IMF CPI provider using sdmx1 Client to fetch IFS ARG.PCPI_IX.

    Exposes CPI_NATIONAL_INDEX (level), CPI_NATIONAL_YOY, CPI_NATIONAL_MOM.
    """

    def _fetch_index(self, start: Optional[str], end: Optional[str]) -> List[Tuple[datetime, float]]:
        # Prefer the official IMF sources known to sdmx: 'IMF_DATA' (api.imf.org) or generic Client with direct data call
        force_source = os.getenv("IMF_CPI_SOURCE", "").upper().strip()
        if force_source == "AR1":
            try:
                ar1 = sdmx.Client("AR1")
                ar1_file = os.getenv("AR1_FILE", "IND.XML")
                msg = ar1.data(ar1_file)
            except Exception as e:
                raise ProviderError(f"AR1 bulk fetch error: {e}")
        else:
            try:
                client = sdmx.Client("IMF_DATA")
            except Exception:
                client = sdmx.Client()

        params = {}
        if start:
            params["startPeriod"] = start
        if end:
            params["endPeriod"] = end

        # Query IFS dataset, key ARG.PCPI_IX
            try:
                msg = client.data("IFS", key="ARG.PCPI_IX", params=params)
            except Exception as e_imf:
                # Fall back to direct URL via generic client if source alias fails
                try:
                    url = "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/ARG.PCPI_IX"
                    if params:
                        from urllib.parse import urlencode
                        url = url + "?" + urlencode(params)
                    msg = client.get(url=url)
                except Exception:
                    # As a secondary fallback, try AR1 bulk SDMX-ML file (no structure) for INDEC
                    try:
                        ar1 = sdmx.Client("AR1")
                        ar1_file = os.getenv("AR1_FILE", "IND.XML")
                        msg = ar1.data(ar1_file)
                    except Exception:
                        raise ProviderError(f"IMFProviderCPI query error: {e_imf}")

        # Convert SDMX message to (ts, value)
        rows: List[Tuple[datetime, float]] = []
        try:
            # Iterate all series and observations
            # sdmx1 DataMessage: msg.data is a list-like of Series; use .series for dict-like access
            data = getattr(msg, "data", None)
            if data is None:
                return rows
            # Handle both dict-like and iterable series containers
            series_iter = []
            if hasattr(data, "series") and data.series:
                # data.series may be a dict-like structure
                if hasattr(data.series, "items"):
                    series_iter = [s for _, s in data.series.items()]
                else:
                    series_iter = list(data.series)
            else:
                series_iter = list(data)

            for s in series_iter:
                obs_list = []
                if hasattr(s, "obs"):
                    obs_list = s.obs
                elif hasattr(s, "observations"):
                    obs_list = s.observations
                elif hasattr(s, "observations_updated"):
                    obs_list = s.observations_updated
                # obs_list may be list of (key, Observation) or dict-like
                if isinstance(obs_list, dict):
                    iterable = obs_list.values()
                else:
                    iterable = obs_list
                for o in iterable:
                    # Observation objects often have .period/.value; fallback to tuple-like
                    try:
                        t = getattr(o, "period", None) or getattr(o, "time", None)
                        v = getattr(o, "value", None)
                    except Exception:
                        try:
                            # tuple-like
                            t, v = o
                        except Exception:
                            continue
                    if t and v is not None:
                        # TIME_PERIOD may be YYYY-MM; normalize to month start
                        t_str = str(t)
                        if len(t_str) == 7 and t_str.count("-") == 1:
                            ts = datetime.fromisoformat(t_str + "-01")
                        else:
                            ts = datetime.fromisoformat(t_str)
                        rows.append((ts, float(v)))
        except Exception as e:
            raise ProviderError(f"IMFProviderCPI parse error: {e}")

        rows.sort(key=lambda x: x[0])
        return rows

    def fetch_timeseries(self, series_code: str, start: Optional[str] = None, end: Optional[str] = None) -> List[Tuple[datetime, float]]:
        if series_code not in ("CPI_NATIONAL_INDEX", "CPI_NATIONAL_YOY", "CPI_NATIONAL_MOM"):
            raise ProviderError("IMFProviderCPI supports CPI series only.")

        rows = self._fetch_index(start, end)
        if series_code == "CPI_NATIONAL_INDEX":
            return rows

        # Compute MoM/YoY locally
        def pct_change(seq: List[Tuple[datetime, float]], k: int) -> List[Tuple[datetime, float]]:
            out: List[Tuple[datetime, float]] = []
            for i in range(len(seq)):
                if i >= k and seq[i - k][1] != 0:
                    out.append((seq[i][0], (seq[i][1] / seq[i - k][1] - 1) * 100))
            return out

        return pct_change(rows, 12) if series_code.endswith("YOY") else pct_change(rows, 1)


