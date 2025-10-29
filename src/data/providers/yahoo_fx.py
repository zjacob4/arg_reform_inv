"""Yahoo Finance FX data provider."""

from datetime import datetime
from typing import List, Tuple, Optional
from .base import SeriesProvider, ProviderError

# Lazy import to avoid hard dependency if not used
def _yf():
    import yfinance as yf
    return yf

YF_MAP = {
    "USDARS_OFFICIAL": "ARS=X",  # Yahoo ticker for USD/ARS spot (alternative ticker)
}


class YahooFXProvider(SeriesProvider):
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        ticker = YF_MAP.get(series_code)
        if not ticker:
            raise ProviderError(f"YahooFXProvider: unknown series_code={series_code}")
        yf = _yf()
        # Use a broad period if start not provided
        period = "max" if not start else None
        kwargs = {}
        if start and end:
            kwargs = {"start": start, "end": end}
        data = yf.download(ticker, period=period, **kwargs, progress=False, interval="1d")
        if data is None or data.empty:
            raise ProviderError("YahooFXProvider: empty dataframe")
        out: List[Tuple[datetime, float]] = []
        # Use 'Close' as rate
        for ts, row in data.iterrows():
            val = row.get("Close")
            if val is None:
                continue
            # yfinance ts can be pandas.Timestamp with tz; normalize to naive UTC
            dt = datetime.utcfromtimestamp(ts.to_pydatetime().timestamp())
            out.append((dt, float(val)))
        return out

