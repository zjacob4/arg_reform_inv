import requests
from datetime import datetime
from typing import List, Tuple, Optional
from .base import SeriesProvider, ProviderError

BLUE_API = "https://api.bluelytics.com.ar/v2"

# Bluelytics only provides current values via /latest endpoint
# We'll expose series codes for blue/parallel and official rates
# Keep the aliases, but add a comment and keep PARALLEL as the canonical key.
NAME_MAP = {
    "USDARS_PARALLEL": "blue",          # canonical
    "USDARS_BLUE": "blue",              # deprecated alias
    "USDARS_OFFICIAL_BLUELYTICS": "oficial",
}

class BluelyticsProvider(SeriesProvider):
    def fetch_timeseries(self, series_code: str, start: Optional[str]=None, end: Optional[str]=None) -> List[Tuple[datetime, float]]:
        kind = NAME_MAP.get(series_code)
        if not kind:
            raise ProviderError(f"BluelyticsProvider: unknown series_code={series_code}")
        
        out: List[Tuple[datetime, float]] = []
        
        # If no date range specified, get latest data
        if not start and not end:
            url = f"{BLUE_API}/latest"
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                raise ProviderError(f"BluelyticsProvider HTTP {r.status_code}")
            
            js = r.json()
            if kind not in js:
                raise ProviderError(f"BluelyticsProvider: {kind} not found in API response")
            
            # Extract current value and timestamp
            data = js[kind]
            value = data.get("value_avg")
            if value is None:
                raise ProviderError(f"BluelyticsProvider: value_avg not found for {kind}")
            
            # Parse last_update timestamp
            last_update_str = js.get("last_update", "")
            try:
                # Parse ISO format: "2025-10-29T19:45:59.078713-03:00"
                if last_update_str:
                    # Remove timezone info for simplicity
                    dt_str = last_update_str.split('+')[0].split('-')[0] if '+' in last_update_str or '-' in last_update_str[-6:] else last_update_str
                    current_time = datetime.fromisoformat(dt_str)
                else:
                    current_time = datetime.now()
            except:
                current_time = datetime.now()
            
            out = [(current_time, float(value))]
        else:
            # Get historical data for date range
            # For simplicity, we'll get data for each day in the range
            # In practice, you might want to optimize this to avoid too many API calls
            from datetime import timedelta
            
            start_date = datetime.fromisoformat(start) if start else datetime.now() - timedelta(days=30)
            end_date = datetime.fromisoformat(end) if end else datetime.now()
            
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                url = f"{BLUE_API}/historical?day={date_str}"
                
                try:
                    r = requests.get(url, timeout=30)
                    if r.status_code == 200:
                        js = r.json()
                        if kind in js:
                            data = js[kind]
                            value = data.get("value_avg")
                            if value is not None:
                                out.append((current_date, float(value)))
                    elif r.status_code == 404:
                        # No data for this date, skip
                        pass
                    else:
                        # Log error but continue
                        print(f"Warning: HTTP {r.status_code} for {date_str}")
                except Exception as e:
                    print(f"Warning: Error fetching {date_str}: {e}")
                
                current_date += timedelta(days=1)
        
        # Apply start/end filtering if provided (redundant but safe)
        if start:
            start_dt = datetime.fromisoformat(start)
            out = [x for x in out if x[0] >= start_dt]
        if end:
            end_dt = datetime.fromisoformat(end)
            out = [x for x in out if x[0] <= end_dt]
        
        if not out:
            raise ProviderError(f"BluelyticsProvider: No data found for {series_code} in date range")
        
        return sorted(out, key=lambda x: x[0])
