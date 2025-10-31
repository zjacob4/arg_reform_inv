"""Pull NDF (Non-Deliverable Forward) curve data for Argentina USD/ARS."""

from typing import Optional
from .db import connect, upsert_timeseries, upsert_series_meta
from .providers.ndf_argentina import NDFArgentinaProvider
from .providers.base import ProviderError
from src.config.series_registry import REGISTRY

# NDF series to pull
NDF_SERIES = ["NDF_1M", "NDF_3M", "NDF_6M", "NDF_12M"]

provider = NDFArgentinaProvider()


def pull_ndf_series(series_id: str, start: Optional[str] = None, end: Optional[str] = None) -> int:
    """Pull a single NDF series and store in database.
    
    Args:
        series_id: Series ID (e.g., "NDF_3M")
        start: Start date in YYYY-MM-DD format
        end: End date in YYYY-MM-DD format
        
    Returns:
        Number of records inserted
    """
    print(f"  Pulling {series_id}...")
    
    try:
        rows = provider.fetch_timeseries(series_id, start=start, end=end)
        if not rows:
            print(f"    WARNING: No data returned for {series_id}")
            return 0
        
        with connect() as conn:
            # Ensure series metadata is registered
            if series_id in REGISTRY:
                spec = REGISTRY[series_id]
                upsert_series_meta(conn, [spec])
            
            # Store data
            upsert_timeseries(conn, series_id, rows)
            print(f"    ✓ Stored {len(rows)} record(s) for {series_id}")
            if rows:
                latest_date = rows[-1][0].date().isoformat()
                latest_value = rows[-1][1]
                print(f"    Latest: {latest_date}, Value: {latest_value:.2f} ARS/USD")
        
        return len(rows)
        
    except ProviderError as e:
        print(f"    ERROR: Provider error for {series_id}: {e}")
        return 0
    except Exception as e:
        print(f"    ERROR: Unexpected error for {series_id}: {type(e).__name__}: {e}")
        return 0


def refresh_ndf_curve(start: Optional[str] = None, end: Optional[str] = None) -> int:
    """Refresh all NDF curve series.
    
    Args:
        start: Start date in YYYY-MM-DD format (default: 1 year ago)
        end: End date in YYYY-MM-DD format (default: today)
        
    Returns:
        Total number of records inserted
    """
    print("NDF Pull: Starting refresh for NDF curve")
    
    if start is None:
        from datetime import datetime, timedelta
        start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    
    total = 0
    for series_id in NDF_SERIES:
        count = pull_ndf_series(series_id, start=start, end=end)
        total += count
    
    print(f"✓ NDF refresh completed: {total} total record(s) inserted")
    return total


def main():
    """CLI entry point."""
    refresh_ndf_curve()


if __name__ == "__main__":
    main()

