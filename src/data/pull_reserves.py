"""Pull foreign reserves data."""

from datetime import datetime, timedelta
from typing import List, Tuple

from .db import connect, upsert_timeseries
from .provider_router import fetch_series
from .providers.base import ProviderError


def pull_reserves_usd() -> List[Tuple[datetime, float]]:
    """Pull USD reserves data.
    
    Tries provider router first, raises error if all providers fail.
    
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ProviderError: If all providers fail to fetch data
    """
    try:
        # Try provider router first
        rows = fetch_series("RESERVES_USD", start="2020-01-01")
        if rows:
            return rows
        else:
            raise ProviderError("All providers returned empty data for RESERVES_USD")
    except ProviderError as e:
        raise ProviderError(f"Failed to fetch RESERVES_USD data: {e}")


def main():
    """Pull reserves data and write to DuckDB."""
    print("Pulling USD reserves data...")
    
    try:
        rows = pull_reserves_usd()
        conn = connect()
        
        try:
            upsert_timeseries(conn, "RESERVES_USD", rows)
            print(f"  Upserted {len(rows)} records for RESERVES_USD")
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error pulling reserves data: {e}")
        raise


if __name__ == "__main__":
    main()

