"""Pull USD/ARS exchange rate data."""

from datetime import datetime, timedelta
from typing import List, Tuple

from .db import connect, upsert_timeseries
from .provider_router import fetch_series
from .providers.base import ProviderError


def pull_usdars_official() -> List[Tuple[datetime, float]]:
    """Pull official USD/ARS exchange rate.
    
    Tries provider router first, raises error if all providers fail.
    
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ProviderError: If all providers fail to fetch data
    """
    try:
        # Try provider router first
        rows = fetch_series("USDARS_OFFICIAL", start="2020-01-01")
        if rows:
            return rows
        else:
            raise ProviderError("All providers returned empty data for USDARS_OFFICIAL")
    except ProviderError as e:
        raise ProviderError(f"Failed to fetch USDARS_OFFICIAL data: {e}")


def pull_usdars_parallel() -> List[Tuple[datetime, float]]:
    """Pull parallel (blue) USD/ARS exchange rate.
    
    Tries provider router first, raises error if all providers fail.
    
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ProviderError: If all providers fail to fetch data
    """
    try:
        # Try provider router first
        rows = fetch_series("USDARS_PARALLEL", start="2020-01-01")
        if rows:
            return rows
        else:
            raise ProviderError("All providers returned empty data for USDARS_PARALLEL")
    except ProviderError as e:
        raise ProviderError(f"Failed to fetch USDARS_PARALLEL data: {e}")


def main():
    """Pull FX data and write to DuckDB."""
    print("Pulling USD/ARS rates...")
    
    try:
        conn = connect()
        
        try:
            # Pull official rate
            official_rows = pull_usdars_official()
            upsert_timeseries(conn, "USDARS_OFFICIAL", official_rows)
            print(f"  Upserted {len(official_rows)} records for USDARS_OFFICIAL")
            
            # Pull parallel rate
            parallel_rows = pull_usdars_parallel()
            upsert_timeseries(conn, "USDARS_PARALLEL", parallel_rows)
            print(f"  Upserted {len(parallel_rows)} records for USDARS_PARALLEL")
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error pulling FX data: {e}")
        raise


if __name__ == "__main__":
    main()

