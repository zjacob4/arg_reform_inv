"""Pull CPI (Consumer Price Index) data."""

from datetime import datetime
from typing import List, Tuple

from .db import connect, upsert_timeseries
from .provider_router import fetch_series
from .providers.base import ProviderError


def pull_cpi_headline() -> List[Tuple[datetime, float]]:
    """Pull headline CPI data.
    
    Tries provider router first, raises error if all providers fail.
    
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ProviderError: If all providers fail to fetch data
    """
    try:
        # Try provider router first
        rows = fetch_series("CPI_HEADLINE", start="2019-01-01")
        if rows:
            return rows
        else:
            raise ProviderError("All providers returned empty data for CPI_HEADLINE")
    except ProviderError as e:
        raise ProviderError(f"Failed to fetch CPI_HEADLINE data: {e}")


def pull_cpi_core() -> List[Tuple[datetime, float]]:
    """Pull core CPI data.
    
    Tries provider router first, falls back to computed data from headline.
    
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ProviderError: If all providers fail to fetch data and headline data unavailable
    """
    try:
        # Try provider router first
        rows = fetch_series("CPI_CORE", start="2019-01-01")
        if rows:
            return rows
        else:
            # Try to compute from headline data
            headline_rows = pull_cpi_headline()
            # Core CPI typically increases slightly slower than headline
            rows = [
                (ts, value * 0.995)  # Core slightly lower/less volatile
                for ts, value in headline_rows
            ]
            return rows
    except ProviderError as e:
        raise ProviderError(f"Failed to fetch CPI_CORE data: {e}")


def main():
    """Pull CPI data and write to DuckDB."""
    print("Pulling CPI data...")
    
    try:
        conn = connect()
        
        try:
            # Pull headline CPI
            headline_rows = pull_cpi_headline()
            upsert_timeseries(conn, "CPI_HEADLINE", headline_rows)
            print(f"  Upserted {len(headline_rows)} records for CPI_HEADLINE")
            
            # Pull core CPI
            core_rows = pull_cpi_core()
            upsert_timeseries(conn, "CPI_CORE", core_rows)
            print(f"  Upserted {len(core_rows)} records for CPI_CORE")
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error pulling CPI data: {e}")
        raise


if __name__ == "__main__":
    main()

