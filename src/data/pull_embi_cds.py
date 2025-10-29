"""Pull EMBI and CDS data."""

from datetime import datetime, timedelta
from typing import List, Tuple

from .db import connect, upsert_timeseries
from .provider_router import fetch_series
from .providers.base import ProviderError


def pull_embi_ar() -> List[Tuple[datetime, float]]:
    """Pull Argentina EMBI spread data.
    
    Tries provider router first, raises error if all providers fail.
    
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ProviderError: If all providers fail to fetch data
    """
    try:
        # Try provider router first
        rows = fetch_series("EMBI_AR", start="2020-01-01")
        if rows:
            return rows
        else:
            raise ProviderError("All providers returned empty data for EMBI_AR")
    except ProviderError as e:
        raise ProviderError(f"Failed to fetch EMBI_AR data: {e}")


def pull_cds_5y() -> List[Tuple[datetime, float]]:
    """Pull 5-year CDS spread data.
    
    Tries provider router first, raises error if all providers fail.
    
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ProviderError: If all providers fail to fetch data
    """
    try:
        # Try provider router first
        rows = fetch_series("CDS_5Y", start="2020-01-01")
        if rows:
            return rows
        else:
            raise ProviderError("All providers returned empty data for CDS_5Y")
    except ProviderError as e:
        raise ProviderError(f"Failed to fetch CDS_5Y data: {e}")


def main():
    """Pull EMBI/CDS data and write to DuckDB."""
    print("Pulling EMBI and CDS data...")
    
    try:
        conn = connect()
        
        try:
            # Pull EMBI
            embi_rows = pull_embi_ar()
            upsert_timeseries(conn, "EMBI_AR", embi_rows)
            print(f"  Upserted {len(embi_rows)} records for EMBI_AR")
            
            # Pull CDS
            cds_rows = pull_cds_5y()
            upsert_timeseries(conn, "CDS_5Y", cds_rows)
            print(f"  Upserted {len(cds_rows)} records for CDS_5Y")
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error pulling EMBI/CDS data: {e}")
        raise


if __name__ == "__main__":
    main()

