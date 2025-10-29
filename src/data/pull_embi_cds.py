"""Pull EMBI and CDS data."""

from datetime import datetime, timedelta
from typing import List, Tuple

from .db import connect, upsert_timeseries


def pull_embi_ar() -> List[Tuple[datetime, float]]:
    """Pull Argentina EMBI spread data.
    
    For now, generates synthetic daily data.
    TODO: Replace with actual API call to Bloomberg or other data source.
    
    Returns:
        List of (datetime, value) tuples
    """
    # Placeholder: generate daily data for last 90 days
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days = 90
    base_spread = 2000.0  # Base EMBI spread in basis points
    
    rows = []
    for i in range(days, 0, -1):
        date = today - timedelta(days=i)
        
        # Skip weekends
        if date.weekday() >= 5:  # Saturday or Sunday
            continue
        
        # Synthetic value with some volatility
        variation = (i % 7) * 10 - 30  # Weekly pattern with noise
        value = base_spread + variation + (i % 3) * 5
        rows.append((date, value))
    
    return rows


def pull_cds_5y() -> List[Tuple[datetime, float]]:
    """Pull 5-year CDS spread data.
    
    For now, generates synthetic daily data.
    TODO: Replace with actual API call to Bloomberg or other data source.
    
    Returns:
        List of (datetime, value) tuples
    """
    # Placeholder: generate daily data for last 90 days
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days = 90
    base_cds = 3500.0  # Base CDS spread in basis points
    
    rows = []
    for i in range(days, 0, -1):
        date = today - timedelta(days=i)
        
        # Skip weekends
        if date.weekday() >= 5:  # Saturday or Sunday
            continue
        
        # Synthetic value correlated with EMBI but slightly different
        variation = (i % 7) * 8 - 25
        value = base_cds + variation + (i % 4) * 7
        rows.append((date, value))
    
    return rows


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

