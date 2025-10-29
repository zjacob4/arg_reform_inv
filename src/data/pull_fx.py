"""Pull USD/ARS exchange rate data."""

from datetime import datetime, timedelta
from typing import List, Tuple

from .db import connect, upsert_timeseries


def pull_usdars_official() -> List[Tuple[datetime, float]]:
    """Pull official USD/ARS exchange rate.
    
    For now, uses a placeholder with recent dates and static rate.
    TODO: Replace with actual API call to BCRA or other data source.
    
    Returns:
        List of (datetime, value) tuples
    """
    # Placeholder: generate last 30 days with a static rate
    # In production, this would call BCRA API or similar
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days = 30
    rate = 850.0  # Placeholder rate
    
    rows = [
        (today - timedelta(days=i), rate + (i * 0.5))  # Slight variation
        for i in range(days, 0, -1)
    ]
    return rows


def pull_usdars_parallel() -> List[Tuple[datetime, float]]:
    """Pull parallel (blue) USD/ARS exchange rate.
    
    For now, generates placeholder data with a gap above official rate.
    TODO: Replace with actual data source (e.g., Ambito Financiero, DolarHoy).
    
    Returns:
        List of (datetime, value) tuples
    """
    # Placeholder: generate parallel rate with ~20% gap above official
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days = 30
    base_official = 850.0
    gap_ratio = 1.20  # 20% gap
    parallel_rate = base_official * gap_ratio
    
    rows = [
        (today - timedelta(days=i), parallel_rate + (i * 0.6))  # Slightly higher variation
        for i in range(days, 0, -1)
    ]
    return rows


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

