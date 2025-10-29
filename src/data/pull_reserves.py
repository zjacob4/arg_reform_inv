"""Pull foreign reserves data."""

from datetime import datetime, timedelta
from typing import List, Tuple

from .db import connect, upsert_timeseries


def pull_reserves_usd() -> List[Tuple[datetime, float]]:
    """Pull USD reserves data.
    
    For now, generates synthetic weekly data.
    TODO: Replace with actual API call to BCRA or other data source.
    
    Returns:
        List of (datetime, value) tuples
    """
    # Placeholder: generate weekly data for last 12 weeks
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    weeks = 12
    base_reserves = 21000.0  # Base value in USD millions
    
    # Generate weekly data (Fridays)
    rows = []
    for i in range(weeks, 0, -1):
        # Get Friday of that week
        days_back = (i - 1) * 7
        date = today - timedelta(days=days_back)
        # Adjust to Friday (weekday 4)
        weekday = date.weekday()
        days_to_friday = (4 - weekday) % 7
        if days_to_friday > 0:
            date = date - timedelta(days=7 - days_to_friday)
        else:
            date = date - timedelta(days=days_to_friday)
        
        # Synthetic value with some variation
        value = base_reserves + (weeks - i) * 50.0 + (i % 3) * 20.0
        rows.append((date, value))
    
    return rows


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

