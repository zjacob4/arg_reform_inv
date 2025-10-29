"""Pull CPI (Consumer Price Index) data."""

from datetime import datetime
from typing import List, Tuple

from .db import connect, upsert_timeseries


def pull_cpi_headline() -> List[Tuple[datetime, float]]:
    """Pull headline CPI data.
    
    For now, generates synthetic monthly data.
    TODO: Replace with actual API call to INDEC or other data source.
    
    Returns:
        List of (datetime, value) tuples
    """
    # Placeholder: generate monthly data for last 24 months
    today = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    months = 24
    base_index = 100.0
    
    rows = []
    for i in range(months, 0, -1):
        # Get first day of each month
        if i == months:
            date = today.replace(day=1)
        else:
            # Go back i months
            year = today.year
            month = today.month - i
            while month <= 0:
                month += 12
                year -= 1
            date = datetime(year, month, 1)
        
        # Synthetic value with monthly inflation
        monthly_inflation = 0.08  # 8% monthly placeholder
        value = base_index * ((1 + monthly_inflation) ** (months - i))
        rows.append((date, value))
    
    return rows


def pull_cpi_core() -> List[Tuple[datetime, float]]:
    """Pull core CPI data.
    
    For now, generates synthetic monthly data.
    TODO: Replace with actual API call to INDEC or other data source.
    
    Returns:
        List of (datetime, value) tuples
    """
    # Similar to headline but slightly different trend
    headline_rows = pull_cpi_headline()
    
    # Core CPI typically increases slightly slower than headline
    rows = [
        (ts, value * 0.995)  # Core slightly lower/less volatile
        for ts, value in headline_rows
    ]
    
    return rows


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

