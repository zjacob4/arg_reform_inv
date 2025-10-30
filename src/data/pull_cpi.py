"""Pull CPI (Consumer Price Index) data via preferred FRED CPI series.

Canonical series stored:
  - CPI_NATIONAL_INDEX (level)
"""

from datetime import datetime
from typing import List, Tuple

from .db import connect, upsert_timeseries
from .provider_router import fetch_series
from .providers.base import ProviderError


def pull_cpi_index() -> List[Tuple[datetime, float]]:
    """Fetch CPI index (level)."""
    rows = fetch_series("CPI_NATIONAL_INDEX", start="2016-12-01")
    if not rows:
        raise ProviderError("All providers returned empty data for CPI_NATIONAL_INDEX")
    return rows


# YoY is temporarily disabled (can be computed later from index if needed)


# MoM temporarily disabled (can be computed later from index)


def refresh_cpi() -> None:
    """Refresh CPI series into DuckDB."""
    conn = connect()
    try:
        idx = pull_cpi_index()
        upsert_timeseries(conn, "CPI_NATIONAL_INDEX", idx)
    finally:
        conn.close()


def main():
    print("Pulling CPI data...")
    try:
        refresh_cpi()
        print("  CPI refresh completed (INDEX)")
    except Exception as e:
        print(f"Error pulling CPI data: {e}")
        raise


if __name__ == "__main__":
    main()

