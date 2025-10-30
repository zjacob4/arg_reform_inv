"""FX gap feature: parallel vs official exchange rate spread."""

from datetime import datetime
from typing import Optional

import duckdb
from pydantic import BaseModel

from ..data.db import connect


class FXGapResult(BaseModel):
    """FX gap computation result."""
    
    # New canonical fields
    official: Optional[float] = None
    parallel: Optional[float] = None
    gap: Optional[float] = None
    official_ts: Optional[datetime] = None
    parallel_ts: Optional[datetime] = None
    status: str = "OK"

    # Back-compat fields (used by dashboard today)
    parallel_rate: Optional[float] = None
    official_rate: Optional[float] = None
    used_parallel: Optional[str] = None
    used_official: Optional[str] = None


def compute(conn: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[FXGapResult]:
    """Compute FX gap = (parallel - official) / official for latest date.

    Official fetch order:
      1) USDARS_OFFICIAL (BCRA)
      2) USDARS_OFFICIAL_BLUELYTICS (fallback)

    Parallel fetch:
      - USDARS_PARALLEL (canonical)

    Returns FXGapResult with fields (official, parallel, gap, official_ts, parallel_ts, status).
    If any side is missing, gap=None and status="MISSING_DATA".
    """
    should_close = conn is None
    if conn is None:
        conn = connect()
    
    try:
        # Parallel: canonical series
        parallel_ts, parallel_val = _get_latest_single(conn, "USDARS_PARALLEL")

        # Official: try BCRA, then Bluelytics official
        official_ts, official_val = _get_latest_single(conn, "USDARS_OFFICIAL")
        used_official = "USDARS_OFFICIAL" if official_val is not None else None
        if official_val is None:
            official_ts, official_val = _get_latest_single(conn, "USDARS_OFFICIAL_BLUELYTICS")
            if official_val is not None:
                used_official = "USDARS_OFFICIAL_BLUELYTICS"

        # If any missing, return MISSING_DATA
        if official_val is None or parallel_val is None:
            return FXGapResult(
                official=official_val,
                parallel=parallel_val,
                gap=None,
                official_ts=official_ts,
                parallel_ts=parallel_ts,
                status="MISSING_DATA",
                # back-compat fields
                official_rate=official_val,
                parallel_rate=parallel_val,
                used_parallel="USDARS_PARALLEL" if parallel_val is not None else None,
                used_official=used_official,
            )

        gap_val = (parallel_val - official_val) / official_val

        return FXGapResult(
            official=official_val,
            parallel=parallel_val,
            gap=gap_val,
            official_ts=official_ts,
            parallel_ts=parallel_ts,
            status="OK",
            # back-compat fields
            official_rate=official_val,
            parallel_rate=parallel_val,
            used_parallel="USDARS_PARALLEL",
            used_official=used_official,
        )
        
    finally:
        if should_close:
            conn.close()


def _get_latest_single(conn: duckdb.DuckDBPyConnection, series_id: str) -> tuple[Optional[datetime], Optional[float]]:
    """Get latest (ts, value) for a single series id."""
    query = """
        SELECT ts, value 
        FROM fact_series 
        WHERE series_id = ?
        ORDER BY ts DESC 
        LIMIT 1
    """
    row = conn.execute(query, [series_id]).fetchone()
    if not row:
        return None, None
    ts, value = row
    return ts, float(value) if value is not None else None

