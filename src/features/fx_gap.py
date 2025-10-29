"""FX gap feature: parallel vs official exchange rate spread."""

from datetime import datetime
from typing import Optional

import duckdb
from pydantic import BaseModel

from ..data.db import connect


class FXGapResult(BaseModel):
    """FX gap computation result."""
    
    value: float
    timestamp: datetime
    parallel_rate: Optional[float] = None
    official_rate: Optional[float] = None


def compute(conn: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[FXGapResult]:
    """Compute FX gap = (parallel - official) / official for latest date.
    
    Args:
        conn: Optional DuckDB connection. If None, creates a new one.
        
    Returns:
        FXGapResult with gap value and timestamp, or None if data unavailable
    """
    should_close = conn is None
    if conn is None:
        conn = connect()
    
    try:
        # Get latest parallel rate
        parallel_query = """
            SELECT ts, value 
            FROM fact_series 
            WHERE series_id = 'USDARS_PARALLEL'
            ORDER BY ts DESC 
            LIMIT 1
        """
        parallel_result = conn.execute(parallel_query).fetchone()
        
        if not parallel_result:
            return None
        
        parallel_ts, parallel_rate = parallel_result
        
        # Get official rate for same or most recent date
        official_query = """
            SELECT ts, value 
            FROM fact_series 
            WHERE series_id = 'USDARS_OFFICIAL'
                AND ts <= ?
            ORDER BY ts DESC 
            LIMIT 1
        """
        official_result = conn.execute(official_query, [parallel_ts]).fetchone()
        
        if not official_result:
            return None
        
        official_ts, official_rate = official_result
        
        # Use the most recent timestamp
        latest_ts = max(parallel_ts, official_ts)
        
        # Calculate gap
        gap = (parallel_rate - official_rate) / official_rate
        
        return FXGapResult(
            value=gap,
            timestamp=latest_ts,
            parallel_rate=parallel_rate,
            official_rate=official_rate,
        )
        
    finally:
        if should_close:
            conn.close()

