"""Reserves momentum feature: 4-week rate of change."""

from datetime import datetime, timedelta
from typing import Optional

import duckdb
from pydantic import BaseModel

from ..data.db import connect


class ReservesMomentumResult(BaseModel):
    """Reserves momentum computation result."""
    
    value: float  # 4-week momentum as decimal (e.g., 0.05 for 5%)
    timestamp: datetime
    reserves_current: Optional[float] = None
    reserves_4w_ago: Optional[float] = None


def compute(conn: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[ReservesMomentumResult]:
    """Compute 4-week reserves momentum = (Res_t - Res_t-28d) / Res_t-28d.
    
    Args:
        conn: Optional DuckDB connection. If None, creates a new one.
        
    Returns:
        ReservesMomentumResult with momentum value and timestamp, or None if data unavailable
    """
    should_close = conn is None
    if conn is None:
        conn = connect()
    
    try:
        # Get latest reserves value
        latest_query = """
            SELECT ts, value 
            FROM fact_series 
            WHERE series_id = 'RESERVES_USD'
            ORDER BY ts DESC 
            LIMIT 1
        """
        latest_result = conn.execute(latest_query).fetchone()
        
        if not latest_result:
            return None
        
        latest_ts, latest_value = latest_result
        
        # Calculate date 28 days ago
        date_4w_ago = latest_ts - timedelta(days=28)
        
        # Get reserves value closest to 4 weeks ago
        past_query = """
            SELECT ts, value 
            FROM fact_series 
            WHERE series_id = 'RESERVES_USD'
                AND ts <= ?
            ORDER BY ts DESC 
            LIMIT 1
        """
        past_result = conn.execute(past_query, [date_4w_ago]).fetchone()
        
        if not past_result:
            return None
        
        past_ts, past_value = past_result
        
        # Calculate momentum
        momentum = (latest_value - past_value) / past_value
        
        return ReservesMomentumResult(
            value=momentum,
            timestamp=latest_ts,
            reserves_current=latest_value,
            reserves_4w_ago=past_value,
        )
        
    finally:
        if should_close:
            conn.close()

