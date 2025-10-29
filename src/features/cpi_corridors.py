"""CPI corridors feature: forecast ranges based on FX pass-through and policy inputs."""

from datetime import datetime, timedelta
from typing import Optional

import duckdb
from pydantic import BaseModel

from ..data.db import connect


class CPICorridorResult(BaseModel):
    """CPI corridor computation result."""
    
    low: float
    mid: float
    high: float
    nowcast_3m_annualized: float
    timestamp: datetime


def _get_wages_stub(fx_pass: float) -> float:
    """Stub wages calculation: wages = fx_pass * 0.5.
    
    Args:
        fx_pass: FX pass-through rate
        
    Returns:
        Wages growth rate
    """
    return fx_pass * 0.5


def _get_regulated_schedule() -> float:
    """Get regulated price growth from simple schedule.
    
    For now, returns a stub value. In production, this would be
    based on actual regulated price indices or policy schedules.
    
    Returns:
        Regulated price growth rate
    """
    # Stub: assume regulated prices grow at 0.6 of headline inflation
    return 0.6


def corridor(
    fx_pass: float,
    regulated: Optional[float] = None,
    wages: Optional[float] = None
) -> dict[str, float]:
    """Calculate CPI corridor given inputs.
    
    Args:
        fx_pass: FX pass-through rate (e.g., 0.5 for 50%)
        regulated: Regulated price growth rate. If None, uses schedule.
        wages: Wages growth rate. If None, uses stub (fx_pass * 0.5).
        
    Returns:
        Dictionary with 'low', 'mid', 'high' corridor bounds
    """
    if wages is None:
        wages = _get_wages_stub(fx_pass)
    
    if regulated is None:
        regulated = _get_regulated_schedule()
    
    # Simple corridor model:
    # Low: minimum of components
    # Mid: weighted average (40% fx_pass, 30% wages, 30% regulated)
    # High: maximum of components
    low = min(fx_pass, wages, regulated) * 0.9
    mid = (fx_pass * 0.4) + (wages * 0.3) + (regulated * 0.3)
    high = max(fx_pass, wages, regulated) * 1.1
    
    return {
        "low": low,
        "mid": mid,
        "high": high,
    }


def _get_latest_core_cpi(conn: duckdb.DuckDBPyConnection) -> Optional[tuple[datetime, float]]:
    """Get latest core CPI value from database.
    
    Args:
        conn: DuckDB connection
        
    Returns:
        Tuple of (timestamp, value) or None if not found
    """
    query = """
        SELECT ts, value 
        FROM fact_series 
        WHERE series_id = 'CPI_CORE'
        ORDER BY ts DESC 
        LIMIT 1
    """
    result = conn.execute(query).fetchone()
    if result:
        return result[0], result[1]
    return None


def compute(
    fx_pass: float,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    regulated: Optional[float] = None,
    wages: Optional[float] = None
) -> Optional[CPICorridorResult]:
    """Compute CPI corridors and 3m-annualized core CPI nowcast.
    
    Args:
        fx_pass: FX pass-through rate
        conn: Optional DuckDB connection. If None, creates a new one.
        regulated: Optional regulated price growth rate
        wages: Optional wages growth rate
        
    Returns:
        CPICorridorResult with corridor bounds and nowcast, or None if data unavailable
    """
    should_close = conn is None
    if conn is None:
        conn = connect()
    
    try:
        # Get latest core CPI
        latest_result = _get_latest_core_cpi(conn)
        if not latest_result:
            return None
        
        latest_ts, latest_value = latest_result
        
        # Calculate corridors
        corridor_result = corridor(fx_pass, regulated, wages)
        
        # Calculate 3-month annualized nowcast using mid corridor
        # Assuming monthly frequency, 3m annualized = monthly_rate^3
        monthly_rate = corridor_result["mid"]
        nowcast_3m_annualized = ((1 + monthly_rate) ** 3) - 1
        
        return CPICorridorResult(
            low=corridor_result["low"],
            mid=corridor_result["mid"],
            high=corridor_result["high"],
            nowcast_3m_annualized=nowcast_3m_annualized,
            timestamp=latest_ts,
        )
        
    finally:
        if should_close:
            conn.close()

