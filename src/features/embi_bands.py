"""EMBI bands feature: fair-value ranges based on policy score."""

from datetime import datetime
from typing import Optional

import duckdb
from pydantic import BaseModel

from ..data.db import connect


class EMBIBandResult(BaseModel):
    """EMBI band computation result."""
    
    low: float  # Lower fair-value band in bps
    mid: float  # Mid fair-value band in bps
    high: float  # Upper fair-value band in bps
    policy_score: float
    current_embi: Optional[float] = None
    timestamp: datetime


def _policy_score_to_bands(score: float) -> dict[str, float]:
    """Map policy score (0-2) to fair-value EMBI bands.
    
    Score interpretation:
    - 0.0: Worst case (worst policies) -> 1800 bps
    - 1.0: Neutral/baseline -> 1400 bps
    - 2.0: Best case (best policies) -> 800 bps
    
    Args:
        score: Policy score between 0 and 2
        
    Returns:
        Dictionary with 'low', 'mid', 'high' bands in basis points
    """
    # Clamp score to 0-2 range
    score = max(0.0, min(2.0, score))
    
    # Linear interpolation between bands
    if score <= 1.0:
        # Between worst (1800) and neutral (1400)
        mid = 1800 - (score * 400)
    else:
        # Between neutral (1400) and best (800)
        mid = 1400 - ((score - 1.0) * 600)
    
    # Define bands as +/- 20% around mid
    low = mid * 0.8
    high = mid * 1.2
    
    return {
        "low": low,
        "mid": mid,
        "high": high,
    }


def compute(
    policy_score: float,
    conn: Optional[duckdb.DuckDBPyConnection] = None
) -> Optional[EMBIBandResult]:
    """Compute EMBI fair-value bands based on policy score.
    
    Args:
        policy_score: Policy score between 0 and 2
        conn: Optional DuckDB connection. If None, creates a new one.
        
    Returns:
        EMBIBandResult with bands and current EMBI, or None if data unavailable
    """
    should_close = conn is None
    if conn is None:
        conn = connect()
    
    try:
        # Get latest EMBI value
        embi_query = """
            SELECT ts, value 
            FROM fact_series 
            WHERE series_id = 'EMBI_AR'
            ORDER BY ts DESC 
            LIMIT 1
        """
        embi_result = conn.execute(embi_query).fetchone()
        
        current_embi = None
        latest_ts = datetime.now()
        
        if embi_result:
            latest_ts, current_embi = embi_result
        
        # Calculate bands from policy score
        bands = _policy_score_to_bands(policy_score)
        
        return EMBIBandResult(
            low=bands["low"],
            mid=bands["mid"],
            high=bands["high"],
            policy_score=policy_score,
            current_embi=current_embi,
            timestamp=latest_ts,
        )
        
    finally:
        if should_close:
            conn.close()

