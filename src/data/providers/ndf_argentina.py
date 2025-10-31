"""NDF (Non-Deliverable Forward) curve provider for Argentina USD/ARS.

NDF curves represent forward FX rates for tenors like 1M, 3M, 6M, 12M.
Since NDF data is often not publicly available, this provider supports:
1. TradingEconomics (if available) - marked as source="TE"
2. Synthetic construction from interest rate differentials - marked as source="SYNTHETIC"
3. State-dependent spreads tied to CDS/EMBI levels

Day-count convention: ACT/365 (actual days / 365) for all calculations.
"""

import os
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
from dotenv import load_dotenv

import requests
from .base import SeriesProvider, ProviderError

load_dotenv()

# Day-count convention: ACT/365 (explicit)
DAY_COUNT_BASIS = 365.0

# TradingEconomics endpoint (if available)
TE_BASE = "https://api.tradingeconomics.com"
TE_API_KEY = os.getenv("TRADING_ECON_API_KEY", "")

# Map series codes to NDF tenor names
NDF_SERIES_MAP = {
    "NDF_1M": {"tenor": "1M", "days": 30},
    "NDF_3M": {"tenor": "3M", "days": 90},
    "NDF_6M": {"tenor": "6M", "days": 180},
    "NDF_12M": {"tenor": "12M", "days": 365},
}

# Synthetic NDF construction parameters
# Base spreads (basis points) - can be overridden via environment variables
# State-dependent multipliers applied based on CDS/EMBI levels
BASE_SPREAD_BPS = {
    "1M": float(os.getenv("NDF_SYNTH_1M_SPREAD_BPS", "50")),  # 50 bps default
    "3M": float(os.getenv("NDF_SYNTH_3M_SPREAD_BPS", "150")),  # 150 bps default
    "6M": float(os.getenv("NDF_SYNTH_6M_SPREAD_BPS", "300")),  # 300 bps default
    "12M": float(os.getenv("NDF_SYNTH_12M_SPREAD_BPS", "600")),  # 600 bps default
}

# State-dependent spread multipliers
# Multipliers increase spreads in stressed states (higher CDS/EMBI)
# Format: (cds_threshold_bps, embi_threshold_bps, multiplier)
STATE_MULTIPLIERS = [
    # (CDS > threshold, EMBI > threshold, multiplier)
    (1200, 1800, 1.5),  # Stressed: CDS > 1200 or EMBI > 1800 -> 1.5x spread
    (1000, 1600, 1.25),  # Elevated: CDS > 1000 or EMBI > 1600 -> 1.25x spread
    (800, 1400, 1.0),   # Normal: CDS <= 800 and EMBI <= 1400 -> 1.0x spread (base)
]

# Enable state-dependent calibration (can be disabled via env)
USE_STATE_CALIBRATION = os.getenv("NDF_USE_STATE_CALIBRATION", "true").lower() == "true"


def _auth_params():
    """Get TradingEconomics auth parameters."""
    if not TE_API_KEY:
        return {}
    # TE API key format: "user:token"
    return {"c": TE_API_KEY}


def _fetch_te_ndf(tenor: str, start: Optional[str] = None, end: Optional[str] = None) -> List[Tuple[datetime, float]]:
    """Try to fetch NDF data from TradingEconomics (if available)."""
    if not TE_API_KEY:
        raise ProviderError("TradingEconomics API key not configured")
    
    # TradingEconomics endpoint for forward rates
    # Format may vary - adjust based on actual TE API documentation
    endpoint = f"{TE_BASE}/markets/currencyforward/USDARS"
    params = _auth_params().copy()
    if start:
        params["d1"] = start
    if end:
        params["d2"] = end
    
    try:
        r = requests.get(endpoint, params=params, timeout=30)
        if r.status_code != 200:
            raise ProviderError(f"TradingEconomics HTTP {r.status_code}")
        
        data = r.json()
        out: List[Tuple[datetime, float]] = []
        for row in data:
            # Parse TE response format (adjust based on actual structure)
            dt = row.get("Date") or row.get("date")
            # Look for tenor-specific fields or filter by tenor
            val = row.get(tenor) or row.get("Forward") or row.get("Value")
            if not dt or val is None:
                continue
            ts = datetime.fromisoformat(str(dt).replace("Z", ""))
            out.append((ts, float(val)))
        return sorted(out, key=lambda x: x[0])
    except Exception as e:
        raise ProviderError(f"TradingEconomics NDF fetch failed: {e}")


def _get_state_multiplier(cds_bps: Optional[float] = None, embi_bps: Optional[float] = None) -> float:
    """Get state-dependent spread multiplier based on CDS/EMBI levels.
    
    Args:
        cds_bps: Current CDS spread in basis points (optional)
        embi_bps: Current EMBI spread in basis points (optional)
        
    Returns:
        Multiplier for base spreads (1.0 = base, >1.0 = elevated risk)
    """
    if not USE_STATE_CALIBRATION:
        return 1.0
    
    if cds_bps is None and embi_bps is None:
        return 1.0  # No data available, use base
    
    # Find first matching state (most stressed first)
    # Sort by multiplier descending so we check highest risk thresholds first
    for cds_thresh, embi_thresh, multiplier in sorted(STATE_MULTIPLIERS, key=lambda x: x[2], reverse=True):
        cds_trigger = cds_bps is not None and cds_bps >= cds_thresh
        embi_trigger = embi_bps is not None and embi_bps >= embi_thresh
        if cds_trigger or embi_trigger:
            return multiplier
    
    return 1.0  # Default: base multiplier


def _get_latest_cds_embi() -> Tuple[Optional[float], Optional[float]]:
    """Fetch latest CDS and EMBI levels for state calibration.
    
    Returns:
        Tuple of (cds_bps, embi_bps) or (None, None) if unavailable
    """
    try:
        from src.data.db import connect
        
        with connect() as conn:
            # Get latest CDS
            cds_result = conn.execute("""
                SELECT value FROM fact_series 
                WHERE series_id = 'CDS_ARG_5Y_USD'
                ORDER BY ts DESC LIMIT 1
            """).fetchone()
            cds_bps = cds_result[0] if cds_result else None
            
            # Get latest EMBI (prefer local, fallback to synth)
            embi_result = conn.execute("""
                SELECT value FROM fact_series 
                WHERE series_id IN ('EMBI_ARG_LOCAL', 'EMBI_ARG_SYNTH_USD')
                ORDER BY ts DESC LIMIT 1
            """).fetchone()
            embi_bps = embi_result[0] if embi_result else None
            
            return (cds_bps, embi_bps)
    except Exception:
        # If database unavailable or series missing, return None
        return (None, None)


def _construct_synthetic_ndf(
    tenor: str,
    spot_rate: Optional[float] = None,
    ars_rate: Optional[float] = None,
    usd_rate: Optional[float] = None,
    days: int = 90,
    apply_state_calibration: bool = True
) -> Optional[float]:
    """Construct synthetic NDF rate from interest rate differential.
    
    Uses covered interest parity: Forward = Spot * (1 + r_dom * t) / (1 + r_for * t)
    Simplified: Forward ≈ Spot * (1 + (r_dom - r_for) * t)
    Where r_dom is ARS rate, r_for is USD rate, t is time fraction.
    
    Day-count convention: ACT/365 (actual days / 365)
    
    Args:
        tenor: Tenor string (e.g., "3M")
        spot_rate: Current spot USD/ARS rate
        ars_rate: ARS interest rate (annualized, decimal, e.g., 0.50 for 50%)
        usd_rate: USD interest rate (annualized, decimal, e.g., 0.05 for 5%)
        days: Days to maturity
        apply_state_calibration: If True, apply CDS/EMBI-based state multipliers
        
    Returns:
        Synthetic forward rate, or None if inputs missing
    """
    if spot_rate is None:
        return None
    
    # If we have interest rates, use covered interest parity
    if ars_rate is not None and usd_rate is not None:
        time_fraction = days / DAY_COUNT_BASIS  # ACT/365
        rate_diff = ars_rate - usd_rate
        forward_rate = spot_rate * (1 + rate_diff * time_fraction)
        return forward_rate
    
    # Fallback: Use spot + spread approximation
    # This is a simplified approach - real NDF pricing is more complex
    base_spread_bps = BASE_SPREAD_BPS.get(tenor, 150)
    
    # Apply state-dependent multiplier if enabled
    if apply_state_calibration:
        cds_bps, embi_bps = _get_latest_cds_embi()
        multiplier = _get_state_multiplier(cds_bps, embi_bps)
        spread_bps = base_spread_bps * multiplier
    else:
        spread_bps = base_spread_bps
    
    spread_decimal = spread_bps / 10000.0  # Convert bps to decimal
    time_fraction = days / DAY_COUNT_BASIS  # ACT/365
    
    # Approximate forward premium
    forward_premium = spread_decimal * time_fraction
    forward_rate = spot_rate * (1 + forward_premium)
    
    return forward_rate


def _fetch_synthetic_ndf(
    series_code: str,
    start: Optional[str] = None,
    end: Optional[str] = None
) -> List[Tuple[datetime, float]]:
    """Construct synthetic NDF series from spot rate and assumptions.
    
    This requires fetching the spot rate first, then applying forward pricing logic.
    Fetches spot rate directly from BCRA provider to avoid circular dependency.
    """
    # Import BCRA provider directly to avoid circular dependency with router
    from .bcra import BCRAProvider
    
    # Get spot rate history directly from BCRA
    bcra = BCRAProvider()
    try:
        spot_rows = bcra.fetch_timeseries("USDARS_OFFICIAL", start=start, end=end)
        if not spot_rows:
            raise ProviderError("Cannot construct synthetic NDF: no spot rate data available")
    except Exception as e:
        raise ProviderError(f"Cannot construct synthetic NDF: failed to fetch spot rate: {e}")
    
    # Get tenor info
    tenor_info = NDF_SERIES_MAP.get(series_code)
    if not tenor_info:
        raise ProviderError(f"Unknown NDF series: {series_code}")
    
    tenor = tenor_info["tenor"]
    days = tenor_info["days"]
    
    # Construct forward rates for each spot date
    out: List[Tuple[datetime, float]] = []
    for ts, spot_rate in spot_rows:
        forward_rate = _construct_synthetic_ndf(tenor, spot_rate=spot_rate, days=days)
        if forward_rate is not None:
            out.append((ts, forward_rate))
    
    return out


class NDFArgentinaProvider(SeriesProvider):
    """NDF curve provider for Argentina USD/ARS forward rates.
    
    Supports series codes:
    - NDF_1M: 1-month forward
    - NDF_3M: 3-month forward
    - NDF_6M: 6-month forward
    - NDF_12M: 12-month forward
    
    Source tagging:
    - TradingEconomics data: marked as source="TE" in series registry
    - Synthetic data: marked as source="SYNTHETIC" in series registry
    
    Synthetic data characteristics:
    - Derived from spot rates → risk of double-counting FX signal
    - Down-weight in composite signals (suggest weight=0.25 vs 1.0 for real NDF)
    - State-dependent spreads tied to CDS/EMBI levels (if enabled)
    - Day-count: ACT/365
    """
    
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        """Fetch NDF time series.
        
        Note: Returns synthetic data (constructed from spot) unless TradingEconomics
        API key is configured. Synthetic data is marked with source="SYNTHETIC"
        in the series registry and should be down-weighted in composite signals.
        
        Args:
            series_code: Must be one of NDF_1M, NDF_3M, NDF_6M, NDF_12M
            start: Start date in YYYY-MM-DD format
            end: End date in YYYY-MM-DD format
            
        Returns:
            List of (datetime, forward_rate) tuples
            
        Raises:
            ProviderError: If series_code unknown or data unavailable
        """
        if series_code not in NDF_SERIES_MAP:
            raise ProviderError(f"NDFArgentinaProvider: unknown series_code {series_code}")
        
        tenor_info = NDF_SERIES_MAP[series_code]
        tenor = tenor_info["tenor"]
        
        # Try TradingEconomics first (if API key available)
        # Real NDF data would be marked as source="TE" in registry
        if TE_API_KEY:
            try:
                return _fetch_te_ndf(tenor, start=start, end=end)
            except Exception:
                # Fall through to synthetic construction
                pass
        
        # Fallback to synthetic construction
        # This is marked as source="SYNTHETIC" in registry
        # Should be down-weighted (e.g., 0.25) to avoid double-counting FX signal
        return _fetch_synthetic_ndf(series_code, start=start, end=end)

