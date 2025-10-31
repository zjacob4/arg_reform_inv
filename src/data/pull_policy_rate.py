"""Pull policy rate (LELIQ) data from BCRA and compute real rates.

Real rate = policy_rate - cpi_nowcast_annualized
"""

from datetime import datetime, timedelta
from typing import Optional
from .db import connect, upsert_timeseries, upsert_series_meta
from .providers.bcra_policy import BCRAPolicyProvider
from .providers.base import ProviderError
from src.config.series_registry import REGISTRY, SeriesSpec

# Series IDs
LELIQ_SERIES_ID = "LELIQ_RATE"
POLICY_RATE_SERIES_ID = "POLICY_RATE"  # Canonical policy rate (LELIQ as primary)
REAL_RATE_SERIES_ID = "REAL_RATE"

provider = BCRAPolicyProvider()


def _get_cpi_nowcast_annualized(conn) -> Optional[float]:
    """Get latest CPI nowcast (3m annualized) for real rate calculation.
    
    Args:
        conn: DuckDB connection
        
    Returns:
        CPI nowcast as annualized percentage (e.g., 25.0 for 25%), or None if unavailable
    """
    # Try to get from CPI corridor computation
    try:
        from src.features.cpi_corridors import compute as compute_cpi_corridor
        
        # Compute CPI corridor with default FX pass-through
        # In production, you'd want to use actual FX gap
        fx_pass = 0.5  # Default assumption - can be made dynamic
        corridor_result = compute_cpi_corridor(fx_pass, conn=conn)
        
        if corridor_result:
            # nowcast_3m_annualized is already annualized
            # Convert from decimal to percentage if needed
            nowcast = corridor_result.nowcast_3m_annualized
            # If already in percentage form (e.g., 0.25 = 25%), convert to percentage
            if nowcast < 1.0:
                return nowcast * 100.0
            return nowcast
        
    except Exception as e:
        # Fallback: try to get from CPI_NATIONAL_INDEX and compute YoY
        try:
            # Get latest two CPI index values to compute YoY
            query = """
                SELECT ts, value 
                FROM fact_series 
                WHERE series_id = 'CPI_NATIONAL_INDEX'
                ORDER BY ts DESC 
                LIMIT 2
            """
            results = conn.execute(query).fetchall()
            if len(results) >= 2:
                latest_ts, latest_val = results[0]
                prev_ts, prev_val = results[1]
                
                # Calculate annualized rate
                # Assuming monthly data
                months_diff = (latest_ts.year - prev_ts.year) * 12 + (latest_ts.month - prev_ts.month)
                if months_diff > 0:
                    monthly_rate = (latest_val / prev_val) - 1.0
                    annualized = ((1 + monthly_rate) ** (12 / months_diff)) - 1.0
                    return annualized * 100.0  # Convert to percentage
        except Exception:
            pass
    
    return None


def pull_leliq_rate(start: Optional[str] = None, end: Optional[str] = None) -> int:
    """Pull LELIQ rate from BCRA and store as POLICY_RATE.
    
    Args:
        start: Start date in YYYY-MM-DD format (default: 2 years ago)
        end: End date in YYYY-MM-DD format (default: today)
        
    Returns:
        Number of records inserted
    """
    if start is None:
        start = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    
    print(f"Policy Rate Pull: Starting refresh for {LELIQ_SERIES_ID}")
    print(f"  Fetching from BCRA (start={start}, end={end})...")
    
    try:
        rows = provider.fetch_timeseries("LELIQ_RATE", start=start, end=end)
        if not rows:
            print(f"  WARNING: No LELIQ data returned from BCRA")
            return 0
        
        print(f"  Fetched {len(rows)} LELIQ data point(s)")
        if rows:
            first_date = rows[0][0].date().isoformat()
            last_date = rows[-1][0].date().isoformat()
            if first_date == last_date:
                print(f"  Date: {first_date}, Rate: {rows[0][1]:.2f}%")
            else:
                print(f"  Date range: {first_date} to {last_date}")
                print(f"  Latest rate: {rows[-1][1]:.2f}% (on {last_date})")
        
        with connect() as conn:
            # Register series metadata
            if POLICY_RATE_SERIES_ID not in REGISTRY:
                # Add to registry if not present
                spec = SeriesSpec(
                    name=POLICY_RATE_SERIES_ID,
                    code=POLICY_RATE_SERIES_ID,
                    freq="D",
                    source="BCRA_LELIQ",
                    units="Annualized percentage"
                )
                upsert_series_meta(conn, [spec])
                print(f"  Registered series: {spec.name} ({spec.source})")
            else:
                spec = REGISTRY[POLICY_RATE_SERIES_ID]
                upsert_series_meta(conn, [spec])
            
            # Store policy rate data
            upsert_timeseries(conn, POLICY_RATE_SERIES_ID, rows)
            print(f"  ✓ Stored {len(rows)} record(s) for {POLICY_RATE_SERIES_ID}")
        
        return len(rows)
        
    except ProviderError as e:
        print(f"  ERROR: Provider error: {e}")
        return 0
    except Exception as e:
        print(f"  ERROR: Unexpected error: {type(e).__name__}: {e}")
        return 0


def compute_real_rate(conn, policy_date: datetime, policy_rate: float, cpi_nowcast: Optional[float]) -> Optional[float]:
    """Compute real rate = policy_rate - cpi_nowcast_annualized.
    
    Args:
        conn: DuckDB connection
        policy_date: Date of policy rate observation
        policy_rate: Policy rate in percentage (e.g., 50.0 for 50%)
        cpi_nowcast: CPI nowcast in percentage (e.g., 25.0 for 25%)
        
    Returns:
        Real rate in percentage, or None if CPI nowcast unavailable
    """
    if cpi_nowcast is None:
        return None
    
    real_rate = policy_rate - cpi_nowcast
    return real_rate


def pull_real_rate(start: Optional[str] = None, end: Optional[str] = None) -> int:
    """Compute and store real rate series.
    
    Real rate = policy_rate - cpi_nowcast_annualized
    Updates when CPI nowcast refreshes.
    
    Args:
        start: Start date in YYYY-MM-DD format (default: 2 years ago)
        end: End date in YYYY-MM-DD format (default: today)
        
    Returns:
        Number of records inserted/updated
    """
    if start is None:
        start = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    
    print(f"  Computing real rate series...")
    
    with connect() as conn:
        # Get all policy rate data
        policy_query = f"""
            SELECT ts, value 
            FROM fact_series 
            WHERE series_id = '{POLICY_RATE_SERIES_ID}'
            AND ts >= '{start}'
            ORDER BY ts ASC
        """
        if end:
            policy_query = policy_query.replace("ORDER BY ts ASC", f"AND ts <= '{end}' ORDER BY ts ASC")
        
        policy_rows = conn.execute(policy_query).fetchall()
        
        if not policy_rows:
            print(f"  WARNING: No policy rate data available for real rate computation")
            return 0
        
        # Compute real rate for each policy rate observation
        real_rate_rows = []
        for ts, policy_rate in policy_rows:
            # Get CPI nowcast (recompute for each date to get most recent available)
            cpi_nowcast = _get_cpi_nowcast_annualized(conn)
            if cpi_nowcast is not None:
                real_rate = compute_real_rate(conn, ts, policy_rate, cpi_nowcast)
                if real_rate is not None:
                    real_rate_rows.append((ts, real_rate))
        
        if not real_rate_rows:
            print(f"  WARNING: Could not compute real rates (CPI nowcast unavailable)")
            return 0
        
        # Register real rate series
        if REAL_RATE_SERIES_ID not in REGISTRY:
            spec = SeriesSpec(
                name=REAL_RATE_SERIES_ID,
                code=REAL_RATE_SERIES_ID,
                freq="D",
                source="DERIVED",
                units="Annualized percentage (policy_rate - cpi_nowcast)"
            )
            upsert_series_meta(conn, [spec])
            print(f"  Registered series: {spec.name}")
        else:
            spec = REGISTRY[REAL_RATE_SERIES_ID]
            upsert_series_meta(conn, [spec])
        
        # Store real rate data
        upsert_timeseries(conn, REAL_RATE_SERIES_ID, real_rate_rows)
        print(f"  ✓ Stored {len(real_rate_rows)} record(s) for {REAL_RATE_SERIES_ID}")
        if real_rate_rows:
            latest = real_rate_rows[-1]
            print(f"  Latest real rate: {latest[1]:.2f}% (policy={policy_rows[-1][1]:.2f}%, cpi_nowcast={cpi_nowcast:.2f}%)")
        
        return len(real_rate_rows)


def refresh_policy_rate(start: Optional[str] = None, end: Optional[str] = None) -> int:
    """Refresh policy rate and real rate series.
    
    Args:
        start: Start date in YYYY-MM-DD format (default: 2 years ago)
        end: End date in YYYY-MM-DD format (default: today)
        
    Returns:
        Total number of records inserted
    """
    total = 0
    
    # Pull LELIQ policy rate
    count = pull_leliq_rate(start=start, end=end)
    total += count
    
    if count > 0:
        # Compute real rate
        real_count = pull_real_rate(start=start, end=end)
        total += real_count
    
    print(f"✓ Policy rate refresh completed: {total} total record(s)")
    return total


def main():
    """CLI entry point."""
    refresh_policy_rate()


if __name__ == "__main__":
    main()

