"""Watcher script to monitor state changes and send alerts."""

import time
from datetime import datetime

import schedule

from ..data.db import connect
from ..features.fx_gap import compute as compute_fx_gap
from ..features.reserves_momentum import compute as compute_reserves_momentum
from ..triggers.gates import evaluate_states
from ..models.sharpe_engine import FXState
from .alerts import send_alert


def get_last_saved_state(conn) -> str | None:
    """Get the last saved overall state from DuckDB.
    
    Args:
        conn: DuckDB connection
        
    Returns:
        Last saved state string or None if not found
    """
    # Create state tracking table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS state_tracking (
            key VARCHAR PRIMARY KEY,
            value VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    
    result = conn.execute("""
        SELECT value 
        FROM state_tracking 
        WHERE key = 'last_overall_state'
        ORDER BY updated_at DESC 
        LIMIT 1
    """).fetchone()
    
    if result:
        return result[0]
    return None


def save_state(conn, state: str) -> None:
    """Save the current overall state to DuckDB.
    
    Args:
        conn: DuckDB connection
        state: Current overall state
    """
    conn.execute("""
        INSERT OR REPLACE INTO state_tracking (key, value, updated_at)
        VALUES ('last_overall_state', ?, ?)
    """, [state, datetime.now()])
    conn.commit()


def get_latest_embi_and_trend(conn) -> tuple[float | None, float | None]:
    """Get latest EMBI level and 30-day trend.
    
    Returns:
        Tuple of (current_level, trend_30d) in basis points
    """
    from datetime import timedelta
    
    # Get latest EMBI
    latest_query = """
        SELECT ts, value 
        FROM fact_series 
        WHERE series_id = 'EMBI_AR'
        ORDER BY ts DESC 
        LIMIT 1
    """
    latest_result = conn.execute(latest_query).fetchone()
    
    if not latest_result:
        return None, None
    
    latest_ts, latest_value = latest_result
    
    # Get value 30 days ago
    date_30d_ago = latest_ts - timedelta(days=30)
    trend_query = """
        SELECT value 
        FROM fact_series 
        WHERE series_id = 'EMBI_AR'
            AND ts <= ?
        ORDER BY ts DESC 
        LIMIT 1
    """
    trend_result = conn.execute(trend_query, [date_30d_ago]).fetchone()
    
    if trend_result:
        value_30d_ago = trend_result[0]
        trend = latest_value - value_30d_ago
    else:
        trend = None
    
    return latest_value, trend


def get_latest_core_cpi_3m_ann(conn) -> float | None:
    """Get latest core CPI 3-month annualized rate.
    
    Returns:
        3-month annualized rate or None
    """
    # Get latest two CPI values for 3-month calculation
    cpi_query = """
        SELECT ts, value 
        FROM fact_series 
        WHERE series_id = 'CPI_CORE'
        ORDER BY ts DESC 
        LIMIT 2
    """
    results = conn.execute(cpi_query).fetchall()
    
    if len(results) < 2:
        return None
    
    latest_ts, latest_value = results[0]
    prev_ts, prev_value = results[1]
    
    # Calculate months between readings
    days_diff = (latest_ts - prev_ts).days
    months = days_diff / 30.0
    
    if months == 0:
        return None
    
    # Calculate 3-month annualized rate
    monthly_rate = (latest_value / prev_value) - 1
    annualized_3m = ((1 + monthly_rate) ** (3 / months)) - 1
    
    return annualized_3m


def check_and_alert() -> None:
    """Check current state against last saved state and alert if changed."""
    try:
        conn = connect()
        
        try:
            # Compute current state
            fx_gap_result = compute_fx_gap(conn=conn)
            reserves_mom_result = compute_reserves_momentum(conn=conn)
            cpi_3m_ann = get_latest_core_cpi_3m_ann(conn)
            embi_level, embi_trend = get_latest_embi_and_trend(conn)
            
            # Check if we have all required data
            if not all([fx_gap_result, reserves_mom_result, cpi_3m_ann is not None, 
                       embi_level is not None, embi_trend is not None]):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Insufficient data for state evaluation")
                return
            
            # Evaluate current state
            evaluation = evaluate_states(
                fx_gap=fx_gap_result.value,
                reserves_mom_4w=reserves_mom_result.value,
                core_cpi_3m_ann=cpi_3m_ann,
                embi_level=embi_level,
                embi_trend_30d=embi_trend,
            )
            
            current_state = evaluation["overall_state"].value
            
            # Get last saved state
            last_state = get_last_saved_state(conn)
            
            if last_state is None:
                # First time, just save the state
                save_state(conn, current_state)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Initial state saved: {current_state}")
            elif last_state != current_state:
                # State changed - send alert
                alert_msg = (
                    f"State change detected: {last_state} → {current_state}. "
                    f"Macro weight: {evaluation['macro_weight']:.1%}, "
                    f"Hedge: {evaluation['hedge_pct']:.0%}. "
                    f"Action: {evaluation['action_note']}"
                )
                send_alert(alert_msg)
                
                # Save new state
                save_state(conn, current_state)
            else:
                # State unchanged
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] State check: {current_state} (unchanged)")
        
        finally:
            conn.close()
            
    except Exception as e:
        error_msg = f"Error during state check: {e}"
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {error_msg}")
        send_alert(error_msg)


def main():
    """Main watcher loop - checks state every 10 minutes."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting state watcher (checks every 10 minutes)")
    print("Press Ctrl+C to stop")
    
    # Schedule checks every 10 minutes
    schedule.every(10).minutes.do(check_and_alert)
    
    # Run initial check
    check_and_alert()
    
    # Run scheduled checks
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute for pending jobs
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Watcher stopped")


if __name__ == "__main__":
    main()

