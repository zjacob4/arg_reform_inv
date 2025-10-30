"""Main Streamlit dashboard for Argentina Reform Investment Analysis."""

import streamlit as st
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from src.data.db import connect
from src.features.fx_gap import compute as compute_fx_gap
from src.features.reserves_momentum import compute as compute_reserves_momentum
from src.features.cpi_corridors import compute as compute_cpi_corridor, corridor
from src.features.embi_bands import compute as compute_embi_bands
from src.triggers.gates import evaluate_states
from src.models.sharpe_engine import FXState
import subprocess
import sys


def get_latest_embi_and_trend(conn: "duckdb.DuckDBPyConnection") -> tuple[float, float]:
    """Get latest EMBI level and 30-day trend.
    
    Returns:
        Tuple of (current_level, trend_30d) in basis points
    """
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


def get_latest_core_cpi_3m_ann(conn: "duckdb.DuckDBPyConnection") -> tuple[float, dict] | tuple[None, None]:
    """Get latest core CPI and 3-month annualized rate.
    
    Returns:
        Tuple of (3m_annualized_rate, corridor_dict) or (None, None)
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
        return None, None
    
    latest_ts, latest_value = results[0]
    prev_ts, prev_value = results[1]
    
    # Calculate months between readings
    days_diff = (latest_ts - prev_ts).days
    months = days_diff / 30.0
    
    if months == 0:
        return None, None
    
    # Calculate 3-month annualized rate
    monthly_rate = (latest_value / prev_value) - 1
    annualized_3m = ((1 + monthly_rate) ** (3 / months)) - 1
    
    # Get corridor (using placeholder fx_pass for now)
    # For now, use simple corridor function with placeholder inputs
    corridor_dict = corridor(fx_pass=0.5)  # 50% fx_pass placeholder
    
    return annualized_3m, corridor_dict


def refresh_data():
    """Trigger data refresh."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "src.data.refresh_all"],
            capture_output=True,
            text=True,
            timeout=300
        )
        if result.returncode == 0:
            return True, result.stdout
        else:
            return False, result.stderr
    except Exception as e:
        return False, str(e)


def run_tests():
    """Run test suite."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "src/tests", "-v"],
            capture_output=True,
            text=True,
            timeout=60
        )
        return result.returncode == 0, result.stdout + result.stderr
    except Exception as e:
        return False, str(e)


# Page config
st.set_page_config(
    page_title="Argentina Reform Investment Analysis",
    page_icon="📊",
    layout="wide",
)

# Initialize session state
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = "Never"

# Sidebar
with st.sidebar:
    st.title("📊 Dashboard")
    st.divider()
    
    st.markdown("**Last refresh:**")
    st.text(st.session_state.last_refresh)
    
    st.divider()
    
    if st.button("🔄 Refresh Data", use_container_width=True):
        with st.spinner("Refreshing data..."):
            success, output = refresh_data()
            if success:
                st.session_state.last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Data refreshed successfully!")
                st.code(output, language="text")
            else:
                st.error("Refresh failed!")
                st.code(output, language="text")
    
    if st.button("🧪 Run Tests", use_container_width=True):
        with st.spinner("Running tests..."):
            success, output = run_tests()
            if success:
                st.success("All tests passed!")
            else:
                st.warning("Some tests failed or had errors")
            st.code(output, language="text")

# Main content
st.title("Argentina Reform Investment Analysis")
st.markdown("---")

# Connect to database
try:
    conn = connect()
    
    # Compute features
    fx_gap_result = compute_fx_gap(conn=conn)
    reserves_mom_result = compute_reserves_momentum(conn=conn)
    cpi_3m_ann, cpi_corridor = get_latest_core_cpi_3m_ann(conn)
    embi_level, embi_trend = get_latest_embi_and_trend(conn)
    
    # Close connection after reading
    conn.close()
    
    # Metric cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("### 💱 FX")
        if fx_gap_result:
            # Format source tags
            official_source = "BCRA" if fx_gap_result.used_official == "USDARS_OFFICIAL" else "Bluelytics"
            parallel_source = "Bluelytics"  # Both USDARS_PARALLEL and USDARS_BLUE come from Bluelytics
            
            st.metric(
                label="Official / Parallel",
                value=f"${fx_gap_result.official_rate:.2f} / ${fx_gap_result.parallel_rate:.2f}" if fx_gap_result.parallel_rate else "N/A",
            )
            
            # Show source tags
            st.caption(f"Official: {fx_gap_result.official_rate:.1f} ({official_source})")
            if fx_gap_result.parallel_rate:
                st.caption(f"Parallel: {fx_gap_result.parallel_rate:.1f} ({parallel_source})")
            
            gap_pct = fx_gap_result.value * 100
            st.metric(
                label="Gap",
                value=f"{gap_pct:.2f}%",
                delta=None,
            )
        else:
            st.warning("No FX data")
    
    with col2:
        st.markdown("### 💰 Reserves")
        if reserves_mom_result:
            mom_pct = reserves_mom_result.value * 100
            st.metric(
                label="4W Momentum",
                value=f"{mom_pct:.2f}%",
                delta=f"{mom_pct:+.2f}%" if mom_pct else None,
            )
            if reserves_mom_result.reserves_current:
                st.caption(f"Current: ${reserves_mom_result.reserves_current:.0f}M")
        else:
            st.warning("No reserves data")
    
    with col3:
        st.markdown("### 📈 CPI")
        if cpi_3m_ann is not None:
            cpi_pct = cpi_3m_ann * 100
            st.metric(
                label="Core 3M Annualized",
                value=f"{cpi_pct:.1f}%",
            )
            if cpi_corridor:
                st.caption(f"Corridor: {cpi_corridor['low']*100:.1f}% - {cpi_corridor['high']*100:.1f}%")
        else:
            st.warning("No CPI data")
    
    with col4:
        st.markdown("### 📊 EMBI")
        if embi_level is not None:
            st.metric(
                label="Level",
                value=f"{embi_level:.0f} bps",
            )
            if embi_trend is not None:
                st.metric(
                    label="Δ 30d",
                    value=f"{embi_trend:+.0f} bps",
                    delta=f"{embi_trend:+.0f} bps" if embi_trend != 0 else None,
                )
        else:
            st.warning("No EMBI data")
    
    st.markdown("---")
    
    # Overall state and evaluation
    if all([fx_gap_result, reserves_mom_result, cpi_3m_ann is not None, embi_level is not None, embi_trend is not None]):
        # Prepare inputs for gate evaluation
        evaluation = evaluate_states(
            fx_gap=fx_gap_result.value,
            reserves_mom_4w=reserves_mom_result.value,
            core_cpi_3m_ann=cpi_3m_ann,
            embi_level=embi_level,
            embi_trend_30d=embi_trend,
        )
        
        # Overall state badge
        state = evaluation["overall_state"]
        if state == FXState.GREEN:
            badge = "🟢 GREEN"
            color = "green"
        elif state == FXState.YELLOW:
            badge = "🟡 YELLOW"
            color = "orange"
        else:
            badge = "🔴 RED"
            color = "red"
        
        st.markdown("### Overall State")
        st.markdown(f'<div style="font-size: 24px; color: {color}; font-weight: bold;">{badge}</div>', unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Action bar
        st.markdown("### Action Bar")
        action_col1, action_col2, action_col3 = st.columns([1, 1, 2])
        
        with action_col1:
            st.metric(
                label="Suggested Macro Weight",
                value=f"{evaluation['macro_weight']:.1%}",
            )
        
        with action_col2:
            st.metric(
                label="Hedge %",
                value=f"{evaluation['hedge_pct']:.0%}",
            )
        
        with action_col3:
            st.markdown("**Rationale:**")
            st.info(evaluation["action_note"])
        
        # Dimension states detail
        with st.expander("View Dimension States"):
            dim_states = evaluation["dimension_states"]
            for dim_name, dim_state in dim_states.items():
                if dim_state == FXState.GREEN:
                    state_emoji = "🟢"
                elif dim_state == FXState.YELLOW:
                    state_emoji = "🟡"
                else:
                    state_emoji = "🔴"
                st.markdown(f"- **{dim_name.replace('_', ' ').title()}:** {state_emoji} {dim_state.value}")
    
    else:
        st.warning("⚠️ Incomplete data - cannot compute overall state. Please refresh data.")
        st.info("Missing data for one or more metrics. Click 'Refresh Data' in the sidebar to pull latest data.")

except Exception as e:
    st.error(f"Error loading dashboard: {e}")
    st.exception(e)

