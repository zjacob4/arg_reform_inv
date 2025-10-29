"""Gate evaluation: multi-dimensional state assessment and position sizing."""

from typing import Dict

from ..models.sharpe_engine import FXState, compute_allocation


def evaluate_fx_gap_state(fx_gap: float) -> FXState:
    """Evaluate FX gap state.
    
    Args:
        fx_gap: FX gap ratio (e.g., 0.15 for 15%)
        
    Returns:
        FX state for gap dimension
    """
    if fx_gap < 0.15:
        return FXState.GREEN
    elif fx_gap < 0.25:  # Soft band
        return FXState.YELLOW
    else:
        return FXState.RED


def evaluate_reserves_momentum_state(reserves_mom_4w: float) -> FXState:
    """Evaluate reserves momentum state.
    
    Args:
        reserves_mom_4w: 4-week reserves momentum (decimal, e.g., 0.03 for 3%)
        
    Returns:
        FX state for reserves dimension
    """
    if reserves_mom_4w > 0.03:
        return FXState.GREEN
    elif reserves_mom_4w > 0.0:  # Soft band: positive but low
        return FXState.YELLOW
    else:
        return FXState.RED


def evaluate_cpi_state(core_cpi_3m_ann: float) -> FXState:
    """Evaluate CPI state.
    
    Args:
        core_cpi_3m_ann: 3-month annualized core CPI (decimal, e.g., 0.25 for 25%)
        
    Returns:
        FX state for CPI dimension
    """
    if core_cpi_3m_ann < 0.25:
        return FXState.GREEN
    elif core_cpi_3m_ann < 0.35:  # Soft band
        return FXState.YELLOW
    else:
        return FXState.RED


def evaluate_embi_state(embi_level: float, embi_trend_30d: float) -> FXState:
    """Evaluate EMBI state.
    
    Args:
        embi_level: Current EMBI level in basis points
        embi_trend_30d: 30-day EMBI trend in basis points
        
    Returns:
        FX state for EMBI dimension
    """
    # GREEN: embi < 1400 OR trend < -300
    if embi_level < 1400 or embi_trend_30d < -300:
        return FXState.GREEN
    # YELLOW: soft bands
    elif embi_level < 1600 or embi_trend_30d < -100:
        return FXState.YELLOW
    else:
        return FXState.RED


def evaluate_overall_state(
    fx_gap: float,
    reserves_mom_4w: float,
    core_cpi_3m_ann: float,
    embi_level: float,
    embi_trend_30d: float
) -> FXState:
    """Evaluate overall state based on all dimensions.
    
    GREEN: (gap<0.15 and mom>0.03) and (core<0.25) and (embi<1400 or trend<-300)
    YELLOW: within soft bands (not GREEN, not all RED)
    RED: otherwise
    
    Args:
        fx_gap: FX gap ratio
        reserves_mom_4w: 4-week reserves momentum
        core_cpi_3m_ann: 3-month annualized core CPI
        embi_level: Current EMBI level in bps
        embi_trend_30d: 30-day EMBI trend in bps
        
    Returns:
        Overall FX state
    """
    # GREEN condition
    gap_ok = fx_gap < 0.15 and reserves_mom_4w > 0.03
    cpi_ok = core_cpi_3m_ann < 0.25
    embi_ok = embi_level < 1400 or embi_trend_30d < -300
    
    if gap_ok and cpi_ok and embi_ok:
        return FXState.GREEN
    
    # YELLOW: if not GREEN and not all RED
    states = [
        evaluate_fx_gap_state(fx_gap),
        evaluate_reserves_momentum_state(reserves_mom_4w),
        evaluate_cpi_state(core_cpi_3m_ann),
        evaluate_embi_state(embi_level, embi_trend_30d),
    ]
    
    if all(s == FXState.RED for s in states):
        return FXState.RED
    
    return FXState.YELLOW


def generate_action_note(
    overall_state: FXState,
    dimension_states: Dict[str, FXState],
    macro_weight: float
) -> str:
    """Generate textual action note based on state assessment.
    
    Args:
        overall_state: Overall FX state
        dimension_states: Dictionary of dimension-specific states
        macro_weight: Suggested macro position weight
        
    Returns:
        Textual action note
    """
    if overall_state == FXState.GREEN:
        base_note = "GREEN: Favorable conditions across key dimensions. "
        if macro_weight > 0.7:
            return base_note + "High conviction position recommended."
        elif macro_weight > 0.4:
            return base_note + "Moderate position size recommended."
        else:
            return base_note + "Conservative position size despite favorable conditions."
    
    elif overall_state == FXState.YELLOW:
        red_dims = [k for k, v in dimension_states.items() if v == FXState.RED]
        if red_dims:
            return f"YELLOW: Monitor closely. Risk areas: {', '.join(red_dims)}. Position size: {macro_weight:.1%}."
        else:
            return f"YELLOW: Mixed signals. Cautious positioning recommended (weight: {macro_weight:.1%})."
    
    else:  # RED
        red_dims = [k for k, v in dimension_states.items() if v == FXState.RED]
        return f"RED: Elevated risks across {len(red_dims)} dimension(s). Minimal or no position recommended. Risk areas: {', '.join(red_dims)}."


def evaluate_states(
    fx_gap: float,
    reserves_mom_4w: float,
    core_cpi_3m_ann: float,
    embi_level: float,
    embi_trend_30d: float,
    exp_return: float = 0.12,
    vol: float = 0.18,
    rf: float = 0.045
) -> Dict:
    """Evaluate states across all dimensions and compute suggested position.
    
    Args:
        fx_gap: FX gap ratio
        reserves_mom_4w: 4-week reserves momentum (decimal)
        core_cpi_3m_ann: 3-month annualized core CPI (decimal)
        embi_level: Current EMBI level in basis points
        embi_trend_30d: 30-day EMBI trend in basis points
        exp_return: Expected return for Sharpe calculation (default 0.12)
        vol: Volatility for Sharpe calculation (default 0.18)
        rf: Risk-free rate (default 0.045)
        
    Returns:
        Dictionary with:
        - dimension_states: Dict of per-dimension states
        - overall_state: Overall FX state
        - macro_weight: Suggested position weight (from Sharpe engine)
        - action_note: Textual action recommendation
    """
    # Evaluate per-dimension states
    gap_state = evaluate_fx_gap_state(fx_gap)
    reserves_state = evaluate_reserves_momentum_state(reserves_mom_4w)
    cpi_state = evaluate_cpi_state(core_cpi_3m_ann)
    embi_state = evaluate_embi_state(embi_level, embi_trend_30d)
    
    dimension_states = {
        "fx_gap": gap_state,
        "reserves_momentum": reserves_state,
        "core_cpi": cpi_state,
        "embi": embi_state,
    }
    
    # Evaluate overall state
    overall_state = evaluate_overall_state(
        fx_gap, reserves_mom_4w, core_cpi_3m_ann, embi_level, embi_trend_30d
    )
    
    # Compute suggested position using Sharpe engine
    allocation = compute_allocation(
        exp_return=exp_return,
        vol=vol,
        rf=rf,
        fx_state=overall_state,
    )
    macro_weight = allocation["position_size"]
    
    # Generate action note
    action_note = generate_action_note(overall_state, dimension_states, macro_weight)
    
    return {
        "dimension_states": dimension_states,
        "overall_state": overall_state,
        "macro_weight": macro_weight,
        "action_note": action_note,
        "sharpe": allocation["sharpe"],
        "hedge_pct": allocation["hedge_pct"],
    }

