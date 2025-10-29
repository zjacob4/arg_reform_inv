"""Sharpe ratio engine for position sizing and hedging."""

import math
from enum import Enum
from typing import Literal


class FXState(Enum):
    """FX market state classification."""
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    RED = "RED"


def compute_sharpe(
    exp_return: float,
    vol: float,
    rf: float = 0.045
) -> float:
    """Compute Sharpe ratio.
    
    Sharpe = (Expected Return - Risk-free Rate) / Volatility
    
    Args:
        exp_return: Expected annual return (decimal, e.g., 0.15 for 15%)
        vol: Annual volatility/standard deviation (decimal, e.g., 0.20 for 20%)
        rf: Risk-free rate (decimal, default 0.045 for 4.5%)
        
    Returns:
        Sharpe ratio (dimensionless)
    """
    if vol == 0:
        return 0.0
    
    return (exp_return - rf) / vol


def sigmoid_position_size(sharpe: float, k: float = 2.0) -> float:
    """Compute smooth position size using sigmoid function.
    
    Uses sigmoid to create smooth transition between 0 and 1 based on Sharpe ratio.
    Position size increases with higher Sharpe ratios.
    
    Args:
        sharpe: Sharpe ratio
        k: Scaling factor for sigmoid steepness (default 2.0)
        
    Returns:
        Position size between 0 and 1, clipped to [0, 1]
    """
    # Sigmoid: 1 / (1 + exp(-k * sharpe))
    # This maps negative sharpe -> ~0, positive sharpe -> ~1
    raw_size = 1.0 / (1.0 + math.exp(-k * sharpe))
    
    # Clip to [0, 1]
    return max(0.0, min(1.0, raw_size))


def compute_hedge_pct(fx_state: FXState | Literal["GREEN", "YELLOW", "RED"]) -> float:
    """Compute hedge percentage based on FX state.
    
    Args:
        fx_state: FX market state (GREEN, YELLOW, or RED)
        
    Returns:
        Hedge percentage (0.0 for GREEN, 0.5 otherwise)
    """
    if isinstance(fx_state, str):
        fx_state = FXState(fx_state)
    
    if fx_state == FXState.GREEN:
        return 0.0
    else:
        return 0.5


def compute_allocation(
    exp_return: float,
    vol: float,
    rf: float = 0.045,
    fx_state: FXState | Literal["GREEN", "YELLOW", "RED"] = FXState.RED,
    k: float = 2.0
) -> dict[str, float]:
    """Compute full allocation metrics.
    
    Args:
        exp_return: Expected annual return (decimal)
        vol: Annual volatility (decimal)
        rf: Risk-free rate (decimal, default 0.045)
        fx_state: FX market state
        k: Sigmoid scaling factor (default 2.0)
        
    Returns:
        Dictionary with sharpe, position_size, hedge_pct
    """
    sharpe = compute_sharpe(exp_return, vol, rf)
    position_size = sigmoid_position_size(sharpe, k)
    hedge_pct = compute_hedge_pct(fx_state)
    
    return {
        "sharpe": sharpe,
        "position_size": position_size,
        "hedge_pct": hedge_pct,
    }

