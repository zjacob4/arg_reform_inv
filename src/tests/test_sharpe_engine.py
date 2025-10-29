"""Unit tests for Sharpe ratio engine module."""

import pytest
import math

from src.models.sharpe_engine import (
    FXState,
    compute_sharpe,
    sigmoid_position_size,
    compute_hedge_pct,
    compute_allocation,
)


def test_compute_sharpe_basic():
    """Test basic Sharpe ratio calculation."""
    # Test with 15% return, 20% vol, 4.5% risk-free
    sharpe = compute_sharpe(exp_return=0.15, vol=0.20, rf=0.045)
    expected = (0.15 - 0.045) / 0.20
    assert abs(sharpe - expected) < 1e-6


def test_compute_sharpe_zero_volatility():
    """Test Sharpe ratio with zero volatility."""
    sharpe = compute_sharpe(exp_return=0.10, vol=0.0, rf=0.045)
    assert sharpe == 0.0


def test_compute_sharpe_negative_excess_return():
    """Test Sharpe ratio with negative excess return."""
    sharpe = compute_sharpe(exp_return=0.02, vol=0.15, rf=0.045)
    # Should be negative since return < risk-free
    assert sharpe < 0


def test_sigmoid_position_size_positive_sharpe():
    """Test sigmoid position size with positive Sharpe."""
    # Positive Sharpe should give position size > 0.5
    size = sigmoid_position_size(sharpe=1.0, k=2.0)
    assert size > 0.5
    assert size < 1.0
    
    # Higher Sharpe should give larger position
    size_higher = sigmoid_position_size(sharpe=2.0, k=2.0)
    assert size_higher > size


def test_sigmoid_position_size_negative_sharpe():
    """Test sigmoid position size with negative Sharpe."""
    # Negative Sharpe should give position size < 0.5
    size = sigmoid_position_size(sharpe=-1.0, k=2.0)
    assert size < 0.5
    assert size >= 0.0  # Clipped


def test_sigmoid_position_size_zero_sharpe():
    """Test sigmoid position size with zero Sharpe."""
    size = sigmoid_position_size(sharpe=0.0, k=2.0)
    # At zero, sigmoid gives 0.5
    assert abs(size - 0.5) < 1e-6


def test_sigmoid_position_size_clipping():
    """Test that position size is clipped to [0, 1]."""
    # Very high Sharpe
    size_high = sigmoid_position_size(sharpe=100.0, k=2.0)
    assert size_high <= 1.0
    
    # Very negative Sharpe
    size_low = sigmoid_position_size(sharpe=-100.0, k=2.0)
    assert size_low >= 0.0
    assert size_low <= 1.0


def test_compute_hedge_pct_green():
    """Test hedge percentage for GREEN FX state."""
    hedge = compute_hedge_pct(FXState.GREEN)
    assert hedge == 0.0


def test_compute_hedge_pct_yellow():
    """Test hedge percentage for YELLOW FX state."""
    hedge = compute_hedge_pct(FXState.YELLOW)
    assert hedge == 0.5


def test_compute_hedge_pct_red():
    """Test hedge percentage for RED FX state."""
    hedge = compute_hedge_pct(FXState.RED)
    assert hedge == 0.5


def test_compute_hedge_pct_string_input():
    """Test hedge percentage with string input."""
    hedge_green = compute_hedge_pct("GREEN")
    assert hedge_green == 0.0
    
    hedge_yellow = compute_hedge_pct("YELLOW")
    assert hedge_yellow == 0.5


def test_compute_allocation():
    """Test full allocation computation."""
    result = compute_allocation(
        exp_return=0.15,
        vol=0.20,
        rf=0.045,
        fx_state=FXState.RED,
        k=2.0
    )
    
    assert "sharpe" in result
    assert "position_size" in result
    assert "hedge_pct" in result
    
    # Verify values
    expected_sharpe = (0.15 - 0.045) / 0.20
    assert abs(result["sharpe"] - expected_sharpe) < 1e-6
    
    assert 0.0 <= result["position_size"] <= 1.0
    assert result["hedge_pct"] == 0.5


def test_compute_allocation_green_fx_state():
    """Test allocation with GREEN FX state (no hedge)."""
    result = compute_allocation(
        exp_return=0.12,
        vol=0.18,
        rf=0.045,
        fx_state=FXState.GREEN,
    )
    
    assert result["hedge_pct"] == 0.0

