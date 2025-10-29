"""Unit tests for FX reserve identity module."""

import pytest

from src.models.fx_reserve_identity import (
    breakeven_deval,
    uncovered_parity_stub,
)


def test_uncovered_parity_stub():
    """Test uncovered parity stub calculation."""
    # Test with 10% annual rate differential
    rates_diff = 0.10
    result = uncovered_parity_stub(rates_diff, days=90)
    
    # Should give approximately 90/365 * 0.10
    expected = 0.10 * (90 / 365.0)
    assert abs(result - expected) < 1e-6
    
    # Test with 180 days
    result_180 = uncovered_parity_stub(rates_diff, days=180)
    expected_180 = 0.10 * (180 / 365.0)
    assert abs(result_180 - expected_180) < 1e-6


def test_breakeven_deval_with_ndf_curve():
    """Test breakeven devaluation calculation with NDF curve."""
    # Test with NDF 3-month curve
    ndf_curve = {"3m": 100.0}  # 100 basis points premium
    result = breakeven_deval(ndf_curve=ndf_curve, rates_diff=0.05)
    
    # Should annualize the 3-month premium
    # 100 basis points = 1.0%, over 3 months -> ~4.06% annualized
    expected = 1.0 * (365.0 / 90.0) * 100.0
    assert abs(result - expected) < 0.1  # Allow small tolerance


def test_breakeven_deval_without_ndf_curve():
    """Test breakeven devaluation using uncovered parity stub."""
    # Test fallback to uncovered parity
    rates_diff = 0.15  # 15% annual rate differential
    result = breakeven_deval(ndf_curve=None, rates_diff=rates_diff)
    
    # Should use uncovered parity stub: 15% * (90/365) * 100
    expected = 0.15 * (90 / 365.0) * 100.0
    assert abs(result - expected) < 0.01


def test_breakeven_deval_zero_rates_diff():
    """Test breakeven devaluation with zero rate differential."""
    result = breakeven_deval(ndf_curve=None, rates_diff=0.0)
    assert result == 0.0


def test_breakeven_deval_negative_rates_diff():
    """Test breakeven devaluation with negative rate differential."""
    rates_diff = -0.05  # Foreign rate higher than domestic
    result = breakeven_deval(ndf_curve=None, rates_diff=rates_diff)
    
    # Should give negative (appreciation expected)
    expected = -0.05 * (90 / 365.0) * 100.0
    assert abs(result - expected) < 0.01
    assert result < 0

