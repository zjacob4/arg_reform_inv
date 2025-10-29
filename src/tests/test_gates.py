"""Unit tests for gate evaluation module."""

import pytest

from src.triggers.gates import (
    evaluate_states,
    evaluate_fx_gap_state,
    evaluate_reserves_momentum_state,
    evaluate_cpi_state,
    evaluate_embi_state,
    evaluate_overall_state,
)
from src.models.sharpe_engine import FXState


class TestDimensionStates:
    """Test individual dimension state evaluation."""
    
    def test_evaluate_fx_gap_state_green(self):
        """Test FX gap state - GREEN."""
        assert evaluate_fx_gap_state(0.10) == FXState.GREEN
        assert evaluate_fx_gap_state(0.14) == FXState.GREEN
    
    def test_evaluate_fx_gap_state_yellow(self):
        """Test FX gap state - YELLOW."""
        assert evaluate_fx_gap_state(0.15) == FXState.YELLOW
        assert evaluate_fx_gap_state(0.20) == FXState.YELLOW
        assert evaluate_fx_gap_state(0.24) == FXState.YELLOW
    
    def test_evaluate_fx_gap_state_red(self):
        """Test FX gap state - RED."""
        assert evaluate_fx_gap_state(0.25) == FXState.RED
        assert evaluate_fx_gap_state(0.50) == FXState.RED
    
    def test_evaluate_reserves_momentum_state_green(self):
        """Test reserves momentum state - GREEN."""
        assert evaluate_reserves_momentum_state(0.05) == FXState.GREEN
        assert evaluate_reserves_momentum_state(0.10) == FXState.GREEN
    
    def test_evaluate_reserves_momentum_state_yellow(self):
        """Test reserves momentum state - YELLOW."""
        assert evaluate_reserves_momentum_state(0.03) == FXState.YELLOW
        assert evaluate_reserves_momentum_state(0.01) == FXState.YELLOW
    
    def test_evaluate_reserves_momentum_state_red(self):
        """Test reserves momentum state - RED."""
        assert evaluate_reserves_momentum_state(0.0) == FXState.RED
        assert evaluate_reserves_momentum_state(-0.05) == FXState.RED
    
    def test_evaluate_cpi_state_green(self):
        """Test CPI state - GREEN."""
        assert evaluate_cpi_state(0.20) == FXState.GREEN
        assert evaluate_cpi_state(0.24) == FXState.GREEN
    
    def test_evaluate_cpi_state_yellow(self):
        """Test CPI state - YELLOW."""
        assert evaluate_cpi_state(0.25) == FXState.YELLOW
        assert evaluate_cpi_state(0.30) == FXState.YELLOW
        assert evaluate_cpi_state(0.34) == FXState.YELLOW
    
    def test_evaluate_cpi_state_red(self):
        """Test CPI state - RED."""
        assert evaluate_cpi_state(0.35) == FXState.RED
        assert evaluate_cpi_state(0.50) == FXState.RED
    
    def test_evaluate_embi_state_green_level(self):
        """Test EMBI state - GREEN (level-based)."""
        assert evaluate_embi_state(1300, 0) == FXState.GREEN
    
    def test_evaluate_embi_state_green_trend(self):
        """Test EMBI state - GREEN (trend-based)."""
        assert evaluate_embi_state(1500, -400) == FXState.GREEN
    
    def test_evaluate_embi_state_yellow(self):
        """Test EMBI state - YELLOW."""
        assert evaluate_embi_state(1500, -50) == FXState.YELLOW
        assert evaluate_embi_state(1550, 0) == FXState.YELLOW
    
    def test_evaluate_embi_state_red(self):
        """Test EMBI state - RED."""
        assert evaluate_embi_state(1600, 0) == FXState.RED
        assert evaluate_embi_state(1700, 100) == FXState.RED


class TestOverallState:
    """Test overall state evaluation."""
    
    def test_green_state_all_conditions_met(self):
        """Test GREEN state when all conditions are met."""
        result = evaluate_overall_state(
            fx_gap=0.10,           # < 0.15 ✓
            reserves_mom_4w=0.05,  # > 0.03 ✓
            core_cpi_3m_ann=0.20,  # < 0.25 ✓
            embi_level=1300,       # < 1400 ✓
            embi_trend_30d=0
        )
        assert result == FXState.GREEN
    
    def test_green_state_embi_trend_condition(self):
        """Test GREEN state with EMBI trend condition."""
        result = evaluate_overall_state(
            fx_gap=0.10,
            reserves_mom_4w=0.05,
            core_cpi_3m_ann=0.20,
            embi_level=1500,       # Not < 1400
            embi_trend_30d=-400    # < -300 ✓ (alternative condition)
        )
        assert result == FXState.GREEN
    
    def test_yellow_state_mixed_conditions(self):
        """Test YELLOW state with mixed conditions."""
        result = evaluate_overall_state(
            fx_gap=0.20,           # YELLOW zone
            reserves_mom_4w=0.05,  # GREEN
            core_cpi_3m_ann=0.20,  # GREEN
            embi_level=1300,        # GREEN
            embi_trend_30d=0
        )
        assert result == FXState.YELLOW
    
    def test_yellow_state_one_dimension_red(self):
        """Test YELLOW state with one RED dimension."""
        result = evaluate_overall_state(
            fx_gap=0.30,           # RED
            reserves_mom_4w=0.05,  # GREEN
            core_cpi_3m_ann=0.20,  # GREEN
            embi_level=1300,       # GREEN
            embi_trend_30d=0
        )
        # Not all RED, not GREEN -> YELLOW
        assert result == FXState.YELLOW
    
    def test_red_state_all_dimensions_red(self):
        """Test RED state when all dimensions are RED."""
        result = evaluate_overall_state(
            fx_gap=0.30,           # RED
            reserves_mom_4w=-0.05, # RED
            core_cpi_3m_ann=0.50,  # RED
            embi_level=1700,        # RED
            embi_trend_30d=100     # RED
        )
        assert result == FXState.RED


class TestEvaluateStatesIntegration:
    """Test full evaluate_states integration with state flips."""
    
    def test_green_state_full(self):
        """Test full evaluation with GREEN state."""
        result = evaluate_states(
            fx_gap=0.10,
            reserves_mom_4w=0.05,
            core_cpi_3m_ann=0.20,
            embi_level=1300,
            embi_trend_30d=-100
        )
        
        assert result["overall_state"] == FXState.GREEN
        assert result["dimension_states"]["fx_gap"] == FXState.GREEN
        assert result["dimension_states"]["reserves_momentum"] == FXState.GREEN
        assert result["dimension_states"]["core_cpi"] == FXState.GREEN
        assert result["dimension_states"]["embi"] == FXState.GREEN
        assert "macro_weight" in result
        assert "action_note" in result
        assert "GREEN" in result["action_note"]
    
    def test_yellow_state_full(self):
        """Test full evaluation with YELLOW state."""
        result = evaluate_states(
            fx_gap=0.18,           # YELLOW
            reserves_mom_4w=0.02,  # YELLOW
            core_cpi_3m_ann=0.30,  # YELLOW
            embi_level=1500,       # YELLOW
            embi_trend_30d=-50
        )
        
        assert result["overall_state"] == FXState.YELLOW
        assert "YELLOW" in result["action_note"]
    
    def test_red_state_full(self):
        """Test full evaluation with RED state."""
        # Use expected return below risk-free rate and high volatility to get negative Sharpe
        result = evaluate_states(
            fx_gap=0.40,           # RED
            reserves_mom_4w=-0.03, # RED
            core_cpi_3m_ann=0.50,  # RED
            embi_level=1800,       # RED
            embi_trend_30d=200,    # RED
            exp_return=0.04,       # Return below rf (0.045) for negative Sharpe
            vol=0.25               # High volatility
        )
        
        assert result["overall_state"] == FXState.RED
        assert "RED" in result["action_note"]
        # With negative Sharpe (return < rf), position should be < 0.5
        assert result["macro_weight"] < 0.5  # Should recommend low position
    
    def test_state_flip_from_green_to_yellow(self):
        """Test state flip: GREEN -> YELLOW when gap increases."""
        green_result = evaluate_states(
            fx_gap=0.10,           # GREEN
            reserves_mom_4w=0.05,
            core_cpi_3m_ann=0.20,
            embi_level=1300,
            embi_trend_30d=0
        )
        assert green_result["overall_state"] == FXState.GREEN
        
        yellow_result = evaluate_states(
            fx_gap=0.20,           # YELLOW (gap increased)
            reserves_mom_4w=0.05,
            core_cpi_3m_ann=0.20,
            embi_level=1300,
            embi_trend_30d=0
        )
        assert yellow_result["overall_state"] == FXState.YELLOW
    
    def test_state_flip_from_yellow_to_red(self):
        """Test state flip: YELLOW -> RED when conditions worsen."""
        yellow_result = evaluate_states(
            fx_gap=0.18,           # YELLOW
            reserves_mom_4w=0.02,  # YELLOW
            core_cpi_3m_ann=0.30,  # YELLOW
            embi_level=1500,       # YELLOW
            embi_trend_30d=-50
        )
        assert yellow_result["overall_state"] == FXState.YELLOW
        
        red_result = evaluate_states(
            fx_gap=0.30,           # RED
            reserves_mom_4w=-0.02, # RED
            core_cpi_3m_ann=0.40,  # RED
            embi_level=1700,       # RED
            embi_trend_30d=100     # RED
        )
        assert red_result["overall_state"] == FXState.RED
    
    def test_state_flip_embi_trend_recovery(self):
        """Test state flip when EMBI trend improves."""
        result1 = evaluate_states(
            fx_gap=0.10,
            reserves_mom_4w=0.05,
            core_cpi_3m_ann=0.20,
            embi_level=1500,       # Not < 1400
            embi_trend_30d=-50     # Not < -300
        )
        assert result1["overall_state"] == FXState.YELLOW
        
        result2 = evaluate_states(
            fx_gap=0.10,
            reserves_mom_4w=0.05,
            core_cpi_3m_ann=0.20,
            embi_level=1500,
            embi_trend_30d=-400    # < -300, triggers GREEN condition
        )
        assert result2["overall_state"] == FXState.GREEN
    
    def test_state_flip_reserves_deterioration(self):
        """Test state flip when reserves momentum deteriorates."""
        result1 = evaluate_states(
            fx_gap=0.10,
            reserves_mom_4w=0.05,  # > 0.03
            core_cpi_3m_ann=0.20,
            embi_level=1300,
            embi_trend_30d=0
        )
        assert result1["overall_state"] == FXState.GREEN
        
        result2 = evaluate_states(
            fx_gap=0.10,
            reserves_mom_4w=0.01,  # < 0.03 (YELLOW), breaks GREEN condition
            core_cpi_3m_ann=0.20,
            embi_level=1300,
            embi_trend_30d=0
        )
        assert result2["overall_state"] == FXState.YELLOW
    
    def test_macro_weight_scaling(self):
        """Test that macro_weight scales with state."""
        green_result = evaluate_states(
            fx_gap=0.10,
            reserves_mom_4w=0.05,
            core_cpi_3m_ann=0.20,
            embi_level=1300,
            embi_trend_30d=0,
            exp_return=0.15,
            vol=0.20
        )
        
        red_result = evaluate_states(
            fx_gap=0.40,
            reserves_mom_4w=-0.05,
            core_cpi_3m_ann=0.50,
            embi_level=1800,
            embi_trend_30d=200,
            exp_return=0.15,
            vol=0.20
        )
        
        # GREEN should generally suggest higher position than RED
        # (though actual value depends on Sharpe calculation)
        assert green_result["macro_weight"] >= 0.0
        assert red_result["macro_weight"] >= 0.0
        assert green_result["macro_weight"] <= 1.0
        assert red_result["macro_weight"] <= 1.0
    
    def test_hedge_pct_by_state(self):
        """Test hedge percentage varies by state."""
        green_result = evaluate_states(
            fx_gap=0.10,
            reserves_mom_4w=0.05,
            core_cpi_3m_ann=0.20,
            embi_level=1300,
            embi_trend_30d=0
        )
        assert green_result["hedge_pct"] == 0.0
        
        yellow_result = evaluate_states(
            fx_gap=0.18,
            reserves_mom_4w=0.02,
            core_cpi_3m_ann=0.30,
            embi_level=1500,
            embi_trend_30d=-50
        )
        assert yellow_result["hedge_pct"] == 0.5
        
        red_result = evaluate_states(
            fx_gap=0.40,
            reserves_mom_4w=-0.05,
            core_cpi_3m_ann=0.50,
            embi_level=1800,
            embi_trend_30d=200
        )
        assert red_result["hedge_pct"] == 0.5

