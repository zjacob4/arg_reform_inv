"""Tests for FX gap feature."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from src.features.fx_gap import compute, FXGapResult, _get_latest_rate


class TestFXGapCompute:
    """Test the main compute function."""
    
    def test_compute_success_with_parallel_series(self):
        """Test successful computation using USDARS_PARALLEL."""
        # Mock database connection
        mock_conn = Mock()
        
        # Mock _get_latest_rate calls
        with patch('src.features.fx_gap._get_latest_rate') as mock_get_rate:
            # First call for parallel (returns USDARS_PARALLEL)
            mock_get_rate.side_effect = [
                (datetime(2024, 1, 1), 1500.0, "USDARS_PARALLEL"),  # parallel
                (datetime(2024, 1, 1), 1000.0, "USDARS_OFFICIAL"),   # official
            ]
            
            result = compute(conn=mock_conn)
            
            assert result is not None
            assert isinstance(result, FXGapResult)
            assert result.value == 0.5  # (1500 - 1000) / 1000
            assert result.parallel_rate == 1500.0
            assert result.official_rate == 1000.0
            assert result.used_parallel == "USDARS_PARALLEL"
            assert result.used_official == "USDARS_OFFICIAL"
    
    def test_compute_success_with_blue_series(self):
        """Test successful computation using USDARS_BLUE fallback."""
        # Mock database connection
        mock_conn = Mock()
        
        # Mock _get_latest_rate calls
        with patch('src.features.fx_gap._get_latest_rate') as mock_get_rate:
            # First call for parallel (returns USDARS_BLUE)
            mock_get_rate.side_effect = [
                (datetime(2024, 1, 1), 1500.0, "USDARS_BLUE"),      # parallel
                (datetime(2024, 1, 1), 1000.0, "USDARS_OFFICIAL"),   # official
            ]
            
            result = compute(conn=mock_conn)
            
            assert result is not None
            assert isinstance(result, FXGapResult)
            assert result.value == 0.5  # (1500 - 1000) / 1000
            assert result.parallel_rate == 1500.0
            assert result.official_rate == 1000.0
            assert result.used_parallel == "USDARS_BLUE"
            assert result.used_official == "USDARS_OFFICIAL"
    
    def test_compute_success_with_official_bluelytics_fallback(self):
        """Test successful computation using USDARS_OFFICIAL_BLUELYTICS fallback."""
        # Mock database connection
        mock_conn = Mock()
        
        # Mock _get_latest_rate calls
        with patch('src.features.fx_gap._get_latest_rate') as mock_get_rate:
            # First call for parallel (returns USDARS_PARALLEL)
            mock_get_rate.side_effect = [
                (datetime(2024, 1, 1), 1500.0, "USDARS_PARALLEL"),  # parallel
                (datetime(2024, 1, 1), 1000.0, "USDARS_OFFICIAL_BLUELYTICS"),  # official
            ]
            
            result = compute(conn=mock_conn)
            
            assert result is not None
            assert isinstance(result, FXGapResult)
            assert result.value == 0.5  # (1500 - 1000) / 1000
            assert result.parallel_rate == 1500.0
            assert result.official_rate == 1000.0
            assert result.used_parallel == "USDARS_PARALLEL"
            assert result.used_official == "USDARS_OFFICIAL_BLUELYTICS"
    
    def test_compute_no_parallel_data(self):
        """Test when no parallel data is available."""
        # Mock database connection
        mock_conn = Mock()
        
        # Mock _get_latest_rate calls
        with patch('src.features.fx_gap._get_latest_rate') as mock_get_rate:
            # First call for parallel (returns None)
            mock_get_rate.side_effect = [
                (None, None, None),  # parallel - no data
                (datetime(2024, 1, 1), 1000.0, "USDARS_OFFICIAL"),   # official
            ]
            
            result = compute(conn=mock_conn)
            
            assert result is None
    
    def test_compute_no_official_data(self):
        """Test when no official data is available."""
        # Mock database connection
        mock_conn = Mock()
        
        # Mock _get_latest_rate calls
        with patch('src.features.fx_gap._get_latest_rate') as mock_get_rate:
            # First call for parallel (returns data)
            mock_get_rate.side_effect = [
                (datetime(2024, 1, 1), 1500.0, "USDARS_PARALLEL"),  # parallel
                (None, None, None),  # official - no data
            ]
            
            result = compute(conn=mock_conn)
            
            assert result is None


class TestGetLatestRate:
    """Test the _get_latest_rate helper function."""
    
    def test_get_latest_rate_success_first_series(self):
        """Test successful retrieval from first series in list."""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = (datetime(2024, 1, 1), 1500.0)
        
        result = _get_latest_rate(mock_conn, ["USDARS_PARALLEL", "USDARS_BLUE"])
        
        assert result == (datetime(2024, 1, 1), 1500.0, "USDARS_PARALLEL")
        mock_conn.execute.assert_called_once()
    
    def test_get_latest_rate_success_second_series(self):
        """Test successful retrieval from second series in list (fallback)."""
        mock_conn = Mock()
        # First call returns None, second call returns data
        mock_conn.execute.return_value.fetchone.side_effect = [None, (datetime(2024, 1, 1), 1500.0)]
        
        result = _get_latest_rate(mock_conn, ["USDARS_PARALLEL", "USDARS_BLUE"])
        
        assert result == (datetime(2024, 1, 1), 1500.0, "USDARS_BLUE")
        assert mock_conn.execute.call_count == 2
    
    def test_get_latest_rate_no_data(self):
        """Test when no series has data."""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = None
        
        result = _get_latest_rate(mock_conn, ["USDARS_PARALLEL", "USDARS_BLUE"])
        
        assert result == (None, None, None)
        assert mock_conn.execute.call_count == 2


class TestFXGapResult:
    """Test the FXGapResult model."""
    
    def test_fx_gap_result_creation(self):
        """Test FXGapResult creation with all fields."""
        result = FXGapResult(
            value=0.5,
            timestamp=datetime(2024, 1, 1),
            parallel_rate=1500.0,
            official_rate=1000.0,
            used_parallel="USDARS_PARALLEL",
            used_official="USDARS_OFFICIAL"
        )
        
        assert result.value == 0.5
        assert result.timestamp == datetime(2024, 1, 1)
        assert result.parallel_rate == 1500.0
        assert result.official_rate == 1000.0
        assert result.used_parallel == "USDARS_PARALLEL"
        assert result.used_official == "USDARS_OFFICIAL"
    
    def test_fx_gap_result_used_parallel_validation(self):
        """Test that used_parallel accepts both USDARS_PARALLEL and USDARS_BLUE."""
        # Test USDARS_PARALLEL
        result1 = FXGapResult(
            value=0.5,
            timestamp=datetime(2024, 1, 1),
            used_parallel="USDARS_PARALLEL"
        )
        assert result1.used_parallel == "USDARS_PARALLEL"
        
        # Test USDARS_BLUE
        result2 = FXGapResult(
            value=0.5,
            timestamp=datetime(2024, 1, 1),
            used_parallel="USDARS_BLUE"
        )
        assert result2.used_parallel == "USDARS_BLUE"
    
    def test_fx_gap_result_used_official_validation(self):
        """Test that used_official accepts both USDARS_OFFICIAL and USDARS_OFFICIAL_BLUELYTICS."""
        # Test USDARS_OFFICIAL
        result1 = FXGapResult(
            value=0.5,
            timestamp=datetime(2024, 1, 1),
            used_official="USDARS_OFFICIAL"
        )
        assert result1.used_official == "USDARS_OFFICIAL"
        
        # Test USDARS_OFFICIAL_BLUELYTICS
        result2 = FXGapResult(
            value=0.5,
            timestamp=datetime(2024, 1, 1),
            used_official="USDARS_OFFICIAL_BLUELYTICS"
        )
        assert result2.used_official == "USDARS_OFFICIAL_BLUELYTICS"


class TestFXGapIntegration:
    """Integration tests for FX gap functionality."""
    
    def test_used_parallel_accepts_both_series_ids(self):
        """Test that the system accepts both USDARS_PARALLEL and USDARS_BLUE for parallel side."""
        # This test ensures the assertion mentioned in the user request works
        result1 = FXGapResult(
            value=0.5,
            timestamp=datetime(2024, 1, 1),
            used_parallel="USDARS_PARALLEL"
        )
        result2 = FXGapResult(
            value=0.5,
            timestamp=datetime(2024, 1, 1),
            used_parallel="USDARS_BLUE"
        )
        
        # Test the assertion pattern requested by user
        assert result1.used_parallel in ("USDARS_PARALLEL", "USDARS_BLUE")
        assert result2.used_parallel in ("USDARS_PARALLEL", "USDARS_BLUE")
        
        # Test that both are valid
        valid_parallel_series = ("USDARS_PARALLEL", "USDARS_BLUE")
        assert result1.used_parallel in valid_parallel_series
        assert result2.used_parallel in valid_parallel_series
