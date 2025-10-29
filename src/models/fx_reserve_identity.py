"""FX reserve identity: breakeven devaluation calculations."""

from typing import Dict, Optional


def uncovered_parity_stub(rates_diff: float, days: int = 90) -> float:
    """Simple uncovered interest parity stub for implied devaluation.
    
    For now, this is a placeholder. In production, this would use
    actual forward rates and interest rate differentials.
    
    Args:
        rates_diff: Interest rate differential (domestic - foreign), annualized decimal
        days: Days to maturity (default 90 for 3-month)
        
    Returns:
        Implied annualized devaluation rate as decimal
    """
    # Simple uncovered parity: expected deval ≈ interest rate differential
    # Adjusted for time to maturity
    return rates_diff * (days / 365.0)


def breakeven_deval(
    ndf_curve: Optional[Dict[str, float]] = None,
    rates_diff: float = 0.0
) -> float:
    """Calculate implied annualized devaluation from NDF curve and interest rate differential.
    
    Uses covered/uncovered interest parity to infer breakeven devaluation.
    For now, accepts ndf_3m and uses uncovered parity stub if NDF curve not provided.
    
    Args:
        ndf_curve: Optional dictionary with NDF rates (e.g., {'3m': 950.0})
                   If provided, uses ndf_3m relative to spot to calculate implied deval
        rates_diff: Interest rate differential (domestic - foreign), annualized decimal
                    Used for uncovered parity calculation if NDF curve not fully specified
        
    Returns:
        Implied annualized devaluation as percentage (e.g., 15.5 for 15.5%)
    """
    if ndf_curve and '3m' in ndf_curve:
        # If we have NDF 3-month rate, calculate implied annualized deval
        # Assuming spot rate is embedded in NDF (NDF/spot - 1 gives forward premium)
        # For simplicity, if NDF_3m represents forward rate:
        # Implied annualized deval = ((ndf_3m / spot - 1) - rates_diff * (90/365)) * (365/90) * 100
        
        # Simplified calculation: NDF forward premium annualized
        # This assumes NDF rate is already adjusted for interest differential
        # In practice, you'd have actual spot and forward rates
        ndf_3m_rate = ndf_curve['3m']
        
        # Placeholder spot (would come from actual data)
        # For now, treat ndf_3m as forward rate relative to some base
        # If ndf_3m represents forward premium in decimal:
        forward_premium_3m = ndf_3m_rate / 100.0  # Assume input is in basis points or percentage
        
        # Annualize the 3-month premium
        annualized_deval = forward_premium_3m * (365.0 / 90.0) * 100.0
        
        return annualized_deval
    
    # Fallback to uncovered parity stub
    implied_deval_decimal = uncovered_parity_stub(rates_diff, days=90)
    return implied_deval_decimal * 100.0  # Convert to percentage

