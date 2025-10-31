"""Quality checks for EMBI bond price data."""


def price_sanity(px):
    """Check if bond price is within reasonable bounds.
    
    Args:
        px: Bond price (typically clean price in USD)
        
    Returns:
        bool: True if price is between 5.0 and 120.0
    """
    return 5.0 <= px <= 120.0


def daily_jump_ok(prev_px, px, max_jump=0.12):
    """Check if daily price change is within acceptable bounds.
    
    Args:
        prev_px: Previous day's price (can be None for first observation)
        px: Current day's price
        max_jump: Maximum allowed relative change (default 12%)
        
    Returns:
        bool: True if the price change is acceptable or if prev_px is None
    """
    if prev_px is None:
        return True
    return abs(px - prev_px) / max(prev_px, 1e-6) <= max_jump

