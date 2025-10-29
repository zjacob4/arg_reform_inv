"""Common utilities for data pulling."""

import requests
from datetime import datetime
from typing import List, Tuple, Any


def fetch_json(url: str) -> dict[str, Any]:
    """Fetch JSON data from a URL.
    
    Args:
        url: URL to fetch JSON from
        
    Returns:
        Parsed JSON data as dictionary
        
    Raises:
        requests.RequestException: If the request fails
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def to_rows(datetimes: List[datetime], values: List[float]) -> List[Tuple[datetime, float]]:
    """Convert separate lists of datetimes and values into row tuples.
    
    Args:
        datetimes: List of datetime objects
        values: List of float values
        
    Returns:
        List of (datetime, value) tuples
        
    Raises:
        ValueError: If lists have different lengths
    """
    if len(datetimes) != len(values):
        raise ValueError(f"datetimes and values must have same length, got {len(datetimes)} and {len(values)}")
    return list(zip(datetimes, values))

