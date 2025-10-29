"""Base provider interface for data sources."""

from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
from datetime import datetime


class SeriesProvider(ABC):
    """Interface for any data provider (IMF, TE, BCRA)."""

    @abstractmethod
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        """Return [(timestamp, value)] for the series_code within [start,end]."""
        ...


class ProviderError(RuntimeError):
    """Error raised by data providers."""
    pass

