"""Series registry for economic and financial indicators."""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class SeriesSpec:
    """Specification for a data series."""
    
    name: str
    code: str
    freq: str  # e.g., 'D' for daily, 'M' for monthly
    source: str  # Data source/provider name
    units: str  # Unit of measurement


# Registry of all available data series
REGISTRY: Dict[str, SeriesSpec] = {
    "USDARS_OFFICIAL": SeriesSpec(
        name="USDARS_OFFICIAL",
        code="USDARS_OFFICIAL",
        freq="D",
        source="BCRA",
        units="ARS per USD",
    ),
    "USDARS_PARALLEL": SeriesSpec(
        name="USDARS_PARALLEL",
        code="USDARS_PARALLEL",
        freq="D",
        source="BLUELYTICS",
        units="ARS per USD (blue)",
    ),
    "RESERVES_USD": SeriesSpec(
        name="RESERVES_USD",
        code="RESERVES_USD",
        freq="D",
        source="BCRA",
        units="USD millions",
    ),
    "CPI_HEADLINE": SeriesSpec(
        name="CPI_HEADLINE",
        code="CPI_HEADLINE",
        freq="M",
        source="IMF_CPI",
        units="Index",
    ),
    "CPI_CORE": SeriesSpec(
        name="CPI_CORE",
        code="CPI_CORE",
        freq="M",
        source="FRED",
        units="Index",
    ),
    # FRED-preferred CPI route (added alongside existing INDEC/IMF options)
    "CPI_NATIONAL_INDEX": SeriesSpec(
        name="CPI_NATIONAL_INDEX",
        code="CPI_NATIONAL_INDEX",
        freq="M",
        source="FRED",
        units="Index",
    ),
    "CPI_NATIONAL_YOY": SeriesSpec(
        name="CPI_NATIONAL_YOY",
        code="CPI_NATIONAL_YOY",
        freq="M",
        source="FRED",
        units="Percent",
    ),
    "CPI_NATIONAL_MOM": SeriesSpec(
        name="CPI_NATIONAL_MOM",
        code="CPI_NATIONAL_MOM",
        freq="M",
        source="FRED",
        units="Percent",
    ),
    "EMBI_ARG_LOCAL": SeriesSpec(
        name="EMBI_ARG_LOCAL",
        code="EMBI_ARG_LOCAL",
        freq="D",
        source="LOCAL",
        units="Basis points",
    ),
    "EMBI_ARG_SYNTH_USD": SeriesSpec(
        name="EMBI_ARG_SYNTH_USD",
        code="EMBI_ARG_SYNTH_USD",
        freq="D",
        source="LOCAL",
        units="Basis points",
    ),
    # Individual bond spreads (computed from local bond quotes)
    "EMBI_ARG_LOCAL__GD30_SPREAD_BPS": SeriesSpec(
        name="EMBI_ARG_LOCAL__GD30_SPREAD_BPS",
        code="EMBI_ARG_LOCAL__GD30_SPREAD_BPS",
        freq="D",
        source="LOCAL",
        units="Basis points",
    ),
    "EMBI_ARG_LOCAL__AL30_SPREAD_BPS": SeriesSpec(
        name="EMBI_ARG_LOCAL__AL30_SPREAD_BPS",
        code="EMBI_ARG_LOCAL__AL30_SPREAD_BPS",
        freq="D",
        source="LOCAL",
        units="Basis points",
    ),
    "EMBI_ARG_LOCAL__GD35_SPREAD_BPS": SeriesSpec(
        name="EMBI_ARG_LOCAL__GD35_SPREAD_BPS",
        code="EMBI_ARG_LOCAL__GD35_SPREAD_BPS",
        freq="D",
        source="LOCAL",
        units="Basis points",
    ),
    "EMBI_ARG_LOCAL__AL35_SPREAD_BPS": SeriesSpec(
        name="EMBI_ARG_LOCAL__AL35_SPREAD_BPS",
        code="EMBI_ARG_LOCAL__AL35_SPREAD_BPS",
        freq="D",
        source="LOCAL",
        units="Basis points",
    ),
    "CDS_5Y": SeriesSpec(
        name="CDS_5Y",
        code="CDS_5Y",
        freq="D",
        source="Bloomberg",
        units="Basis points",
    ),
    "CDS_ARG_5Y_USD": SeriesSpec(
        name="CDS_ARG_5Y_USD",
        code="CDS_ARG_5Y_USD",
        freq="D",
        source="WGB",
        units="Basis points",
    ),
    "PRIMARY_BALANCE": SeriesSpec(
        name="PRIMARY_BALANCE",
        code="PRIMARY_BALANCE",
        freq="M",
        source="MECON",
        units="ARS billions",
    ),
    # NDF (Non-Deliverable Forward) curve series
    "NDF_1M": SeriesSpec(
        name="NDF_1M",
        code="NDF_1M",
        freq="D",
        source="SYNTHETIC",
        units="ARS per USD (1M forward)",
    ),
    "NDF_3M": SeriesSpec(
        name="NDF_3M",
        code="NDF_3M",
        freq="D",
        source="SYNTHETIC",
        units="ARS per USD (3M forward)",
    ),
    "NDF_6M": SeriesSpec(
        name="NDF_6M",
        code="NDF_6M",
        freq="D",
        source="SYNTHETIC",
        units="ARS per USD (6M forward)",
    ),
    "NDF_12M": SeriesSpec(
        name="NDF_12M",
        code="NDF_12M",
        freq="D",
        source="SYNTHETIC",
        units="ARS per USD (12M forward)",
    ),
    # Policy rates
    "LELIQ_RATE": SeriesSpec(
        name="LELIQ_RATE",
        code="LELIQ_RATE",
        freq="D",
        source="BCRA",
        units="Annualized percentage",
    ),
    "POLICY_RATE": SeriesSpec(
        name="POLICY_RATE",
        code="POLICY_RATE",
        freq="D",
        source="BCRA_LELIQ",
        units="Annualized percentage",
    ),
    "REAL_RATE": SeriesSpec(
        name="REAL_RATE",
        code="REAL_RATE",
        freq="D",
        source="DERIVED",
        units="Annualized percentage (policy_rate - cpi_nowcast)",
    ),
}


def get_series_spec(name: str) -> Optional[SeriesSpec]:
    """Get series specification by name.
    
    Args:
        name: Series name/identifier
        
    Returns:
        SeriesSpec if found, None otherwise
    """
    return REGISTRY.get(name)


def list_all_series() -> List[str]:
    """List all registered series names.
    
    Returns:
        List of series names
    """
    return sorted(REGISTRY.keys())

