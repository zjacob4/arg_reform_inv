"""Refresh all data series from their sources."""

import sys

from .pull_fx import main as pull_fx_main
from .pull_reserves import main as pull_reserves_main
from .pull_cpi import main as pull_cpi_main
from .pull_embi_resilient import refresh_embi_resilient
from .pull_embi_synthetic import refresh_embi_synth_usd
from .pull_cds import refresh_cds_arg_5y
from .pull_ndf import refresh_ndf_curve
from .pull_policy_rate import refresh_policy_rate


def main():
    """Run all data pullers sequentially."""
    print("=" * 60)
    print("Refreshing all data series...")
    print("=" * 60)
    
    pullers = [
        ("FX (USD/ARS)", pull_fx_main),
        ("Reserves", pull_reserves_main),
        ("CPI", pull_cpi_main),
        ("EMBI (Resilient)", lambda: refresh_embi_resilient(start="2024-01-01", min_bonds=2)),
    ]
    
    failed = []
    
    for name, puller_func in pullers:
        try:
            puller_func()
            print()
        except Exception as e:
            print(f"  ERROR: {name} failed: {e}")
            failed.append(name)
            print()
    
    # EMBI synthetic refresh with journal logging
    print("=" * 60)
    print("EMBI Synthetic (USD)")
    print("=" * 60)
    try:
        refresh_embi_synth_usd(start="2024-01-01")
        print("  EMBI synthetic refreshed successfully")
        print()
    except Exception as e:
        print(f"  ERROR: EMBI synthetic refresh failed: {e}")
        failed.append("EMBI Synthetic")
        print()
    
    # CDS refresh with journal logging
    print("=" * 60)
    print("CDS (Argentina 5Y USD)")
    print("=" * 60)
    try:
        refresh_cds_arg_5y(mode="latest")
        print("  CDS refreshed successfully")
        print()
    except Exception as e:
        print(f"  ERROR: CDS refresh failed: {e}")
        failed.append("CDS")
        print()
    
    # NDF curve refresh
    print("=" * 60)
    print("NDF Curve (Argentina USD/ARS)")
    print("=" * 60)
    try:
        refresh_ndf_curve(start="2024-01-01")
        print("  NDF curve refreshed successfully")
        print()
    except Exception as e:
        print(f"  ERROR: NDF curve refresh failed: {e}")
        failed.append("NDF")
        print()
    
    # Policy rate refresh
    print("=" * 60)
    print("Policy Rate (LELIQ & Real Rate)")
    print("=" * 60)
    try:
        refresh_policy_rate(start="2023-01-01")  # 2+ years backfill
        print("  Policy rate refreshed successfully")
        print()
    except Exception as e:
        print(f"  ERROR: Policy rate refresh failed: {e}")
        failed.append("Policy Rate")
        print()
    
    print("=" * 60)
    if failed:
        print(f"Refresh completed with errors: {', '.join(failed)}")
        sys.exit(1)
    else:
        print("All data refreshed successfully!")
        print("=" * 60)


if __name__ == "__main__":
    main()

