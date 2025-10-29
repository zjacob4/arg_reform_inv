"""Refresh all data series from their sources."""

import sys

from .pull_fx import main as pull_fx_main
from .pull_reserves import main as pull_reserves_main
from .pull_cpi import main as pull_cpi_main
from .pull_embi_cds import main as pull_embi_cds_main


def main():
    """Run all data pullers sequentially."""
    print("=" * 60)
    print("Refreshing all data series...")
    print("=" * 60)
    
    pullers = [
        ("FX (USD/ARS)", pull_fx_main),
        ("Reserves", pull_reserves_main),
        ("CPI", pull_cpi_main),
        ("EMBI/CDS", pull_embi_cds_main),
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
    
    print("=" * 60)
    if failed:
        print(f"Refresh completed with errors: {', '.join(failed)}")
        sys.exit(1)
    else:
        print("All data refreshed successfully!")
        print("=" * 60)


if __name__ == "__main__":
    main()

