"""Refresh all data series from their sources."""

import sys

from .pull_fx import main as pull_fx_main
from .pull_reserves import main as pull_reserves_main
from .pull_cpi import main as pull_cpi_main
from .pull_embi_resilient import refresh_embi_resilient
from .pull_embi_synthetic import refresh_embi_synth_usd


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
        try:
            from .journal import init_journal, log_action
            init_journal()
            log_action("ERROR", f"refresh_embi_synth_usd failed: {e}", {"stage": "embi_synth"})
        except ImportError:
            # Journal module not available, continue without logging
            pass
        except Exception as journal_error:
            # Journal logging failed, but continue
            print(f"  WARNING: Journal logging also failed: {journal_error}")
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

