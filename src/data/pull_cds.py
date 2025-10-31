from typing import Optional, Literal
from src.data.providers.wgb_cds import (
    fetch_latest_argentina_cds_5y_wgb,
    fetch_history_argentina_cds_5y_wgb,
    WGBCDSProviderError,
)
from src.data.db import connect, upsert_timeseries, upsert_series_meta
from src.config.series_registry import REGISTRY

SERIES_ID = "CDS_ARG_5Y_USD"  # bps

def refresh_cds_arg_5y(start: Optional[str] = None, end: Optional[str] = None, mode: Literal["latest","history"]="latest") -> int:
    """
    mode="latest": store a single point (now, latest bps)
    mode="history": attempt to parse full table/history if available
    """
    print(f"CDS Pull: Starting refresh for {SERIES_ID} (mode={mode})")
    
    try:
        if mode == "history":
            print(f"  Fetching historical CDS data from WGB (start={start}, end={end})...")
            rows = fetch_history_argentina_cds_5y_wgb(start=start, end=end)
        else:
            print(f"  Fetching latest CDS data from WGB...")
            rows = fetch_latest_argentina_cds_5y_wgb()
        
        if not rows:
            print(f"  WARNING: No CDS data returned from WGB")
            if mode == "latest":
                print(f"  DEBUG: The page loads values via JavaScript (found placeholder '----')")
                print(f"  SUGGESTION: Try 'mode=history' to parse historical table, or use a JavaScript renderer")
            else:
                print(f"  DEBUG: This might mean:")
                print(f"    - The HTML structure has changed")
                print(f"    - The page requires JavaScript to render")
                print(f"    - The element isn't found by any search strategy")
            return 0
        
        print(f"  Fetched {len(rows)} CDS data point(s)")
        if rows:
            first_date = rows[0][0].date().isoformat()
            last_date = rows[-1][0].date().isoformat()
            if first_date == last_date:
                print(f"  Date: {first_date}, Value: {rows[0][1]:.2f} bps")
            else:
                print(f"  Date range: {first_date} to {last_date}")
                print(f"  Latest value: {rows[-1][1]:.2f} bps (on {last_date})")
        
    except WGBCDSProviderError as e:
        print(f"  ERROR: CDS fetch failed: {e}")
        return 0
    except Exception as e:
        print(f"  ERROR: Unexpected error during CDS fetch: {type(e).__name__}: {e}")
        return 0

    print(f"  Registering series metadata for {SERIES_ID}...")
    with connect() as conn:
        # Ensure series metadata is registered
        if SERIES_ID in REGISTRY:
            spec = REGISTRY[SERIES_ID]
            upsert_series_meta(conn, [spec])
            print(f"  Series registered: {spec.name} ({spec.source}, {spec.freq})")
        else:
            print(f"  WARNING: {SERIES_ID} not found in registry")
        
        print(f"  Storing {len(rows)} data point(s) in database...")
        upsert_timeseries(conn, SERIES_ID, rows)
        print(f"  ✓ CDS refresh completed successfully for {SERIES_ID}")
    
    return 1
