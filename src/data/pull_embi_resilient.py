from datetime import datetime
import yaml
from src.data.db import upsert_timeseries
from src.data.providers.eod_bonds import fetch_quotes_for_universe
from src.models.embi_local import compute_embi_local

def _load_meta(p="src/data/bonds/arg_usd_bonds.yaml"):
    import pathlib
    with open(pathlib.Path(p), "r") as f:
        return yaml.safe_load(f)

def refresh_embi_resilient(start=None, min_bonds=2, meta_path="src/data/bonds/arg_usd_bonds.yaml"):
    print("=" * 60)
    print("EMBI (Resilient) Refresh")
    print("=" * 60)
    
    # Load bond metadata
    print(f"\n[1/5] Loading bond metadata from: {meta_path}")
    bonds_meta = _load_meta(meta_path)
    print(f"  Found {len(bonds_meta)} bonds in metadata:")
    total_weight = 0.0
    for b in bonds_meta:
        ticker = b.get("ticker", "?")
        isin = b.get("isin", "?")
        coupon = b.get("coupon", 0.0)
        maturity = b.get("maturity", "?")
        weight = b.get("weight", 0.0)
        total_weight += weight
        print(f"    - {ticker}: ISIN={isin}, Coupon={coupon}%, Maturity={maturity}, Weight={weight:.2%}")
    print(f"  Total portfolio weight: {total_weight:.2%}")
    
    # Fetch quotes
    print(f"\n[2/5] Fetching bond quotes (start={start or 'full history'})")
    quotes = fetch_quotes_for_universe(bonds_meta, start=start)
    
    # Analyze which bonds have quotes
    used = [t for t, rows in quotes.items() if rows]
    print(f"  Quotes retrieved for {len(used)} bond(s): {', '.join(used)}")
    
    if not quotes:
        print("  ❌ ERROR: No quotes retrieved. Cannot compute EMBI.")
        return 0
    
    # Show quote date ranges
    for ticker, rows in quotes.items():
        if rows:
            dates = [ts for ts, _ in rows]
            min_date = min(dates).strftime("%Y-%m-%d")
            max_date = max(dates).strftime("%Y-%m-%d")
            print(f"    {ticker}: {len(rows)} quotes from {min_date} to {max_date}")
    
    # Check minimum bonds requirement
    if len(used) < min_bonds:
        print(f"  ⚠️  WARNING: Only {len(used)} bond(s) with quotes (minimum: {min_bonds})")
    else:
        print(f"  ✅ Met minimum bond requirement: {len(used)} >= {min_bonds}")
    
    # Determine asof date
    asof = max(ts for rows in quotes.values() for ts,_ in rows)
    print(f"\n[3/5] Computing EMBI as of: {asof.strftime('%Y-%m-%d')}")
    
    # Compute EMBI
    print("\n[4/5] Computing bond spreads and weighted EMBI index:")
    res = compute_embi_local(asof, quotes, meta_path)
    
    if res["embi_local_bps"] is None:
        print("  ❌ ERROR: EMBI computation returned None. Check bond prices and UST curve.")
        return 0
    
    # Print detailed calculation results
    print(f"  UST Curve Reference Date: {asof.date()}")
    if res.get("ust_curve"):
        curve_pts = len(res["ust_curve"]) if isinstance(res["ust_curve"], list) else "N/A"
        print(f"  UST Curve Points: {curve_pts}")
    
    print(f"\n  Per-bond spread calculations:")
    total_weighted_spread = 0.0
    total_weight = 0.0
    
    for detail in res.get("details", []):
        ticker = detail.get("ticker", "?")
        spr_bps = detail.get("spr_bps", 0.0)
        ytm = detail.get("ytm", 0.0)
        ust = detail.get("ust", 0.0)
        ttm = detail.get("ttm", 0.0)
        w = detail.get("w", 0.0)
        
        # Find the bond price used
        bond_price = None
        if ticker in quotes and quotes[ticker]:
            for ts, px in reversed(quotes[ticker]):
                if ts <= asof:
                    bond_price = px
                    break
        
        total_weighted_spread += w * spr_bps
        total_weight += w
        
        print(f"    {ticker}:")
        print(f"      Price: ${bond_price:.2f}" if bond_price else "      Price: N/A")
        print(f"      YTM: {ytm*100:.2f}%")
        print(f"      UST: {ust*100:.2f}%")
        print(f"      TTM: {ttm:.2f} years")
        print(f"      Spread: {spr_bps:.1f} bps")
        print(f"      Weight: {w:.2%}")
        print(f"      Contribution: {w * spr_bps:.1f} bps")
    
    print(f"\n  Weighted EMBI Index Calculation:")
    print(f"    Sum(weight × spread) = {total_weighted_spread:.1f} bps")
    print(f"    Sum(weights) = {total_weight:.2%}")
    final_embi = res["embi_local_bps"]
    print(f"    EMBI = {total_weighted_spread:.1f} / {total_weight:.2%} = {final_embi:.1f} bps")
    
    # Store result
    print(f"\n[5/5] Storing EMBI result to database")
    upsert_timeseries("EMBI_ARG_LOCAL", [(res["asof"], res["embi_local_bps"])])
    print(f"  ✅ Stored EMBI_ARG_LOCAL = {final_embi:.1f} bps as of {asof.strftime('%Y-%m-%d')}")
    
    return 1
