import yaml
from datetime import datetime
from statistics import fmean
from typing import Dict, List, Tuple

from src.data.providers.eubond_csv import load_eubond_csv, filter_argentina, build_bond_meta
from src.data.providers.eod_bonds import fetch_eubond_eod
from src.data.providers.eod_gbond import fetch_gbond_series
from src.models.synthetic_usd_spread import (
    pick_latest_on_or_before, approx_ytm_from_price,
    implied_usd_yield, to_spread_bps
)
from src.data.db import connect, upsert_timeseries, upsert_series_meta
from src.config.series_registry import SeriesSpec


CSV_PATH = "data/EUBOND_LIST_OF_SYMBOLS.csv"

def refresh_embi_synth_usd(start="2024-01-01"):
    print("=" * 60)
    print("EMBI Synthetic (USD) Refresh")
    print("=" * 60)
    
    # Load and filter bonds
    print(f"\n[1/6] Loading EUBOND CSV from: {CSV_PATH}")
    df = load_eubond_csv(CSV_PATH)
    print(f"  Loaded {len(df)} total bonds from CSV")
    
    arg = filter_argentina(df)
    print(f"  Found {len(arg)} Argentina bonds")
    
    meta = build_bond_meta(arg)
    print(f"  Built metadata for {len(meta)} bonds")

    # prune to 3–6 bonds with good names (reduce noise)
    print(f"\n[2/6] Filtering bonds:")
    before_filter = len(meta)
    meta = [m for m in meta if "argentina" in m["name"].lower()]
    print(f"  After 'argentina' name filter: {len(meta)} bonds")
    meta = [m for m in meta if m["currency"] == "EUR"]
    print(f"  After EUR currency filter: {len(meta)} bonds")
    meta = meta[:6]  # cap
    print(f"  After cap (max 6): {len(meta)} bonds")

    if not meta:
        print("  ❌ ERROR: No bonds remaining after filtering")
        return 0
    
    print(f"\n  Selected bonds:")
    for m in meta:
        code = m.get("vendor_code", "?")
        name = m.get("name", "?")
        currency = m.get("currency", "?")
        coupon = m.get("coupon", 0.0)
        print(f"    - {code}: {name} ({currency}, Coupon={coupon}%)")

    # normalize weights
    wsum = sum(m.get("weight",0.25) for m in meta) or 1.0
    for m in meta:
        m["weight"] = m.get("weight",0.25)/wsum
    
    total_weight = sum(m["weight"] for m in meta)
    print(f"\n  Normalized weights (total: {total_weight:.2%}):")
    for m in meta:
        print(f"    {m.get('vendor_code', '?')}: {m['weight']:.2%}")

    # fetch UST & Bund (10Y as a baseline proxy)
    print(f"\n[3/6] Fetching benchmark yields (start={start}):")
    print(f"  Fetching US 10Y Treasury (US10Y)...", end=" ", flush=True)
    us10 = fetch_gbond_series("US10Y", start)
    print(f"✅ {len(us10)} data points")
    if us10:
        latest_us = us10[-1][1] * 100  # convert to percentage
        print(f"    Latest US10Y: {latest_us:.2f}%")
    
    print(f"  Fetching German 10Y Bund (DE10Y)...", end=" ", flush=True)
    de10 = fetch_gbond_series("DE10Y", start)
    print(f"✅ {len(de10)} data points")
    if de10:
        latest_de = de10[-1][1] * 100  # convert to percentage
        print(f"    Latest DE10Y: {latest_de:.2f}%")

    # pull quotes for each bond
    print(f"\n[4/6] Fetching EUBOND quotes for each bond:")
    per_bond: Dict[str, List[Tuple[datetime, float, float]]] = {}
    for m in meta:
        code = m["vendor_code"]
        name = m.get("name", "?")
        print(f"  {code} ({name}): Fetching...", end=" ", flush=True)
        try:
            rows = fetch_eubond_eod(code, start=start)
            # rows: [(ts, close_px, maybe_yield)]
            # convert to [(ts, y_eur)]
            out=[]
            yield_count = 0
            computed_count = 0
            for ts, px, yopt in rows:
                if yopt is not None:
                    y_eur = yopt
                    yield_count += 1
                else:
                    y_eur = approx_ytm_from_price(px, m.get("coupon"), m.get("freq",2))
                    computed_count += 1
                out.append((ts, y_eur))
            per_bond[code] = out
            if rows:
                dates = [ts for ts,_,_ in rows]
                min_date = min(dates).strftime("%Y-%m-%d")
                max_date = max(dates).strftime("%Y-%m-%d")
                print(f"✅ {len(rows)} quotes ({yield_count} with yield, {computed_count} computed) from {min_date} to {max_date}")
            else:
                print(f"⚠️  No quotes returned")
        except Exception as e:
            error_msg = str(e)[:100]
            print(f"❌ Error: {error_msg}")
            per_bond[code] = []

    # compute index on common dates (by latest available up to that day)
    # choose the most recent date present in any series as "asof"
    print(f"\n[5/6] Computing synthetic USD spreads:")
    all_ts = []
    for rows in per_bond.values():
        all_ts.extend([ts for ts,_ in rows])
    if not all_ts:
        print("  ❌ ERROR: No timestamps available from any bond")
        return 0
    asof = max(all_ts)
    print(f"  Reference date (asof): {asof.strftime('%Y-%m-%d')}")

    # pick latest yields as of 'asof'
    ust10 = pick_latest_on_or_before(us10, asof)
    de10  = pick_latest_on_or_before(de10, asof)
    if ust10 is None or de10 is None:
        print("  ❌ ERROR: Missing UST or Bund yields for reference date")
        return 0
    
    print(f"  UST 10Y: {ust10*100:.2f}%")
    print(f"  Bund 10Y: {de10*100:.2f}%")

    print(f"\n  Per-bond calculations:")
    details=[]
    idx=0.0; wsum=0.0
    for m in meta:
        code = m["vendor_code"]
        name = m.get("name", "?")
        y_eur = pick_latest_on_or_before(per_bond.get(code,[]), asof)
        if y_eur is None:
            print(f"    {code}: ⚠️  No EUR yield available for {asof.strftime('%Y-%m-%d')}")
            continue
        y_usd = implied_usd_yield(y_eur, ust10, de10)
        spr_bps = to_spread_bps(y_usd, ust10)
        details.append({"code": code, "name": name, "w": m["weight"], "y_eur": y_eur, "y_usd": y_usd, "spr_bps": spr_bps})
        idx += m["weight"] * spr_bps
        wsum += m["weight"]
        
        print(f"    {code} ({name}):")
        print(f"      EUR Yield: {y_eur*100:.2f}%")
        print(f"      Implied USD Yield: {y_usd*100:.2f}%")
        print(f"      Spread: {spr_bps:.1f} bps")
        print(f"      Weight: {m['weight']:.2%}")
        print(f"      Contribution: {m['weight'] * spr_bps:.1f} bps")

    if wsum == 0:
        print("\n  ❌ ERROR: No bonds with valid yields. Cannot compute index.")
        return 0
    
    final_embi = idx / wsum if wsum > 0 else 0.0
    print(f"\n  Weighted EMBI Index Calculation:")
    print(f"    Sum(weight × spread) = {idx:.1f} bps")
    print(f"    Sum(weights) = {wsum:.2%}")
    print(f"    EMBI_SYNTH_USD = {idx:.1f} / {wsum:.2%} = {final_embi:.1f} bps")

    # store index + details
    print(f"\n[6/6] Storing results to database:")
    conn = connect()
    try:
        # Register dynamic series IDs first (required by foreign key constraint)
        print(f"  Registering {1 + len(details)} series in metadata...")
        series_specs = [
            SeriesSpec(
                name="EMBI_ARG_SYNTH_USD",
                code="EMBI_ARG_SYNTH_USD",
                freq="D",
                source="LOCAL",
                units="Basis points",
            )
        ]
        # Add individual bond spread series
        for d in details:
            sid = f"EMBI_ARG_SYNTH_USD__{d['code']}_SPREAD_BPS"
            series_specs.append(
                SeriesSpec(
                    name=sid,
                    code=sid,
                    freq="D",
                    source="LOCAL",
                    units="Basis points",
                )
            )
        upsert_series_meta(conn, series_specs)
        print(f"  ✅ Registered series metadata")
        
        # Now insert the data
        print(f"  Storing EMBI_ARG_SYNTH_USD = {final_embi:.1f} bps as of {asof.strftime('%Y-%m-%d')}...")
        upsert_timeseries(conn, "EMBI_ARG_SYNTH_USD", [(asof, final_embi)])
        print(f"  ✅ Stored main index")
        
        for d in details:
            sid = f"EMBI_ARG_SYNTH_USD__{d['code']}_SPREAD_BPS"
            upsert_timeseries(conn, sid, [(asof, d["spr_bps"])])
        print(f"  ✅ Stored {len(details)} individual bond spreads")
    finally:
        conn.close()
    
    print(f"\n✅ EMBI Synthetic (USD) refresh completed successfully!")
    print("=" * 60)
    return 1
