from datetime import datetime
from typing import Dict, List, Tuple
from .bond_spread import load_meta, bond_spread_bps
from src.data.db import connect, upsert_timeseries

def compute_embi_local(asof: datetime, quotes: Dict[str, List[Tuple[datetime,float]]], meta_path: str) -> dict:
    meta=load_meta(meta_path)
    curve=fetch_ust_curve(asof.date().isoformat())
    def ust_fn(ttm): return interp_ust(ttm, curve)
    total=0.0; wsum=0.0; details=[]
    for ticker, rows in quotes.items():
        if ticker not in meta or not rows: continue
        # use last price on/ before asof
        px=None
        for ts, p in reversed(rows):
            if ts<=asof:
                px=p; break
        if px is None: continue
        spr, ytm, ust, ttm = bond_spread_bps(asof, px, meta[ticker], ust_fn)
        w = meta[ticker].get("weight", 0.25)
        total += w*spr; wsum += w
        details.append({"ticker": ticker, "spr_bps": spr, "ytm": ytm, "ust": ust, "ttm": ttm, "w": w})
    idx = total/wsum if wsum>0 else None
    return {"asof": asof, "embi_local_bps": idx, "details": details, "ust_curve": curve}

def store_details(asof: datetime, details: List[dict]) -> None:
    """Store individual bond spreads as separate time series.
    
    Creates series IDs like "EMBI_ARG_LOCAL__{TICKER}_SPREAD_BPS"
    for each bond in the details list.
    """
    if not details:
        return
    
    conn = connect()
    try:
        for d in details:
            sid = f"EMBI_ARG_LOCAL__{d['ticker']}_SPREAD_BPS"
            upsert_timeseries(conn, sid, [(asof, d["spr_bps"])])
    finally:
        conn.close()
