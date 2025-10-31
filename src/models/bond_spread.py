from datetime import datetime, date
from typing import List, Tuple, Dict
import math, yaml

def years_to_maturity(asof: datetime, maturity: date) -> float:
    return (maturity - asof.date()).days / 365.25

def approx_ytm(clean_price: float, coupon: float, freq: int, ytm_guess: float = 0.12) -> float:
    """
    Quick Newton method on standard fixed coupon; good enough for spread math.
    face=100.0; coupon in %; clean price in price-per-100.
    """
    face=100.0
    c = coupon/100.0*face/freq
    y=ytm_guess
    for _ in range(20):
        # assume N periods ~ 8–20 (duration bonds); can refine with schedule
        # simple 10-period approximation improves stability; real impl should build exact cashflows
        N=10
        pv=0.0; dv=0.0
        for k in range(1,N+1):
            df=(1+y/freq)**k
            pv += c/df
            dv += -k*c/(freq*df)
        pv += face/(1+y/freq)**N
        dv += -N*face/(freq*(1+y/freq)**N)
        f_val = pv - clean_price
        if abs(f_val) < 1e-6: break
        y -= f_val/dv
        if y< -0.99: y=0.0001
    return max(y,0.0)

def load_meta(path: str) -> Dict[str,dict]:
    with open(path,"r") as f:
        arr=yaml.safe_load(f)
    return {x["ticker"].upper(): x for x in arr}

def bond_spread_bps(asof: datetime, px: float, meta: dict, ust_curve_fn) -> float:
    ytm = approx_ytm(px, meta["coupon"], meta["freq"])
    # Handle maturity as string or date object from YAML
    mat = meta["maturity"]
    if isinstance(mat, str):
        mat_date = datetime.fromisoformat(mat).date()
    elif isinstance(mat, date):
        mat_date = mat
    else:
        mat_date = datetime.fromisoformat(str(mat)).date()
    ttm = max(0.5, years_to_maturity(asof, mat_date))
    ust = ust_curve_fn(ttm)
    spr = (ytm - ust)*10000.0
    return spr, ytm, ust, ttm
