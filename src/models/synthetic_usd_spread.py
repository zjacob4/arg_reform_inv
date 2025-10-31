from datetime import datetime
from typing import Dict, List, Tuple, Optional
import math

def pick_latest_on_or_before(rows: List[Tuple[datetime, float]], asof: datetime) -> Optional[float]:
    v=None
    for ts, val in rows:
        if ts <= asof:
            v = val
        else:
            break
    return v

def approx_ytm_from_price(px: float, coupon: Optional[float], freq: int = 2, guess: float = 0.12) -> float:
    # fallback if the feed didn’t include yield; price per 100 notional
    if not coupon:
        # no coupon info—approx yield ~ discount to par, very rough
        return max(0.0, (100.0/ max(px,1e-6) - 1.0))
    c = (coupon/100.0) * 100.0 / freq
    y = guess
    for _ in range(20):
        pv=0.0; dv=0.0
        N=10
        for k in range(1,N+1):
            df=(1+y/freq)**k
            pv += c/df
            dv += -k*c/(freq*df)
        pv += 100.0/(1+y/freq)**N
        dv += -N*100.0/(freq*(1+y/freq)**N)
        f = pv - px
        if abs(f) < 1e-8: break
        y -= f/max(dv,1e-9)
        if y < -0.99: y=0.0001
    return max(y,0.0)

def implied_usd_yield(y_eur: float, ust_T: float, bund_T: float) -> float:
    return y_eur + (ust_T - bund_T)

def to_spread_bps(y_usd: float, ust_T: float) -> float:
    return (y_usd - ust_T) * 10000.0
