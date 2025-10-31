import re
import pandas as pd
from datetime import date

ARG_KEYS = ("argentina", "republic of argentina")

def load_eubond_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # normalize headers
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    # common columns in EOD export: Code, Name, Country, Currency, Type, ISIN, ...
    for must in ["code","name"]:
        assert any(must == c for c in df.columns), f"CSV missing '{must}'"
    return df

def filter_argentina(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in df.columns if c in ("name","country","issuer","description","industry","sector")]
    mask = df[cols].astype(str).apply(lambda s: s.str.contains("argentina", case=False, na=False)).any(axis=1)
    out = df[mask].copy()
    # prefer EUR bonds
    if "currency" in out.columns:
        out = out[out["currency"].str.upper().fillna("").str.contains("EUR")]
    return out

_COUPON_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_MAT_RE    = re.compile(r"(20\d{2}|19\d{2})$|(\d{2}/\d{2}/\d{2})|(\d{2}/\d{2}/\d{4})")

def parse_coupon_maturity(name: str):
    coup = None; mat = None
    if name:
        m = _COUPON_RE.search(name)
        if m:
            coup = float(m.group(1))
        m2 = _MAT_RE.search(name.strip())
        if m2:
            token = m2.group(0)
            # normalize to YYYY-MM-DD if possible
            if re.match(r"^\d{4}$", token):
                mat = f"{token}-12-31"
            else:
                # try dd/mm/yyyy or dd/mm/yy
                dd, mm, yy = token.split("/")
                yy = "20"+yy if len(yy)==2 else yy
                mat = f"{yy}-{mm}-{dd}"
    return coup, mat

def build_bond_meta(arg_df: pd.DataFrame) -> list[dict]:
    meta=[]
    for _, r in arg_df.iterrows():
        code = str(r["code"]).strip()
        name = str(r["name"]).strip()
        cur  = str(r.get("currency","")).upper()
        if not code: continue
        coupon, maturity = parse_coupon_maturity(name)
        meta.append({
            "vendor_code": code,      # EUBOND code to use with /eod/{code}.EUBOND
            "name": name,
            "currency": cur or "EUR",
            "coupon": coupon,         # may be None; we’ll fall back if missing
            "freq": 1 if coupon else 2,  # guess annual; fine for approx
            "maturity": maturity,     # may be None; we’ll infer tenor from Name if possible
            "weight": 0.25            # will be normalized later
        })
    # keep a small top set to avoid odd legacy lines; prefer longer tenors
    return meta
