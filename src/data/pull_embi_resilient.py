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
    bonds_meta = _load_meta(meta_path)
    quotes = fetch_quotes_for_universe(bonds_meta, start=start)

    # Keep only bonds with recent quotes
    used = [t for t, rows in quotes.items() if rows]
    
    if not quotes:
        # fallback path will be appended later if you add TE—log and stop for now
        return 0

    asof = max(ts for rows in quotes.values() for ts,_ in rows)
    res = compute_embi_local(asof, quotes, meta_path)
    if res["embi_local_bps"] is None:
        return 0

    upsert_timeseries("EMBI_ARG_LOCAL", [(res["asof"], res["embi_local_bps"])])
    # (optional) store per-bond spreads if you added store_details()
    # store_details(res["asof"], res["details"])
    return 1
