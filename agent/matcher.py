import pandas as pd
from typing import Dict, Any, Tuple

def _band(val, tol: dict) -> Tuple[float, float]:
    if val is None:
        return (-1e18, 1e18)
    if "tol_abs" in tol:
        return val - tol["tol_abs"], val + tol["tol_abs"]
    if "tol_pct" in tol:
        d = abs(val) * tol["tol_pct"] / 100.0
        return val - d, val + d
    return val, val

def find_best_recipe(now: Dict[str, Any], df: pd.DataFrame, match_tags: Dict[str, dict]) -> Dict[str, Any]:
    """Filter df by tolerance bands around `now`, then pick lowest specific power run window."""
    if df is None or df.empty:
        return {"setpoints": {}, "n_rows": 0}

    mask = pd.Series(True, index=df.index)
    for tag, tol in match_tags.items():
        if tag in df.columns and tag in now:
            lo, hi = _band(now[tag], tol)
            mask &= df[tag].between(lo, hi)

    candidates = df.loc[mask].copy()
    if candidates.empty:
        return {"setpoints": {}, "n_rows": 0}

    sort_key = "specific_power_kwh_per_ton" if "specific_power_kwh_per_ton" in candidates.columns else None
    if sort_key:
        candidates = candidates.sort_values(sort_key)

    top = candidates.head(60)
    recipe = top.median(numeric_only=True).to_dict()
    return {"setpoints": recipe, "n_rows": int(len(candidates))}
