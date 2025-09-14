import pandas as pd

def load_data(xlsx_path: str) -> dict:
    """Load all sheets from the Excel (requires openpyxl)."""
    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as e:
        raise RuntimeError(
            f"Failed to open {xlsx_path}. Ensure the file exists and 'openpyxl' is installed. Original: {e}"
        )
    return {name: xls.parse(name) for name in xls.sheet_names}

def _safe_roll(df: pd.DataFrame, col: str, w: int, suffix: str) -> None:
    if col in df.columns:
        df[f"{col}_{suffix}"] = df[col].rolling(w, min_periods=1).mean()

def build_features(sheets: dict) -> pd.DataFrame:
    """
    Expect a sheet like 'hourly_kpis'. Adds a few rolling stats needed by models.
    Keep it simple; you can expand as needed.
    """
    # Pick the first sheet that looks like hourly KPIs
    df = None
    for name in ["hourly_kpis", "Hour", "hourly"]:
        if name in sheets:
            df = sheets[name].copy()
            break
    if df is None:
        # fallback: pick the biggest table
        df = max(sheets.values(), key=lambda d: d.shape[0]).copy()

    # Basic rolling features (examples)
    for col in ["specific_power_kwh_per_ton", "o2_percent", "bagfilter_dp_pa_meas",
                "co_ppm", "nox_mg_Nm3", "stack_dust_mg_Nm3"]:
        _safe_roll(df, col, 3, "roll_3h")

    # Fill obvious NaNs conservatively
    df = df.fillna(method="ffill").fillna(method="bfill")
    return df
