"""
XGBoost Data Preparation

Shapes the joined Y-9C + FRED dataset into a flat feature matrix
suitable for XGBoost (or any scikit-learn compatible estimator).

Each row is one (rssd_id, quarter).
Features: lags of all Y-9C columns + FRED economic covariates.
Targets: the Y-9C columns at t + horizon quarters ahead.

Usage
-----
from src.y9c.models.xgb import prepare_xgb_data

X_train, X_val, y_train, y_val, meta = prepare_xgb_data()

# Then fit with XGBoost:
#   from xgboost import XGBRegressor
#   from sklearn.multioutput import MultiOutputRegressor
#   model = MultiOutputRegressor(XGBRegressor()).fit(X_train, y_train)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

try:
    from ..datasets import load_joined
    from ..fred_data import FRED_SERIES_NAMES
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
    from src.y9c.datasets import load_joined
    from src.y9c.fred_data import FRED_SERIES_NAMES


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def _add_lags(
    df: pd.DataFrame,
    cols: list[str],
    n_lags: int,
    group_col: str = "rssd_id",
) -> pd.DataFrame:
    """Add lag columns for each column in *cols*, grouped by institution."""
    for col in cols:
        for lag in range(1, n_lags + 1):
            df[f"{col}__lag{lag}"] = df.groupby(group_col)[col].shift(lag)
    return df


def _add_rolling(
    df: pd.DataFrame,
    cols: list[str],
    windows: list[int],
    group_col: str = "rssd_id",
) -> pd.DataFrame:
    """Add rolling mean columns for each column in *cols*."""
    for col in cols:
        for w in windows:
            df[f"{col}__roll{w}"] = (
                df.groupby(group_col)[col]
                .transform(lambda s, _w=w: s.shift(1).rolling(_w, min_periods=1).mean())
            )
    return df


# ---------------------------------------------------------------------------
# Main preparation function
# ---------------------------------------------------------------------------

def prepare_xgb_data(
    rssd_ids: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    n_lags: int = 4,
    rolling_windows: list[int] | None = None,
    horizon: int = 1,
    val_frac: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Build a flat feature matrix for XGBoost multi-output regression.

    Parameters
    ----------
    rssd_ids : list of str, optional
        Institutions to include.  None = all.
    start_year / end_year : int, optional
        Year range for Y-9C data.
    n_lags : int
        Number of quarterly lags to create for each Y-9C column.
    rolling_windows : list of int, optional
        Rolling-mean window sizes.  Defaults to [4, 8].
    horizon : int
        Forecast horizon in quarters (target is t + horizon).
    val_frac : float
        Fraction of the timeline (from the end) used for validation.
        Split is done by date to avoid look-ahead leakage.

    Returns
    -------
    X_train, X_val : pd.DataFrame
        Feature matrices (lags + rolling stats + econ covariates).
    y_train, y_val : pd.DataFrame
        Target matrices (all Y-9C columns at t + horizon).
    meta : dict
        feature_cols, target_cols, econ_cols, horizon, n_lags, cutoff_date.
    """
    if rolling_windows is None:
        rolling_windows = [4, 8]

    df = load_joined(
        rssd_ids=rssd_ids,
        start_year=start_year,
        end_year=end_year,
        use_account_names=True,
    )

    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    meta_cols = ["rssd_id", "institution_name", "report_date", "year", "quarter"]
    econ_cols = [c for c in df.columns if c in FRED_SERIES_NAMES]
    target_cols = [c for c in df.columns if c not in meta_cols and c not in econ_cols]

    df = df.sort_values(["rssd_id", "report_date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Build targets: each target col shifted back by -horizon per group
    # ------------------------------------------------------------------
    target_df = df[["rssd_id", "report_date"]].copy()
    for col in target_cols:
        target_df[col] = df.groupby("rssd_id")[col].shift(-horizon)

    # ------------------------------------------------------------------
    # Build features: lags + rolling stats of Y-9C cols + current econ
    # ------------------------------------------------------------------
    feat_df = df[meta_cols + econ_cols + target_cols].copy()
    feat_df = _add_lags(feat_df, target_cols, n_lags)
    feat_df = _add_rolling(feat_df, target_cols, rolling_windows)

    # Quarter / year as numeric cyclical features
    feat_df["quarter_sin"] = np.sin(2 * np.pi * feat_df["quarter"] / 4)
    feat_df["quarter_cos"] = np.cos(2 * np.pi * feat_df["quarter"] / 4)

    # Drop original (non-lagged) Y-9C cols from features — use only lags
    lag_cols = [c for c in feat_df.columns if "__lag" in c or "__roll" in c]
    cycle_cols = ["quarter_sin", "quarter_cos"]
    feature_cols = econ_cols + lag_cols + cycle_cols

    feat_df = feat_df[meta_cols + feature_cols].copy()

    # ------------------------------------------------------------------
    # Drop rows where we can't form a valid sample (NaN targets / lags)
    # ------------------------------------------------------------------
    combined = feat_df.merge(
        target_df.rename(columns={c: f"__tgt_{c}" for c in target_cols}),
        on=["rssd_id", "report_date"],
    )

    tgt_renamed = [f"__tgt_{c}" for c in target_cols]
    combined = combined.dropna(
        subset=[c for c in lag_cols[:n_lags]] + tgt_renamed[:1],
        how="all",
    )

    # ------------------------------------------------------------------
    # Temporal train / val split (by date, not random)
    # ------------------------------------------------------------------
    dates = sorted(combined["report_date"].unique())
    cutoff_idx = int(len(dates) * (1 - val_frac))
    cutoff_date = dates[cutoff_idx]

    is_train = combined["report_date"] < cutoff_date
    train = combined[is_train]
    val = combined[~is_train]

    X_train = train[feature_cols].reset_index(drop=True)
    X_val = val[feature_cols].reset_index(drop=True)
    y_train = train[tgt_renamed].rename(
        columns={f"__tgt_{c}": c for c in target_cols}
    ).reset_index(drop=True)
    y_val = val[tgt_renamed].rename(
        columns={f"__tgt_{c}": c for c in target_cols}
    ).reset_index(drop=True)

    print(
        f"XGBoost prep: {len(X_train):,} train rows, {len(X_val):,} val rows | "
        f"{len(feature_cols)} features → {len(target_cols)} targets "
        f"(horizon={horizon}q, lags={n_lags}q)"
    )
    print(f"  Val cutoff: {pd.Timestamp(cutoff_date).date()}")

    meta = {
        "feature_cols": feature_cols,
        "target_cols": target_cols,
        "econ_cols": econ_cols,
        "lag_cols": lag_cols,
        "horizon": horizon,
        "n_lags": n_lags,
        "rolling_windows": rolling_windows,
        "cutoff_date": cutoff_date,
        "n_institutions": df["rssd_id"].nunique(),
    }

    return X_train, X_val, y_train, y_val, meta


if __name__ == "__main__":
    X_train, X_val, y_train, y_val, meta = prepare_xgb_data()
    print(f"X_train: {X_train.shape}, X_val: {X_val.shape}")
    print(meta)
