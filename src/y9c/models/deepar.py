"""
DeepAR Data Preparation

Shapes the joined Y-9C + FRED dataset into the GluonTS ListDataset format
expected by the DeepAR estimator.  Model building is handled separately.

Each (rssd_id, target_column) pair becomes one time-series entry.
FRED economic series are passed as dynamic real-valued covariates.

Usage
-----
from src.y9c.models.deepar import prepare_deepar_data

train_ds, val_ds, meta = prepare_deepar_data()
# → meta contains column lists, freq, prediction_length, etc.

# Pass to GluonTS later:
#   from gluonts.dataset.common import ListDataset
#   dataset = ListDataset(train_ds, freq=meta["freq"])
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
# Helpers
# ---------------------------------------------------------------------------

def _financial_columns(df: pd.DataFrame, econ_cols: list[str], meta_cols: list[str]) -> list[str]:
    """Return columns that are Y-9C financial metrics (not meta or econ)."""
    return [c for c in df.columns if c not in meta_cols and c not in econ_cols]


def _to_period_index(series: pd.Series, freq: str = "QE") -> pd.PeriodIndex:
    return pd.PeriodIndex(pd.to_datetime(series).dt.to_period("Q"))


# ---------------------------------------------------------------------------
# Main preparation function
# ---------------------------------------------------------------------------

def prepare_deepar_data(
    rssd_ids: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    prediction_length: int = 4,
    val_periods: int = 4,
    freq: str = "Q",
    min_history: int = 8,
) -> tuple[list[dict], list[dict], dict]:
    """
    Prepare data for a DeepAR model (via GluonTS).

    Parameters
    ----------
    rssd_ids : list of str, optional
        Institutions to include.  None = all.
    start_year / end_year : int, optional
        Year range for Y-9C data.
    prediction_length : int
        Number of quarters ahead to forecast.
    val_periods : int
        Quarters withheld from the end of each series for validation.
    freq : str
        GluonTS frequency string ("Q" = quarterly).
    min_history : int
        Minimum non-null observations required for a series to be included.

    Returns
    -------
    train_entries : list[dict]
        List of GluonTS-compatible entry dicts for training.
    val_entries : list[dict]
        Same series extended to include validation window.
    meta : dict
        Metadata: freq, prediction_length, target_cols, econ_cols,
        n_train_series, n_institutions.

        To build a GluonTS dataset::

            from gluonts.dataset.common import ListDataset
            train_ds = ListDataset(train_entries, freq=meta["freq"])
    """
    df = load_joined(
        rssd_ids=rssd_ids,
        start_year=start_year,
        end_year=end_year,
        use_account_names=True,
    )

    if df.empty:
        return [], [], {}

    meta_cols = ["rssd_id", "institution_name", "report_date", "year", "quarter"]
    econ_cols = [c for c in df.columns if c in FRED_SERIES_NAMES]
    target_cols = _financial_columns(df, econ_cols, meta_cols)

    print(
        f"DeepAR prep: {df['rssd_id'].nunique()} institutions, "
        f"{len(target_cols)} targets, {len(econ_cols)} econ covariates"
    )

    train_entries: list[dict] = []
    val_entries: list[dict] = []

    for rssd_id, grp in df.groupby("rssd_id"):
        grp = grp.sort_values("report_date").reset_index(drop=True)

        # Quarter-start period used as GluonTS start
        start_period = str(pd.Period(grp["report_date"].iloc[0], freq="Q"))

        # Economic covariates: shape (n_econ, T)
        econ_matrix = grp[econ_cols].to_numpy(dtype=float).T  # (E, T)

        for col in target_cols:
            target_vals = grp[col].to_numpy(dtype=float)

            # Skip series that are mostly NaN
            valid = int(np.sum(~np.isnan(target_vals)))
            if valid < min_history:
                continue

            # Forward-fill then back-fill NaNs (common in regulatory data)
            target_vals = (
                pd.Series(target_vals).ffill().bfill().to_numpy(dtype=float)
            )

            base_entry = {
                "item_id": f"{rssd_id}::{col}",
                "start": start_period,
                "feat_dynamic_real": econ_matrix,
            }

            # Training series: all but last val_periods steps
            cutoff = len(target_vals) - val_periods
            if cutoff < min_history:
                continue

            train_entries.append(
                {**base_entry, "target": target_vals[:cutoff]}
            )
            val_entries.append(
                {**base_entry, "target": target_vals}
            )

    print(
        f"DeepAR prep: {len(train_entries)} train series, "
        f"{len(val_entries)} val series"
    )

    meta = {
        "freq": freq,
        "prediction_length": prediction_length,
        "val_periods": val_periods,
        "target_cols": target_cols,
        "econ_cols": econ_cols,
        "n_train_series": len(train_entries),
        "n_institutions": df["rssd_id"].nunique(),
    }

    return train_entries, val_entries, meta


if __name__ == "__main__":
    train, val, meta = prepare_deepar_data()
    print(f"train series: {len(train)}, val series: {len(val)}")
    print(meta)
