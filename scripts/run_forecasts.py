"""
Batch-precompute XGBoost forecasts for every BHC (Y-9C) institution across a
curated set of core line items and both forecast horizons (1Y / 3Y).

This is meant to run as part of the data-update pipeline -- right after fresh
data has been exported to data/hf_export/ and before it is uploaded to
Hugging Face -- so the Streamlit dashboard's Model Forecasts / Monitoring
tabs can load precomputed results instantly instead of every viewer
re-training a model live.

Usage:
    python scripts/run_forecasts.py
    python scripts/run_forecasts.py --max-folds 8 --processes 8

Reads (from --export-dir, default data/hf_export/):
    financial_data.parquet, institutions.parquet, fred_data.parquet

Writes (to --export-dir):
    forecast_metrics.parquet     -- one row per (institution, target, horizon)
    forecast_predictions.parquet -- rolling backtest fold predictions
    forecast_future.parquet      -- the single forward-looking point forecast
    forecast_importance.parquet  -- top feature importances per model
"""

from __future__ import annotations

import argparse
import sys
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.y9c.config import get_all_mdrm_codes
from src.y9c.forecasting import (
    HORIZON_LABELS,
    MODEL_LABELS,
    build_model_panel,
    convert_ytd_to_quarterly,
    run_forecast,
)

EXPORT_DIR = Path(__file__).parent.parent / "data" / "hf_export"
MODEL_NAME = "xgboost"
MODEL_LABEL = MODEL_LABELS[MODEL_NAME]  # predictions/future/importance store the label to match metrics
# Full balance sheet + income statement catalog (CORE_PREDICTORS in forecasting.py
# is still used as the feature set for each target, this just controls which
# line items get their own forecast).
CORE_TARGETS = sorted(
    code for code, info in get_all_mdrm_codes().items()
    if info["statement"] in {"balance_sheet", "income_statement"}
)
TOP_IMPORTANCE_FEATURES = 15


def _process_institution(args) -> tuple[list, list, list, list]:
    rssd_id, bank_panel, horizons, max_folds = args
    metrics_rows, prediction_frames, future_frames, importance_frames = [], [], [], []

    for target_code in CORE_TARGETS:
        for horizon in horizons:
            try:
                result = run_forecast(
                    bank_panel,
                    rssd_id,
                    target_code,
                    MODEL_NAME,
                    horizon,
                    max_folds=max_folds,
                    model_n_jobs=1,
                )
            except Exception:
                continue

            metrics_rows.append({
                "rssd_id": rssd_id,
                "target_code": target_code,
                "target_name": result.target_name,
                "statement_type": result.statement_type,
                **result.metrics.iloc[0].to_dict(),
            })

            if not result.predictions.empty:
                pred = result.predictions.copy()
                pred.insert(0, "model", MODEL_LABEL)
                pred.insert(0, "horizon_quarters", horizon)
                pred.insert(0, "target_code", target_code)
                pred.insert(0, "rssd_id", rssd_id)
                prediction_frames.append(pred)

            if not result.future_forecast.empty:
                fut = result.future_forecast.copy()
                fut.insert(0, "model", MODEL_LABEL)
                fut.insert(0, "horizon_quarters", horizon)
                fut.insert(0, "target_code", target_code)
                fut.insert(0, "rssd_id", rssd_id)
                future_frames.append(fut)

            if not result.feature_importance.empty:
                imp = result.feature_importance.head(TOP_IMPORTANCE_FEATURES).copy()
                imp.insert(0, "model", MODEL_LABEL)
                imp.insert(0, "horizon_quarters", horizon)
                imp.insert(0, "target_code", target_code)
                imp.insert(0, "rssd_id", rssd_id)
                importance_frames.append(imp)

    return metrics_rows, prediction_frames, future_frames, importance_frames


def main(export_dir: Path = EXPORT_DIR, max_folds: int = 8, processes: int | None = None) -> list[Path]:
    fin = pd.read_parquet(export_dir / "financial_data.parquet")
    inst = pd.read_parquet(export_dir / "institutions.parquet")
    fred_path = export_dir / "fred_data.parquet"
    fred = pd.read_parquet(fred_path) if fred_path.exists() else pd.DataFrame({"report_date": pd.Series(dtype="datetime64[ns]")})

    panel = build_model_panel(convert_ytd_to_quarterly(fin), inst, fred)
    horizons = list(HORIZON_LABELS.values())
    rssd_ids = panel["rssd_id"].unique().tolist()

    print(f"Batch forecasting {len(rssd_ids)} institutions x {len(CORE_TARGETS)} core targets x {len(horizons)} horizons ({MODEL_NAME})...")

    tasks = [
        (rssd_id, panel[panel["rssd_id"] == rssd_id].copy(), horizons, max_folds)
        for rssd_id in rssd_ids
    ]

    workers = processes or max(1, cpu_count() - 1)
    t0 = time.time()
    with Pool(workers) as pool:
        results = pool.map(_process_institution, tasks)
    elapsed = time.time() - t0

    all_metrics, all_predictions, all_future, all_importance = [], [], [], []
    for metrics_rows, prediction_frames, future_frames, importance_frames in results:
        all_metrics.extend(metrics_rows)
        all_predictions.extend(prediction_frames)
        all_future.extend(future_frames)
        all_importance.extend(importance_frames)

    print(f"Done in {elapsed:.1f}s ({workers} workers) -- {len(all_metrics):,} (institution, target, horizon) combos succeeded.")

    export_dir.mkdir(parents=True, exist_ok=True)
    out_paths: list[Path] = []

    metrics_df = pd.DataFrame(all_metrics)
    out = export_dir / "forecast_metrics.parquet"
    metrics_df.to_parquet(out, index=False, compression="snappy")
    print(f"  Saved {len(metrics_df):,} rows -> {out}")
    out_paths.append(out)

    predictions_df = pd.concat(all_predictions, ignore_index=True) if all_predictions else pd.DataFrame()
    out = export_dir / "forecast_predictions.parquet"
    predictions_df.to_parquet(out, index=False, compression="snappy")
    print(f"  Saved {len(predictions_df):,} rows -> {out}")
    out_paths.append(out)

    future_df = pd.concat(all_future, ignore_index=True) if all_future else pd.DataFrame()
    out = export_dir / "forecast_future.parquet"
    future_df.to_parquet(out, index=False, compression="snappy")
    print(f"  Saved {len(future_df):,} rows -> {out}")
    out_paths.append(out)

    importance_df = pd.concat(all_importance, ignore_index=True) if all_importance else pd.DataFrame()
    out = export_dir / "forecast_importance.parquet"
    importance_df.to_parquet(out, index=False, compression="snappy")
    print(f"  Saved {len(importance_df):,} rows -> {out}")
    out_paths.append(out)

    return out_paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", type=Path, default=EXPORT_DIR)
    parser.add_argument("--max-folds", type=int, default=8, help="Cap the walk-forward backtest to the most recent N folds per (institution, target, horizon).")
    parser.add_argument("--processes", type=int, default=None, help="Worker processes (default: CPU count - 1).")
    args = parser.parse_args()

    main(export_dir=args.export_dir, max_folds=args.max_folds, processes=args.processes)
