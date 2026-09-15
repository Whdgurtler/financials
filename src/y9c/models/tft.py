"""
TFT (Temporal Fusion Transformer) Data Preparation

Data loading is handled by :mod:`src.y9c.datasets` (reusable).
This module re-exports the helpers for convenience and is the place to
add TFT-specific feature engineering (lags, normalisation, encoding, etc.).

Usage
-----
from src.y9c.models.tft import load_joined

df = load_joined()                         # all institutions
df = load_joined(rssd_ids=["1447376"])     # USAA only
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

try:
    from ..datasets import load_y9c_wide, load_economic_wide, load_joined  # noqa: F401
    from ..config import get_all_mdrm_codes
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
    from src.y9c.datasets import load_y9c_wide, load_economic_wide, load_joined  # noqa: F401
    from src.y9c.config import get_all_mdrm_codes


@dataclass
class TFTTrainingResult:
    model: object
    trainer: object
    training_dataset: object
    validation_dataset: object


def prepare_tft_frame(
    frame: pd.DataFrame,
    target_column: str = "BHCK4301",
) -> tuple[pd.DataFrame, list[str]]:
    """Prepare a wide Y-9C/FRED frame for PyTorch Forecasting."""
    required = {"rssd_id", "report_date", target_column}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Missing TFT columns: {sorted(missing)}")

    data = frame.copy()
    data["rssd_id"] = data["rssd_id"].astype(str)
    data["report_date"] = pd.to_datetime(data["report_date"])
    data = data.sort_values(["rssd_id", "report_date"])
    data["time_idx"] = data.groupby("rssd_id").cumcount()
    if "BHCK2170" in data:
        data["log_total_assets"] = np.log1p(data["BHCK2170"].clip(lower=0))
    else:
        data["log_total_assets"] = 0.0

    known_meta = {"rssd_id", "report_date", "year", "quarter", "time_idx", "log_total_assets"}
    y9c_codes = set(get_all_mdrm_codes())
    fred_columns = [
        column for column in data.columns
        if column not in known_meta and column not in y9c_codes and column != target_column
    ]
    data[fred_columns] = data[fred_columns].apply(pd.to_numeric, errors="coerce")
    data[fred_columns] = data[fred_columns].ffill().bfill().fillna(0.0)
    data[target_column] = pd.to_numeric(data[target_column], errors="coerce")
    data = data.dropna(subset=[target_column]).reset_index(drop=True)
    return data, fred_columns


def train_tft(
    frame: pd.DataFrame,
    target_column: str = "BHCK4301",
    max_encoder_length: int = 20,
    max_prediction_length: int = 4,
    batch_size: int = 64,
    max_epochs: int = 1,
    limit_train_batches: int | float = 1.0,
) -> TFTTrainingResult:
    """Train a global Temporal Fusion Transformer on all institutions."""
    try:
        from lightning.pytorch import Trainer
        from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
        from pytorch_forecasting.data import GroupNormalizer
    except ImportError as exc:
        raise ImportError(
            "TFT training requires pytorch-forecasting and lightning. "
            "Install the optional neural-training dependencies first."
        ) from exc

    data, fred_columns = prepare_tft_frame(frame, target_column)
    min_required = max_encoder_length + max_prediction_length
    valid_groups = data.groupby("rssd_id")["time_idx"].max()
    valid_groups = valid_groups[valid_groups >= min_required - 1].index
    data = data[data["rssd_id"].isin(valid_groups)].copy()
    if data.empty:
        raise ValueError("No institution has enough history for the requested TFT windows.")

    training_cutoff = data["time_idx"].max() - max_prediction_length
    training_dataset = TimeSeriesDataSet(
        data[data["time_idx"] <= training_cutoff],
        time_idx="time_idx",
        target=target_column,
        group_ids=["rssd_id"],
        static_categoricals=["rssd_id"],
        static_reals=["log_total_assets"],
        time_varying_known_reals=["time_idx", *fred_columns],
        time_varying_unknown_reals=[target_column],
        max_encoder_length=max_encoder_length,
        max_prediction_length=max_prediction_length,
        target_normalizer=GroupNormalizer(groups=["rssd_id"]),
        allow_missing_timesteps=True,
    )
    validation_dataset = TimeSeriesDataSet.from_dataset(
        training_dataset,
        data,
        min_prediction_idx=training_cutoff + 1,
        stop_randomization=True,
    )

    train_loader = training_dataset.to_dataloader(train=True, batch_size=batch_size, num_workers=0)
    validation_loader = validation_dataset.to_dataloader(train=False, batch_size=batch_size, num_workers=0)
    model = TemporalFusionTransformer.from_dataset(
        training_dataset,
        learning_rate=1e-3,
        hidden_size=16,
        attention_head_size=4,
        dropout=0.1,
        hidden_continuous_size=8,
    )
    trainer = Trainer(
        max_epochs=max_epochs,
        accelerator="auto",
        devices=1,
        enable_checkpointing=False,
        logger=False,
        enable_model_summary=False,
        limit_train_batches=limit_train_batches,
    )
    trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=validation_loader)
    return TFTTrainingResult(model, trainer, training_dataset, validation_dataset)

if __name__ == "__main__":
    df = load_joined()
    print(df)
