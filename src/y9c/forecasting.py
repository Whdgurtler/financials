from __future__ import annotations

from dataclasses import dataclass
import warnings

import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.exceptions import ConvergenceWarning
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor

from .config import get_all_mdrm_codes


MODEL_LABELS = {
    "random_forest": "Random Forest",
    "xgboost": "XGBoost",
}

HORIZON_LABELS = {
    "1 Year (4Q)": 4,
    "3 Years (12Q)": 12,
}

CORE_PREDICTORS = {
    "balance_sheet": [
        "BHCK2170",
        "BHCKB528",
        "BHCK2122",
        "BHCK1754",
        "BHCK1773",
        "BHCK3545",
        "BHCK2948",
        "BHCK3210",
        "BHCK3632",
        "BHDM6636",
        "BHDM6631",
        "BHCK3200",
    ],
    "income_statement": [
        "BHCK4010",
        "BHCK4073",
        "BHCK4074",
        "BHCK4230",
        "BHCKJJ33",
        "BHCK4079",
        "BHCK4093",
        "BHCK4135",
        "BHCK4301",
        "BHCK4302",
        "BHCK2170",
        "BHCKB528",
        "BHCK3210",
        "BHDM6636",
    ],
}

# Candidate variables are deliberately selected by economic relationship before
# XGBoost is trained. XGBoost can rank these candidates, but it must not search
# the full regulatory catalog for accidental correlations.
ECONOMIC_FEATURE_GROUPS = {
    "rates": ["Fed Funds Rate", "2Y Treasury Yield", "10Y Treasury Yield", "Yield Curve (10Y-2Y)"],
    "macro": ["Unemployment Rate", "CPI Inflation (YoY %)"],
    "credit": ["HY Credit Spread (OAS)", "IG Credit Spread (OAS)", "Mortgage Delinquency", "Credit Card Delinquency"],
}

ACCOUNT_FEATURE_GROUPS = {
    "scale_credit": ["BHCK2170", "BHCKB528", "BHCK2122", "BHCK5369"],
    "securities": ["BHCK1754", "BHCK1773", "BHCK3545"],
    "funding_capital": ["BHDM6636", "BHDM6631", "BHCK2948", "BHCK3210", "BHCK3632", "BHCK3200"],
    "interest_income": ["BHCK4010", "BHCK4074"],
    "interest_expense": ["BHCK4073"],
    "credit_cost": ["BHCK4230", "BHCKJJ33"],
    "operating_results": ["BHCK4079", "BHCK4093", "BHCK4135", "BHCK4301", "BHCK4302"],
}

# Each statement target gets economically related account groups and FRED
# groups. The target itself is always added by _feature_columns.
TARGET_FEATURE_GROUPS = {
    "balance_sheet": {
        "scale_credit": {"rates", "macro", "credit"},
        "securities": {"rates", "macro"},
        "funding_capital": {"rates", "macro"},
    },
    "income_statement": {
        "interest_income": {"rates", "macro", "credit"},
        "interest_expense": {"rates", "macro"},
        "credit_cost": {"credit", "macro", "rates"},
        "operating_results": {"macro", "rates", "credit"},
    },
}


@dataclass(frozen=True)
class ForecastResult:
    target_code: str
    target_name: str
    statement_type: str
    model_name: str
    horizon_quarters: int
    predictions: pd.DataFrame
    metrics: pd.DataFrame
    feature_importance: pd.DataFrame
    future_forecast: pd.DataFrame


def _all_code_info() -> dict[str, dict[str, str]]:
    return get_all_mdrm_codes()


def statement_targets(panel_df: pd.DataFrame, statement_type: str) -> pd.DataFrame:
    code_info = _all_code_info()
    rows = []
    for code, info in code_info.items():
        if info["statement"] != statement_type:
            continue
        if code in panel_df.columns:
            rows.append({
                "mdrm_code": code,
                "account_name": info["description"],
                "category": info["category"],
            })

    return pd.DataFrame(rows).sort_values(["category", "account_name"]).reset_index(drop=True)


def convert_ytd_to_quarterly(financial_long: pd.DataFrame) -> pd.DataFrame:
    """Convert cumulative income-statement filings to quarterly observations."""
    financial = financial_long.copy()
    mask = financial["statement_type"] == "income_statement"
    income = financial[mask].copy().sort_values(
        ["rssd_id", "mdrm_code", "year", "quarter"]
    )
    income["value"] = income.groupby(
        ["rssd_id", "mdrm_code", "year"]
    )["value"].transform(lambda values: values.diff().fillna(values))
    return pd.concat([financial[~mask], income], ignore_index=True)


def build_model_panel(
    financial_long: pd.DataFrame,
    institutions: pd.DataFrame,
    fred_wide: pd.DataFrame,
) -> pd.DataFrame:
    meta_cols = ["rssd_id", "report_date", "year", "quarter"]

    financial = financial_long.copy()
    financial["report_date"] = pd.to_datetime(financial["report_date"])
    pivoted = (
        financial
        .pivot_table(
            index=meta_cols,
            columns="mdrm_code",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )
    pivoted.columns.name = None

    institution_names = institutions[["rssd_id", "name"]].rename(columns={"name": "institution_name"})
    pivoted = pivoted.merge(institution_names, on="rssd_id", how="left")

    fred = fred_wide.copy()
    fred["report_date"] = pd.to_datetime(fred["report_date"])

    ordered_meta = ["rssd_id", "institution_name", "report_date", "year", "quarter"]
    non_meta = [col for col in pivoted.columns if col not in ordered_meta]
    panel = pivoted[ordered_meta + non_meta].merge(fred, on="report_date", how="left")
    return panel.sort_values(["rssd_id", "report_date"]).reset_index(drop=True)


def model_options() -> list[str]:
    return list(MODEL_LABELS.keys())


def quarter_end_shift(date_value: pd.Timestamp, quarters: int) -> pd.Timestamp:
    period = pd.Period(pd.Timestamp(date_value), freq="Q")
    return (period + quarters).to_timestamp("Q")


def _fred_columns(panel_df: pd.DataFrame) -> list[str]:
    meta_cols = {"rssd_id", "institution_name", "report_date", "year", "quarter"}
    code_cols = set(_all_code_info().keys())
    return [col for col in panel_df.columns if col not in meta_cols and col not in code_cols]


def candidate_feature_plan(target_code: str, panel_df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Return the pre-model account and economic candidates for one target."""
    code_info = _all_code_info()
    if target_code not in code_info:
        raise ValueError(f"Unknown target code: {target_code}")

    statement_type = code_info[target_code]["statement"]
    target_category = code_info[target_code]["category"]
    statement_groups = TARGET_FEATURE_GROUPS.get(statement_type, {})
    account_groups = set()
    economic_groups = set()
    for group_name, group_economic_names in statement_groups.items():
        group_codes = ACCOUNT_FEATURE_GROUPS.get(group_name, [])
        if target_code in group_codes or target_category == group_name:
            account_groups.add(group_name)
            economic_groups.update(group_economic_names)

    if not account_groups:
        account_groups = set(statement_groups)
        for group_names in statement_groups.values():
            economic_groups.update(group_names)

    account_codes = []
    for group_name in account_groups:
        account_codes.extend(ACCOUNT_FEATURE_GROUPS[group_name])
    account_codes = list(dict.fromkeys(code for code in account_codes if code in panel_df.columns))
    selected_economic_names = {
        name
        for group_name in economic_groups
        for name in ECONOMIC_FEATURE_GROUPS.get(group_name, [])
    }
    economic_columns = [name for name in _fred_columns(panel_df) if name in selected_economic_names]
    return account_codes, economic_columns


def _feature_columns(
    statement_type: str,
    target_code: str,
    panel_df: pd.DataFrame,
    predictor_codes: list[str] | None = None,
) -> list[str]:
    predictors = list(predictor_codes) if predictor_codes is not None else list(CORE_PREDICTORS[statement_type])
    if target_code not in predictors:
        predictors.append(target_code)
    return [code for code in predictors if code in panel_df.columns]


def _build_model(model_name: str, n_jobs: int = -1):
    if model_name == "random_forest":
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", RandomForestRegressor(
                n_estimators=250,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=n_jobs,
            )),
        ])

    if model_name == "xgboost":
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", XGBRegressor(
                n_estimators=300,
                max_depth=4,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                reg_lambda=1.0,
                random_state=42,
                n_jobs=n_jobs,
            )),
        ])

    raise ValueError(f"Unsupported model: {model_name}")


def _prepare_problem(
    panel_df: pd.DataFrame,
    rssd_id: str,
    target_code: str,
    horizon_quarters: int,
    n_lags: int = 4,
    predictor_codes: list[str] | None = None,
    economic_columns: list[str] | None = None,
) -> tuple[pd.DataFrame, list[str], str, str]:
    code_info = _all_code_info()
    if target_code not in code_info:
        raise ValueError(f"Unknown target code: {target_code}")

    statement_type = code_info[target_code]["statement"]
    target_name = code_info[target_code]["description"]
    default_predictors, default_economics = candidate_feature_plan(target_code, panel_df)
    available_fred = _fred_columns(panel_df)
    fred_cols = [col for col in (economic_columns if economic_columns is not None else default_economics) if col in available_fred]
    predictor_codes = _feature_columns(
        statement_type,
        target_code,
        panel_df,
        predictor_codes if predictor_codes is not None else default_predictors,
    )

    bank = panel_df[panel_df["rssd_id"] == rssd_id].sort_values("report_date").copy()
    if bank.empty:
        raise ValueError(f"No data found for RSSD {rssd_id}")

    keep_cols = ["report_date", "year", "quarter"] + predictor_codes + fred_cols
    bank = bank[keep_cols].drop_duplicates("report_date").copy()
    bank["report_date"] = pd.to_datetime(bank["report_date"])
    report_start = bank["report_date"].min()
    report_end = bank["report_date"].max()
    bank = bank.set_index("report_date").reindex(
        pd.date_range(report_start, report_end, freq="QE")
    ).rename_axis("report_date").reset_index()
    periods = bank["report_date"].dt.to_period("Q")
    bank["year"] = periods.dt.year
    bank["quarter"] = periods.dt.quarter

    feature_frame = bank[["report_date", "year", "quarter"]].copy()
    for code in predictor_codes:
        series = bank[code]
        feature_frame[f"{code}__current"] = series
        for lag in range(1, n_lags + 1):
            feature_frame[f"{code}__lag{lag}"] = series.shift(lag)
        feature_frame[f"{code}__roll4"] = series.shift(1).rolling(4, min_periods=1).mean()
        feature_frame[f"{code}__roll8"] = series.shift(1).rolling(8, min_periods=1).mean()

    for name in fred_cols:
        series = bank[name]
        feature_frame[f"{name}__current"] = series
        for lag in range(1, n_lags + 1):
            feature_frame[f"{name}__lag{lag}"] = series.shift(lag)
        feature_frame[f"{name}__roll4"] = series.shift(1).rolling(4, min_periods=1).mean()
        feature_frame[f"{name}__roll8"] = series.shift(1).rolling(8, min_periods=1).mean()

    feature_frame["quarter_sin"] = np.sin(2 * np.pi * feature_frame["quarter"] / 4)
    feature_frame["quarter_cos"] = np.cos(2 * np.pi * feature_frame["quarter"] / 4)
    feature_frame["target"] = bank[target_code].shift(-horizon_quarters)
    feature_frame["target_date"] = bank["report_date"].shift(-horizon_quarters)

    feature_cols = [
        col for col in feature_frame.columns
        if col not in {"report_date", "year", "quarter", "target", "target_date"}
    ]

    return feature_frame, feature_cols, statement_type, target_name


def _extract_feature_importance(model, feature_cols: list[str]) -> pd.DataFrame:
    estimator = model.named_steps["model"] if hasattr(model, "named_steps") else model

    if hasattr(estimator, "feature_importances_"):
        values = estimator.feature_importances_
    elif hasattr(estimator, "coef_"):
        values = np.abs(np.ravel(estimator.coef_))
    else:
        values = np.zeros(len(feature_cols))

    importance = pd.DataFrame({
        "feature": feature_cols,
        "importance": values,
    })
    return importance.sort_values("importance", ascending=False).reset_index(drop=True)


def _usable_feature_columns(df: pd.DataFrame, feature_cols: list[str]) -> list[str]:
    usable = []
    for col in feature_cols:
        if col in df.columns and df[col].notna().any():
            usable.append(col)
    return usable


def _safe_mape(actual: pd.Series, predicted: pd.Series) -> float:
    actual_vals = np.asarray(actual, dtype=float)
    pred_vals = np.asarray(predicted, dtype=float)
    mask = actual_vals != 0
    if not np.any(mask):
        return float("nan")
    return float(np.mean(np.abs((actual_vals[mask] - pred_vals[mask]) / actual_vals[mask])) * 100)


def run_forecast(
    panel_df: pd.DataFrame,
    rssd_id: str,
    target_code: str,
    model_name: str,
    horizon_quarters: int,
    n_lags: int = 4,
    predictor_codes: list[str] | None = None,
    economic_columns: list[str] | None = None,
    min_train_size: int = 16,
    max_folds: int | None = None,
    model_n_jobs: int = -1,
) -> ForecastResult:
    """Train ``model_name`` for one (institution, target, horizon).

    ``max_folds`` caps the walk-forward backtest to the most recent N folds
    (still trained on all history up to each fold) — useful for batch
    precomputation across many institutions/targets where the full backtest
    over every historical quarter is unnecessary. The final model used for
    ``future_forecast`` is always trained on the complete available history
    regardless of ``max_folds``.
    """
    feature_frame, feature_cols, statement_type, target_name = _prepare_problem(
        panel_df=panel_df,
        rssd_id=rssd_id,
        target_code=target_code,
        horizon_quarters=horizon_quarters,
        n_lags=n_lags,
        predictor_codes=predictor_codes,
        economic_columns=economic_columns,
    )

    train_df = feature_frame.dropna(subset=["target"]).reset_index(drop=True)
    train_df = train_df.iloc[n_lags:].reset_index(drop=True)
    feature_cols = _usable_feature_columns(train_df, feature_cols)

    if not feature_cols:
        raise ValueError(f"No usable features available for {target_name}.")

    if len(train_df) <= min_train_size:
        raise ValueError(
            f"Not enough history for {target_name} with horizon {horizon_quarters}Q. "
            f"Need more than {min_train_size + n_lags} usable quarters, found {len(train_df)}."
        )

    first_split_idx = min_train_size
    if max_folds is not None:
        first_split_idx = max(min_train_size, len(train_df) - max_folds)

    predictions = []
    for split_idx in range(first_split_idx, len(train_df)):
        train_slice = train_df.iloc[:split_idx]
        test_row = train_df.iloc[[split_idx]]
        split_feature_cols = _usable_feature_columns(train_slice, feature_cols)
        if not split_feature_cols:
            continue
        model = clone(_build_model(model_name, n_jobs=model_n_jobs))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=ConvergenceWarning)
            model.fit(train_slice[split_feature_cols], train_slice["target"])
        predicted = float(model.predict(test_row[split_feature_cols])[0])
        actual = float(test_row["target"].iloc[0])
        origin_date = pd.Timestamp(test_row["report_date"].iloc[0])
        target_date = pd.Timestamp(test_row["target_date"].iloc[0])
        predictions.append({
            "fold": split_idx - min_train_size + 1,
            "origin_date": origin_date,
            "target_date": target_date,
            "actual": actual,
            "predicted": predicted,
            "residual": actual - predicted,
            "abs_error": abs(actual - predicted),
            "ape": abs((actual - predicted) / actual) * 100 if actual != 0 else np.nan,
        })

    predictions_df = pd.DataFrame(predictions)
    metrics_df = pd.DataFrame([{
        "model": MODEL_LABELS[model_name],
        "horizon_quarters": horizon_quarters,
        "folds": len(predictions_df),
        "mae": mean_absolute_error(predictions_df["actual"], predictions_df["predicted"]),
        "rmse": float(np.sqrt(mean_squared_error(predictions_df["actual"], predictions_df["predicted"]))),
        "mape": _safe_mape(predictions_df["actual"], predictions_df["predicted"]),
        "r2": r2_score(predictions_df["actual"], predictions_df["predicted"]),
    }])

    full_model = clone(_build_model(model_name, n_jobs=model_n_jobs))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=ConvergenceWarning)
        full_model.fit(train_df[feature_cols], train_df["target"])
    importance_df = _extract_feature_importance(full_model, feature_cols)

    forecast_candidates = feature_frame[feature_frame["target"].isna()].dropna(subset=feature_cols, how="all")
    future_forecast_df = pd.DataFrame(columns=["origin_date", "forecast_date", "forecast_value"])
    if not forecast_candidates.empty:
        latest_row = forecast_candidates.tail(1)
        origin_date = pd.Timestamp(latest_row["report_date"].iloc[0])
        forecast_value = float(full_model.predict(latest_row[feature_cols])[0])
        future_forecast_df = pd.DataFrame([{
            "origin_date": origin_date,
            "forecast_date": quarter_end_shift(origin_date, horizon_quarters),
            "forecast_value": forecast_value,
        }])

    return ForecastResult(
        target_code=target_code,
        target_name=target_name,
        statement_type=statement_type,
        model_name=model_name,
        horizon_quarters=horizon_quarters,
        predictions=predictions_df,
        metrics=metrics_df,
        feature_importance=importance_df,
        future_forecast=future_forecast_df,
    )