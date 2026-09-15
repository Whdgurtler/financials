"""
Bank Holding Company Y-9C Financial Dashboard — Streamlit

Deploy to Hugging Face Spaces:
  1. Update README.md frontmatter: sdk: streamlit  |  app_file: streamlit_app.py
  2. Remove heavy ML packages (torch, gluonts) from requirements if hitting memory limits
  3. Set FRED_API_KEY in the Space Secrets panel (Settings → Repository secrets)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

from src.y9c.forecasting import (
    ForecastResult,
    HORIZON_LABELS,
    MODEL_LABELS,
    build_model_panel,
    convert_ytd_to_quarterly,
    run_forecast,
    statement_targets,
)

# ─────────────────────────────────────────────────────────────────────────────
# Page config  (must be the very first Streamlit call)
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Y-9C Bank Holding Co. Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  #MainMenu, footer, header {visibility: hidden;}

  .app-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 55%, #0f3460 100%);
    padding: 24px 32px 18px;
    border-radius: 12px;
    margin-bottom: 18px;
  }
  .app-header h1 {
    color: #e2e8f0; font-size: 1.85rem; font-weight: 700;
    margin: 0; letter-spacing: -0.4px;
  }
  .app-header p { color: #94a3b8; font-size: 0.85rem; margin: 4px 0 0; }

  .kpi-card {
    background: #1e293b;
    border-radius: 10px;
    padding: 14px 18px 12px;
    border-left: 3px solid #3b82f6;
    margin-bottom: 4px;
  }
  .kpi-card.up   { border-left-color: #10b981; }
  .kpi-card.down { border-left-color: #ef4444; }
  .kpi-label  { font-size: 0.7rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.6px; }
  .kpi-value  { font-size: 1.4rem; font-weight: 700; color: #f1f5f9; margin: 4px 0 2px; }
  .kpi-delta  { font-size: 0.75rem; }
  .kpi-delta.pos { color: #10b981; }
  .kpi-delta.neg { color: #ef4444; }
  .kpi-delta.neu { color: #64748b; }

  .sec-header {
    font-size: 1rem; font-weight: 600; color: #cbd5e1;
    margin: 18px 0 8px; padding-bottom: 5px;
    border-bottom: 1px solid #334155;
  }

  div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
HF_REPO = "Wgurtler/y9c-data"

METRIC_CHOICES: list[tuple[str, str]] = [
    # Balance Sheet
    ("Total Assets",                    "BHCK2170"),
    ("Net Loans & Leases",              "BHCKB528"),
    ("Loans & Leases (Gross)",          "BHCK2122"),
    ("AFS Securities",                  "BHCK1773"),
    ("HTM Securities",                  "BHCK1754"),
    ("Trading Assets",                  "BHCK3545"),
    ("Loans Held for Sale",             "BHCK5369"),
    ("Goodwill",                        "BHCK3163"),
    ("Total Liabilities",               "BHCK2948"),
    ("Total Equity",                    "BHCK3210"),
    ("Retained Earnings",               "BHCK3632"),
    ("Domestic IB Deposits",            "BHDM6636"),
    ("Domestic NIB Deposits",           "BHDM6631"),
    ("Subordinated Debt",               "BHCK3200"),
    # Income Statement
    ("Total Interest Income",           "BHCK4010"),
    ("Total Interest Expense",          "BHCK4073"),
    ("Net Interest Income",             "BHCK4074"),
    ("Provision for Credit Losses",     "BHCKJJ33"),
    ("Total Noninterest Income",        "BHCK4079"),
    ("Total Noninterest Expense",       "BHCK4093"),
    ("Salaries & Benefits",             "BHCK4135"),
    ("Net Income",                      "BHCK4301"),
    ("Applicable Income Taxes",         "BHCK4302"),
]
NAME_TO_MDRM: dict[str, str] = {n: m for n, m in METRIC_CHOICES}

KEY_METRICS = [
    ("BHCK2170", "Total Assets"),
    ("BHCK3210", "Total Equity"),
    ("BHCKB528", "Net Loans & Leases"),
    ("BHCK4074", "Net Interest Income"),
    ("BHCK4301", "Net Income"),
    ("BHCK4079", "Noninterest Income"),
]

CALL_REPORT_METRICS: list[tuple[str, str]] = [
    ("Total Assets", "BHCK2170"),
    ("Total Liabilities", "BHCK2948"),
    ("Total Equity", "BHCK3210"),
    ("Net Loans & Leases", "BHCKB528"),
    ("Interest-Bearing Deposits", "BHDM6636"),
    ("Total Interest Income", "BHCK4010"),
    ("Total Interest Expense", "BHCK4073"),
    ("Total Noninterest Income", "BHCK4079"),
    ("Total Noninterest Expense", "BHCK4093"),
    ("Net Income", "BHCK4301"),
]

CALL_REPORT_CAPITAL_METRICS: list[tuple[str, str]] = [
    ("CET1 Capital Ratio", "FDIC_IDT1RWAJR"),
    ("Tier 1 Risk-Based Capital Ratio", "FDIC_RBC1AAJ"),
    ("Total Risk-Based Capital Ratio", "FDIC_RBCRWAJ"),
    ("Tier 1 Capital Ratio", "FDIC_IDT1CER"),
]

CALL_REPORT_NAME_TO_MDRM: dict[str, str] = {n: m for n, m in CALL_REPORT_METRICS}

CLUSTER_FEATURE_DEFAULTS = [
    "Total Assets", "Net Loans & Leases", "Total Equity",
    "Net Interest Income", "Total Noninterest Income", "Net Income",
]

COLORS = ["#3b82f6", "#a855f7", "#f59e0b", "#ef4444", "#10b981",
          "#06b6d4", "#ec4899", "#8b5cf6", "#14b8a6", "#f97316"]

PLOTLY_BASE = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(15,30,60,0.4)",
    font=dict(family="Inter, system-ui, sans-serif", size=12, color="#cbd5e1"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    margin=dict(l=55, r=20, t=55, b=45),
    height=360,
    hovermode="x unified",
    xaxis=dict(showgrid=False, zeroline=False),
    yaxis=dict(showgrid=True, gridcolor="#1e293b", zeroline=False),
)

# ─────────────────────────────────────────────────────────────────────────────
# Data loading / caching
# ─────────────────────────────────────────────────────────────────────────────

def _try_read_hf_parquet(path: str) -> pd.DataFrame | None:
    try:
        return pd.read_parquet(path)
    except Exception:
        return None

@st.cache_data(show_spinner="Loading data from Hugging Face…")
def _load_raw() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame | None, pd.DataFrame | None]:
    fin = pd.read_parquet(f"hf://datasets/{HF_REPO}/financial_data.parquet")
    inst = pd.read_parquet(f"hf://datasets/{HF_REPO}/institutions.parquet")
    fred = pd.read_parquet(f"hf://datasets/{HF_REPO}/fred_data.parquet")
    fred["report_date"] = pd.to_datetime(fred["report_date"])

    bank_fin = _try_read_hf_parquet(f"hf://datasets/{HF_REPO}/bank_financial_data.parquet")
    bank_inst = _try_read_hf_parquet(f"hf://datasets/{HF_REPO}/bank_institutions.parquet")

    return fin, inst, fred, bank_fin, bank_inst


@st.cache_data(show_spinner="Loading precomputed forecasts…")
def _load_forecast_store() -> dict[str, pd.DataFrame]:
    """Load the batch-precomputed XGBoost forecasts (scripts/run_forecasts.py).

    Falls back to empty frames if the artifacts aren't published yet, in which
    case callers should train live instead.
    """
    names = [
        "forecast_metrics",
        "forecast_predictions",
        "forecast_future",
        "forecast_importance",
    ]
    store = {}
    for name in names:
        df = _try_read_hf_parquet(f"hf://datasets/{HF_REPO}/{name}.parquet")
        store[name] = df if df is not None else pd.DataFrame()
    if not store["forecast_metrics"].empty:
        for name in names:
            store[name]["rssd_id"] = store[name]["rssd_id"].astype(str)
    return store


def _precomputed_forecast(
    store: dict[str, pd.DataFrame],
    rssd_id: str,
    target_code: str,
    model_name: str,
    horizon_quarters: int,
) -> ForecastResult | None:
    metrics = store["forecast_metrics"]
    if metrics.empty:
        return None
    # Metrics store the human-readable label ("XGBoost"); predictions/future/
    # importance store the internal key ("xgboost") -- accept either.
    model_label = MODEL_LABELS.get(model_name, model_name)
    key_mask = lambda df: (
        (df["rssd_id"] == str(rssd_id))
        & (df["target_code"] == target_code)
        & (df["model"].isin({model_name, model_label}))
        & (df["horizon_quarters"] == horizon_quarters)
    )
    metrics_row = metrics[key_mask(metrics)]
    if metrics_row.empty:
        return None

    predictions = store["forecast_predictions"]
    future = store["forecast_future"]
    importance = store["forecast_importance"]
    drop_cols = ["rssd_id", "target_code", "horizon_quarters", "model"]

    return ForecastResult(
        target_code=target_code,
        target_name=metrics_row["target_name"].iloc[0],
        statement_type=metrics_row["statement_type"].iloc[0],
        model_name=model_name,
        horizon_quarters=horizon_quarters,
        predictions=predictions[key_mask(predictions)].drop(columns=drop_cols).reset_index(drop=True),
        metrics=metrics_row[["model", "horizon_quarters", "folds", "mae", "rmse", "mape", "r2"]].reset_index(drop=True),
        feature_importance=importance[key_mask(importance)].drop(columns=drop_cols).reset_index(drop=True),
        future_forecast=future[key_mask(future)].drop(columns=drop_cols).reset_index(drop=True),
    )


@st.cache_data(show_spinner="Transforming YTD → quarterly…")
def _convert_ytd_to_quarterly(fin: pd.DataFrame) -> pd.DataFrame:
    return convert_ytd_to_quarterly(fin)


@st.cache_data(show_spinner=False)
def _institution_options(fin: pd.DataFrame, inst: pd.DataFrame) -> pd.DataFrame:
    assets = fin[fin["mdrm_code"] == "BHCK2170"]
    if assets.empty:
        latest = (
            fin.sort_values(["year", "quarter"]).groupby("rssd_id", as_index=False).tail(1)
            [["rssd_id"]]
            .assign(total_assets=np.nan)
        )
    else:
        max_y = assets["year"].max()
        max_q = assets[assets["year"] == max_y]["quarter"].max()
        latest = (
            assets[(assets["year"] == max_y) & (assets["quarter"] == max_q)]
            [["rssd_id", "value"]].rename(columns={"value": "total_assets"})
        )

    merged = latest.merge(inst[["rssd_id", "name"]], on="rssd_id", how="left")
    merged["display"] = merged["name"].fillna(merged["rssd_id"].astype(str))
    return merged.sort_values("total_assets", ascending=False).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def _available_quarters(fin: pd.DataFrame) -> list[str]:
    q = fin.groupby(["year", "quarter"]).size().reset_index()
    q = q.sort_values(["year", "quarter"], ascending=[False, False])
    return [f"{int(r['year'])} Q{int(r['quarter'])}" for _, r in q.iterrows()]


@st.cache_data(show_spinner=False)
def _filter_institution(_fin_q: pd.DataFrame, rssd_id: str) -> pd.DataFrame:
    """Slice and sort data for a single institution (fin_q already YTD-converted)."""
    df = _fin_q[_fin_q["rssd_id"] == rssd_id][
        ["report_date", "year", "quarter", "mdrm_code", "value",
         "account_name", "statement_type", "category"]
    ].sort_values(["year", "quarter"]).copy()
    return df


@st.cache_data(show_spinner=False)
def _fred_series_map(fred_df: pd.DataFrame) -> dict[str, pd.Series]:
    data = {}
    for col in fred_df.columns:
        if col == "report_date":
            continue
        s = pd.Series(fred_df[col].values, index=pd.to_datetime(fred_df["report_date"])).dropna()
        data[col] = s
    return data


@st.cache_data(show_spinner="Building model panel…")
def _model_panel(fin_q: pd.DataFrame, inst_df: pd.DataFrame, fred_df: pd.DataFrame) -> pd.DataFrame:
    return build_model_panel(fin_q, inst_df, fred_df)


@st.cache_data(show_spinner="Running forecast model…")
def _cached_forecast(
    panel_df: pd.DataFrame,
    rssd_id: str,
    target_code: str,
    model_name: str,
    horizon_quarters: int,
):
    precomputed = _precomputed_forecast(
        _load_forecast_store(), rssd_id, target_code, model_name, horizon_quarters
    )
    if precomputed is not None:
        return precomputed
    return run_forecast(
        panel_df=panel_df,
        rssd_id=rssd_id,
        target_code=target_code,
        model_name=model_name,
        horizon_quarters=horizon_quarters,
    )


@st.cache_data(show_spinner="Running rolling backtests…")
def _cached_monitoring(
    panel_df: pd.DataFrame,
    rssd_id: str,
    target_code: str,
    model_name: str,
):
    store = _load_forecast_store()
    results = {}
    for horizon in HORIZON_LABELS.values():
        precomputed = _precomputed_forecast(store, rssd_id, target_code, model_name, horizon)
        results[horizon] = precomputed if precomputed is not None else run_forecast(
            panel_df=panel_df,
            rssd_id=rssd_id,
            target_code=target_code,
            model_name=model_name,
            horizon_quarters=horizon,
        )
    return results


@st.cache_data(show_spinner=False)
def _statement_target_frame(panel_df: pd.DataFrame, statement_type: str) -> pd.DataFrame:
    return statement_targets(panel_df, statement_type)


@st.cache_data(show_spinner="Computing clusters…")
def _compute_clusters(
    _fin_q: pd.DataFrame,
    _inst_df: pd.DataFrame,
    quarter_str: str,
    feature_names: tuple[str, ...],
    k: int,
) -> tuple[pd.DataFrame, float, float]:
    """Cross-sectional KMeans + PCA on all institutions for a given quarter."""
    parts = quarter_str.split()
    cy, cq = int(parts[0]), int(parts[1][1])
    mdrms = [NAME_TO_MDRM[n] for n in feature_names if n in NAME_TO_MDRM]

    snap = _fin_q[
        (_fin_q["year"] == cy) & (_fin_q["quarter"] == cq) &
        (_fin_q["mdrm_code"].isin(mdrms))
    ][["rssd_id", "mdrm_code", "value"]].copy()

    wide = snap.pivot_table(index="rssd_id", columns="mdrm_code", values="value", aggfunc="first")
    # Rename columns to human-readable names
    wide.columns = [
        next((n for n, m in METRIC_CHOICES if m == c), c) for c in wide.columns
    ]
    # Drop extremely sparse rows
    wide = wide.dropna(thresh=max(1, len(feature_names) // 2))

    # Attach display names via dict lookup (avoids index-clobbering merges)
    name_map = dict(zip(_inst_df["rssd_id"], _inst_df["name"]))

    if len(wide) < 2:
        wide["display_name"] = [name_map.get(rid, str(rid)) for rid in wide.index]
        wide["cluster"] = 0
        wide["pc1"] = 0.0
        wide["pc2"] = 0.0
        assets_q = _fin_q[
            (_fin_q["year"] == cy) & (_fin_q["quarter"] == cq) &
            (_fin_q["mdrm_code"] == "BHCK2170")
        ]
        assets_map = dict(zip(assets_q["rssd_id"], assets_q["value"]))
        wide["total_assets"] = [assets_map.get(rid, np.nan) for rid in wide.index]
        return wide, float("nan"), float("nan")

    k = min(k, len(wide))

    wide["display_name"] = [name_map.get(rid, str(rid)) for rid in wide.index]

    feat_cols = [c for c in wide.columns if c not in ("display_name",)]
    X = wide[feat_cols].fillna(0).values
    Xs = StandardScaler().fit_transform(X)

    km = KMeans(n_clusters=k, n_init=20, random_state=42)
    wide["cluster"] = km.fit_predict(Xs)

    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(Xs)
    ev = pca.explained_variance_ratio_
    wide["pc1"], wide["pc2"] = coords[:, 0], coords[:, 1]

    # Add total assets for marker sizing via dict lookup
    assets_q = _fin_q[
        (_fin_q["year"] == cy) & (_fin_q["quarter"] == cq) &
        (_fin_q["mdrm_code"] == "BHCK2170")
    ]
    assets_map = dict(zip(assets_q["rssd_id"], assets_q["value"]))
    wide["total_assets"] = [assets_map.get(rid, np.nan) for rid in wide.index]

    return wide, float(ev[0]), float(ev[1])


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

def fmt(v, mode: str = "currency") -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "N/A"
    if mode == "currency":
        av = abs(v)
        if av >= 1e9:  return f"${v/1e9:.2f}T"
        if av >= 1e6:  return f"${v/1e6:.1f}B"
        if av >= 1e3:  return f"${v/1e3:.1f}M"
        return f"${v:,.0f}K"
    if mode == "pct":
        return f"{v:+.1f}%"
    return str(v)


def _fmt_cell(v) -> str:
    if pd.isna(v):
        return "—"
    return fmt(float(v))


def get_kpi(df: pd.DataFrame, mdrm: str, year: int, qtr: int):
    cur = df[(df["year"] == year) & (df["quarter"] == qtr) & (df["mdrm_code"] == mdrm)]["value"]
    pri = df[(df["year"] == year - 1) & (df["quarter"] == qtr) & (df["mdrm_code"] == mdrm)]["value"]
    c = float(cur.values[0]) if len(cur) > 0 else None
    p = float(pri.values[0]) if len(pri) > 0 else None
    yoy = ((c - p) / abs(p) * 100) if c is not None and p and p != 0 else None
    return c, p, yoy


# ─────────────────────────────────────────────────────────────────────────────
# Chart builders
# ─────────────────────────────────────────────────────────────────────────────

def timeseries_fig(
    df: pd.DataFrame,
    metrics: list[tuple[str, str]],
    title: str,
    sel_year: int,
    sel_qtr: int,
    start_year: int | None = None,
    height: int = 360,
) -> go.Figure:
    fig = go.Figure()
    x_labels: list[str] = []
    for i, (name, mdrm) in enumerate(metrics):
        md = df[df["mdrm_code"] == mdrm].sort_values(["year", "quarter"])
        if start_year:
            md = md[md["year"] >= start_year]
        if md.empty:
            continue
        lbl = [f"{int(r['year'])} Q{int(r['quarter'])}" for _, r in md.iterrows()]
        x_labels = lbl
        fig.add_trace(go.Scatter(
            x=lbl, y=md["value"] / 1e6,
            mode="lines+markers", name=name,
            line=dict(color=COLORS[i % len(COLORS)], width=2),
            marker=dict(size=4),
        ))
    sel_lbl = f"{sel_year} Q{sel_qtr}"
    if sel_lbl in x_labels:
        idx = x_labels.index(sel_lbl)
        fig.add_vline(x=idx, line=dict(color="#f59e0b", width=1.5, dash="dot"),
                      annotation_text="◄ selected", annotation_position="top right",
                      annotation_font=dict(color="#f59e0b", size=10))
    layout = {**PLOTLY_BASE, "height": height}
    fig.update_layout(title=dict(text=title, font=dict(size=14)), yaxis_title="$ Billions", **layout)
    return fig


def yoy_bar_fig(
    df: pd.DataFrame,
    metrics: list[tuple[str, str]],
    title: str,
    year: int,
    qtr: int,
) -> go.Figure:
    names, cur_vals, pri_vals = [], [], []
    for name, mdrm in metrics:
        c = df[(df["year"] == year) & (df["quarter"] == qtr) & (df["mdrm_code"] == mdrm)]["value"]
        p = df[(df["year"] == year - 1) & (df["quarter"] == qtr) & (df["mdrm_code"] == mdrm)]["value"]
        names.append(name)
        cur_vals.append(float(c.values[0]) / 1e6 if len(c) > 0 else 0.0)
        pri_vals.append(float(p.values[0]) / 1e6 if len(p) > 0 else 0.0)
    fig = go.Figure()
    fig.add_trace(go.Bar(name=f"{year - 1} Q{qtr}", x=names, y=pri_vals, marker_color="#475569"))
    fig.add_trace(go.Bar(name=f"{year} Q{qtr}",     x=names, y=cur_vals, marker_color="#3b82f6"))
    layout = {**PLOTLY_BASE, "height": 320, "barmode": "group"}
    fig.update_layout(title=dict(text=title, font=dict(size=14)), yaxis_title="$ Billions", **layout)
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap data
# ─────────────────────────────────────────────────────────────────────────────
bhc_fin_raw, bhc_inst_df, fred_df, bank_fin_raw, bank_inst_df = _load_raw()

scope_data: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {
    "BHC (Y-9C)": (bhc_fin_raw, bhc_inst_df)
}

if bank_fin_raw is not None and bank_inst_df is not None and not bank_fin_raw.empty and not bank_inst_df.empty:
    scope_data["Bank (Call Report)"] = (bank_fin_raw, bank_inst_df)

if "reporting_scope" not in st.session_state:
    st.session_state.reporting_scope = "BHC (Y-9C)"

if st.session_state.reporting_scope not in scope_data:
    st.session_state.reporting_scope = "BHC (Y-9C)"

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
# NOTE: the scope selectbox must be rendered (and session_state updated) BEFORE
# any data is derived from st.session_state.reporting_scope. Otherwise the rest
# of this run would use the *previous* scope's data while the header/caption
# already reflects the newly selected scope, producing a one-run-stale mismatch
# (e.g. "Bank (Call Report)" label shown alongside BHC institutions/values).
with st.sidebar:
    st.markdown("## 🏦 Controls")

    scope_options = list(scope_data.keys())
    scope_idx = scope_options.index(st.session_state.reporting_scope)
    selected_scope = st.selectbox(
        "Reporting scope",
        scope_options,
        index=scope_idx,
        help="Switch between holding company Y-9C data and optional bank-level data if available.",
    )
    if selected_scope != st.session_state.reporting_scope:
        # Scope changed: drop the stale institution selection so the default
        # for the new scope is recomputed below instead of raising a
        # ValueError (or silently reusing an institution name that doesn't
        # exist in the new scope).
        st.session_state.pop("selected_institution", None)
    st.session_state.reporting_scope = selected_scope

    active_fin_raw, active_inst_df = scope_data[st.session_state.reporting_scope]
    fin_q = _convert_ytd_to_quarterly(active_fin_raw)
    inst_opts = _institution_options(active_fin_raw, active_inst_df)
    inst_names = inst_opts["display"].tolist()
    name_to_rssd = dict(zip(inst_opts["display"], inst_opts["rssd_id"]))
    quarter_choices = _available_quarters(active_fin_raw)
    fred_series = _fred_series_map(fred_df)
    model_panel = _model_panel(fin_q, active_inst_df, fred_df)

    if "selected_institution" not in st.session_state or st.session_state.selected_institution not in inst_names:
        default = next((n for n in inst_names if "usaa" in n.lower()), inst_names[0])
        st.session_state.selected_institution = default

    sel_inst = st.selectbox(
        "Institution",
        inst_names,
        index=inst_names.index(st.session_state.selected_institution),
    )
    st.session_state.selected_institution = sel_inst

    sel_quarter = st.selectbox("As-Of Quarter", quarter_choices, index=0)
    years_back = st.select_slider("History window", options=[1, 3, 5, 10, "All"], value=3)

    st.markdown("---")
    st.caption(
        "**Data:** [FR Y-9C](https://www.federalreserve.gov/) · [FRED](https://fred.stlouisfed.org/)  \n"
        "**Dataset:** [Wgurtler/y9c-data](https://huggingface.co/datasets/Wgurtler/y9c-data)"
    )

# Parse controls
q_parts  = sel_quarter.split()
sel_year = int(q_parts[0])
sel_qtr  = int(q_parts[1][1])
start_year: int | None = None if years_back == "All" else sel_year - int(years_back) + 1

rssd = name_to_rssd[sel_inst]
df   = _filter_institution(fin_q, rssd)

# ─────────────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="app-header">
  <h1>📊 Bank Holding Company Y-9C Dashboard</h1>
    <p>{st.session_state.reporting_scope} &nbsp;·&nbsp; {sel_inst} &nbsp;·&nbsp; {sel_quarter}</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Main tabs
# ─────────────────────────────────────────────────────────────────────────────
tab_overview, tab_call_report, tab_cluster, tab_custom, tab_fred, tab_models, tab_monitoring, tab_tables = st.tabs([
    "📊 Overview",
    "🏛 Call Report",
    "🔬 Cluster Analysis",
    "📈 Custom Charts",
    "🌐 Economic Context",
    "🤖 Model Forecasts",
    "🩺 Monitoring",
    "🗂 Data Tables",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    st.markdown(f'<div class="sec-header">Key Metrics — {sel_quarter}</div>', unsafe_allow_html=True)

    kpi_cols = st.columns(6)
    for col, (mdrm, name) in zip(kpi_cols, KEY_METRICS):
        v, _, yoy = get_kpi(df, mdrm, sel_year, sel_qtr)
        direction = "up" if (yoy or 0) > 0 else ("down" if (yoy or 0) < 0 else "")
        delta_cls = "pos" if (yoy or 0) > 0 else ("neg" if (yoy or 0) < 0 else "neu")
        delta_txt = fmt(yoy, "pct") + " YoY" if yoy is not None else "N/A"
        col.markdown(f"""
        <div class="kpi-card {direction}">
          <div class="kpi-label">{name}</div>
          <div class="kpi-value">{fmt(v)}</div>
          <div class="kpi-delta {delta_cls}">{delta_txt}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("")
    st.markdown('<div class="sec-header">Trend Analysis</div>', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            timeseries_fig(df,
                [("Total Assets", "BHCK2170"), ("Net Loans & Leases", "BHCKB528"), ("Total Equity", "BHCK3210")],
                "Balance Sheet Trends", sel_year, sel_qtr, start_year),
            use_container_width=True,
        )
    with c2:
        st.plotly_chart(
            timeseries_fig(df,
                [("Net Interest Income", "BHCK4074"), ("Noninterest Income", "BHCK4079"), ("Net Income", "BHCK4301")],
                "Income Statement Trends", sel_year, sel_qtr, start_year),
            use_container_width=True,
        )

    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(
            timeseries_fig(df,
                [("Domestic IB Deposits", "BHDM6636"), ("Domestic NIB Deposits", "BHDM6631")],
                "Deposit Trends", sel_year, sel_qtr, start_year),
            use_container_width=True,
        )
    with c4:
        st.plotly_chart(
            timeseries_fig(df,
                [("Total Noninterest Expense", "BHCK4093"), ("Salaries & Benefits", "BHCK4135")],
                "Expense Trends", sel_year, sel_qtr, start_year),
            use_container_width=True,
        )

    st.markdown('<div class="sec-header">Year-over-Year Comparison</div>', unsafe_allow_html=True)
    c5, c6 = st.columns(2)
    with c5:
        st.plotly_chart(
            yoy_bar_fig(df,
                [("Total Assets", "BHCK2170"), ("Net Loans & Leases", "BHCKB528"),
                 ("Total Equity", "BHCK3210"), ("Total Liabilities", "BHCK2948")],
                "Balance Sheet — YoY", sel_year, sel_qtr),
            use_container_width=True,
        )
    with c6:
        st.plotly_chart(
            yoy_bar_fig(df,
                [("Net Interest Income", "BHCK4074"), ("Noninterest Income", "BHCK4079"),
                 ("Total Noninterest Expense", "BHCK4093"), ("Net Income", "BHCK4301")],
                "Income Statement — YoY", sel_year, sel_qtr),
            use_container_width=True,
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — CALL REPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_call_report:
    st.markdown('<div class="sec-header">Call Report Analytics</div>', unsafe_allow_html=True)

    if st.session_state.reporting_scope != "Bank (Call Report)":
        st.info("Switch Reporting scope to Bank (Call Report) to use this tab.")
    else:
        def _point_value(code: str, year: int, quarter: int) -> float | None:
            s = df[(df["year"] == year) & (df["quarter"] == quarter) & (df["mdrm_code"] == code)]["value"]
            if s.empty:
                return None
            return float(s.iloc[0])

        def _latest_capital_period() -> tuple[int, int] | None:
            cap_codes = {"FDIC_IDT1RWAJR", "FDIC_RBC1AAJ", "FDIC_RBCRWAJ", "FDIC_IDT1CER"}
            cap_hist = df[df["mdrm_code"].isin(cap_codes)][["year", "quarter"]].drop_duplicates()
            if cap_hist.empty:
                return None
            cap_hist = cap_hist.sort_values(["year", "quarter"])
            last = cap_hist.iloc[-1]
            return int(last["year"]), int(last["quarter"])

        def _ratio(numer: float | None, denom: float | None) -> float | None:
            if numer is None or denom is None or denom == 0:
                return None
            return (numer / denom) * 100

        assets = _point_value("BHCK2170", sel_year, sel_qtr)
        loans = _point_value("BHCKB528", sel_year, sel_qtr)
        deposits = _point_value("BHDM6636", sel_year, sel_qtr)
        equity = _point_value("BHCK3210", sel_year, sel_qtr)
        net_income = _point_value("BHCK4301", sel_year, sel_qtr)
        interest_income = _point_value("BHCK4010", sel_year, sel_qtr)
        interest_expense = _point_value("BHCK4073", sel_year, sel_qtr)
        net_interest_income = _point_value("BHCK4074", sel_year, sel_qtr)
        noninterest_income = _point_value("BHCK4079", sel_year, sel_qtr)
        noninterest_expense = _point_value("BHCK4093", sel_year, sel_qtr)
        borrowing_codes = ["BHDMB993", "BHCKB995", "BHCK2332", "BHCKB571"]
        borrowing_values = [_point_value(c, sel_year, sel_qtr) for c in borrowing_codes]
        borrowings = sum(v for v in borrowing_values if v is not None) if any(
            v is not None for v in borrowing_values
        ) else None
        cap_year, cap_qtr = sel_year, sel_qtr
        cap_tier1_risk = _point_value("FDIC_RBC1AAJ", cap_year, cap_qtr)
        cap_total_risk = _point_value("FDIC_RBCRWAJ", cap_year, cap_qtr)
        cap_cet1 = _point_value("FDIC_IDT1RWAJR", cap_year, cap_qtr)
        cap_tier1 = _point_value("FDIC_IDT1CER", cap_year, cap_qtr)

        if all(v is None for v in [cap_tier1_risk, cap_total_risk, cap_cet1, cap_tier1]):
            latest_cap = _latest_capital_period()
            if latest_cap is not None:
                cap_year, cap_qtr = latest_cap
                cap_tier1_risk = _point_value("FDIC_RBC1AAJ", cap_year, cap_qtr)
                cap_total_risk = _point_value("FDIC_RBCRWAJ", cap_year, cap_qtr)
                cap_cet1 = _point_value("FDIC_IDT1RWAJR", cap_year, cap_qtr)
                cap_tier1 = _point_value("FDIC_IDT1CER", cap_year, cap_qtr)

        kcr1, kcr2, kcr3, kcr4 = st.columns(4)
        kcr1.metric(
            "Loans / Assets",
            f"{_ratio(loans, assets):.2f}%" if _ratio(loans, assets) is not None else "N/A",
            help=(
                "**Formula:** Net Loans & Leases ÷ Total Assets × 100\n\n"
                "**MDRM codes:** BHCKB528 ÷ BHCK2170\n\n"
                "Share of the balance sheet deployed in lending. Higher values indicate a more "
                "loan-heavy (vs. securities/cash-heavy) asset mix."
            ),
        )
        kcr2.metric(
            "Deposits / Assets",
            f"{_ratio(deposits, assets):.2f}%" if _ratio(deposits, assets) is not None else "N/A",
            help=(
                "**Formula:** Interest-Bearing Deposits ÷ Total Assets × 100\n\n"
                "**MDRM codes:** BHDM6636 ÷ BHCK2170\n\n"
                "Reliance on (interest-bearing) customer deposits as a funding source for the "
                "balance sheet."
            ),
        )
        kcr3.metric(
            "Equity / Assets",
            f"{_ratio(equity, assets):.2f}%" if _ratio(equity, assets) is not None else "N/A",
            help=(
                "**Formula:** Total Equity Capital ÷ Total Assets × 100\n\n"
                "**MDRM codes:** BHCK3210 ÷ BHCK2170\n\n"
                "Simple, non-risk-weighted leverage ratio — how much of the balance sheet is "
                "funded by equity vs. liabilities."
            ),
        )
        annualized_roa = _ratio((net_income * 4) if net_income is not None else None, assets)
        kcr4.metric(
            "ROA (annualized)",
            f"{annualized_roa:.2f}%" if annualized_roa is not None else "N/A",
            help=(
                "**Formula:** (Net Income × 4) ÷ Total Assets × 100\n\n"
                "**MDRM codes:** BHCK4301 ÷ BHCK2170\n\n"
                "Quarterly net income is annualized (×4) then divided by total assets to estimate "
                "full-year Return on Assets — a core profitability measure."
            ),
        )

        st.markdown('<div class="sec-header">Profitability &amp; Efficiency Ratios</div>', unsafe_allow_html=True)
        pe1, pe2, pe3, pe4 = st.columns(4)
        annualized_roe = _ratio((net_income * 4) if net_income is not None else None, equity)
        pe1.metric(
            "ROE (annualized)",
            f"{annualized_roe:.2f}%" if annualized_roe is not None else "N/A",
            help=(
                "**Return on Equity**\n\n"
                "**Formula:** (Net Income × 4) ÷ Total Equity Capital × 100\n\n"
                "**MDRM codes:** BHCK4301 ÷ BHCK3210\n\n"
                "Quarterly net income is annualized (×4) then divided by total equity — how much "
                "profit is generated per dollar of shareholder capital."
            ),
        )
        rev_base = (
            (net_interest_income + noninterest_income)
            if net_interest_income is not None and noninterest_income is not None
            else None
        )
        efficiency_ratio = _ratio(noninterest_expense, rev_base)
        pe2.metric(
            "Efficiency Ratio",
            f"{efficiency_ratio:.2f}%" if efficiency_ratio is not None else "N/A",
            help=(
                "**Formula:** Total Noninterest Expense ÷ (Net Interest Income + Total "
                "Noninterest Income) × 100\n\n"
                "**MDRM codes:** BHCK4093 ÷ (BHCK4074 + BHCK4079)\n\n"
                "Overhead cost as a share of total revenue. Lower is better — indicates fewer "
                "dollars of expense needed to generate each dollar of revenue."
            ),
        )
        annualized_asset_yield = _ratio((interest_income * 4) if interest_income is not None else None, assets)
        pe3.metric(
            "Asset Yield",
            f"{annualized_asset_yield:.2f}%" if annualized_asset_yield is not None else "N/A",
            help=(
                "**Formula:** (Total Interest Income × 4) ÷ Total Assets × 100\n\n"
                "**MDRM codes:** BHCK4010 ÷ BHCK2170\n\n"
                "Annualized interest income earned relative to total assets — a proxy for the "
                "average yield generated on the earning-asset base."
            ),
        )
        funding_base = (
            (deposits + borrowings)
            if deposits is not None and borrowings is not None
            else deposits
        )
        annualized_funding_cost = _ratio(
            (interest_expense * 4) if interest_expense is not None else None, funding_base
        )
        pe4.metric(
            "Deposit/Borrowing Expense Ratio",
            f"{annualized_funding_cost:.2f}%" if annualized_funding_cost is not None else "N/A",
            help=(
                "**Formula:** (Total Interest Expense × 4) ÷ (Deposits + Borrowings) × 100\n\n"
                "**MDRM codes:** BHCK4073 ÷ (BHDM6636 + BHDMB993 + BHCKB995 + BHCK2332 + "
                "BHCKB571)\n\n"
                "Annualized cost of funds — interest expense relative to interest-bearing "
                "deposits and borrowed money (fed funds purchased, repos, and other borrowed "
                "money)."
            ),
        )

        st.markdown('<div class="sec-header">Regulatory Capital Ratios</div>', unsafe_allow_html=True)
        if (cap_year, cap_qtr) != (sel_year, sel_qtr):
            st.caption(f"Showing latest available capital quarter for this bank: {cap_year} Q{cap_qtr}.")
        cap1, cap2, cap3, cap4 = st.columns(4)
        cap1.metric(
            "CET1",
            f"{cap_cet1:.2f}%" if cap_cet1 is not None else "N/A",
            help=(
                "**Common Equity Tier 1 (CET1) capital ratio**\n\n"
                "**Formula:** CET1 Capital ÷ Risk-Weighted Assets × 100\n\n"
                "**FDIC field:** IDT1RWAJR\n\n"
                "The core Basel III capital adequacy measure — highest-quality capital (common "
                "stock + retained earnings) relative to risk-weighted assets. Regulatory minimum "
                "is 4.5%, with additional buffers typically required."
            ),
        )
        cap2.metric(
            "Tier 1 Risk-Based",
            f"{cap_tier1_risk:.2f}%" if cap_tier1_risk is not None else "N/A",
            help=(
                "**Tier 1 risk-based capital ratio**\n\n"
                "**Formula:** Tier 1 Capital ÷ Risk-Weighted Assets × 100\n\n"
                "**FDIC field:** RBC1AAJ\n\n"
                "Tier 1 capital (CET1 + additional Tier 1 instruments) as a share of risk-weighted "
                "assets. Regulatory minimum is 6%."
            ),
        )
        cap3.metric(
            "Total Risk-Based",
            f"{cap_total_risk:.2f}%" if cap_total_risk is not None else "N/A",
            help=(
                "**Total risk-based capital ratio**\n\n"
                "**Formula:** Total Capital (Tier 1 + Tier 2) ÷ Risk-Weighted Assets × 100\n\n"
                "**FDIC field:** RBCRWAJ\n\n"
                "The broadest regulatory capital ratio, including supplementary (Tier 2) capital "
                "such as loan-loss reserves and subordinated debt. Regulatory minimum is 8%."
            ),
        )
        cap4.metric(
            "Tier 1 Capital",
            f"{cap_tier1:.2f}%" if cap_tier1 is not None else "N/A",
            help=(
                "**Tier 1 leverage ratio**\n\n"
                "**Formula:** Tier 1 Capital ÷ Average Total Consolidated Assets × 100\n\n"
                "**FDIC field:** IDT1CER\n\n"
                "Unlike the other ratios, the denominator is total assets rather than "
                "risk-weighted assets — a non-risk-based backstop measure. Regulatory minimum is "
                "typically 4%."
            ),
        )

        available_codes = set(df["mdrm_code"].dropna().astype(str).unique())
        capital_pairs = [
            (name, code) for name, code in CALL_REPORT_CAPITAL_METRICS
            if code in available_codes
        ]
        if capital_pairs:
            st.plotly_chart(
                timeseries_fig(
                    df,
                    capital_pairs,
                    f"{sel_inst} — Regulatory capital ratio trends",
                    sel_year,
                    sel_qtr,
                    start_year,
                    height=360,
                ),
                use_container_width=True,
            )
        else:
            st.info("Regulatory capital ratio fields are not available yet for this bank dataset version.")

        st.markdown('<div class="sec-header">Call Report Trend Explorer</div>', unsafe_allow_html=True)
        call_metric_options = [
            name for name, code in CALL_REPORT_METRICS if code in available_codes
        ]
        default_call_metrics = [
            m for m in ["Total Assets", "Net Loans & Leases", "Interest-Bearing Deposits", "Net Income"]
            if m in call_metric_options
        ]
        selected_call_metrics = st.multiselect(
            "Call Report metrics",
            options=call_metric_options,
            default=default_call_metrics,
            key="call_report_metrics",
        )

        if selected_call_metrics:
            call_pairs = [(name, CALL_REPORT_NAME_TO_MDRM[name]) for name in selected_call_metrics]
            st.plotly_chart(
                timeseries_fig(
                    df,
                    call_pairs,
                    f"{sel_inst} — Call Report trends",
                    sel_year,
                    sel_qtr,
                    start_year,
                    height=430,
                ),
                use_container_width=True,
            )
        else:
            st.info("Select at least one Call Report metric.")

        st.markdown('<div class="sec-header">Balance Sheet Mix (Selected Quarter)</div>', unsafe_allow_html=True)
        mix_rows = [
            ("Assets", assets),
            ("Loans", loans),
            ("Deposits", deposits),
            ("Equity", equity),
            ("Liabilities", _point_value("BHCK2948", sel_year, sel_qtr)),
        ]
        mix_df = pd.DataFrame(mix_rows, columns=["component", "value"]).dropna()
        if mix_df.empty:
            st.info("No call report mix values are available for this quarter.")
        else:
            fig_mix = px.bar(
                mix_df,
                x="component",
                y="value",
                color="component",
                title=f"{sel_inst} — {sel_year} Q{sel_qtr}",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_mix.update_layout(yaxis_title="$", **{**PLOTLY_BASE, "height": 360, "showlegend": False})
            st.plotly_chart(fig_mix, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CLUSTER ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
with tab_cluster:
    st.markdown('<div class="sec-header">Peer Cluster Analysis</div>', unsafe_allow_html=True)
    st.caption(
        "All institutions are clustered by financial profile using K-Means on the selected metrics, "
        "then projected to 2-D via PCA. The ★ marks the currently selected institution. "
        "**Click any point** to load that institution across all tabs."
    )

    fc1, fc2, fc3 = st.columns([3, 1, 1])
    with fc1:
        cluster_features = st.multiselect(
            "Clustering features",
            options=[n for n, _ in METRIC_CHOICES],
            default=CLUSTER_FEATURE_DEFAULTS,
            key="cluster_feat",
        )
    with fc2:
        cluster_qtr_str = st.selectbox("Cluster as-of", quarter_choices, index=0, key="cluster_qtr")
    with fc3:
        n_clusters = st.slider("# Clusters (k)", 2, 8, 4, key="n_clusters")

    if len(cluster_features) < 2:
        st.info("Select at least 2 features to enable clustering.")
    else:
        cdf, ev1, ev2 = _compute_clusters(
            fin_q, active_inst_df,
            cluster_qtr_str,
            tuple(cluster_features),
            n_clusters,
        )

        if len(cdf) < 2:
            st.info(
                "Not enough institutions with complete values for the selected quarter and features to run clustering. "
                "Try fewer features or a different quarter."
            )
        else:
            cdf["cluster_label"] = "Cluster " + (cdf["cluster"] + 1).astype(str)
            # Marker size proportional to sqrt(assets), default to 1 if missing
            ta = cdf["total_assets"].fillna(1e6) if "total_assets" in cdf.columns else pd.Series(1e6, index=cdf.index)
            cdf["_sz"] = np.sqrt(np.clip(ta / 1e6, 0.5, 2000))

            # Build scatter
            fig_cl = px.scatter(
                cdf.reset_index(),
                x="pc1", y="pc2",
                color="cluster_label",
                size="_sz",
                size_max=45,
                hover_name="display_name",
                custom_data=["display_name"],
                color_discrete_sequence=px.colors.qualitative.Set2,
                labels={
                    "pc1": f"PC 1 · {ev1 * 100:.1f}% variance explained" if pd.notna(ev1) else "PC 1",
                    "pc2": f"PC 2 · {ev2 * 100:.1f}% variance explained" if pd.notna(ev2) else "PC 2",
                    "cluster_label": "Cluster",
                },
                title=f"Institution Clusters — {cluster_qtr_str}  (k={n_clusters}, PCA projection)",
            )

            # Star for the currently selected institution
            if rssd in cdf.index:
                row = cdf.loc[rssd]
                fig_cl.add_trace(go.Scatter(
                    x=[row["pc1"]], y=[row["pc2"]],
                    mode="markers+text",
                    marker=dict(symbol="star", size=20, color="#f59e0b",
                                line=dict(color="white", width=1.5)),
                    text=[sel_inst.split()[-1]],
                    textposition="top center",
                    textfont=dict(color="#f59e0b", size=10),
                    name=f"★ {sel_inst}",
                    customdata=[[sel_inst]],
                    showlegend=True,
                ))

            fig_cl.update_layout(
                **{**PLOTLY_BASE,
                   "height": 540,
                   "hovermode": "closest",
                   "xaxis": dict(showgrid=True, gridcolor="#1e293b", zeroline=True,
                                 zerolinecolor="#334155", zerolinewidth=1),
                   "yaxis": dict(showgrid=True, gridcolor="#1e293b", zeroline=True,
                                 zerolinecolor="#334155", zerolinewidth=1),
                   },
            )

            # Render with point-selection support
            sel_event = st.plotly_chart(
                fig_cl,
                use_container_width=True,
                on_select="rerun",
                key="cluster_scatter",
            )

            # Handle click → update selected institution
            if sel_event and getattr(sel_event, "selection", None):
                pts = sel_event.selection.points
                if pts:
                    pt = pts[0]
                    cd = pt.get("customdata")
                    clicked = cd[0] if cd else None
                    if clicked and clicked in inst_names and clicked != st.session_state.selected_institution:
                        st.session_state.selected_institution = clicked
                        st.rerun()

            # ── Cluster summary table ──────────────────────────────────────────
            st.markdown('<div class="sec-header">Cluster Summary</div>', unsafe_allow_html=True)

            feat_in_df = [f for f in cluster_features if f in cdf.columns]
            agg_dict: dict = {"# Institutions": ("display_name", "count")}
            for f in feat_in_df:
                agg_dict[f"Avg {f}"] = (f, "mean")
            summary = cdf.groupby("cluster_label").agg(**agg_dict).reset_index()
            for col in summary.columns:
                if col.startswith("Avg "):
                    summary[col] = summary[col].apply(fmt)
            summary = summary.rename(columns={"cluster_label": "Cluster"})
            st.dataframe(summary, use_container_width=True, hide_index=True)

            # ── Cluster members explorer ───────────────────────────────────────
            st.markdown('<div class="sec-header">Cluster Members</div>', unsafe_allow_html=True)
            cl_filter = st.selectbox(
                "Show members of",
                sorted(cdf["cluster_label"].unique()),
                key="cl_member_filter",
            )
            members = cdf[cdf["cluster_label"] == cl_filter].copy()
            disp_cols = ["display_name", "total_assets"] + feat_in_df
            disp_cols = [c for c in disp_cols if c in members.columns]
            members = members[disp_cols].sort_values("total_assets", ascending=False)
            for col in members.columns:
                if col not in ("display_name",):
                    members[col] = members[col].apply(fmt)
            members = members.rename(columns={"display_name": "Institution", "total_assets": "Total Assets"})
            st.dataframe(members, use_container_width=True, hide_index=True)

            # ── Cluster scatter heat (PC1 distribution per cluster) ────────────
            st.markdown('<div class="sec-header">PC1 Distribution by Cluster</div>', unsafe_allow_html=True)
            fig_box = go.Figure()
            for i, cl in enumerate(sorted(cdf["cluster_label"].unique())):
                sub = cdf[cdf["cluster_label"] == cl]
                fig_box.add_trace(go.Violin(
                    x=[cl] * len(sub), y=sub["pc1"].values,
                    name=cl, box_visible=True, meanline_visible=True,
                    fillcolor=px.colors.qualitative.Set2[i % len(px.colors.qualitative.Set2)],
                    line_color="white", opacity=0.6,
                ))
            fig_box.update_layout(
                **{**PLOTLY_BASE, "height": 320, "showlegend": False},
                title="PC 1 Score Distribution per Cluster",
                xaxis_title="Cluster", yaxis_title="PC 1 Score",
            )
            st.plotly_chart(fig_box, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CUSTOM CHARTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_custom:
    st.markdown('<div class="sec-header">Custom Metric Chart</div>', unsafe_allow_html=True)
    custom_sel = st.multiselect(
        "Select metrics to chart",
        options=[n for n, _ in METRIC_CHOICES],
        default=["Total Assets", "Net Loans & Leases", "Net Interest Income", "Net Income"],
        key="custom_metrics",
    )
    if custom_sel:
        pairs = [(n, NAME_TO_MDRM[n]) for n in custom_sel if n in NAME_TO_MDRM]
        st.plotly_chart(
            timeseries_fig(df, pairs, f"{sel_inst} — Trend", sel_year, sel_qtr, start_year, height=420),
            use_container_width=True,
        )
        st.plotly_chart(
            yoy_bar_fig(df, pairs, "Year-over-Year Comparison", sel_year, sel_qtr),
            use_container_width=True,
        )
    else:
        st.info("Select at least one metric above.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — ECONOMIC CONTEXT (FRED)
# ══════════════════════════════════════════════════════════════════════════════
with tab_fred:
    st.markdown('<div class="sec-header">Macro Economic Context (FRED)</div>', unsafe_allow_html=True)

    fred = fred_series

    if not fred:
        st.info(
            "FRED data unavailable.  "
            "Set `FRED_API_KEY` in your `.env` file (local) or Hugging Face Space secrets to enable this tab."
        )
    else:
        def _fred_line(series_names: list[str], title: str, y_label: str = "%") -> go.Figure:
            fig = go.Figure()
            for i, sn in enumerate(series_names):
                if sn not in fred:
                    continue
                s = fred[sn]
                if start_year:
                    s = s[s.index.year >= start_year]
                x = [f"{d.year} Q{(d.month - 1) // 3 + 1}" for d in s.index]
                fig.add_trace(go.Scatter(
                    x=x, y=s.values, name=sn,
                    line=dict(color=COLORS[i % len(COLORS)], width=2),
                    mode="lines",
                ))
            fig.update_layout(title=dict(text=title, font=dict(size=14)),
                              yaxis_title=y_label, **PLOTLY_BASE)
            return fig

        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(
                _fred_line(["Fed Funds Rate", "2Y Treasury Yield", "10Y Treasury Yield"],
                           "Interest Rates", "%"),
                use_container_width=True,
            )
        with c2:
            st.plotly_chart(
                _fred_line(["Yield Curve (10Y-2Y)"], "Yield Curve (10Y − 2Y)", "%"),
                use_container_width=True,
            )

        c3, c4 = st.columns(2)
        with c3:
            st.plotly_chart(
                _fred_line(["HY Credit Spread (OAS)", "IG Credit Spread (OAS)"],
                           "Credit Spreads (OAS)", "%"),
                use_container_width=True,
            )
        with c4:
            st.plotly_chart(
                _fred_line(["Unemployment Rate", "CPI Inflation (YoY %)",
                            "Mortgage Delinquency", "Credit Card Delinquency"],
                           "Delinquency & Macro", "%"),
                use_container_width=True,
            )

        # ── Overlay: bank metric vs FRED ──────────────────────────────────
        st.markdown('<div class="sec-header">Bank Metric vs Economic Indicator</div>',
                    unsafe_allow_html=True)

        oc1, oc2 = st.columns(2)
        with oc1:
            overlay_bank = st.selectbox(
                "Bank metric (left axis)",
                [n for n, _ in METRIC_CHOICES],
                index=next((i for i, (n, _) in enumerate(METRIC_CHOICES) if "Net Interest Income" in n), 0),
                key="ov_bank",
            )
        with oc2:
            overlay_fred_key = st.selectbox(
                "FRED series (right axis)",
                list(fred.keys()),
                index=next((i for i, k in enumerate(fred) if "Fed Funds" in k), 0),
                key="ov_fred",
            )

        fig_ov = go.Figure()
        mdrm_ov = NAME_TO_MDRM.get(overlay_bank)
        if mdrm_ov:
            bdata = df[df["mdrm_code"] == mdrm_ov].sort_values(["year", "quarter"])
            if start_year:
                bdata = bdata[bdata["year"] >= start_year]
            bx = [f"{int(r['year'])} Q{int(r['quarter'])}" for _, r in bdata.iterrows()]
            fig_ov.add_trace(go.Scatter(
                x=bx, y=bdata["value"] / 1e6,
                name=overlay_bank, yaxis="y",
                line=dict(color="#3b82f6", width=2),
            ))
        if overlay_fred_key in fred:
            fs = fred[overlay_fred_key]
            if start_year:
                fs = fs[fs.index.year >= start_year]
            fx = [f"{d.year} Q{(d.month - 1) // 3 + 1}" for d in fs.index]
            fig_ov.add_trace(go.Scatter(
                x=fx, y=fs.values,
                name=overlay_fred_key, yaxis="y2",
                line=dict(color="#ef4444", width=2, dash="dot"),
            ))

        ov_layout = {**PLOTLY_BASE, "height": 440, "hovermode": "x unified"}
        ov_layout["yaxis"] = dict(
            title=f"{overlay_bank} ($ Billions)",
            color="#3b82f6",
            showgrid=True,
            gridcolor="#1e293b",
        )
        ov_layout["yaxis2"] = dict(
            title=overlay_fred_key,
            overlaying="y",
            side="right",
            color="#ef4444",
            showgrid=False,
        )
        fig_ov.update_layout(
            title=dict(text=f"{overlay_bank}  vs  {overlay_fred_key}", font=dict(size=14)),
            **ov_layout,
        )
        st.plotly_chart(fig_ov, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — MODEL FORECASTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_models:
    st.markdown('<div class="sec-header">Line Item Forecasts</div>', unsafe_allow_html=True)
    st.caption(
        "Runs bank-level rolling forecasts from the published Hugging Face dataset using the selected model and line item. "
        "Historical predictions are out-of-sample rolling cross-validation results, and the future point forecast is trained on all available quarters."
    )

    statement_labels = {
        "balance_sheet": "Balance Sheet",
        "income_statement": "Income Statement",
    }
    model_keys = list(MODEL_LABELS.keys())
    horizon_labels = list(HORIZON_LABELS.keys())

    mf1, mf2, mf3, mf4 = st.columns([1, 2, 1, 1])
    with mf1:
        model_statement = st.selectbox(
            "Statement",
            options=list(statement_labels.keys()),
            format_func=lambda key: statement_labels[key],
            key="model_statement",
        )

    model_targets = _statement_target_frame(model_panel, model_statement)
    model_target_options = model_targets["mdrm_code"].tolist()
    if not model_target_options:
        st.info("No compatible model targets were found for the selected reporting scope.")
    else:
        default_model_target = "BHCK4301" if model_statement == "income_statement" and "BHCK4301" in model_target_options else model_target_options[0]

        with mf2:
            selected_model_target = st.selectbox(
                "Line item",
                options=model_target_options,
                index=model_target_options.index(default_model_target),
                format_func=lambda code: f"{model_targets.loc[model_targets['mdrm_code'] == code, 'account_name'].iloc[0]} ({code})",
                key="model_target",
            )
        with mf3:
            selected_model_name = st.selectbox(
                "Model",
                options=model_keys,
                format_func=lambda key: MODEL_LABELS[key],
                key="model_name",
            )
        with mf4:
            selected_horizon_label = st.selectbox(
                "Forecast horizon",
                options=horizon_labels,
                key="model_horizon",
            )

        try:
            forecast_result = _cached_forecast(
                model_panel,
                rssd,
                selected_model_target,
                selected_model_name,
                HORIZON_LABELS[selected_horizon_label],
            )

            actual_series = df[df["mdrm_code"] == selected_model_target].sort_values(["year", "quarter"])
            fig_model = go.Figure()
            if not actual_series.empty:
                fig_model.add_trace(go.Scatter(
                    x=[f"{int(r['year'])} Q{int(r['quarter'])}" for _, r in actual_series.iterrows()],
                    y=actual_series["value"] / 1e6,
                    name="Actual history",
                    mode="lines+markers",
                    line=dict(color="#94a3b8", width=2),
                ))

            pred_df = forecast_result.predictions.copy()
            pred_df["target_label"] = pred_df["target_date"].dt.to_period("Q").astype(str).str.replace("Q", " Q", regex=False)
            fig_model.add_trace(go.Scatter(
                x=pred_df["target_label"],
                y=pred_df["actual"] / 1e6,
                name="CV actual",
                mode="lines",
                line=dict(color="#38bdf8", width=2),
            ))
            fig_model.add_trace(go.Scatter(
                x=pred_df["target_label"],
                y=pred_df["predicted"] / 1e6,
                name="CV predicted",
                mode="lines+markers",
                line=dict(color="#f59e0b", width=2, dash="dot"),
                marker=dict(size=5),
            ))

            if not forecast_result.future_forecast.empty:
                future_row = forecast_result.future_forecast.iloc[0]
                future_label = pd.Period(future_row["forecast_date"], freq="Q").strftime("%Y Q%q")
                origin_label = pd.Period(future_row["origin_date"], freq="Q").strftime("%Y Q%q")
                fig_model.add_trace(go.Scatter(
                    x=[origin_label, future_label],
                    y=[np.nan, future_row["forecast_value"] / 1e6],
                    name="Future forecast",
                    mode="lines+markers",
                    line=dict(color="#22c55e", width=3, dash="dash"),
                    marker=dict(size=9),
                ))

            fig_model.update_layout(
                title=f"{MODEL_LABELS[selected_model_name]} forecast — {forecast_result.target_name}",
                yaxis_title="$ Billions",
                **{**PLOTLY_BASE, "height": 440},
            )
            st.plotly_chart(fig_model, use_container_width=True)

            k1, k2, k3, k4 = st.columns(4)
            metrics_row = forecast_result.metrics.iloc[0]
            k1.metric("MAE", fmt(metrics_row["mae"]))
            k2.metric("RMSE", fmt(metrics_row["rmse"]))
            k3.metric("MAPE", f"{metrics_row['mape']:.1f}%" if pd.notna(metrics_row["mape"]) else "N/A")
            k4.metric("R²", f"{metrics_row['r2']:.2f}")

            mc1, mc2 = st.columns([1, 1])
            with mc1:
                st.markdown('<div class="sec-header">Future Forecast</div>', unsafe_allow_html=True)
                if forecast_result.future_forecast.empty:
                    st.info("No forward forecast could be produced for the selected line item.")
                else:
                    future_display = forecast_result.future_forecast.copy()
                    future_display["origin_date"] = future_display["origin_date"].dt.strftime("%Y-%m-%d")
                    future_display["forecast_date"] = future_display["forecast_date"].dt.strftime("%Y-%m-%d")
                    future_display["forecast_value"] = future_display["forecast_value"].apply(fmt)
                    st.dataframe(future_display, use_container_width=True, hide_index=True)

            with mc2:
                st.markdown('<div class="sec-header">Top Drivers</div>', unsafe_allow_html=True)
                top_features = forecast_result.feature_importance.head(12).copy()
                fig_imp = px.bar(
                    top_features.sort_values("importance"),
                    x="importance",
                    y="feature",
                    orientation="h",
                    color="importance",
                    color_continuous_scale="Blues",
                    title="Feature importance",
                )
                fig_imp.update_layout(**{**PLOTLY_BASE, "height": 420, "showlegend": False})
                st.plotly_chart(fig_imp, use_container_width=True)

            st.markdown('<div class="sec-header">Rolling Prediction Details</div>', unsafe_allow_html=True)
            pred_display = pred_df[["fold", "origin_date", "target_date", "actual", "predicted", "residual", "abs_error", "ape"]].copy()
            pred_display["origin_date"] = pred_display["origin_date"].dt.strftime("%Y-%m-%d")
            pred_display["target_date"] = pred_display["target_date"].dt.strftime("%Y-%m-%d")
            for col in ["actual", "predicted", "residual", "abs_error"]:
                pred_display[col] = pred_display[col].apply(fmt)
            pred_display["ape"] = pred_display["ape"].apply(lambda v: f"{v:.1f}%" if pd.notna(v) else "N/A")
            st.dataframe(pred_display, use_container_width=True, hide_index=True)
        except ValueError as exc:
            st.info(str(exc))

# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — MONITORING
# ══════════════════════════════════════════════════════════════════════════════
with tab_monitoring:
    st.markdown('<div class="sec-header">Rolling Cross-Validation Monitoring</div>', unsafe_allow_html=True)
    st.caption(
        "Compares 1-year-ahead and 3-year-ahead rolling out-of-sample forecasts for the selected bank, line item, and model. "
        "Use this tab to monitor where forecast error is improving or breaking down over time."
    )

    mon1, mon2, mon3 = st.columns([1, 2, 1])
    with mon1:
        monitoring_statement = st.selectbox(
            "Statement",
            options=list(statement_labels.keys()),
            format_func=lambda key: statement_labels[key],
            key="monitoring_statement",
        )

    monitoring_targets = _statement_target_frame(model_panel, monitoring_statement)
    monitoring_target_codes = monitoring_targets["mdrm_code"].tolist()
    if not monitoring_target_codes:
        st.info("No compatible monitoring targets were found for the selected reporting scope.")
    else:
        default_monitoring_target = "BHCK2170" if monitoring_statement == "balance_sheet" and "BHCK2170" in monitoring_target_codes else monitoring_target_codes[0]

        with mon2:
            monitoring_target = st.selectbox(
                "Line item",
                options=monitoring_target_codes,
                index=monitoring_target_codes.index(default_monitoring_target),
                format_func=lambda code: f"{monitoring_targets.loc[monitoring_targets['mdrm_code'] == code, 'account_name'].iloc[0]} ({code})",
                key="monitoring_target",
            )
        with mon3:
            monitoring_model = st.selectbox(
                "Model",
                options=model_keys,
                format_func=lambda key: MODEL_LABELS[key],
                key="monitoring_model",
            )

        try:
            monitoring_results = _cached_monitoring(model_panel, rssd, monitoring_target, monitoring_model)

            metrics_frames = []
            pred_frames = []
            for label, horizon in HORIZON_LABELS.items():
                result = monitoring_results[horizon]
                metric_row = result.metrics.copy()
                metric_row.insert(0, "horizon", label)
                metrics_frames.append(metric_row)

                preds = result.predictions.copy()
                preds["horizon"] = label
                preds["target_label"] = preds["target_date"].dt.to_period("Q").astype(str).str.replace("Q", " Q", regex=False)
                pred_frames.append(preds)

            metrics_compare = pd.concat(metrics_frames, ignore_index=True)
            preds_compare = pd.concat(pred_frames, ignore_index=True)

            cmon1, cmon2 = st.columns(2)
            with cmon1:
                st.markdown('<div class="sec-header">Metric Comparison</div>', unsafe_allow_html=True)
                display_metrics = metrics_compare[["horizon", "folds", "mae", "rmse", "mape", "r2"]].copy()
                display_metrics["mae"] = display_metrics["mae"].apply(fmt)
                display_metrics["rmse"] = display_metrics["rmse"].apply(fmt)
                display_metrics["mape"] = display_metrics["mape"].apply(lambda v: f"{v:.1f}%" if pd.notna(v) else "N/A")
                display_metrics["r2"] = display_metrics["r2"].apply(lambda v: f"{v:.2f}")
                st.dataframe(display_metrics, use_container_width=True, hide_index=True)

            with cmon2:
                st.markdown('<div class="sec-header">Average Absolute Error by Horizon</div>', unsafe_allow_html=True)
                fig_metric_bar = px.bar(
                    metrics_compare,
                    x="horizon",
                    y="mae",
                    color="horizon",
                    title="MAE by forecast horizon",
                    color_discrete_sequence=["#38bdf8", "#f59e0b"],
                )
                fig_metric_bar.update_layout(**{**PLOTLY_BASE, "height": 320, "showlegend": False})
                st.plotly_chart(fig_metric_bar, use_container_width=True)

            fig_monitor = go.Figure()
            for label, color in [("1 Year (4Q)", "#38bdf8"), ("3 Years (12Q)", "#f59e0b")]:
                sub = preds_compare[preds_compare["horizon"] == label]
                fig_monitor.add_trace(go.Scatter(
                    x=sub["target_label"],
                    y=sub["actual"] / 1e6,
                    name=f"Actual ({label})",
                    mode="lines",
                    line=dict(color=color, width=2),
                ))
                fig_monitor.add_trace(go.Scatter(
                    x=sub["target_label"],
                    y=sub["predicted"] / 1e6,
                    name=f"Predicted ({label})",
                    mode="lines+markers",
                    line=dict(color=color, width=2, dash="dot"),
                    marker=dict(size=5),
                ))
            fig_monitor.update_layout(
                title="Rolling out-of-sample predictions by horizon",
                yaxis_title="$ Billions",
                **{**PLOTLY_BASE, "height": 430},
            )
            st.plotly_chart(fig_monitor, use_container_width=True)

            fig_error = px.line(
                preds_compare,
                x="target_date",
                y="abs_error",
                color="horizon",
                markers=True,
                title="Absolute error over time",
                color_discrete_sequence=["#38bdf8", "#f59e0b"],
            )
            fig_error.update_layout(yaxis_title="Absolute error", **{**PLOTLY_BASE, "height": 360})
            st.plotly_chart(fig_error, use_container_width=True)

            detail_display = preds_compare[["horizon", "fold", "origin_date", "target_date", "actual", "predicted", "abs_error", "ape"]].copy()
            detail_display["origin_date"] = detail_display["origin_date"].dt.strftime("%Y-%m-%d")
            detail_display["target_date"] = detail_display["target_date"].dt.strftime("%Y-%m-%d")
            for col in ["actual", "predicted", "abs_error"]:
                detail_display[col] = detail_display[col].apply(fmt)
            detail_display["ape"] = detail_display["ape"].apply(lambda v: f"{v:.1f}%" if pd.notna(v) else "N/A")
            st.markdown('<div class="sec-header">Fold-Level Monitoring Table</div>', unsafe_allow_html=True)
            st.dataframe(detail_display, use_container_width=True, hide_index=True)
        except ValueError as exc:
            st.info(str(exc))

# ══════════════════════════════════════════════════════════════════════════════
# TAB 8 — DATA TABLES
# ══════════════════════════════════════════════════════════════════════════════
with tab_tables:
    def _pivot_table(stmt_type: str) -> pd.DataFrame | None:
        sub = df[df["statement_type"] == stmt_type].copy()
        if sub.empty:
            return None
        piv = sub.pivot_table(
            index=["category", "account_name"],
            columns=["year", "quarter"],
            values="value",
            aggfunc="first",
        )
        piv.columns = [f"{int(y)} Q{int(q)}" for y, q in piv.columns]
        # Apply formatting to every cell
        for col in piv.columns:
            piv[col] = piv[col].apply(_fmt_cell)
        piv.index.names = ["Category", "Line Item"]
        return piv.reset_index().sort_values(["Category", "Line Item"]).reset_index(drop=True)

    st.markdown('<div class="sec-header">Balance Sheet</div>', unsafe_allow_html=True)
    bs_tbl = _pivot_table("balance_sheet")
    if bs_tbl is not None:
        st.dataframe(bs_tbl, use_container_width=True, hide_index=True)
    else:
        st.info("No balance sheet data for the selected institution.")

    st.markdown('<div class="sec-header">Income Statement (quarterly)</div>', unsafe_allow_html=True)
    is_tbl = _pivot_table("income_statement")
    if is_tbl is not None:
        st.dataframe(is_tbl, use_container_width=True, hide_index=True)
    else:
        st.info("No income statement data for the selected institution.")

    st.markdown('<div class="sec-header">Download Raw Data</div>', unsafe_allow_html=True)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇ Download as CSV",
        data=csv_bytes,
        file_name=f"{sel_inst.replace(' ', '_')}_{sel_quarter.replace(' ', '_')}.csv",
        mime="text/csv",
    )
