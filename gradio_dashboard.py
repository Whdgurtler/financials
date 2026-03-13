"""
Bank Holding Company Y-9C Financial Dashboard
Gradio interface for viewing quarterly financial data with Y-o-Y comparisons.
Supports all major U.S. bank holding companies.
"""

import gradio as gr
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from src.y9c.fred_data import load_fred_data, FRED_SERIES_NAMES, get_unit

HF_REPO = "Wgurtler/y9c-data"
_FINANCIAL_DF: pd.DataFrame | None = None
_INSTITUTIONS_DF: pd.DataFrame | None = None


def _load_hf_data():
    global _FINANCIAL_DF, _INSTITUTIONS_DF
    if _FINANCIAL_DF is None:
        print("Loading data from Hugging Face...")
        _FINANCIAL_DF = pd.read_parquet(f"hf://datasets/{HF_REPO}/financial_data.parquet")
        _INSTITUTIONS_DF = pd.read_parquet(f"hf://datasets/{HF_REPO}/institutions.parquet")
        print(f"Loaded {len(_FINANCIAL_DF):,} rows")


def load_financial_data(rssd_id=None):
    """Load financial data for a specific institution."""
    _load_hf_data()
    df = _FINANCIAL_DF
    if rssd_id:
        df = df[df["rssd_id"] == rssd_id]
    return df[["report_date", "year", "quarter", "mdrm_code", "value",
               "account_name", "statement_type", "category"]].sort_values(["year", "quarter"])


def get_available_institutions():
    """Get list of institutions sorted by latest quarter total assets."""
    try:
        _load_hf_data()
        fin = _FINANCIAL_DF
        assets = fin[fin["mdrm_code"] == "BHCK2170"]
        max_year = assets["year"].max()
        max_quarter = assets[assets["year"] == max_year]["quarter"].max()
        latest = assets[(assets["year"] == max_year) & (assets["quarter"] == max_quarter)]
        result = latest[["rssd_id", "value"]].rename(columns={"value": "total_assets"})
        result = result.merge(_INSTITUTIONS_DF[["rssd_id", "name"]], on="rssd_id", how="left")
        return result[["rssd_id", "name", "total_assets"]].sort_values("total_assets", ascending=False)
    except Exception:
        return pd.DataFrame(columns=["rssd_id", "name", "total_assets"])


def get_quarter_data(df, year, quarter):
    """Get data for a specific quarter."""
    return df[(df["year"] == year) & (df["quarter"] == quarter)]


def get_prior_year_quarter_data(df, year, quarter):
    """Get data for same quarter in prior year."""
    return df[(df["year"] == year - 1) & (df["quarter"] == quarter)]


def format_value(value, format_type="currency"):
    """Format values for display. DB values are in thousands of dollars."""
    if pd.isna(value) or value is None:
        return "N/A"
    if format_type == "currency":
        if abs(value) >= 1e9:       # >= $1 trillion
            return f"${value/1e9:.2f}T"
        elif abs(value) >= 1e6:     # >= $1 billion
            return f"${value/1e6:.1f}B"
        elif abs(value) >= 1e3:     # >= $1 million
            return f"${value/1e3:.1f}M"
        else:
            return f"${value:,.0f}K"
    elif format_type == "percent":
        return f"{value:+.1f}%"
    return str(value)


def calculate_yoy_change(current, prior):
    """Calculate year-over-year percentage change."""
    if prior == 0 or pd.isna(prior) or pd.isna(current) or prior is None or current is None:
        return None
    return ((current - prior) / abs(prior)) * 100


def create_summary_stats(df, selected_year, selected_quarter):
    """Create summary statistics with Y-o-Y comparisons for selected quarter."""
    current_data = get_quarter_data(df, selected_year, selected_quarter)
    prior_year_data = get_prior_year_quarter_data(df, selected_year, selected_quarter)

    # Key metrics to display
    key_metrics = [
        ("BHCK2170", "Total Assets"),
        ("BHCK3210", "Total Equity"),
        ("BHCKB528", "Net Loans"),
        ("BHCK4074", "Net Interest Income"),
        ("BHCK4301", "Net Income"),
        ("BHCK4079", "Noninterest Income"),
    ]

    stats = []
    for mdrm, name in key_metrics:
        current_val = current_data[current_data["mdrm_code"] == mdrm]["value"].values
        prior_val = prior_year_data[prior_year_data["mdrm_code"] == mdrm]["value"].values if len(prior_year_data) > 0 else []

        current = current_val[0] if len(current_val) > 0 else None
        prior = prior_val[0] if len(prior_val) > 0 else None

        yoy = calculate_yoy_change(current, prior)

        stats.append({
            "metric": name,
            "current": current,
            "prior": prior,
            "yoy": yoy
        })

    return stats


def create_timeseries_chart(df, metrics, title, selected_year, selected_quarter, start_year=None):
    """Create a timeseries chart for given metrics with selected quarter highlighted."""
    fig = go.Figure()

    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B', '#95C623']
    x_labels = []

    for i, (mdrm, name) in enumerate(metrics):
        metric_data = df[df["mdrm_code"] == mdrm].sort_values(["year", "quarter"])
        if start_year is not None:
            metric_data = metric_data[metric_data["year"] >= start_year]

        if len(metric_data) > 0:
            # Create x-axis labels like "2023 Q4"
            x_labels = [f"{row['year']} Q{row['quarter']}" for _, row in metric_data.iterrows()]

            fig.add_trace(go.Scatter(
                x=x_labels,
                y=metric_data["value"] / 1e6,  # Convert to millions
                mode='lines+markers',
                name=name,
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=6)
            ))

    # Add vertical line for selected quarter using shape (works with categorical x-axis)
    selected_label = f"{selected_year} Q{selected_quarter}"
    if selected_label in x_labels:
        selected_idx = x_labels.index(selected_label)
        fig.add_shape(
            type="line",
            x0=selected_idx, x1=selected_idx,
            y0=0, y1=1,
            yref="paper",
            line=dict(color="red", width=2, dash="dash")
        )
        fig.add_annotation(
            x=selected_idx, y=1.05,
            yref="paper",
            text="Selected",
            showarrow=False,
            font=dict(color="red", size=10)
        )

    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        xaxis_title="Quarter",
        yaxis_title="Value ($ Billions)",
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=60, r=40, t=80, b=60),
        height=400,
        template="plotly_white"
    )

    return fig


def create_bar_chart_yoy(df, metrics, title, selected_year, selected_quarter):
    """Create a bar chart comparing selected quarter vs prior year."""
    current_data = get_quarter_data(df, selected_year, selected_quarter)
    prior_year_data = get_prior_year_quarter_data(df, selected_year, selected_quarter)

    names = []
    current_vals = []
    prior_vals = []

    for mdrm, name in metrics:
        current = current_data[current_data["mdrm_code"] == mdrm]["value"].values
        prior = prior_year_data[prior_year_data["mdrm_code"] == mdrm]["value"].values if len(prior_year_data) > 0 else []

        names.append(name)
        current_vals.append(current[0] / 1e6 if len(current) > 0 else 0)
        prior_vals.append(prior[0] / 1e6 if len(prior) > 0 else 0)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name=f'{selected_year - 1} Q{selected_quarter}',
        x=names,
        y=prior_vals,
        marker_color='#A0A0A0'
    ))

    fig.add_trace(go.Bar(
        name=f'{selected_year} Q{selected_quarter}',
        x=names,
        y=current_vals,
        marker_color='#2E86AB'
    ))

    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        yaxis_title="Value ($ Millions)",
        barmode='group',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=60, r=40, t=80, b=60),
        height=400,
        template="plotly_white"
    )

    return fig


def create_summary_html(stats):
    """Create HTML for summary stats cards."""
    html_parts = []

    # First row (3 cards)
    html_parts.append('<div style="display: flex; gap: 15px; margin-bottom: 15px;">')
    for stat in stats[:3]:
        yoy_text = f"{stat['yoy']:+.1f}% Y-o-Y" if stat['yoy'] else "N/A"
        yoy_color = "green" if stat['yoy'] and stat['yoy'] > 0 else "red" if stat['yoy'] and stat['yoy'] < 0 else "gray"

        html_parts.append(f'''
            <div style="flex: 1; text-align: center; padding: 15px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #2E86AB;">
                <div style="font-size: 14px; color: #666; margin-bottom: 5px;">{stat['metric']}</div>
                <div style="font-size: 24px; font-weight: bold; color: #333;">{format_value(stat['current'])}</div>
                <div style="font-size: 12px; color: {yoy_color}; margin-top: 5px;">{yoy_text}</div>
            </div>
        ''')
    html_parts.append('</div>')

    # Second row (3 cards)
    html_parts.append('<div style="display: flex; gap: 15px;">')
    for stat in stats[3:6]:
        yoy_text = f"{stat['yoy']:+.1f}% Y-o-Y" if stat['yoy'] else "N/A"
        yoy_color = "green" if stat['yoy'] and stat['yoy'] > 0 else "red" if stat['yoy'] and stat['yoy'] < 0 else "gray"

        html_parts.append(f'''
            <div style="flex: 1; text-align: center; padding: 15px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #A23B72;">
                <div style="font-size: 14px; color: #666; margin-bottom: 5px;">{stat['metric']}</div>
                <div style="font-size: 24px; font-weight: bold; color: #333;">{format_value(stat['current'])}</div>
                <div style="font-size: 12px; color: {yoy_color}; margin-top: 5px;">{yoy_text}</div>
            </div>
        ''')
    html_parts.append('</div>')

    return ''.join(html_parts)


# All chartable metrics (display name -> mdrm code)
METRIC_CHOICES = [
    # Balance Sheet
    ("Total Assets", "BHCK2170"),
    ("Net Loans & Leases", "BHCKB528"),
    ("Loans & Leases (Gross)", "BHCK2122"),
    ("AFS Securities", "BHCK1773"),
    ("HTM Securities", "BHCK1754"),
    ("Trading Assets", "BHCK3545"),
    ("Loans Held for Sale", "BHCK5369"),
    ("Goodwill", "BHCK3163"),
    ("Total Liabilities", "BHCK2948"),
    ("Total Equity", "BHCK3210"),
    ("Retained Earnings", "BHCK3632"),
    ("Domestic IB Deposits", "BHDM6636"),
    ("Domestic NIB Deposits", "BHDM6631"),
    ("Subordinated Debt", "BHCK3200"),
    # Income Statement
    ("Total Interest Income", "BHCK4010"),
    ("Total Interest Expense", "BHCK4073"),
    ("Net Interest Income", "BHCK4074"),
    ("Provision for Loan Losses", "BHCK4230"),
    ("Provision for Credit Losses", "BHCKJJ33"),
    ("Total Noninterest Income", "BHCK4079"),
    ("Total Noninterest Expense", "BHCK4093"),
    ("Salaries & Benefits", "BHCK4135"),
    ("Net Income", "BHCK4301"),
    ("Income Before Taxes", "BHCK4301"),
    ("Applicable Income Taxes", "BHCK4302"),
    # Sub-items
    ("Interest Income - RE Loans", "BHCK4107"),
    ("Interest Income - C&I Loans", "BHCK4069"),
    ("Interest Income - Consumer Loans", "BHCKF821"),
    ("Interest Income - Securities (Taxable)", "BHCK4060"),
    ("Interest Income - Securities (Tax-Exempt)", "BHCK4062"),
    ("Noninterest Income - Service Charges", "BHCKC886"),
    ("Noninterest Income - Trading", "BHCKC888"),
    ("Noninterest Income - Insurance", "BHCKC013"),
    ("Noninterest Income - Servicing Fees", "BHCKB493"),
    ("Insurance Assets (General Account)", "BHCKK194"),
    ("Separate Account Assets", "BHCKC249"),
]
NAME_TO_MDRM = {name: mdrm for name, mdrm in METRIC_CHOICES}
DEFAULT_CUSTOM_METRICS = ["Total Assets", "Net Loans & Leases", "Net Interest Income", "Net Income"]

def _fred_x_labels(series, start_year=None):
    """Convert a quarterly FRED series index to 'YYYY Qn' strings."""
    s = series
    if start_year is not None:
        s = s[s.index.year >= start_year]
    return s, [f"{d.year} Q{(d.month-1)//3+1}" for d in s.index]


def create_fred_chart(fred_data, series_names, title, y_label="%", start_year=None):
    """Line chart for one or more FRED series."""
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B', '#95C623']
    fig = go.Figure()
    for i, name in enumerate(series_names):
        if name not in fred_data:
            continue
        s, x = _fred_x_labels(fred_data[name], start_year)
        fig.add_trace(go.Scatter(
            x=x, y=s.values, name=name,
            line=dict(color=colors[i % len(colors)], width=2),
            mode='lines'
        ))
    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        xaxis_title="Quarter", yaxis_title=y_label,
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=60, r=40, t=80, b=60), height=400, template="plotly_white"
    )
    return fig


def create_overlay_chart(df, bank_metric_name, fred_series_name, fred_data,
                          selected_year, selected_quarter, start_year=None):
    """Dual-axis chart: bank metric (left) vs FRED series (right)."""
    fig = go.Figure()

    mdrm = NAME_TO_MDRM.get(bank_metric_name)
    if mdrm and df is not None:
        metric_data = df[df["mdrm_code"] == mdrm].sort_values(["year", "quarter"])
        if start_year:
            metric_data = metric_data[metric_data["year"] >= start_year]
        x_bank = [f"{r['year']} Q{r['quarter']}" for _, r in metric_data.iterrows()]
        fig.add_trace(go.Scatter(
            x=x_bank, y=metric_data["value"] / 1e6,
            name=bank_metric_name,
            line=dict(color='#2E86AB', width=2),
            yaxis='y'
        ))

    if fred_series_name and fred_series_name in fred_data:
        s, x_fred = _fred_x_labels(fred_data[fred_series_name], start_year)
        fig.add_trace(go.Scatter(
            x=x_fred, y=s.values,
            name=fred_series_name,
            line=dict(color='#C73E1D', width=2, dash='dot'),
            yaxis='y2'
        ))

    fig.update_layout(
        title=dict(text="Bank Metric vs Economic Indicator", font=dict(size=16)),
        xaxis_title="Quarter",
        yaxis=dict(title=f"{bank_metric_name} ($ Billions)", color='#2E86AB'),
        yaxis2=dict(
            title=f"{fred_series_name} ({get_unit(fred_series_name)})" if fred_series_name else "",
            overlaying='y', side='right', color='#C73E1D'
        ),
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=60, r=60, t=80, b=60), height=450, template="plotly_white"
    )
    return fig


# Global data storage
GLOBAL_DF = None
CURRENT_RSSD = None
NAME_TO_RSSD = {}
FRED_DATA = {}


def _fmt_tval(v):
    """Format table values. DB values are in thousands of dollars."""
    if pd.isna(v):
        return "—"
    if abs(v) >= 1_000_000_000:    # >= $1 trillion
        return f"${v/1_000_000_000:.2f}T"
    if abs(v) >= 1_000_000:        # >= $1 billion
        return f"${v/1_000_000:.1f}B"
    if abs(v) >= 1_000:            # >= $1 million
        return f"${v/1_000:.1f}M"
    return f"${v:,.0f}K"


def create_statement_table(df, selected_year, selected_quarter, statement_type):
    """Build a formatted DataFrame for the balance sheet or income statement tab."""
    data = get_quarter_data(df, selected_year, selected_quarter)
    stmt = data[data["statement_type"] == statement_type][["category", "account_name", "value"]].copy()
    if stmt.empty:
        return pd.DataFrame(columns=["Category", "Line Item", "Value"])
    stmt["Value"] = stmt["value"].apply(_fmt_tval)
    stmt["Category"] = stmt["category"].str.replace("_", " ").str.title()
    stmt = stmt.rename(columns={"account_name": "Line Item"})
    return stmt[["Category", "Line Item", "Value"]].sort_values(["Category", "Line Item"]).reset_index(drop=True)


def get_data(rssd_id=None):
    """Get or load the dataframe from database for a specific institution."""
    global GLOBAL_DF, CURRENT_RSSD
    if GLOBAL_DF is None or CURRENT_RSSD != rssd_id:
        CURRENT_RSSD = rssd_id
        GLOBAL_DF = load_financial_data(rssd_id)
        if len(GLOBAL_DF) == 0:
            raise ValueError("No data in database. Run 'python -m src.y9c.cli --init' to download and load data.")
    return GLOBAL_DF


def update_overlay(institution_str, selected_quarter_str, years_back_str,
                   overlay_bank, overlay_fred):
    """Update only the overlay chart — keeps page position stable."""
    rssd_id = NAME_TO_RSSD.get(institution_str, institution_str) if institution_str else None
    df = get_data(rssd_id)
    parts = selected_quarter_str.split()
    selected_year = int(parts[0])
    selected_quarter = int(parts[1][1])
    start_year = None if years_back_str == "All" else selected_year - int(years_back_str) + 1
    return create_overlay_chart(df, overlay_bank, overlay_fred, FRED_DATA,
                                selected_year, selected_quarter, start_year)


def update_dashboard(institution_str, selected_quarter_str, years_back_str="3",
                     custom_metric_names=None, overlay_bank="Net Interest Income",
                     overlay_fred="Fed Funds Rate"):
    """Update all dashboard components based on selected institution and quarter."""
    rssd_id = NAME_TO_RSSD.get(institution_str, institution_str) if institution_str else None
    df = get_data(rssd_id)

    parts = selected_quarter_str.split()
    selected_year = int(parts[0])
    selected_quarter = int(parts[1][1])

    start_year = None if years_back_str == "All" else selected_year - int(years_back_str) + 1

    stats = create_summary_stats(df, selected_year, selected_quarter)
    summary_html = create_summary_html(stats)

    balance_metrics = [
        ("BHCK2170", "Total Assets"),
        ("BHCK3210", "Total Equity"),
        ("BHCKB528", "Net Loans"),
    ]
    fig_balance = create_timeseries_chart(df, balance_metrics, "Balance Sheet Trends", selected_year, selected_quarter, start_year)

    income_metrics = [
        ("BHCK4074", "Net Interest Income"),
        ("BHCK4079", "Noninterest Income"),
        ("BHCK4301", "Net Income"),
    ]
    fig_income = create_timeseries_chart(df, income_metrics, "Income Statement Trends", selected_year, selected_quarter, start_year)

    deposit_metrics = [
        ("BHDM6636", "Interest-bearing Deposits"),
        ("BHCK4010", "Total Interest Income"),
        ("BHCK4073", "Total Interest Expense"),
    ]
    fig_deposits = create_timeseries_chart(df, deposit_metrics, "Interest Income & Expense Trends", selected_year, selected_quarter, start_year)

    expense_metrics = [
        ("BHCK4093", "Total Noninterest Expense"),
        ("BHCK4230", "Provision for Loan Losses"),
    ]
    fig_expense = create_timeseries_chart(df, expense_metrics, "Expense Trends", selected_year, selected_quarter, start_year)

    yoy_balance = [
        ("BHCK2170", "Total Assets"),
        ("BHCK3210", "Equity"),
        ("BHCKB528", "Net Loans"),
        ("BHDM6636", "Deposits"),
    ]
    fig_yoy_balance = create_bar_chart_yoy(df, yoy_balance, "Balance Sheet Y-o-Y Comparison", selected_year, selected_quarter)

    yoy_income = [
        ("BHCK4074", "Net Interest Income"),
        ("BHCK4079", "Noninterest Income"),
        ("BHCK4093", "Noninterest Expense"),
        ("BHCK4301", "Net Income"),
    ]
    fig_yoy_income = create_bar_chart_yoy(df, yoy_income, "Income Statement Y-o-Y Comparison", selected_year, selected_quarter)

    # Custom chart
    selected_names = custom_metric_names if custom_metric_names else DEFAULT_CUSTOM_METRICS
    custom_pairs = [(NAME_TO_MDRM[n], n) for n in selected_names if n in NAME_TO_MDRM]
    fig_custom = create_timeseries_chart(df, custom_pairs, "Custom Metrics", selected_year, selected_quarter, start_year)

    # Statement tables
    df_bs = create_statement_table(df, selected_year, selected_quarter, "balance_sheet")
    df_is = create_statement_table(df, selected_year, selected_quarter, "income_statement")

    # FRED / economic charts
    fig_rates = create_fred_chart(
        FRED_DATA,
        ["Fed Funds Rate", "2Y Treasury Yield", "10Y Treasury Yield"],
        "Interest Rates", "%", start_year
    )
    fig_curve = create_fred_chart(
        FRED_DATA, ["Yield Curve (10Y-2Y)"], "Yield Curve Spread (10Y - 2Y)", "%", start_year
    )
    fig_spreads = create_fred_chart(
        FRED_DATA, ["HY Credit Spread (OAS)", "IG Credit Spread (OAS)"],
        "Credit Spreads", "%", start_year
    )
    fig_macro = create_fred_chart(
        FRED_DATA, ["Unemployment Rate", "CPI Inflation (YoY %)"],
        "Macro Indicators", "%", start_year
    )
    fig_overlay = create_overlay_chart(
        df, overlay_bank, overlay_fred, FRED_DATA,
        selected_year, selected_quarter, start_year
    )

    return (summary_html, fig_balance, fig_income, fig_deposits, fig_expense,
            fig_yoy_balance, fig_yoy_income, fig_custom, df_bs, df_is,
            fig_rates, fig_curve, fig_spreads, fig_macro, fig_overlay)


def create_dashboard():
    """Create the Gradio dashboard interface."""
    global FRED_DATA
    print("Loading FRED economic data...")
    FRED_DATA = load_fred_data()

    institutions_df = get_available_institutions()
    if len(institutions_df) == 0:
        raise ValueError("No data in database. Run 'python -m src.y9c.cli --init' to download and load data.")

    global NAME_TO_RSSD
    NAME_TO_RSSD = {
        (row['name'] or row['rssd_id']): row['rssd_id']
        for _, row in institutions_df.iterrows()
    }
    institution_choices = list(NAME_TO_RSSD.keys())
    default_institution = institution_choices[0]
    default_rssd = institutions_df.iloc[0]["rssd_id"]

    df = get_data(default_rssd)

    quarters_df = df.groupby(["year", "quarter"]).size().reset_index()
    quarters_df = quarters_df.sort_values(["year", "quarter"], ascending=[False, False])
    quarter_choices = [f"{row['year']} Q{row['quarter']}" for _, row in quarters_df.iterrows()]

    default_quarter = quarter_choices[0] if quarter_choices else "2024 Q4"
    default_years = "3"

    default_overlay_bank = "Net Interest Income"
    default_overlay_fred = "Fed Funds Rate"
    initial_outputs = update_dashboard(
        default_institution, default_quarter, default_years,
        DEFAULT_CUSTOM_METRICS, default_overlay_bank, default_overlay_fred
    )

    with gr.Blocks(title="Bank Holding Company Y-9C Dashboard") as demo:
        gr.Markdown("# Bank Holding Company Financial Dashboard\n### FR Y-9C Regulatory Data Analysis")

        with gr.Row():
            institution_dropdown = gr.Dropdown(choices=institution_choices, value=default_institution, label="Select Institution")
            quarter_dropdown = gr.Dropdown(choices=quarter_choices, value=default_quarter, label="Select As-Of Date")
            years_dropdown = gr.Dropdown(choices=["1", "3", "5", "10", "All"], value=default_years, label="Years of History")

        with gr.Tabs():
            with gr.Tab("Overview"):
                gr.Markdown("## Key Metrics Summary")
                summary_html = gr.HTML(value=initial_outputs[0])
                gr.Markdown("---\n## Trend Analysis")
                with gr.Row():
                    plot_balance = gr.Plot(value=initial_outputs[1])
                    plot_income = gr.Plot(value=initial_outputs[2])
                with gr.Row():
                    plot_deposits = gr.Plot(value=initial_outputs[3])
                    plot_expense = gr.Plot(value=initial_outputs[4])
                gr.Markdown("---\n## Year-over-Year Comparison")
                with gr.Row():
                    plot_yoy_balance = gr.Plot(value=initial_outputs[5])
                    plot_yoy_income = gr.Plot(value=initial_outputs[6])

            with gr.Tab("Custom Chart"):
                custom_metrics_dropdown = gr.Dropdown(
                    choices=[name for name, _ in METRIC_CHOICES],
                    value=DEFAULT_CUSTOM_METRICS,
                    multiselect=True,
                    label="Select Metrics to Chart"
                )
                plot_custom = gr.Plot(value=initial_outputs[7])

            with gr.Tab("Balance Sheet"):
                table_bs = gr.Dataframe(value=initial_outputs[8], interactive=False)

            with gr.Tab("Income Statement"):
                table_is = gr.Dataframe(value=initial_outputs[9], interactive=False)

            with gr.Tab("Economic Context"):
                gr.Markdown("### Interest Rates & Macro Environment")
                with gr.Row():
                    plot_rates = gr.Plot(value=initial_outputs[10])
                    plot_curve = gr.Plot(value=initial_outputs[11])
                with gr.Row():
                    plot_spreads = gr.Plot(value=initial_outputs[12])
                    plot_macro = gr.Plot(value=initial_outputs[13])
                gr.Markdown("---\n### Overlay: Bank Metric vs Economic Indicator")
                with gr.Row():
                    overlay_bank_dropdown = gr.Dropdown(
                        choices=[name for name, _ in METRIC_CHOICES],
                        value=default_overlay_bank,
                        label="Bank Metric (left axis)"
                    )
                    overlay_fred_dropdown = gr.Dropdown(
                        choices=FRED_SERIES_NAMES,
                        value=default_overlay_fred,
                        label="Economic Series (right axis)"
                    )
                plot_overlay = gr.Plot(value=initial_outputs[14])

        gr.Markdown('<div style="text-align: center; color: #666; font-size: 12px; margin-top: 20px;">Data Source: FR Y-9C Regulatory Filings + FRED (St. Louis Fed)</div>')

        main_inputs = [institution_dropdown, quarter_dropdown, years_dropdown,
                       custom_metrics_dropdown, overlay_bank_dropdown, overlay_fred_dropdown]
        main_outputs = [summary_html, plot_balance, plot_income, plot_deposits, plot_expense,
                        plot_yoy_balance, plot_yoy_income, plot_custom, table_bs, table_is,
                        plot_rates, plot_curve, plot_spreads, plot_macro, plot_overlay]

        # Main controls trigger full update
        for ctrl in [institution_dropdown, quarter_dropdown, years_dropdown, custom_metrics_dropdown]:
            ctrl.change(fn=update_dashboard, inputs=main_inputs, outputs=main_outputs)

        # Overlay dropdowns only update the overlay chart (no page jump)
        overlay_inputs = [institution_dropdown, quarter_dropdown, years_dropdown,
                          overlay_bank_dropdown, overlay_fred_dropdown]
        overlay_bank_dropdown.change(fn=update_overlay, inputs=overlay_inputs, outputs=[plot_overlay])
        overlay_fred_dropdown.change(fn=update_overlay, inputs=overlay_inputs, outputs=[plot_overlay])

    return demo


if __name__ == "__main__":
    demo = create_dashboard()
    demo.launch()
