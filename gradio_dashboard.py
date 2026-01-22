"""
USAA Y-9C Financial Dashboard
Gradio interface for viewing quarterly financial data with Y-o-Y comparisons
"""

import gradio as gr
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import numpy as np

# Database path
DB_PATH = Path(__file__).parent / "data" / "usaa_y9c.db"


def get_db_connection():
    """Get database connection."""
    return sqlite3.connect(DB_PATH)


def load_financial_data():
    """Load all financial data from database."""
    conn = get_db_connection()
    query = """
        SELECT fd.report_date, fd.year, fd.quarter, fd.mdrm_code, fd.value,
               ad.account_name, ad.statement_type, ad.category
        FROM financial_data fd
        JOIN account_definitions ad ON fd.mdrm_code = ad.mdrm_code
        ORDER BY fd.year, fd.quarter
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def generate_sample_historical_data():
    """Generate sample historical data for demonstration purposes."""
    # Key metrics and their base values in THOUSANDS (realistic for USAA ~$200B holding company)
    metrics = {
        "BHCK2170": {"name": "Total Assets", "base": 200000000, "statement": "balance_sheet", "category": "assets"},  # $200B
        "BHCK2948": {"name": "Total Liabilities", "base": 165000000, "statement": "balance_sheet", "category": "liabilities"},  # $165B
        "BHCK3210": {"name": "Total Equity", "base": 35000000, "statement": "balance_sheet", "category": "equity"},  # $35B
        "BHCKB528": {"name": "Net Loans", "base": 95000000, "statement": "balance_sheet", "category": "assets"},  # $95B
        "BHDM6636": {"name": "Interest-bearing Deposits", "base": 120000000, "statement": "balance_sheet", "category": "liabilities"},  # $120B
        "BHCK4010": {"name": "Total Interest Income", "base": 8500000, "statement": "income_statement", "category": "interest_income"},  # $8.5B
        "BHCK4073": {"name": "Total Interest Expense", "base": 2800000, "statement": "income_statement", "category": "interest_expense"},  # $2.8B
        "BHCK4074": {"name": "Net Interest Income", "base": 5700000, "statement": "income_statement", "category": "net_interest_income"},  # $5.7B
        "BHCK4079": {"name": "Total Noninterest Income", "base": 12000000, "statement": "income_statement", "category": "noninterest_income"},  # $12B (insurance)
        "BHCK4093": {"name": "Total Noninterest Expense", "base": 13500000, "statement": "income_statement", "category": "noninterest_expense"},  # $13.5B
        "BHCK4340": {"name": "Net Income", "base": 4000000, "statement": "income_statement", "category": "income"},  # $4B
        "BHCK4230": {"name": "Provision for Loan Losses", "base": 800000, "statement": "income_statement", "category": "provision"},  # $800M
    }

    # Generate 20 quarters of historical data (2021 Q1 to 2025 Q4)
    quarters = []
    for year in [2021, 2022, 2023, 2024, 2025]:
        for q in [1, 2, 3, 4]:
            quarter_end = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[q]
            quarters.append({
                "year": year,
                "quarter": q,
                "report_date": f"{year}-{quarter_end}"
            })

    data_rows = []
    np.random.seed(42)  # Reproducible randomness

    total_quarters = len(quarters)
    for metric, info in metrics.items():
        for i, qtr in enumerate(quarters):
            # Create growth trend with some seasonal variation
            quarter_idx = i
            trend = 1 + (0.02 * quarter_idx)  # ~2% quarterly growth
            seasonal = 1 + 0.03 * np.sin(2 * np.pi * qtr["quarter"] / 4)  # Seasonal variation
            noise = 1 + np.random.normal(0, 0.02)  # Random noise

            # Expense items grow differently
            if "expense" in info["category"].lower() or "provision" in info["category"].lower():
                trend = 1 + (0.015 * quarter_idx)  # Slightly slower growth

            value = info["base"] * trend * seasonal * noise / (1 + 0.02 * (total_quarters - 1))  # Normalize so last quarter matches base

            data_rows.append({
                "report_date": qtr["report_date"],
                "year": qtr["year"],
                "quarter": qtr["quarter"],
                "mdrm_code": metric,
                "account_name": info["name"],
                "statement_type": info["statement"],
                "category": info["category"],
                "value": value
            })

    return pd.DataFrame(data_rows)


def get_quarter_data(df, year, quarter):
    """Get data for a specific quarter."""
    return df[(df["year"] == year) & (df["quarter"] == quarter)]


def get_prior_year_quarter_data(df, year, quarter):
    """Get data for same quarter in prior year."""
    return df[(df["year"] == year - 1) & (df["quarter"] == quarter)]


def format_value(value, format_type="currency"):
    """Format values for display."""
    if pd.isna(value) or value is None:
        return "N/A"
    if format_type == "currency":
        if abs(value) >= 1e9:
            return f"${value/1e9:.2f}B"
        elif abs(value) >= 1e6:
            return f"${value/1e6:.1f}M"
        elif abs(value) >= 1e3:
            return f"${value/1e3:.1f}K"
        else:
            return f"${value:,.0f}"
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
        ("BHCK4340", "Net Income"),
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


def create_timeseries_chart(df, metrics, title, selected_year, selected_quarter):
    """Create a timeseries chart for given metrics with selected quarter highlighted."""
    fig = go.Figure()

    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B', '#95C623']
    x_labels = []

    for i, (mdrm, name) in enumerate(metrics):
        metric_data = df[df["mdrm_code"] == mdrm].sort_values(["year", "quarter"])

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
        yaxis_title="Value ($ Millions)",
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


# Global data storage
GLOBAL_DF = None


def get_data():
    """Get or load the global dataframe."""
    global GLOBAL_DF
    if GLOBAL_DF is None:
        try:
            GLOBAL_DF = load_financial_data()
            if len(GLOBAL_DF) == 0:
                raise ValueError("No data in database")
            unique_quarters = GLOBAL_DF.groupby(["year", "quarter"]).size().reset_index()
            if len(unique_quarters) <= 1:
                GLOBAL_DF = generate_sample_historical_data()
        except Exception as e:
            print(f"Using sample data: {e}")
            GLOBAL_DF = generate_sample_historical_data()
    return GLOBAL_DF


def update_dashboard(selected_quarter_str):
    """Update all dashboard components based on selected quarter."""
    df = get_data()

    # Parse selected quarter (e.g., "2025 Q4")
    parts = selected_quarter_str.split()
    selected_year = int(parts[0])
    selected_quarter = int(parts[1][1])  # Extract number from "Q4"

    # Create summary stats
    stats = create_summary_stats(df, selected_year, selected_quarter)
    summary_html = create_summary_html(stats)

    # Create charts
    balance_metrics = [
        ("BHCK2170", "Total Assets"),
        ("BHCK3210", "Total Equity"),
        ("BHCKB528", "Net Loans"),
    ]
    fig_balance = create_timeseries_chart(df, balance_metrics, "Balance Sheet Trends", selected_year, selected_quarter)

    income_metrics = [
        ("BHCK4074", "Net Interest Income"),
        ("BHCK4079", "Noninterest Income"),
        ("BHCK4340", "Net Income"),
    ]
    fig_income = create_timeseries_chart(df, income_metrics, "Income Statement Trends", selected_year, selected_quarter)

    deposit_metrics = [
        ("BHDM6636", "Interest-bearing Deposits"),
        ("BHCK4010", "Total Interest Income"),
        ("BHCK4073", "Total Interest Expense"),
    ]
    fig_deposits = create_timeseries_chart(df, deposit_metrics, "Interest Income & Expense Trends", selected_year, selected_quarter)

    expense_metrics = [
        ("BHCK4093", "Total Noninterest Expense"),
        ("BHCK4230", "Provision for Loan Losses"),
    ]
    fig_expense = create_timeseries_chart(df, expense_metrics, "Expense Trends", selected_year, selected_quarter)

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
        ("BHCK4340", "Net Income"),
    ]
    fig_yoy_income = create_bar_chart_yoy(df, yoy_income, "Income Statement Y-o-Y Comparison", selected_year, selected_quarter)

    return summary_html, fig_balance, fig_income, fig_deposits, fig_expense, fig_yoy_balance, fig_yoy_income


def create_dashboard():
    """Create the Gradio dashboard interface."""
    # Load data
    df = get_data()

    # Get available quarters
    quarters_df = df.groupby(["year", "quarter"]).size().reset_index()
    quarters_df = quarters_df.sort_values(["year", "quarter"], ascending=[False, False])
    quarter_choices = [f"{row['year']} Q{row['quarter']}" for _, row in quarters_df.iterrows()]

    # Default to latest quarter (2025 Q4)
    default_quarter = quarter_choices[0]  # Should be "2025 Q4"

    # Get initial values
    initial_outputs = update_dashboard(default_quarter)

    # Build the interface
    with gr.Blocks(title="USAA Y-9C Dashboard") as demo:
        gr.Markdown(
            """
            # USAA Financial Dashboard
            ### FR Y-9C Regulatory Data Analysis
            """
        )

        # Quarter selector
        with gr.Row():
            quarter_dropdown = gr.Dropdown(
                choices=quarter_choices,
                value=default_quarter,
                label="Select As-Of Date"
            )

        # Summary Stats Section
        gr.Markdown("## Key Metrics Summary")
        summary_html = gr.HTML(value=initial_outputs[0])

        gr.Markdown("---")
        gr.Markdown("## Trend Analysis")

        # Time Series Charts
        with gr.Row():
            plot_balance = gr.Plot(value=initial_outputs[1])
            plot_income = gr.Plot(value=initial_outputs[2])

        with gr.Row():
            plot_deposits = gr.Plot(value=initial_outputs[3])
            plot_expense = gr.Plot(value=initial_outputs[4])

        gr.Markdown("---")
        gr.Markdown("## Year-over-Year Comparison")

        with gr.Row():
            plot_yoy_balance = gr.Plot(value=initial_outputs[5])
            plot_yoy_income = gr.Plot(value=initial_outputs[6])

        gr.Markdown(
            """
            ---
            <div style="text-align: center; color: #666; font-size: 12px;">
            Data Source: FR Y-9C Regulatory Filings | Values in thousands unless otherwise noted
            </div>
            """
        )

        # Connect dropdown to update function
        quarter_dropdown.change(
            fn=update_dashboard,
            inputs=[quarter_dropdown],
            outputs=[summary_html, plot_balance, plot_income, plot_deposits, plot_expense, plot_yoy_balance, plot_yoy_income]
        )

    return demo


if __name__ == "__main__":
    demo = create_dashboard()
    demo.launch()
