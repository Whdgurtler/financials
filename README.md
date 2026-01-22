# USAA Y-9C Financial Data Scraper

A Python package for downloading, processing, and analyzing FR Y-9C regulatory filings for bank holding companies.

## Overview

This project provides tools to:
- Download FR Y-9C bulk data files from FFIEC and Chicago Fed
- Parse and store quarterly financial data in SQLite
- Visualize financial trends with an interactive Gradio dashboard

## Project Structure

```
usaa_y9c_scraper/
├── src/
│   ├── y9c/                    # Core scraper package
│   │   ├── __init__.py
│   │   ├── config.py           # MDRM codes and configuration
│   │   ├── database.py         # SQLite database operations
│   │   ├── downloader.py       # Data file downloads
│   │   ├── loader.py           # Data parsing and loading
│   │   └── cli.py              # Command-line interface
│   └── dashboard/              # Visualization package
│       ├── __init__.py
│       └── app.py              # Gradio dashboard
├── data/
│   ├── raw/                    # Downloaded ZIP files
│   ├── processed/              # Extracted data files
│   ├── exports/                # CSV exports
│   └── usaa_y9c.db            # SQLite database
├── requirements.txt
└── README.md
```

## Installation

```bash
# Clone the repository
git clone https://github.com/Whdgurtler/financials.git
cd financials

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Command-Line Interface

```bash
# Full initialization (download and load all data)
python -m src.y9c.cli --init

# Load data from specific year range
python -m src.y9c.cli --init --start 2020 --end 2025

# Quarterly update (download only new data)
python -m src.y9c.cli --update

# View data summary
python -m src.y9c.cli --summary

# Export to CSV
python -m src.y9c.cli --export

# Show configuration
python -m src.y9c.cli --config
```

### Dashboard

Launch the interactive financial dashboard:

```bash
python -m src.dashboard.app
```

Or use the legacy script:

```bash
python gradio_dashboard.py
```

The dashboard provides:
- Key metrics summary with Y-o-Y comparisons
- Interactive quarter selector
- Balance sheet and income statement trend charts
- Year-over-year comparison bar charts

### Python API

```python
from src.y9c import (
    initialize_database,
    load_all_data,
    get_balance_sheet,
    get_income_statement,
    get_time_series,
    USAA_HOLDING_COMPANY_RSSD,
)

# Initialize and load data
initialize_database()
load_all_data(start_year=2020)

# Query balance sheet
bs = get_balance_sheet(USAA_HOLDING_COMPANY_RSSD, year=2024, quarter=4)
print(f"Total Assets: ${bs['Total assets']:,.0f}")

# Get time series for a specific metric
ts = get_time_series(USAA_HOLDING_COMPANY_RSSD, mdrm_code="BHCK2170")
for date, value in ts:
    print(f"{date}: ${value:,.0f}")
```

## Data Sources

- **FFIEC NIC** (2021+): Current FR Y-9C filings
- **Chicago Fed** (pre-2021): Historical FR Y-9C filings

## Key Metrics (MDRM Codes)

### Balance Sheet
- `BHCK2170` - Total Assets
- `BHCK2948` - Total Liabilities
- `BHCK3210` - Total Equity Capital
- `BHCKB528` - Net Loans and Leases

### Income Statement
- `BHCK4010` - Total Interest Income
- `BHCK4073` - Total Interest Expense
- `BHCK4074` - Net Interest Income
- `BHCK4079` - Total Noninterest Income
- `BHCK4093` - Total Noninterest Expense
- `BHCK4340` - Net Income

## License

MIT License
