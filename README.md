# Bank Holding Company Y-9C Financial Data Scraper

A Python tool for downloading, processing, and analyzing FR Y-9C regulatory filings for all U.S. bank holding companies.

## Overview

This project provides tools to:
- Download FR Y-9C bulk data files for **all ~4,000 bank holding companies** from FFIEC and Chicago Fed
- Parse and store quarterly financial data in SQLite (~4 million records)
- Visualize financial trends with an interactive Gradio dashboard
- Compare any bank holding company's financials over time with YTD metrics

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Download data and initialize database
python -m src.y9c.cli --init

# Launch the dashboard
python gradio_dashboard.py
```

Then open http://127.0.0.1:7860 in your browser.

## Project Structure

```
y9c_scraper/
├── src/y9c/                 # Core package
│   ├── config.py            # MDRM codes and configuration
│   ├── database.py          # SQLite database operations
│   ├── downloader.py        # Data file download (FFIEC/Chicago Fed)
│   ├── loader.py            # Data parsing and loading
│   └── cli.py               # Command-line interface
├── src/dashboard/           # Dashboard module
│   └── app.py               # Gradio dashboard interface
├── gradio_dashboard.py      # Main dashboard entry point
├── data/                    # Data directory (gitignored)
│   ├── raw/                 # Downloaded ZIP files
│   ├── processed/           # Extracted data files
│   └── usaa_y9c.db          # SQLite database
└── requirements.txt
```

## Command-Line Interface

```bash
# Full initialization (download 2000-2025 and load all data)
python -m src.y9c.cli --init

# Load data from specific year range
python -m src.y9c.cli --init --start 2020 --end 2025

# Quarterly update (download only new data)
python -m src.y9c.cli --update

# View data summary
python -m src.y9c.cli --summary

# Export to CSV
python -m src.y9c.cli --export
```

## Dashboard Features

The interactive dashboard includes:
- **Institution selector** - Choose from ~50 major bank holding companies (JPMorgan, Bank of America, Wells Fargo, Citigroup, USAA, etc.)
- **Quarter selector** - View data from 2000-2025
- **Key metrics summary** - Total Assets, Equity, Net Loans, Net Interest Income, Net Income, Noninterest Income
- **YTD statistics** - Year-to-date Total Revenue, Total Expense, and Net Income with Y-o-Y comparisons
- **Trend charts** - Balance sheet and income statement trends over time
- **Y-o-Y comparisons** - Bar charts comparing current quarter to prior year

## Data Sources

| Source | Period | Method |
|--------|--------|--------|
| Chicago Fed | 1986 - 2021 Q1 | Direct CSV download (no auth) |
| FFIEC NIC | 2021 Q2+ | Playwright with stealth |

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

**Note:** All values in FR Y-9C filings are reported in **thousands of dollars**.

## Supported Institutions

The dashboard includes major bank holding companies such as:
- JPMorgan Chase, Bank of America, Wells Fargo, Citigroup (Big 4)
- Goldman Sachs, Morgan Stanley, U.S. Bancorp, PNC, Truist
- Capital One, Charles Schwab, American Express
- TD Bank, BMO, HSBC, RBC, MUFG (Foreign-owned US BHCs)
- USAA, Synchrony, Discover, Ally Financial

## License

MIT License
