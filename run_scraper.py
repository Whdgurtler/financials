"""
Y-9C Data Scraper - Main Entry Point

This script orchestrates the full Y-9C data collection process:
1. Downloads bulk data files from FFIEC/Chicago Fed
2. Creates/initializes the SQLite database
3. Parses and loads data for USAA holding company
4. Filters to income statement and balance sheet line items

Usage:
    # Full initial load (2000 to present)
    python run_scraper.py --init

    # Quarterly update (only new data)
    python run_scraper.py --update

    # Export data to CSV
    python run_scraper.py --export

    # View data summary
    python run_scraper.py --summary
"""

import argparse
from pathlib import Path
from datetime import datetime

from y9c_config import USAA_HOLDING_COMPANY_RSSD, get_all_mdrm_codes
from y9c_database import (
    initialize_database,
    get_connection,
    get_balance_sheet,
    get_income_statement,
    get_all_periods,
    export_to_csv,
    DB_PATH,
)
from y9c_downloader import download_all_y9c_data, check_existing_data
from y9c_loader import load_all_data, incremental_update, validate_data


def full_initialization(start_year=2000, end_year=None):
    """
    Perform full initialization:
    1. Download all data files
    2. Initialize database
    3. Load all data
    """
    print("=" * 70)
    print("USAA Y-9C Data Scraper - Full Initialization")
    print("=" * 70)

    # Step 1: Initialize database
    print("\n[1/3] Initializing database...")
    initialize_database()

    # Step 2: Download data files
    print("\n[2/3] Downloading Y-9C bulk data files...")
    downloaded = download_all_y9c_data(start_year, end_year)
    print(f"Downloaded {len(downloaded)} files.")

    # Step 3: Load data into database
    print("\n[3/3] Loading data into database...")
    total = load_all_data(start_year, end_year, USAA_HOLDING_COMPANY_RSSD)

    print("\n" + "=" * 70)
    print("Initialization Complete!")
    print(f"Database: {DB_PATH}")
    print(f"Total records: {total}")
    print("=" * 70)


def quarterly_update():
    """
    Perform quarterly update:
    1. Download any new data files
    2. Load new data into database
    """
    print("=" * 70)
    print("USAA Y-9C Data Scraper - Quarterly Update")
    print("=" * 70)

    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    # Download recent quarters (current year and previous)
    print("\n[1/2] Checking for new data files...")
    download_all_y9c_data(current_year - 1, current_year)

    # Load new data
    print("\n[2/2] Loading new data...")
    new_records = incremental_update(USAA_HOLDING_COMPANY_RSSD)

    print("\n" + "=" * 70)
    print("Update Complete!")
    print(f"New records loaded: {new_records}")
    print("=" * 70)


def show_summary():
    """Display summary of available data."""
    print("=" * 70)
    print(f"USAA Y-9C Data Summary (RSSD: {USAA_HOLDING_COMPANY_RSSD})")
    print("=" * 70)

    # Get available periods
    periods = get_all_periods(USAA_HOLDING_COMPANY_RSSD)

    if not periods:
        print("\nNo data loaded yet. Run with --init to download and load data.")
        return

    print(f"\nAvailable periods: {len(periods)}")
    print(f"Date range: {periods[0][2]} to {periods[-1][2]}")

    # Get most recent balance sheet
    latest_year, latest_quarter, latest_date = periods[-1]
    print(f"\n--- Most Recent Data ({latest_date}) ---")

    bs = get_balance_sheet(USAA_HOLDING_COMPANY_RSSD, year=latest_year, quarter=latest_quarter)
    inc = get_income_statement(USAA_HOLDING_COMPANY_RSSD, year=latest_year, quarter=latest_quarter)

    print(f"\nBalance Sheet Items: {len(bs)}")
    if 'Total assets' in bs:
        print(f"  Total Assets: ${bs['Total assets']:,.0f} (thousands)")
    if 'Total liabilities' in bs:
        print(f"  Total Liabilities: ${bs['Total liabilities']:,.0f} (thousands)")
    if 'Total equity capital' in bs:
        print(f"  Total Equity: ${bs['Total equity capital']:,.0f} (thousands)")

    print(f"\nIncome Statement Items: {len(inc)}")
    if 'Total interest income' in inc:
        print(f"  Total Interest Income: ${inc['Total interest income']:,.0f} (thousands)")
    if 'Net interest income' in inc:
        print(f"  Net Interest Income: ${inc['Net interest income']:,.0f} (thousands)")
    if 'Net income attributable to holding company' in inc:
        print(f"  Net Income: ${inc['Net income attributable to holding company']:,.0f} (thousands)")

    # Data validation
    print("\n--- Data Coverage ---")
    validate_data()


def export_data(output_dir=None):
    """Export all data to CSV files."""
    if output_dir is None:
        output_dir = Path(__file__).parent / "data" / "exports"

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d")

    print("Exporting data to CSV...")

    # Export balance sheet
    bs_path = output_dir / f"usaa_balance_sheet_{timestamp}.csv"
    export_to_csv(USAA_HOLDING_COMPANY_RSSD, bs_path, statement_type='balance_sheet')

    # Export income statement
    inc_path = output_dir / f"usaa_income_statement_{timestamp}.csv"
    export_to_csv(USAA_HOLDING_COMPANY_RSSD, inc_path, statement_type='income_statement')

    # Export all data
    all_path = output_dir / f"usaa_all_financial_data_{timestamp}.csv"
    export_to_csv(USAA_HOLDING_COMPANY_RSSD, all_path)

    print(f"\nExported files to: {output_dir}")
    print(f"  - {bs_path.name}")
    print(f"  - {inc_path.name}")
    print(f"  - {all_path.name}")


def print_config():
    """Print current configuration."""
    print("=" * 70)
    print("Configuration")
    print("=" * 70)

    print(f"\nTarget Institution:")
    print(f"  RSSD ID: {USAA_HOLDING_COMPANY_RSSD}")
    print(f"  Name: United Services Automobile Association")

    print(f"\nDatabase: {DB_PATH}")

    all_codes = get_all_mdrm_codes()
    print(f"\nMDRM Codes Configured: {len(all_codes)}")

    by_statement = {}
    for code, info in all_codes.items():
        stmt = info['statement']
        by_statement[stmt] = by_statement.get(stmt, 0) + 1

    for stmt, count in sorted(by_statement.items()):
        print(f"  {stmt}: {count}")


def main():
    parser = argparse.ArgumentParser(
        description="USAA Y-9C Data Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_scraper.py --init              # Full download and load (2000-present)
  python run_scraper.py --init --start 2015 # Load from 2015 onwards
  python run_scraper.py --update            # Quarterly incremental update
  python run_scraper.py --summary           # View data summary
  python run_scraper.py --export            # Export to CSV files
  python run_scraper.py --config            # Show configuration
        """
    )

    parser.add_argument("--init", action="store_true",
                        help="Full initialization: download all data and load into database")
    parser.add_argument("--update", action="store_true",
                        help="Quarterly update: download and load only new data")
    parser.add_argument("--summary", action="store_true",
                        help="Show summary of available data")
    parser.add_argument("--export", action="store_true",
                        help="Export data to CSV files")
    parser.add_argument("--config", action="store_true",
                        help="Show current configuration")
    parser.add_argument("--start", type=int, default=2000,
                        help="Start year for initialization (default: 2000)")
    parser.add_argument("--end", type=int, default=None,
                        help="End year for initialization (default: current year)")

    args = parser.parse_args()

    if args.init:
        full_initialization(args.start, args.end)
    elif args.update:
        quarterly_update()
    elif args.summary:
        show_summary()
    elif args.export:
        export_data()
    elif args.config:
        print_config()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
