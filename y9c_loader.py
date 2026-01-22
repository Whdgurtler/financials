"""
Y-9C Data Loader
Parses downloaded Y-9C bulk data files and loads them into the database.

This module handles:
1. Parsing caret-delimited (^) text files from FFIEC
2. Parsing SAS transport files (.xpt) from Chicago Fed
3. Filtering data to relevant MDRM codes
4. Loading filtered data into SQLite database
"""

import os
import zipfile
from pathlib import Path
from datetime import datetime
import csv

from y9c_config import (
    get_mdrm_codes_list,
    USAA_HOLDING_COMPANY_RSSD,
    get_all_mdrm_codes,
)
from y9c_database import (
    bulk_insert_financial_data,
    record_load,
    get_loaded_quarters,
    get_connection,
)

DATA_DIR = Path(__file__).parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent / "data" / "processed"


def parse_caret_delimited_file(file_path, target_rssd=None, mdrm_filter=None):
    """
    Parse a caret-delimited (^) text file from FFIEC.

    The FFIEC bulk data files use caret (^) as delimiter because
    some values contain commas.

    Args:
        file_path: Path to the text file
        target_rssd: If specified, only return data for this institution
        mdrm_filter: List of MDRM codes to include (None = all)

    Returns:
        List of dictionaries with parsed data
    """
    records = []

    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            # First line is header
            header_line = f.readline().strip()
            headers = header_line.split('^')

            # Normalize headers to uppercase
            headers = [h.strip().upper() for h in headers]

            # Find the RSSD column
            rssd_col = None
            for i, h in enumerate(headers):
                if 'RSSD' in h or h == 'IDRSSD':
                    rssd_col = i
                    break

            if rssd_col is None:
                print(f"  Warning: Could not find RSSD column in {file_path}")
                return records

            for line in f:
                values = line.strip().split('^')

                # Skip if wrong number of columns
                if len(values) != len(headers):
                    continue

                # Check RSSD filter
                rssd = values[rssd_col].strip()
                if target_rssd and rssd != target_rssd:
                    continue

                # Create record dictionary
                record = {}
                for i, header in enumerate(headers):
                    record[header] = values[i].strip() if i < len(values) else ''

                records.append(record)

    except Exception as e:
        print(f"  Error parsing {file_path}: {e}")

    return records


def extract_financial_data(records, year, quarter, mdrm_filter=None):
    """
    Extract financial data from parsed records.

    Converts wide-format data (one row per institution with many columns)
    to long-format (one row per institution-period-account combination).

    Args:
        records: List of record dictionaries from parse_caret_delimited_file
        year: Reporting year
        quarter: Reporting quarter (1-4)
        mdrm_filter: List of MDRM codes to extract (None = all from config)

    Returns:
        List of tuples (rssd_id, report_date, year, quarter, mdrm_code, value)
    """
    if mdrm_filter is None:
        mdrm_filter = get_mdrm_codes_list()

    # Create report date
    quarter_ends = {1: '03-31', 2: '06-30', 3: '09-30', 4: '12-31'}
    report_date = f"{year}-{quarter_ends[quarter]}"

    data_tuples = []

    for record in records:
        # Get RSSD ID
        rssd = record.get('IDRSSD') or record.get('RSSD9001') or record.get('RSSD')
        if not rssd:
            continue

        # Extract each MDRM code
        for mdrm in mdrm_filter:
            # The MDRM code in the file might be uppercase
            value = record.get(mdrm) or record.get(mdrm.upper())

            if value and value not in ('', 'NA', 'N/A', '.'):
                try:
                    # Convert to float, handling thousands formatting
                    numeric_value = float(value.replace(',', ''))
                    data_tuples.append((
                        rssd,
                        report_date,
                        year,
                        quarter,
                        mdrm,
                        numeric_value
                    ))
                except ValueError:
                    # Skip non-numeric values
                    pass

    return data_tuples


def process_zip_file(zip_path, target_rssd=None, mdrm_filter=None):
    """
    Process a ZIP file containing Y-9C data.

    Args:
        zip_path: Path to the ZIP file
        target_rssd: Filter to specific institution
        mdrm_filter: Filter to specific MDRM codes

    Returns:
        List of record dictionaries
    """
    records = []

    try:
        # Create extraction directory based on zip filename
        extract_dir = PROCESSED_DIR / Path(zip_path).stem
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            for member in zf.namelist():
                # Process text files
                if member.lower().endswith('.txt') or member.lower().endswith('.csv'):
                    # Full path for extracted file
                    extracted_path = extract_dir / member

                    # Extract if not already extracted
                    if not extracted_path.exists():
                        zf.extract(member, extract_dir)
                        print(f"    Extracted: {member}")

                    # Parse the file
                    file_records = parse_caret_delimited_file(
                        extracted_path,
                        target_rssd=target_rssd,
                        mdrm_filter=mdrm_filter
                    )
                    records.extend(file_records)
                    print(f"    Found {len(file_records)} records for target RSSD")

    except zipfile.BadZipFile:
        print(f"  Bad ZIP file: {zip_path}")
    except Exception as e:
        print(f"  Error processing {zip_path}: {e}")

    return records


def load_quarter(year, quarter, target_rssd=USAA_HOLDING_COMPANY_RSSD, force=False):
    """
    Load data for a specific quarter.

    Args:
        year: Year to load
        quarter: Quarter to load (1-4)
        target_rssd: Institution to load (default: USAA)
        force: Force reload even if already loaded

    Returns:
        Number of records loaded
    """
    # Check if already loaded
    loaded = get_loaded_quarters()
    if (year, quarter) in loaded and not force:
        print(f"  {year} Q{quarter} already loaded. Use force=True to reload.")
        return 0

    # Find the ZIP file
    zip_patterns = [
        DATA_DIR / f"BHCF_{year}Q{quarter}.zip",
        DATA_DIR / f"BHCF_{year}Q{quarter}_chicago.zip",
    ]

    zip_path = None
    for pattern in zip_patterns:
        if pattern.exists():
            zip_path = pattern
            break

    if not zip_path:
        print(f"  No data file found for {year} Q{quarter}")
        return 0

    print(f"  Processing {zip_path.name}...")

    # Get MDRM codes to filter
    mdrm_filter = get_mdrm_codes_list()

    # Process the ZIP file
    records = process_zip_file(zip_path, target_rssd=target_rssd, mdrm_filter=mdrm_filter)

    if not records:
        print(f"  No records found for RSSD {target_rssd} in {year} Q{quarter}")
        record_load(year, quarter, str(zip_path), 0, 'no_data')
        return 0

    # Extract financial data
    data_tuples = extract_financial_data(records, year, quarter, mdrm_filter)

    if not data_tuples:
        print(f"  No matching MDRM codes found in {year} Q{quarter}")
        record_load(year, quarter, str(zip_path), 0, 'no_matching_codes')
        return 0

    # Bulk insert into database
    inserted = bulk_insert_financial_data(data_tuples)
    print(f"  Loaded {len(data_tuples)} data points for {year} Q{quarter}")

    # Record the load
    record_load(year, quarter, str(zip_path), len(data_tuples), 'completed')

    return len(data_tuples)


def load_all_data(start_year=2000, end_year=None, target_rssd=USAA_HOLDING_COMPANY_RSSD):
    """
    Load all available data into the database.

    Args:
        start_year: First year to load
        end_year: Last year to load (default: current year)
        target_rssd: Institution to load

    Returns:
        Total records loaded
    """
    if end_year is None:
        end_year = datetime.now().year

    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_year = datetime.now().year

    total_loaded = 0

    print(f"Loading Y-9C data for RSSD {target_rssd}...")
    print(f"Period: {start_year} to {end_year}")
    print("=" * 60)

    for year in range(start_year, end_year + 1):
        max_quarter = 4
        if year == current_year:
            max_quarter = current_quarter

        for quarter in range(1, max_quarter + 1):
            loaded = load_quarter(year, quarter, target_rssd)
            total_loaded += loaded

    print("=" * 60)
    print(f"Total: {total_loaded} data points loaded")

    return total_loaded


def incremental_update(target_rssd=USAA_HOLDING_COMPANY_RSSD):
    """
    Perform incremental update - only load new quarters.

    This is the function to call for quarterly updates.

    Args:
        target_rssd: Institution to update

    Returns:
        Number of new records loaded
    """
    # Get already loaded quarters
    loaded_quarters = set(get_loaded_quarters())

    # Determine quarters to check
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    new_loaded = 0

    print("Checking for new data...")

    # Check current year and previous year for any missing quarters
    for year in range(current_year - 1, current_year + 1):
        max_quarter = 4 if year < current_year else current_quarter

        for quarter in range(1, max_quarter + 1):
            if (year, quarter) not in loaded_quarters:
                print(f"  Found missing: {year} Q{quarter}")
                loaded = load_quarter(year, quarter, target_rssd)
                new_loaded += loaded

    if new_loaded == 0:
        print("  No new data to load.")
    else:
        print(f"  Loaded {new_loaded} new data points.")

    return new_loaded


def validate_data():
    """Validate loaded data for completeness."""
    conn = get_connection()
    cursor = conn.cursor()

    # Check data coverage
    cursor.execute("""
        SELECT year, quarter, COUNT(DISTINCT mdrm_code) as code_count,
               COUNT(*) as record_count
        FROM financial_data
        WHERE rssd_id = ?
        GROUP BY year, quarter
        ORDER BY year, quarter
    """, (USAA_HOLDING_COMPANY_RSSD,))

    print("\nData Coverage Summary:")
    print("-" * 50)
    print(f"{'Year':<6}{'Qtr':<6}{'MDRM Codes':<15}{'Records':<10}")
    print("-" * 50)

    for row in cursor.fetchall():
        print(f"{row['year']:<6}{row['quarter']:<6}{row['code_count']:<15}{row['record_count']:<10}")

    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load Y-9C data into database")
    parser.add_argument("--start-year", type=int, default=2000,
                        help="First year to load (default: 2000)")
    parser.add_argument("--end-year", type=int, default=None,
                        help="Last year to load (default: current year)")
    parser.add_argument("--rssd", type=str, default=USAA_HOLDING_COMPANY_RSSD,
                        help=f"Institution RSSD ID (default: {USAA_HOLDING_COMPANY_RSSD})")
    parser.add_argument("--update", action="store_true",
                        help="Perform incremental update only")
    parser.add_argument("--validate", action="store_true",
                        help="Validate loaded data")

    args = parser.parse_args()

    if args.validate:
        validate_data()
    elif args.update:
        incremental_update(args.rssd)
    else:
        load_all_data(args.start_year, args.end_year, args.rssd)
