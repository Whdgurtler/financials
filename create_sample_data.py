"""
Create sample Y-9C data file for testing the pipeline.

This generates a minimal BHCF file in the FFIEC caret-delimited format
with sample USAA data to verify the loader works correctly.
"""

import zipfile
from pathlib import Path
from datetime import datetime

from y9c_config import (
    USAA_HOLDING_COMPANY_RSSD,
    get_mdrm_codes_list,
    BALANCE_SHEET_ITEMS,
    INCOME_STATEMENT_ITEMS,
)

DATA_DIR = Path(__file__).parent / "data" / "raw"


def create_sample_bhcf_file(year, quarter, rssd_id=USAA_HOLDING_COMPANY_RSSD):
    """
    Create a sample BHCF file in FFIEC format for testing.

    Args:
        year: Year for the sample data
        quarter: Quarter (1-4)
        rssd_id: Institution RSSD ID

    Returns:
        Path to created ZIP file
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Get all MDRM codes we need
    mdrm_codes = get_mdrm_codes_list()

    # Create header row
    header = ["IDRSSD", "RSSD9001", "RSSD9999"] + mdrm_codes
    header_line = "^".join(header)

    # Create sample data row for USAA
    # Generate plausible sample values (in thousands)
    sample_values = {
        # Balance sheet - Assets
        "BHCK2170": "150000000",  # Total assets ~$150B
        "BHCK0081": "5000000",    # Cash
        "BHCK0395": "2000000",    # Interest-bearing balances
        "BHCK1754": "10000000",   # HTM securities
        "BHCK1773": "25000000",   # AFS securities
        "BHCK2122": "80000000",   # Loans and leases
        "BHCK3123": "1500000",    # Allowance for losses
        "BHCKB528": "78500000",   # Net loans
        "BHCK2145": "1000000",    # Premises
        "BHCK3163": "500000",     # Goodwill
        "BHCK2160": "3000000",    # Other assets

        # Balance sheet - Liabilities
        "BHDM6631": "20000000",   # Noninterest deposits
        "BHDM6636": "100000000",  # Interest-bearing deposits
        "BHCK2948": "130000000",  # Total liabilities

        # Balance sheet - Equity
        "BHCK3210": "20000000",   # Total equity
        "BHCK3632": "15000000",   # Retained earnings
        "BHCK3300": "150000000",  # Total liab + equity

        # Income statement
        "BHCK4010": "5000000",    # Total interest income
        "BHCK4073": "1500000",    # Total interest expense
        "BHCK4074": "3500000",    # Net interest income
        "BHCK4230": "500000",     # Provision for losses
        "BHCK4079": "2000000",    # Total noninterest income
        "BHCK4093": "3000000",    # Total noninterest expense
        "BHCK4135": "1800000",    # Salaries
        "BHCK4301": "2000000",    # Pretax income
        "BHCK4302": "500000",     # Income taxes
        "BHCK4340": "1500000",    # Net income
    }

    # Build data row
    data_values = [rssd_id, rssd_id, f"{year}{quarter:02d}31"]
    for code in mdrm_codes:
        data_values.append(sample_values.get(code, ""))

    data_line = "^".join(data_values)

    # Create the text file content
    content = header_line + "\n" + data_line + "\n"

    # Create ZIP file
    txt_filename = f"BHCF_{year}{quarter:02d}31.txt"
    zip_path = DATA_DIR / f"BHCF_{year}Q{quarter}.zip"

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(txt_filename, content)

    print(f"Created sample file: {zip_path}")
    print(f"  Contains data for RSSD {rssd_id}")
    print(f"  Period: {year} Q{quarter}")

    return zip_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create sample Y-9C data for testing")
    parser.add_argument("--year", type=int, default=2023, help="Year (default: 2023)")
    parser.add_argument("--quarter", type=int, default=4, help="Quarter (default: 4)")

    args = parser.parse_args()

    create_sample_bhcf_file(args.year, args.quarter)
    print("\nNow run: python run_scraper.py --init --start 2023 --end 2023")
