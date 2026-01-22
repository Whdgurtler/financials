"""
Y-9C Data Downloader
Downloads FR Y-9C bulk data files from the Federal Reserve / FFIEC.

This script handles:
1. Historical data from Chicago Fed (1986-2020)
2. Current data from FFIEC NIC (2021+)

The data is downloaded as caret-delimited (^) text files.

NOTE: The FFIEC NIC website requires browser-based downloading due to
JavaScript-based interface. This module provides:
1. Selenium-based automated download (if selenium is installed)
2. Manual download instructions with file placement guidance
3. Detection of manually placed files
"""

import os
import requests
import zipfile
import io
from datetime import datetime
from pathlib import Path
import time
import shutil

# Configuration
DATA_DIR = Path(__file__).parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent / "data" / "processed"
MANUAL_DOWNLOAD_DIR = Path(__file__).parent / "data" / "manual_downloads"

# FFIEC Download URL for manual instructions
FFIEC_DOWNLOAD_URL = "https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload"


def ensure_directories():
    """Create necessary directories if they don't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def get_quarter_dates(year, quarter):
    """Return the quarter end date in YYYYMMDD format."""
    quarter_ends = {
        1: f"{year}0331",
        2: f"{year}0630",
        3: f"{year}0930",
        4: f"{year}1231",
    }
    return quarter_ends[quarter]


def check_for_manual_download(year, quarter):
    """
    Check if a file was manually downloaded and placed in the manual_downloads folder.

    Expected filename patterns:
    - BHCF_{year}Q{quarter}.zip
    - BHCF_{year}{quarter}.zip
    - bhcf_{year}q{quarter}.zip (case insensitive)
    - Any ZIP file containing the year and quarter

    Args:
        year: Year
        quarter: Quarter (1-4)

    Returns:
        Path to file if found, None otherwise
    """
    ensure_directories()

    # Check various filename patterns
    patterns = [
        f"BHCF_{year}Q{quarter}.zip",
        f"BHCF_{year}{quarter}.zip",
        f"bhcf_{year}q{quarter}.zip",
        f"BHCF{year}Q{quarter}.zip",
    ]

    for pattern in patterns:
        # Check in manual downloads folder
        manual_path = MANUAL_DOWNLOAD_DIR / pattern
        if manual_path.exists():
            # Move to raw data folder with standard naming
            target_path = DATA_DIR / f"BHCF_{year}Q{quarter}.zip"
            shutil.copy2(manual_path, target_path)
            print(f"  Found manual download: {manual_path.name} -> {target_path.name}")
            return target_path

    # Also check for any file that might match (downloads often have different names)
    for f in MANUAL_DOWNLOAD_DIR.glob("*.zip"):
        if str(year) in f.name and str(quarter) in f.name:
            target_path = DATA_DIR / f"BHCF_{year}Q{quarter}.zip"
            shutil.copy2(f, target_path)
            print(f"  Found manual download: {f.name} -> {target_path.name}")
            return target_path

    return None


def download_nic_data_selenium(year, quarter):
    """
    Download Y-9C data from FFIEC NIC using Selenium.

    This method uses a browser to navigate the JavaScript-based download interface.

    Args:
        year: Year
        quarter: Quarter (1-4)

    Returns:
        Path to downloaded file or None if failed
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select, WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        print("  Selenium not installed. Install with: pip install selenium")
        return None

    output_file = DATA_DIR / f"BHCF_{year}Q{quarter}.zip"

    if output_file.exists():
        print(f"  File already exists: {output_file.name}")
        return output_file

    # Configure Chrome for downloads
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": str(DATA_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    })

    try:
        driver = webdriver.Chrome(options=chrome_options)
        wait = WebDriverWait(driver, 30)

        print(f"  Opening FFIEC download page for {year} Q{quarter}...")
        driver.get(FFIEC_DOWNLOAD_URL)

        # Wait for and select report type (BHCF)
        report_select = wait.until(EC.presence_of_element_located((By.ID, "ReportType")))
        Select(report_select).select_by_value("BHCF")

        # Select year
        year_select = wait.until(EC.presence_of_element_located((By.ID, "SelectedYear")))
        Select(year_select).select_by_value(str(year))

        # Select quarter
        quarter_select = wait.until(EC.presence_of_element_located((By.ID, "SelectedQuarter")))
        Select(quarter_select).select_by_value(str(quarter))

        # Click download button
        download_btn = wait.until(EC.element_to_be_clickable((By.ID, "btnDownload")))
        download_btn.click()

        # Wait for download to complete
        time.sleep(10)  # Give time for download

        driver.quit()

        # Rename downloaded file if needed
        if output_file.exists():
            print(f"  Downloaded: {output_file.name}")
            return output_file
        else:
            # Look for downloaded file with different name
            for f in DATA_DIR.glob("*.zip"):
                if f.stat().st_mtime > time.time() - 60:  # Modified in last minute
                    f.rename(output_file)
                    print(f"  Downloaded and renamed: {output_file.name}")
                    return output_file

        print(f"  Download may have failed for {year} Q{quarter}")
        return None

    except Exception as e:
        print(f"  Selenium error for {year} Q{quarter}: {e}")
        return None


def download_nic_data(year, quarter, max_retries=3):
    """
    Download Y-9C data from FFIEC NIC for a specific quarter.

    The NIC provides data in caret-delimited (^) text files within a ZIP archive.

    This function will:
    1. Check if file already exists
    2. Check for manually downloaded files
    3. Try Selenium-based download
    4. Fall back to direct HTTP request (may fail due to JavaScript requirement)

    Args:
        year: Year (e.g., 2023)
        quarter: Quarter (1-4)
        max_retries: Number of retry attempts

    Returns:
        Path to downloaded file or None if failed
    """
    date_str = get_quarter_dates(year, quarter)
    output_file = DATA_DIR / f"BHCF_{year}Q{quarter}.zip"

    # Check if file already exists
    if output_file.exists():
        print(f"  File already exists: {output_file.name}")
        return output_file

    # Check for manually downloaded file
    manual_file = check_for_manual_download(year, quarter)
    if manual_file:
        return manual_file

    # Try Selenium-based download
    selenium_result = download_nic_data_selenium(year, quarter)
    if selenium_result:
        return selenium_result

    # Fall back to direct HTTP request (usually blocked by FFIEC)
    url = f"https://www.ffiec.gov/npw/FinancialReport/ReturnFinancialReportZip?rpt=BHCF&date={date_str}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/zip, application/octet-stream, */*",
    }

    for attempt in range(max_retries):
        try:
            print(f"  Trying direct download {year} Q{quarter} (attempt {attempt + 1})...")
            response = requests.get(url, headers=headers, timeout=120)

            if response.status_code == 200:
                # Check if we got a zip file
                if response.content[:2] == b'PK':  # ZIP file magic number
                    with open(output_file, 'wb') as f:
                        f.write(response.content)
                    print(f"  Saved: {output_file.name}")
                    return output_file
                else:
                    break  # Got HTML page, not zip
            elif response.status_code == 404:
                print(f"  Data not available for {year} Q{quarter}")
                return None
            elif response.status_code == 403:
                break  # Access denied, need browser

        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            print(f"  Error on attempt {attempt + 1}: {e}")

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

    # Direct download failed - provide manual instructions
    print(f"\n  ** Manual download required for {year} Q{quarter} **")
    print(f"  1. Go to: {FFIEC_DOWNLOAD_URL}")
    print(f"  2. Select: Report Type = BHCF")
    print(f"  3. Select: Year = {year}")
    print(f"  4. Select: Quarter = {quarter}")
    print(f"  5. Click Download")
    print(f"  6. Save file to: {MANUAL_DOWNLOAD_DIR}")
    print(f"  7. Re-run this script\n")

    return None


def download_chicago_fed_data(year, quarter, max_retries=3):
    """
    Download historical Y-9C data from Chicago Fed (pre-2021).

    The Chicago Fed provides SAS transport files (.xpt) in ZIP archives.

    Args:
        year: Year (e.g., 2015)
        quarter: Quarter (1-4)
        max_retries: Number of retry attempts

    Returns:
        Path to downloaded file or None if failed
    """
    # Chicago Fed URL pattern
    url = f"https://www.chicagofed.org/api/sitecore/BHCHome/BHCDownload?SelectedQuarter={quarter}&SelectedYear={year}"

    output_file = DATA_DIR / f"BHCF_{year}Q{quarter}_chicago.zip"

    if output_file.exists():
        print(f"  File already exists: {output_file.name}")
        return output_file

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
    }

    for attempt in range(max_retries):
        try:
            print(f"  Downloading {year} Q{quarter} from Chicago Fed...")
            response = requests.get(url, headers=headers, timeout=120, allow_redirects=True)

            if response.status_code == 200 and len(response.content) > 1000:
                with open(output_file, 'wb') as f:
                    f.write(response.content)
                print(f"  Saved: {output_file.name}")
                return output_file
            elif response.status_code == 404:
                print(f"  Data not available for {year} Q{quarter}")
                return None
            else:
                print(f"  HTTP {response.status_code} (size: {len(response.content)}) for {year} Q{quarter}")

        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            print(f"  Error on attempt {attempt + 1}: {e}")

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

    return None


def extract_zip_file(zip_path):
    """
    Extract ZIP file contents to processed directory.

    Args:
        zip_path: Path to ZIP file

    Returns:
        List of extracted file paths
    """
    if not zip_path or not zip_path.exists():
        return []

    extract_dir = PROCESSED_DIR / zip_path.stem
    extract_dir.mkdir(exist_ok=True)

    extracted_files = []

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for member in zf.namelist():
                # Extract to processed directory
                target_path = extract_dir / member

                # Skip if already extracted
                if target_path.exists():
                    extracted_files.append(target_path)
                    continue

                zf.extract(member, extract_dir)
                extracted_files.append(target_path)
                print(f"    Extracted: {member}")

    except zipfile.BadZipFile:
        print(f"  Bad ZIP file: {zip_path}")
    except Exception as e:
        print(f"  Error extracting {zip_path}: {e}")

    return extracted_files


def download_all_y9c_data(start_year=2000, end_year=None):
    """
    Download all Y-9C data from start_year to end_year.

    Uses Chicago Fed for historical data (pre-2021) and NIC for recent data (2021+).

    Args:
        start_year: First year to download (default: 2000)
        end_year: Last year to download (default: current year)

    Returns:
        Dictionary of {(year, quarter): file_path} for successful downloads
    """
    ensure_directories()

    if end_year is None:
        end_year = datetime.now().year

    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_year = datetime.now().year

    downloaded_files = {}

    print(f"Downloading Y-9C data from {start_year} to {end_year}...")
    print("=" * 60)

    for year in range(start_year, end_year + 1):
        print(f"\nYear {year}:")

        # Determine how many quarters to download for this year
        max_quarter = 4
        if year == current_year:
            # Don't try to download future quarters
            max_quarter = current_quarter

        for quarter in range(1, max_quarter + 1):
            # Use different source based on year
            if year < 2021:
                # Try Chicago Fed first
                zip_path = download_chicago_fed_data(year, quarter)
                if not zip_path:
                    # Fall back to NIC
                    zip_path = download_nic_data(year, quarter)
            else:
                # Use NIC for 2021+
                zip_path = download_nic_data(year, quarter)

            if zip_path:
                downloaded_files[(year, quarter)] = zip_path
                # Extract the file
                extract_zip_file(zip_path)

            # Be nice to the servers
            time.sleep(1)

    print("\n" + "=" * 60)
    print(f"Download complete. {len(downloaded_files)} files downloaded.")

    return downloaded_files


def check_existing_data():
    """Check what data files already exist."""
    ensure_directories()

    existing = []
    for f in DATA_DIR.glob("BHCF_*.zip"):
        # Parse filename to get year and quarter
        try:
            parts = f.stem.replace("BHCF_", "").replace("_chicago", "").split("Q")
            year = int(parts[0])
            quarter = int(parts[1])
            existing.append((year, quarter, f))
        except (ValueError, IndexError):
            pass

    return sorted(existing)


def generate_download_instructions(start_year=2000, end_year=None):
    """
    Generate manual download instructions for missing quarters.

    Creates a text file with step-by-step instructions for downloading
    data from the FFIEC website.

    Args:
        start_year: First year to check
        end_year: Last year to check

    Returns:
        Path to instructions file
    """
    ensure_directories()

    if end_year is None:
        end_year = datetime.now().year

    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_year = datetime.now().year

    # Get existing files
    existing = {(y, q) for y, q, _ in check_existing_data()}

    # Find missing quarters
    missing = []
    for year in range(start_year, end_year + 1):
        max_quarter = 4 if year < current_year else current_quarter
        for quarter in range(1, max_quarter + 1):
            if (year, quarter) not in existing:
                missing.append((year, quarter))

    if not missing:
        print("All data files are present!")
        return None

    # Generate instructions file
    instructions_file = Path(__file__).parent / "DOWNLOAD_INSTRUCTIONS.txt"

    with open(instructions_file, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("Y-9C DATA MANUAL DOWNLOAD INSTRUCTIONS\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Missing quarters: {len(missing)}\n\n")

        f.write("STEP 1: Go to the FFIEC Financial Data Download page:\n")
        f.write(f"        {FFIEC_DOWNLOAD_URL}\n\n")

        f.write("STEP 2: For each quarter below, do the following:\n")
        f.write("        a) Select Report Type: BHCF (FR Y-9C)\n")
        f.write("        b) Select the Year\n")
        f.write("        c) Select the Quarter\n")
        f.write("        d) Click 'Download'\n")
        f.write("        e) Save the ZIP file\n\n")

        f.write("STEP 3: Place all downloaded files in:\n")
        f.write(f"        {MANUAL_DOWNLOAD_DIR}\n\n")

        f.write("STEP 4: Re-run the scraper:\n")
        f.write("        python run_scraper.py --init\n\n")

        f.write("-" * 70 + "\n")
        f.write("QUARTERS TO DOWNLOAD:\n")
        f.write("-" * 70 + "\n\n")

        for year, quarter in missing:
            date_str = get_quarter_dates(year, quarter)
            f.write(f"  [ ] {year} Q{quarter}  (Report Date: {date_str[:4]}-{date_str[4:6]}-{date_str[6:]})\n")

        f.write("\n" + "=" * 70 + "\n")
        f.write(f"Total files to download: {len(missing)}\n")
        f.write("=" * 70 + "\n")

    print(f"\nDownload instructions saved to: {instructions_file}")
    print(f"Missing quarters: {len(missing)}")

    return instructions_file


def print_missing_quarters(start_year=2000, end_year=None):
    """Print a summary of missing quarters."""
    ensure_directories()

    if end_year is None:
        end_year = datetime.now().year

    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_year = datetime.now().year

    # Get existing files
    existing = {(y, q) for y, q, _ in check_existing_data()}

    print(f"\nData Coverage Summary ({start_year} - {end_year}):")
    print("-" * 50)

    missing_count = 0
    for year in range(start_year, end_year + 1):
        max_quarter = 4 if year < current_year else current_quarter
        year_status = []

        for quarter in range(1, max_quarter + 1):
            if (year, quarter) in existing:
                year_status.append(f"Q{quarter}[OK]")
            else:
                year_status.append(f"Q{quarter}[--]")
                missing_count += 1

        print(f"  {year}: {' '.join(year_status)}")

    print("-" * 50)
    print(f"Existing: {len(existing)} | Missing: {missing_count}")

    return missing_count


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download FR Y-9C bulk data files")
    parser.add_argument("--start-year", type=int, default=2000,
                        help="First year to download (default: 2000)")
    parser.add_argument("--end-year", type=int, default=None,
                        help="Last year to download (default: current year)")
    parser.add_argument("--check", action="store_true",
                        help="Check existing downloaded files")
    parser.add_argument("--status", action="store_true",
                        help="Show download status by quarter")
    parser.add_argument("--instructions", action="store_true",
                        help="Generate manual download instructions")

    args = parser.parse_args()

    if args.check:
        existing = check_existing_data()
        print(f"Found {len(existing)} existing data files:")
        for year, quarter, path in existing:
            print(f"  {year} Q{quarter}: {path.name}")
    elif args.status:
        print_missing_quarters(args.start_year, args.end_year)
    elif args.instructions:
        generate_download_instructions(args.start_year, args.end_year)
    else:
        download_all_y9c_data(args.start_year, args.end_year)
