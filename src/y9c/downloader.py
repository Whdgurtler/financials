"""
Y-9C Data Downloader

Downloads FR Y-9C bulk data files from the Federal Reserve / FFIEC.

This script handles:
1. Historical data from Chicago Fed (1986-2020)
2. Current data from FFIEC NIC (2021+)
"""

import requests
import zipfile
from datetime import datetime
from pathlib import Path
import time
import shutil

# Data directories at project root
DATA_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
MANUAL_DOWNLOAD_DIR = Path(__file__).parent.parent.parent / "data" / "manual_downloads"

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
    """Check if a file was manually downloaded."""
    ensure_directories()

    patterns = [
        f"BHCF_{year}Q{quarter}.zip",
        f"BHCF_{year}{quarter}.zip",
        f"bhcf_{year}q{quarter}.zip",
        f"BHCF{year}Q{quarter}.zip",
    ]

    for pattern in patterns:
        manual_path = MANUAL_DOWNLOAD_DIR / pattern
        if manual_path.exists():
            target_path = DATA_DIR / f"BHCF_{year}Q{quarter}.zip"
            shutil.copy2(manual_path, target_path)
            print(f"  Found manual download: {manual_path.name} -> {target_path.name}")
            return target_path

    for f in MANUAL_DOWNLOAD_DIR.glob("*.zip"):
        if str(year) in f.name and str(quarter) in f.name:
            target_path = DATA_DIR / f"BHCF_{year}Q{quarter}.zip"
            shutil.copy2(f, target_path)
            print(f"  Found manual download: {f.name} -> {target_path.name}")
            return target_path

    return None


def download_nic_data_selenium(year, quarter):
    """Download Y-9C data from FFIEC NIC using Selenium."""
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

        report_select = wait.until(EC.presence_of_element_located((By.ID, "ReportType")))
        Select(report_select).select_by_value("BHCF")

        year_select = wait.until(EC.presence_of_element_located((By.ID, "SelectedYear")))
        Select(year_select).select_by_value(str(year))

        quarter_select = wait.until(EC.presence_of_element_located((By.ID, "SelectedQuarter")))
        Select(quarter_select).select_by_value(str(quarter))

        download_btn = wait.until(EC.element_to_be_clickable((By.ID, "btnDownload")))
        download_btn.click()

        time.sleep(10)

        driver.quit()

        if output_file.exists():
            print(f"  Downloaded: {output_file.name}")
            return output_file
        else:
            for f in DATA_DIR.glob("*.zip"):
                if f.stat().st_mtime > time.time() - 60:
                    f.rename(output_file)
                    print(f"  Downloaded and renamed: {output_file.name}")
                    return output_file

        print(f"  Download may have failed for {year} Q{quarter}")
        return None

    except Exception as e:
        print(f"  Selenium error for {year} Q{quarter}: {e}")
        return None


def download_nic_data(year, quarter, max_retries=3):
    """Download Y-9C data from FFIEC NIC for a specific quarter."""
    date_str = get_quarter_dates(year, quarter)
    output_file = DATA_DIR / f"BHCF_{year}Q{quarter}.zip"

    if output_file.exists():
        print(f"  File already exists: {output_file.name}")
        return output_file

    manual_file = check_for_manual_download(year, quarter)
    if manual_file:
        return manual_file

    selenium_result = download_nic_data_selenium(year, quarter)
    if selenium_result:
        return selenium_result

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
                if response.content[:2] == b'PK':
                    with open(output_file, 'wb') as f:
                        f.write(response.content)
                    print(f"  Saved: {output_file.name}")
                    return output_file
                else:
                    break
            elif response.status_code == 404:
                print(f"  Data not available for {year} Q{quarter}")
                return None
            elif response.status_code == 403:
                break

        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            print(f"  Error on attempt {attempt + 1}: {e}")

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

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
    """Download historical Y-9C data from Chicago Fed (pre-2021)."""
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
    """Extract ZIP file contents to processed directory."""
    if not zip_path or not zip_path.exists():
        return []

    extract_dir = PROCESSED_DIR / zip_path.stem
    extract_dir.mkdir(exist_ok=True)

    extracted_files = []

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for member in zf.namelist():
                target_path = extract_dir / member

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
    """Download all Y-9C data from start_year to end_year."""
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

        max_quarter = 4
        if year == current_year:
            max_quarter = current_quarter

        for quarter in range(1, max_quarter + 1):
            if year < 2021:
                zip_path = download_chicago_fed_data(year, quarter)
                if not zip_path:
                    zip_path = download_nic_data(year, quarter)
            else:
                zip_path = download_nic_data(year, quarter)

            if zip_path:
                downloaded_files[(year, quarter)] = zip_path
                extract_zip_file(zip_path)

            time.sleep(1)

    print("\n" + "=" * 60)
    print(f"Download complete. {len(downloaded_files)} files downloaded.")

    return downloaded_files


def check_existing_data():
    """Check what data files already exist."""
    ensure_directories()

    existing = []
    for f in DATA_DIR.glob("BHCF_*.zip"):
        try:
            parts = f.stem.replace("BHCF_", "").replace("_chicago", "").split("Q")
            year = int(parts[0])
            quarter = int(parts[1])
            existing.append((year, quarter, f))
        except (ValueError, IndexError):
            pass

    return sorted(existing)


def generate_download_instructions(start_year=2000, end_year=None):
    """Generate manual download instructions for missing quarters."""
    ensure_directories()

    if end_year is None:
        end_year = datetime.now().year

    current_quarter = (datetime.now().month - 1) // 3 + 1
    current_year = datetime.now().year

    existing = {(y, q) for y, q, _ in check_existing_data()}

    missing = []
    for year in range(start_year, end_year + 1):
        max_quarter = 4 if year < current_year else current_quarter
        for quarter in range(1, max_quarter + 1):
            if (year, quarter) not in existing:
                missing.append((year, quarter))

    if not missing:
        print("All data files are present!")
        return None

    instructions_file = Path(__file__).parent.parent.parent / "DOWNLOAD_INSTRUCTIONS.txt"

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
        f.write("        python -m src.y9c.cli --init\n\n")

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
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--instructions", action="store_true")

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
