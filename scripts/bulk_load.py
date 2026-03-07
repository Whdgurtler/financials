"""
Bulk download and load all Y-9C quarters from 2002 Q1 to 2026 Q1.
Runs download + load sequentially, then filters to >= $10B institutions.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.y9c.downloader import download_chicago_fed_data, download_nic_data, get_quarter_dates
from src.y9c.loader import load_quarter
from src.y9c.database import get_connection

START_YEAR = 2002
END_YEAR = 2026
END_QUARTER = 1  # stop at Q1 2026

ASSET_THRESHOLD_THOUSANDS = 10_000_000  # $10B in thousands


def iter_quarters(start_year, end_year, end_quarter):
    for year in range(start_year, end_year + 1):
        max_q = 4 if year < end_year else end_quarter
        for quarter in range(1, max_q + 1):
            yield year, quarter


def download_quarter(year, quarter):
    """Download one quarter, returns path or None."""
    if year < 2021 or (year == 2021 and quarter == 1):
        result = download_chicago_fed_data(year, quarter)
        if not result:
            result = download_nic_data(year, quarter)
    else:
        result = download_nic_data(year, quarter)
    return result


def filter_small_institutions():
    """Delete all records for institutions that never crossed $10B in assets."""
    print("\nFiltering institutions with < $10B in assets...")
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(DISTINCT rssd_id) FROM financial_data
    """)
    total_before = cur.fetchone()[0]

    cur.execute("""
        DELETE FROM financial_data
        WHERE rssd_id IN (
            SELECT rssd_id FROM financial_data
            WHERE mdrm_code = 'BHCK2170'
            GROUP BY rssd_id
            HAVING MAX(value) < ?
        )
    """, (ASSET_THRESHOLD_THOUSANDS,))
    conn.commit()

    cur.execute("SELECT COUNT(DISTINCT rssd_id) FROM financial_data")
    total_after = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM financial_data")
    records_remaining = cur.fetchone()[0]

    conn.close()
    removed = total_before - total_after
    print(f"  Removed {removed} institutions below $10B threshold")
    print(f"  Kept {total_after} institutions ({records_remaining:,} records)")


def main():
    quarters = list(iter_quarters(START_YEAR, END_YEAR, END_QUARTER))
    total = len(quarters)
    print(f"Processing {total} quarters ({START_YEAR} Q1 - {END_YEAR} Q{END_QUARTER})\n")

    downloaded = 0
    loaded = 0
    failed = []

    for i, (year, quarter) in enumerate(quarters, 1):
        print(f"[{i}/{total}] {year} Q{quarter}")
        path = download_quarter(year, quarter)
        if path:
            downloaded += 1
            n = load_quarter(year, quarter, target_rssd=None)
            loaded += n
            print(f"  -> {n:,} records loaded")
        else:
            failed.append((year, quarter))
        time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"Download complete: {downloaded}/{total} quarters")
    print(f"Total records loaded: {loaded:,}")
    if failed:
        print(f"Failed quarters ({len(failed)}): {failed[:10]}")

    filter_small_institutions()

    print("\nDone.")


if __name__ == "__main__":
    main()
