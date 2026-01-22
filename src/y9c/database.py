"""
Y-9C Database Module

Creates and manages a SQLite database for storing Y-9C regulatory data.

Database Schema:
- institutions: Institution identifiers and metadata
- account_definitions: MDRM codes and their descriptions
- financial_data: Actual financial data values
- load_history: Track data loads for incremental updates
"""

import sqlite3
from pathlib import Path
from datetime import datetime

from .config import get_all_mdrm_codes, USAA_HOLDING_COMPANY_RSSD

# Database path - stored in data/ directory at project root
DB_PATH = Path(__file__).parent.parent.parent / "data" / "usaa_y9c.db"


def get_connection():
    """Get a database connection."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def create_schema():
    """Create the database schema."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS institutions (
            rssd_id TEXT PRIMARY KEY,
            name TEXT,
            city TEXT,
            state TEXT,
            entity_type TEXT,
            primary_regulator TEXT,
            parent_rssd_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS account_definitions (
            mdrm_code TEXT PRIMARY KEY,
            account_name TEXT NOT NULL,
            statement_type TEXT NOT NULL,
            category TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rssd_id TEXT NOT NULL,
            report_date DATE NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            mdrm_code TEXT NOT NULL,
            value REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(rssd_id, report_date, mdrm_code),
            FOREIGN KEY (mdrm_code) REFERENCES account_definitions(mdrm_code)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS load_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            source_file TEXT,
            records_loaded INTEGER,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'completed',
            UNIQUE(year, quarter)
        )
    """)

    # Create indexes
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_financial_data_rssd
        ON financial_data(rssd_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_financial_data_date
        ON financial_data(report_date)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_financial_data_mdrm
        ON financial_data(mdrm_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_financial_data_year_quarter
        ON financial_data(year, quarter)
    """)

    conn.commit()
    conn.close()
    print("Database schema created successfully.")


def populate_account_definitions():
    """Populate the account_definitions table with MDRM codes."""
    conn = get_connection()
    cursor = conn.cursor()

    all_codes = get_all_mdrm_codes()

    insert_count = 0
    for mdrm_code, info in all_codes.items():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO account_definitions
                (mdrm_code, account_name, statement_type, category)
                VALUES (?, ?, ?, ?)
            """, (mdrm_code, info["description"], info["statement"], info["category"]))
            insert_count += 1
        except sqlite3.Error as e:
            print(f"Error inserting {mdrm_code}: {e}")

    conn.commit()
    conn.close()
    print(f"Populated {insert_count} account definitions.")


def add_institution(rssd_id, name, city=None, state=None, entity_type=None,
                    primary_regulator=None, parent_rssd_id=None):
    """Add or update an institution."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO institutions
        (rssd_id, name, city, state, entity_type, primary_regulator, parent_rssd_id, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (rssd_id, name, city, state, entity_type, primary_regulator, parent_rssd_id,
          datetime.now()))

    conn.commit()
    conn.close()


def insert_financial_data(rssd_id, report_date, mdrm_code, value, year, quarter):
    """Insert a single financial data record."""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT OR REPLACE INTO financial_data
            (rssd_id, report_date, year, quarter, mdrm_code, value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (rssd_id, report_date, year, quarter, mdrm_code, value))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error inserting data: {e}")
    finally:
        conn.close()


def bulk_insert_financial_data(data_records):
    """
    Bulk insert financial data records.

    Args:
        data_records: List of tuples (rssd_id, report_date, year, quarter, mdrm_code, value)

    Returns:
        Number of records inserted
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.executemany("""
            INSERT OR REPLACE INTO financial_data
            (rssd_id, report_date, year, quarter, mdrm_code, value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, data_records)
        conn.commit()
        return cursor.rowcount
    except sqlite3.Error as e:
        print(f"Error in bulk insert: {e}")
        return 0
    finally:
        conn.close()


def record_load(year, quarter, source_file, records_loaded, status='completed'):
    """Record a data load in the history table."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO load_history
        (year, quarter, source_file, records_loaded, status)
        VALUES (?, ?, ?, ?, ?)
    """, (year, quarter, source_file, records_loaded, status))

    conn.commit()
    conn.close()


def get_loaded_quarters():
    """Get list of (year, quarter) tuples that have been loaded."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT year, quarter FROM load_history
        WHERE status = 'completed'
        ORDER BY year, quarter
    """)

    results = [(row['year'], row['quarter']) for row in cursor.fetchall()]
    conn.close()
    return results


def get_balance_sheet(rssd_id, report_date=None, year=None, quarter=None):
    """Get balance sheet data for an institution."""
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT ad.account_name, ad.category, fd.value
        FROM financial_data fd
        JOIN account_definitions ad ON fd.mdrm_code = ad.mdrm_code
        WHERE fd.rssd_id = ?
        AND ad.statement_type = 'balance_sheet'
    """
    params = [rssd_id]

    if report_date:
        query += " AND fd.report_date = ?"
        params.append(report_date)
    elif year and quarter:
        query += " AND fd.year = ? AND fd.quarter = ?"
        params.extend([year, quarter])

    query += " ORDER BY ad.category, ad.account_name"

    cursor.execute(query, params)
    results = {row['account_name']: row['value'] for row in cursor.fetchall()}
    conn.close()
    return results


def get_income_statement(rssd_id, report_date=None, year=None, quarter=None):
    """Get income statement data for an institution."""
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT ad.account_name, ad.category, fd.value
        FROM financial_data fd
        JOIN account_definitions ad ON fd.mdrm_code = ad.mdrm_code
        WHERE fd.rssd_id = ?
        AND ad.statement_type = 'income_statement'
    """
    params = [rssd_id]

    if report_date:
        query += " AND fd.report_date = ?"
        params.append(report_date)
    elif year and quarter:
        query += " AND fd.year = ? AND fd.quarter = ?"
        params.extend([year, quarter])

    query += " ORDER BY ad.category, ad.account_name"

    cursor.execute(query, params)
    results = {row['account_name']: row['value'] for row in cursor.fetchall()}
    conn.close()
    return results


def get_time_series(rssd_id, mdrm_code=None, account_name=None, start_year=None, end_year=None):
    """Get time series data for a specific account."""
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT fd.report_date, fd.year, fd.quarter, fd.value, ad.account_name
        FROM financial_data fd
        JOIN account_definitions ad ON fd.mdrm_code = ad.mdrm_code
        WHERE fd.rssd_id = ?
    """
    params = [rssd_id]

    if mdrm_code:
        query += " AND fd.mdrm_code = ?"
        params.append(mdrm_code)
    elif account_name:
        query += " AND ad.account_name LIKE ?"
        params.append(f"%{account_name}%")

    if start_year:
        query += " AND fd.year >= ?"
        params.append(start_year)
    if end_year:
        query += " AND fd.year <= ?"
        params.append(end_year)

    query += " ORDER BY fd.report_date"

    cursor.execute(query, params)
    results = [(row['report_date'], row['value']) for row in cursor.fetchall()]
    conn.close()
    return results


def get_all_periods(rssd_id):
    """Get all available periods for an institution."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT year, quarter, report_date
        FROM financial_data
        WHERE rssd_id = ?
        ORDER BY year, quarter
    """, (rssd_id,))

    results = [(row['year'], row['quarter'], row['report_date']) for row in cursor.fetchall()]
    conn.close()
    return results


def export_to_csv(rssd_id, output_path, statement_type=None):
    """Export data to CSV format."""
    import csv

    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT fd.report_date, fd.year, fd.quarter, fd.mdrm_code,
               ad.account_name, ad.statement_type, ad.category, fd.value
        FROM financial_data fd
        JOIN account_definitions ad ON fd.mdrm_code = ad.mdrm_code
        WHERE fd.rssd_id = ?
    """
    params = [rssd_id]

    if statement_type:
        query += " AND ad.statement_type = ?"
        params.append(statement_type)

    query += " ORDER BY fd.report_date, ad.statement_type, ad.category, ad.account_name"

    cursor.execute(query, params)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['report_date', 'year', 'quarter', 'mdrm_code',
                         'account_name', 'statement_type', 'category', 'value'])
        for row in cursor.fetchall():
            writer.writerow(list(row))

    conn.close()
    print(f"Exported data to {output_path}")


def initialize_database():
    """Initialize the database with schema and USAA institution data."""
    create_schema()
    populate_account_definitions()

    add_institution(
        rssd_id=USAA_HOLDING_COMPANY_RSSD,
        name="United Services Automobile Association",
        city="San Antonio",
        state="TX",
        entity_type="Savings & Loan Holding Company",
        primary_regulator="FRS"
    )

    print("Database initialized successfully.")
    print(f"Database location: {DB_PATH}")


if __name__ == "__main__":
    initialize_database()
