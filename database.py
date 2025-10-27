"""
Database management for UN Military Expenditures data.
Handles SQLite database creation, updates, and queries.
"""

import sqlite3
import os
from typing import Dict, List, Any, Optional
import pandas as pd
import config


class MilexDatabase:
    """Manages the SQLite database for military expenditures data."""

    def __init__(self, db_path: str = "milex_data.db"):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def _create_tables(self):
        """Create database tables if they don't exist."""
        assert self.cursor is not None and self.conn is not None

        # Build the CREATE TABLE statement with all 385 fields
        field_columns = []
        for field_name in config.ALL_FIELD_NAMES:
            # Normalize field name for SQL column (replace special chars with underscores)
            column_name = config.field_name_to_column_name(field_name)
            field_columns.append(f"{column_name} REAL")

        field_columns_sql = ",\n                ".join(field_columns)

        # Main expenditures table with all 385 MILEX fields
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS expenditures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country TEXT NOT NULL,
                year INTEGER NOT NULL,
                national_currency TEXT,
                unit_of_measure TEXT,
                total_expenditure_all REAL,
                explanatory_remarks TEXT,
                nil_report_expenditure TEXT,
                single_figure_report_expenditure TEXT,
                page_link TEXT,
                scrape_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                {field_columns_sql},
                UNIQUE(country, year)
            )
        """

        self.cursor.execute(create_table_sql)

        # Add single_figure_report_expenditure column if it doesn't exist (migration)
        try:
            self.cursor.execute(
                """
                ALTER TABLE expenditures 
                ADD COLUMN single_figure_report_expenditure TEXT
                """
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            # Column already exists, ignore
            pass

        # Metadata table for tracking scraping progress
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS scraping_metadata (
                country TEXT,
                year INTEGER,
                status TEXT,
                error_message TEXT,
                last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (country, year)
            )
        """
        )

        self.conn.commit()

    def insert_or_update_expenditure(self, data: Dict[str, Any]):
        """Insert or update expenditure data for a country-year.

        Args:
            data: Dictionary containing expenditure data including:
                - country, year, national_currency, unit_of_measure,
                  total_expenditure_all, explanatory_remarks, page_link
                - field_data: dict mapping field names to values
        """
        assert self.cursor is not None and self.conn is not None

        # Prepare base columns and values
        columns = [
            "country",
            "year",
            "national_currency",
            "unit_of_measure",
            "total_expenditure_all",
            "explanatory_remarks",
            "nil_report_expenditure",
            "single_figure_report_expenditure",
            "page_link",
        ]

        values = [
            data.get("country"),
            data.get("year"),
            data.get("national_currency"),
            data.get("unit_of_measure"),
            data.get("total_expenditure_all"),
            data.get("explanatory_remarks"),
            data.get("nil_report_expenditure"),
            data.get("single_figure_report_expenditure"),
            data.get("page_link"),
        ]

        # Add field data columns
        field_data = data.get("field_data", {})
        for field_name, field_value in field_data.items():
            # Normalize field name to column name
            column_name = config.field_name_to_column_name(field_name)
            columns.append(column_name)
            values.append(field_value)

        # Build INSERT OR REPLACE query
        placeholders = ", ".join(["?" for _ in values])
        columns_str = ", ".join(columns)

        self.cursor.execute(
            f"""
            INSERT OR REPLACE INTO expenditures 
            ({columns_str})
            VALUES ({placeholders})
            """,
            values,
        )

        self.conn.commit()

    def update_scraping_status(
        self, country: str, year: int, status: str, error_message: Optional[str] = None
    ):
        """Update scraping status for a country-year combination.

        Args:
            country: Country code
            year: Year
            status: Status ('success', 'failed', 'no_data', 'in_progress')
            error_message: Optional error message
        """
        assert self.cursor is not None and self.conn is not None

        self.cursor.execute(
            """
            INSERT OR REPLACE INTO scraping_metadata
            (country, year, status, error_message, last_attempt)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
            (country, year, status, error_message),
        )

        self.conn.commit()

    def get_expenditure(self, country: str, year: int) -> Optional[Dict[str, Any]]:
        """Get expenditure data for a specific country-year.

        Args:
            country: Country code
            year: Year

        Returns:
            Dictionary with expenditure data or None if not found
        """
        assert self.cursor is not None

        self.cursor.execute(
            """
            SELECT * FROM expenditures
            WHERE country = ? AND year = ?
        """,
            (country, year),
        )

        row = self.cursor.fetchone()
        if not row:
            return None

        columns = [desc[0] for desc in self.cursor.description]
        result = dict(zip(columns, row))

        return result

    def export_to_dataframe(self) -> pd.DataFrame:
        """Export all data to a pandas DataFrame in flat format.

        Returns:
            DataFrame with one row per country-year and columns for all fields
        """
        # Get all data - with 385 fields, this is now a simple query
        df = pd.read_sql_query(
            """
            SELECT * FROM expenditures
            ORDER BY country, year
        """,
            self.conn,
        )

        return df

    def get_scraping_progress(self) -> pd.DataFrame:
        """Get scraping progress statistics.

        Returns:
            DataFrame with scraping status summary
        """
        return pd.read_sql_query(
            """
            SELECT status, COUNT(*) as count
            FROM scraping_metadata
            GROUP BY status
        """,
            self.conn,
        )

    def get_failed_scrapes(self) -> pd.DataFrame:
        """Get list of failed scraping attempts.

        Returns:
            DataFrame with failed scraping attempts
        """
        return pd.read_sql_query(
            """
            SELECT country, year, error_message, last_attempt
            FROM scraping_metadata
            WHERE status = 'failed'
            ORDER BY last_attempt DESC
        """,
            self.conn,
        )

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
