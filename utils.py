#!/usr/bin/env python3
"""
Utility script for common database operations.
"""

import argparse
from database import MilexDatabase
import config


def list_countries():
    """List all countries in the database."""
    with MilexDatabase() as db:
        query = "SELECT DISTINCT country FROM expenditures ORDER BY country"
        import pandas as pd

        df = pd.read_sql_query(query, db.conn)

        if df.empty:
            print("No countries in database yet.")
            return

        print(f"Countries in database: {len(df)}")
        print("-" * 60)
        for country in df["country"]:
            print(f"  {country}")


def list_years():
    """List all years in the database."""
    with MilexDatabase() as db:
        query = "SELECT DISTINCT year FROM expenditures ORDER BY year"
        import pandas as pd

        df = pd.read_sql_query(query, db.conn)

        if df.empty:
            print("No years in database yet.")
            return

        print(f"Years in database: {df['year'].min()} - {df['year'].max()}")
        print("-" * 60)
        for year in df["year"]:
            print(f"  {year}")


def list_categories():
    """List all unique category names in the database."""
    print("This feature is not available - the database stores field data as columns.")
    print("Use --export to see all available field columns in the CSV output.")


def show_stats():
    """Show database statistics."""
    with MilexDatabase() as db:
        import pandas as pd

        # Total records
        total = pd.read_sql_query("SELECT COUNT(*) as count FROM expenditures", db.conn)
        print(f"Total country-year records: {total['count'].iloc[0]}")

        # Countries
        countries = pd.read_sql_query(
            "SELECT COUNT(DISTINCT country) as count FROM expenditures", db.conn
        )
        print(f"Unique countries: {countries['count'].iloc[0]}")

        # Years
        years = pd.read_sql_query(
            "SELECT MIN(year) as min_year, MAX(year) as max_year FROM expenditures",
            db.conn,
        )
        print(f"Year range: {years['min_year'].iloc[0]} - {years['max_year'].iloc[0]}")

        # Scraping status
        print("\nScraping status:")
        progress = db.get_scraping_progress()
        if not progress.empty:
            for _, row in progress.iterrows():
                print(f"  {row['status']}: {row['count']}")


def view_record(country: str, year: int):
    """View a specific record."""
    with MilexDatabase() as db:
        data = db.get_expenditure(country.upper(), year)

        if not data:
            print(f"No data found for {country} {year}")
            return

        print(f"Data for {country} {year}")
        print("=" * 60)
        print(f"Country: {data.get('country')}")
        print(f"Year: {data.get('year')}")
        print(f"National Currency: {data.get('national_currency')}")
        print(f"Unit of Measure: {data.get('unit_of_measure')}")
        print(f"Total Expenditure: {data.get('total_expenditure_all')}")
        print(f"Explanatory Remarks: {data.get('explanatory_remarks')}")
        print(f"Nil Report: {data.get('nil_report_expenditure')}")
        print(f"Single Figure Report: {data.get('single_figure_report_expenditure')}")
        print(f"Page Link: {data.get('page_link')}")

        categories = data.get("categories", {})
        if categories:
            print(f"\nCategories ({len(categories)}):")
            for cat, value in sorted(categories.items()):
                print(f"  {cat}: {value}")


def clear_database():
    """Clear all data from database (with confirmation)."""
    response = input("Are you sure you want to clear the entire database? (yes/no): ")

    if response.lower() != "yes":
        print("Cancelled.")
        return

    with MilexDatabase() as db:
        assert db.cursor is not None and db.conn is not None
        db.cursor.execute("DELETE FROM scraping_metadata")
        db.cursor.execute("DELETE FROM expenditures")
        db.conn.commit()

        print("Database cleared successfully.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="UN MILEX Database Utilities")

    parser.add_argument(
        "command",
        choices=[
            "list-countries",
            "list-years",
            "list-categories",
            "stats",
            "view",
            "clear",
        ],
        help="Command to execute",
    )

    parser.add_argument("--country", type=str, help="Country code (for view command)")
    parser.add_argument("--year", type=int, help="Year (for view command)")

    args = parser.parse_args()

    if args.command == "list-countries":
        list_countries()

    elif args.command == "list-years":
        list_years()

    elif args.command == "list-categories":
        list_categories()

    elif args.command == "stats":
        show_stats()

    elif args.command == "view":
        if not args.country or not args.year:
            print("Error: --country and --year required for view command")
            return 1
        view_record(args.country, args.year)

    elif args.command == "clear":
        clear_database()

    return 0


if __name__ == "__main__":
    exit(main())
