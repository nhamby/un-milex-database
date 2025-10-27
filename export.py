"""
Export utility for UN Military Expenditures database.
Exports SQLite database to CSV format.
"""

import argparse
import os
from datetime import datetime

from database import MilexDatabase


def export_to_csv(output_file: str = "milex_data.csv"):
    """Export database to CSV file.

    Args:
        output_file: Path to output CSV file
    """
    print(f"Exporting database to {output_file}...")

    # Open database
    with MilexDatabase() as db:
        # Export to DataFrame
        df = db.export_to_dataframe()

        if df.empty:
            print("No data to export!")
            return

        # Sort by country and year
        df = df.sort_values(["country", "year"])

        # Export to CSV
        df.to_csv(output_file, index=False, na_rep="NA")

        print(f"Exported {len(df)} rows to {output_file}")
        print(f"  Countries: {df['country'].nunique()}")
        print(f"  Columns: {len(df.columns)}")


def export_summary_stats(output_file: str = "milex_summary.csv"):
    """Export summary statistics to CSV.

    Args:
        output_file: Path to output CSV file
    """
    print(f"Exporting summary statistics to {output_file}...")

    with MilexDatabase() as db:
        df = db.export_to_dataframe()

        if df.empty:
            print("No data to export!")
            return

        # Calculate summary stats
        summary = (
            df.groupby("country")
            .agg(
                {
                    "year": ["min", "max", "count"],
                    "total_expenditure_all": ["mean", "min", "max"],
                }
            )
            .reset_index()
        )

        # Flatten column names
        summary.columns = ["_".join(col).strip("_") for col in summary.columns.values]

        # Export
        summary.to_csv(output_file, index=False)

        print(f"Exported summary for {len(summary)} countries to {output_file}")


def export_category_pivot(output_file: str = "milex_categories.csv"):
    """Export only category data in pivot format.

    This feature is deprecated as the database now stores all field data as columns.
    Use export_to_csv() for the full data export instead.

    Args:
        output_file: Path to output CSV file
    """
    print("Category pivot export is no longer available.")
    print(
        "The database now stores all 385 MILEX fields as columns in the expenditures table."
    )
    print(
        "Use the regular export (--export) to get all data including all field columns."
    )


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export UN Military Expenditures database to CSV"
    )

    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="milex_data.csv",
        help="Output CSV file path (default: milex_data.csv)",
    )

    parser.add_argument(
        "--summary",
        action="store_true",
        help="Export summary statistics instead of full data",
    )

    parser.add_argument(
        "--categories", action="store_true", help="Export only category pivot table"
    )

    parser.add_argument("--all", action="store_true", help="Export all reports")

    args = parser.parse_args()

    try:
        if args.all:
            # Export everything
            export_to_csv("milex_data.csv")
            export_summary_stats("milex_summary.csv")
            print("\nAll exports completed successfully!")

        elif args.summary:
            export_summary_stats(args.output)

        elif args.categories:
            export_category_pivot(args.output)

        else:
            # Default: export full data
            export_to_csv(args.output)

    except Exception as e:
        print(f"Error during export: {str(e)}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
