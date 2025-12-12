"""
Database transformation script for UN MILEX data.
Adds derived columns for dimensional analysis and unit normalization.
"""

import sqlite3
import argparse
import re
import shutil
from pathlib import Path
from typing import Optional


def parse_unit_multiplier(unit_of_measure: Optional[str]) -> Optional[float]:
    """Parse unit of measure string and return numeric multiplier.

    Args:
        unit_of_measure: Unit string like "Thousands", "Millions", "No Multiplier"

    Returns:
        Numeric multiplier (e.g., 1000 for "Thousands", 1 for "No Multiplier")
        None if unable to parse
    """
    if not unit_of_measure:
        return None

    unit_lower = unit_of_measure.lower().strip()

    # Define multipliers
    multipliers = {
        "thousand": 1_000,
        "million": 1_000_000,
        "billion": 1_000_000_000,
        "trillion": 1_000_000_000_000,
        "no multiplier": 1,
        "units": 1,
        "ones": 1,
    }

    # Check for each multiplier keyword
    for keyword, value in multipliers.items():
        if keyword in unit_lower:
            return value

    # Try to extract number if present (e.g., "1000s")
    number_match = re.search(r"(\d+)", unit_lower)
    if number_match:
        return float(number_match.group(1))

    # Default to 1 if we can't determine
    print(f"Warning: Could not parse unit '{unit_of_measure}', defaulting to 1")
    return 1.0


def add_derived_columns(
    db_path: str = "milex_data.db", output_path: Optional[str] = None
):
    """Add derived columns to a copy of the database.

    Args:
        db_path: Path to source SQLite database
        output_path: Path for output database (default: adds '_transformed' suffix)
    """
    # Determine output path
    source_path = Path(db_path)
    if output_path is None:
        output_db = (
            source_path.parent / f"{source_path.stem}_transformed{source_path.suffix}"
        )
    else:
        output_db = Path(output_path)

    # Copy the original database
    print(f"Copying database: {db_path} -> {output_db}")
    shutil.copy2(db_path, output_db)

    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()

    print(f"Processing database: {output_db}")
    print("-" * 60)

    # Step 1: Add unit_of_measure_number column if it doesn't exist
    try:
        cursor.execute(
            """
            ALTER TABLE expenditures 
            ADD COLUMN unit_of_measure_number REAL
            """
        )
        print("✓ Added column: unit_of_measure_number")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("✓ Column already exists: unit_of_measure_number")
        else:
            raise

    # Step 2: Add total_expenditure_all_dim_analysis column if it doesn't exist
    try:
        cursor.execute(
            """
            ALTER TABLE expenditures 
            ADD COLUMN total_expenditure_all_dim_analysis REAL
            """
        )
        print("✓ Added column: total_expenditure_all_dim_analysis")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("✓ Column already exists: total_expenditure_all_dim_analysis")
        else:
            raise

    conn.commit()

    # Step 3: Populate unit_of_measure_number
    print("\nPopulating unit_of_measure_number...")

    cursor.execute(
        """
        SELECT id, country, year, unit_of_measure 
        FROM expenditures
        WHERE unit_of_measure IS NOT NULL
        """
    )

    records = cursor.fetchall()
    print(f"Found {len(records)} records with unit_of_measure data")

    # Track statistics
    stats = {
        "thousand": 0,
        "million": 0,
        "billion": 0,
        "no_multiplier": 0,
        "other": 0,
        "null": 0,
    }

    updates = []
    for record_id, country, year, unit in records:
        multiplier = parse_unit_multiplier(unit)

        # Track statistics
        if multiplier == 1_000:
            stats["thousand"] += 1
        elif multiplier == 1_000_000:
            stats["million"] += 1
        elif multiplier == 1_000_000_000:
            stats["billion"] += 1
        elif multiplier == 1:
            stats["no_multiplier"] += 1
        else:
            stats["other"] += 1

        updates.append((multiplier, record_id))

    cursor.executemany(
        """
        UPDATE expenditures 
        SET unit_of_measure_number = ?
        WHERE id = ?
        """,
        updates,
    )
    conn.commit()
    print(f"✓ Updated {len(updates)} records")

    # Print statistics
    print("\nUnit of Measure Statistics:")
    print(f"  Thousands:      {stats['thousand']:,}")
    print(f"  Millions:       {stats['million']:,}")
    print(f"  Billions:       {stats['billion']:,}")
    print(f"  No Multiplier:  {stats['no_multiplier']:,}")
    print(f"  Other:          {stats['other']:,}")

    # Step 4: Populate total_expenditure_all_dim_analysis
    print("\nPopulating total_expenditure_all_dim_analysis...")

    cursor.execute(
        """
        SELECT id, country, year, total_expenditure_all, unit_of_measure_number
        FROM expenditures
        WHERE total_expenditure_all IS NOT NULL 
          AND unit_of_measure_number IS NOT NULL
        """
    )

    records = cursor.fetchall()
    print(f"Found {len(records)} records with both total_expenditure and unit data")

    dim_updates = []
    for record_id, country, year, total_exp, unit_num in records:
        normalized_value = total_exp * unit_num
        dim_updates.append((normalized_value, record_id))

    cursor.executemany(
        """
        UPDATE expenditures 
        SET total_expenditure_all_dim_analysis = ?
        WHERE id = ?
        """,
        dim_updates,
    )
    conn.commit()
    print(f"✓ Updated {len(dim_updates)} records")

    # Step 5: Show sample results
    print("\nSample Results:")
    print("-" * 60)

    cursor.execute(
        """
        SELECT country, year, unit_of_measure, unit_of_measure_number,
               total_expenditure_all, total_expenditure_all_dim_analysis
        FROM expenditures
        WHERE total_expenditure_all IS NOT NULL 
          AND total_expenditure_all_dim_analysis IS NOT NULL
        ORDER BY total_expenditure_all_dim_analysis DESC
        LIMIT 10
        """
    )

    results = cursor.fetchall()
    print(
        f"{'Country':<8} {'Year':<6} {'Unit':<15} {'Multiplier':<12} {'Original':<15} {'Normalized':<20}"
    )
    print("-" * 100)
    for country, year, unit, mult, orig, norm in results:
        print(
            f"{country:<8} {year:<6} {unit:<15} {mult:<12.0f} {orig:<15,.2f} {norm:<20,.2f}"
        )

    conn.close()
    print(f"\n✓ Transformation complete! Output saved to: {output_db}")
    return str(output_db)


def show_statistics(db_path: str = "milex_data.db"):
    """Show statistics about the transformed data.

    Args:
        db_path: Path to SQLite database
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print(f"Database Statistics: {db_path}")
    print("=" * 60)

    # Check if columns exist
    cursor.execute("PRAGMA table_info(expenditures)")
    columns = [row[1] for row in cursor.fetchall()]

    has_unit_num = "unit_of_measure_number" in columns
    has_dim_analysis = "total_expenditure_all_dim_analysis" in columns

    print(f"\nColumn Status:")
    print(f"  unit_of_measure_number: {'✓ EXISTS' if has_unit_num else '✗ MISSING'}")
    print(
        f"  total_expenditure_all_dim_analysis: {'✓ EXISTS' if has_dim_analysis else '✗ MISSING'}"
    )

    if not has_unit_num or not has_dim_analysis:
        print("\nRun with --transform to add missing columns")
        conn.close()
        return

    # Count records
    cursor.execute(
        "SELECT COUNT(*) FROM expenditures WHERE unit_of_measure_number IS NOT NULL"
    )
    unit_count = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM expenditures WHERE total_expenditure_all_dim_analysis IS NOT NULL"
    )
    dim_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM expenditures")
    total_count = cursor.fetchone()[0]

    print(f"\nData Coverage:")
    print(f"  Total records: {total_count:,}")
    print(
        f"  With unit_of_measure_number: {unit_count:,} ({100*unit_count/total_count:.1f}%)"
    )
    print(f"  With dim_analysis: {dim_count:,} ({100*dim_count/total_count:.1f}%)")

    # Top spenders
    print(f"\nTop 10 Spenders (Normalized):")
    print("-" * 60)
    cursor.execute(
        """
        SELECT country, year, total_expenditure_all_dim_analysis, unit_of_measure
        FROM expenditures
        WHERE total_expenditure_all_dim_analysis IS NOT NULL
        ORDER BY total_expenditure_all_dim_analysis DESC
        LIMIT 10
        """
    )

    results = cursor.fetchall()
    for i, (country, year, amount, unit) in enumerate(results, 1):
        print(f"{i:2}. {country} ({year}): {amount:,.0f} ({unit})")

    conn.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Transform UN MILEX database with derived columns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Apply transformations (creates milex_data_transformed.db)
  python transform_db.py --transform
  
  # Apply transformations with custom output path
  python transform_db.py --transform --output transformed.db
  
  # Show statistics
  python transform_db.py --stats
  
  # Use custom database file
  python transform_db.py --transform --db custom_data.db
        """,
    )

    parser.add_argument(
        "--db",
        type=str,
        default="milex_data.db",
        help="Path to source SQLite database (default: milex_data.db)",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Path for output database (default: adds '_transformed' suffix to input name)",
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--transform",
        action="store_true",
        help="Apply transformations to a copy of the database",
    )
    mode_group.add_argument(
        "--stats", action="store_true", help="Show statistics about transformed data"
    )

    args = parser.parse_args()

    if args.stats:
        show_statistics(args.db)
    else:
        add_derived_columns(args.db, args.output)


if __name__ == "__main__":
    main()
