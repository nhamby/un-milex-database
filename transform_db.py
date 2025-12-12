"""
Database transformation script for UN MILEX data.
Adds derived columns for dimensional analysis and unit normalization.
Includes currency conversion to USD and inflation adjustment.
"""

import sqlite3
import argparse
import json
import re
import shutil
import time
from pathlib import Path
from typing import Optional, Union, Dict, Tuple
import pandas as pd
import requests


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


def extract_currency_code(national_currency: Optional[str]) -> Optional[str]:
    """Extract ISO currency code from national_currency string.

    Args:
        national_currency: String like "Albania Lek(e) (ALL)" or "Euro (EUR)"

    Returns:
        ISO currency code (e.g., "ALL", "EUR") or None if not found
    """
    if not national_currency:
        return None

    # Look for 3-letter code in parentheses at the end
    # Pattern: (XXX) at end of string where XXX is 3 uppercase letters
    match = re.search(r"\(([A-Z]{3})\)\s*$", national_currency)
    if match:
        return match.group(1)

    return None


def get_exchange_rates_cache_path() -> Path:
    """Get path to exchange rates cache file."""
    return Path(__file__).parent / "exchange_rates_cache.json"


def get_cpi_cache_path() -> Path:
    """Get path to CPI cache file."""
    return Path(__file__).parent / "cpi_cache.json"


def load_exchange_rates_cache() -> Dict:
    """Load exchange rates from cache file."""
    cache_path = get_exchange_rates_cache_path()
    if cache_path.exists():
        with open(cache_path, "r") as f:
            return json.load(f)
    return {}


def save_exchange_rates_cache(cache: Dict):
    """Save exchange rates to cache file."""
    cache_path = get_exchange_rates_cache_path()
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)


def load_cpi_cache() -> Dict:
    """Load CPI data from cache file."""
    cache_path = get_cpi_cache_path()
    if cache_path.exists():
        with open(cache_path, "r") as f:
            return json.load(f)
    return {}


def save_cpi_cache(cache: Dict):
    """Save CPI data to cache file."""
    cache_path = get_cpi_cache_path()
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)


def fetch_exchange_rate(currency_code: str, year: int, cache: Dict) -> Optional[float]:
    """Fetch exchange rate from currency to USD for December of given year.

    Uses the Frankfurter API (https://frankfurter.app) for historical rates.
    Fetches a date range to minimize API calls.

    Args:
        currency_code: ISO 4217 currency code (e.g., "EUR", "GBP")
        year: The year for which to get December exchange rate
        cache: Cache dictionary for storing fetched rates

    Returns:
        Exchange rate (units of local currency per 1 USD), or None if unavailable
    """
    cache_key = f"{currency_code}_{year}"
    if cache_key in cache:
        return cache[cache_key]

    # Skip if already USD
    if currency_code == "USD":
        cache[cache_key] = 1.0
        return 1.0

    # Try to get rates for December using a date range (single API call)
    try:
        # Request the last two weeks of December to find any available date
        start_date = f"{year}-12-15"
        end_date = f"{year}-12-31"
        url = f"https://api.frankfurter.app/{start_date}..{end_date}?from=USD&to={currency_code}"
        response = requests.get(url, timeout=15)

        if response.status_code == 200:
            data = response.json()
            if "rates" in data and data["rates"]:
                # Get the latest available date in the range
                dates = sorted(data["rates"].keys(), reverse=True)
                for date in dates:
                    if currency_code in data["rates"][date]:
                        rate = data["rates"][date][currency_code]
                        cache[cache_key] = rate
                        print(
                            f"  ✓ {currency_code} {year}: 1 USD = {rate:.4f} {currency_code}"
                        )
                        return rate

    except requests.exceptions.Timeout:
        print(f"  ⚠ Timeout for {currency_code} {year}, trying fallback...")
    except requests.exceptions.RequestException as e:
        print(f"  ⚠ Error fetching {currency_code} {year}: {e}")

    # Fallback: try a single date in mid-year
    try:
        url = f"https://api.frankfurter.app/{year}-06-15?from=USD&to={currency_code}"
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if "rates" in data and currency_code in data["rates"]:
                rate = data["rates"][currency_code]
                cache[cache_key] = rate
                print(
                    f"  ⚠ {currency_code} {year}: Using mid-year rate: 1 USD = {rate:.4f} {currency_code}"
                )
                return rate
    except requests.exceptions.RequestException:
        pass

    # Currency not supported by API - mark as None to avoid retrying
    print(f"  ✗ Could not find exchange rate for {currency_code} in {year}")
    cache[cache_key] = None
    return None


def fetch_cpi_data(cache: Dict) -> Dict[str, float]:
    """Fetch US CPI-U data from FRED (Federal Reserve Economic Data).

    Uses the Consumer Price Index for All Urban Consumers (CPIAUCSL).
    Returns monthly CPI values indexed to 1982-84 = 100.

    Args:
        cache: Cache dictionary for storing CPI data

    Returns:
        Dictionary mapping "YYYY-MM" to CPI value
    """
    if "cpi_data" in cache and len(cache["cpi_data"]) > 0:
        return cache["cpi_data"]

    print("\nFetching US CPI data from FRED...")

    try:
        # FRED provides CPI data in CSV format
        # Series: CPIAUCSL (Consumer Price Index for All Urban Consumers)
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL"
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            lines = response.text.strip().split("\n")
            cpi_data = {}

            for line in lines[1:]:  # Skip header
                parts = line.split(",")
                if len(parts) >= 2:
                    date_str = parts[0]  # Format: YYYY-MM-DD
                    try:
                        cpi_value = float(parts[1])
                        # Extract YYYY-MM
                        year_month = date_str[:7]
                        cpi_data[year_month] = cpi_value
                    except (ValueError, IndexError):
                        continue

            if cpi_data:
                cache["cpi_data"] = cpi_data
                print(f"  ✓ Loaded {len(cpi_data)} months of CPI data")
                return cpi_data

    except requests.exceptions.RequestException as e:
        print(f"  ⚠ Error fetching CPI data: {e}")

    # Fallback: Manually constructed CPI data for key years
    # Source: US Bureau of Labor Statistics (approximate December values)
    print("  Using fallback CPI data...")
    fallback_cpi = {
        "1998-12": 164.3,
        "1999-12": 168.3,
        "2000-12": 174.0,
        "2001-12": 176.7,
        "2002-12": 180.9,
        "2003-12": 184.3,
        "2004-12": 190.3,
        "2005-12": 196.8,
        "2006-12": 201.8,
        "2007-12": 210.0,
        "2008-12": 210.2,
        "2009-12": 215.9,
        "2010-12": 219.2,
        "2011-12": 225.7,
        "2012-12": 229.6,
        "2013-12": 233.0,
        "2014-12": 234.8,
        "2015-12": 236.5,
        "2016-12": 241.4,
        "2017-12": 246.5,
        "2018-12": 251.2,
        "2019-12": 256.2,
        "2020-12": 260.5,
        "2021-12": 278.8,
        "2022-12": 296.8,
        "2023-12": 306.7,
        "2024-12": 315.5,
        "2025-11": 318.5,
    }
    cache["cpi_data"] = fallback_cpi
    return fallback_cpi


def get_inflation_multiplier(
    from_year: int, cpi_data: Dict[str, float]
) -> Optional[float]:
    """Calculate inflation multiplier to convert from December of given year to Nov 2025 USD.

    Args:
        from_year: The year from which to convert
        cpi_data: Dictionary mapping "YYYY-MM" to CPI value

    Returns:
        Multiplier to apply (Nov 2025 CPI / Dec from_year CPI), or None if data unavailable
    """
    # Get CPI for December of the source year
    from_key = f"{from_year}-12"

    # Target: November 2025
    to_key = "2025-11"

    # If Nov 2025 not available, try Oct 2025, then most recent
    if to_key not in cpi_data:
        for fallback in ["2025-10", "2025-09", "2025-08", "2024-12", "2024-11"]:
            if fallback in cpi_data:
                to_key = fallback
                break

    if from_key not in cpi_data:
        # Try to find nearest available month
        for month in range(12, 0, -1):
            alt_key = f"{from_year}-{month:02d}"
            if alt_key in cpi_data:
                from_key = alt_key
                break

    if from_key not in cpi_data or to_key not in cpi_data:
        return None

    from_cpi = cpi_data[from_key]
    to_cpi = cpi_data[to_key]

    return to_cpi / from_cpi


def export_db_to_csv(db_path: Union[str, Path]) -> str:
    """Export a SQLite database to CSV format.

    Args:
        db_path: Path to SQLite database

    Returns:
        Path to the exported CSV file
    """
    db_path_obj = Path(db_path)
    csv_path = db_path_obj.with_suffix(".csv")

    print(f"\nExporting database to CSV: {csv_path}")

    conn = sqlite3.connect(db_path_obj)
    df = pd.read_sql_query(
        """
        SELECT * FROM expenditures
        ORDER BY country, year
        """,
        conn,
    )
    conn.close()

    df.to_csv(csv_path, index=False, na_rep="NA")
    print(f"✓ Exported {len(df)} rows to {csv_path}")

    return str(csv_path)


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

    # Step 2b: Add total_expenditure_usd column (converted to USD at Dec rates)
    try:
        cursor.execute(
            """
            ALTER TABLE expenditures 
            ADD COLUMN total_expenditure_usd REAL
            """
        )
        print("✓ Added column: total_expenditure_usd")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("✓ Column already exists: total_expenditure_usd")
        else:
            raise

    # Step 2c: Add total_expenditure_usd_2025 column (inflation-adjusted to Nov 2025)
    try:
        cursor.execute(
            """
            ALTER TABLE expenditures 
            ADD COLUMN total_expenditure_usd_2025 REAL
            """
        )
        print("✓ Added column: total_expenditure_usd_2025")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("✓ Column already exists: total_expenditure_usd_2025")
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

    # Step 5: Convert to USD using December exchange rates
    print("\n" + "=" * 60)
    print("Step 5: Converting to USD (December exchange rates)")
    print("=" * 60)

    # Load exchange rate cache
    exchange_cache = load_exchange_rates_cache()

    # Get CPI data for inflation adjustment
    cpi_cache = load_cpi_cache()
    cpi_data = fetch_cpi_data(cpi_cache)
    save_cpi_cache(cpi_cache)

    cursor.execute(
        """
        SELECT id, country, year, national_currency, total_expenditure_all_dim_analysis
        FROM expenditures
        WHERE total_expenditure_all_dim_analysis IS NOT NULL 
          AND national_currency IS NOT NULL
          AND national_currency != '()'
        """
    )

    records = cursor.fetchall()
    print(f"\nFound {len(records)} records to convert to USD")

    # Group by currency and year to minimize API calls
    currency_year_rates: Dict[Tuple[str, int], Optional[float]] = {}

    # First pass: identify all unique currency/year combinations
    unique_combos = set()
    for record_id, country, year, national_currency, dim_value in records:
        currency_code = extract_currency_code(national_currency)
        if currency_code:
            unique_combos.add((currency_code, year))

    print(f"Found {len(unique_combos)} unique currency/year combinations")
    print("\nFetching exchange rates...")

    # Fetch all needed exchange rates
    for currency_code, year in sorted(unique_combos):
        rate = fetch_exchange_rate(currency_code, year, exchange_cache)
        currency_year_rates[(currency_code, year)] = rate
        time.sleep(0.2)  # Rate limiting for API

    # Save cache after fetching
    save_exchange_rates_cache(exchange_cache)

    # Second pass: calculate USD values
    usd_updates = []
    usd_2025_updates = []
    stats_usd = {"converted": 0, "no_rate": 0, "no_currency": 0}

    for record_id, country, year, national_currency, dim_value in records:
        currency_code = extract_currency_code(national_currency)

        if not currency_code:
            stats_usd["no_currency"] += 1
            continue

        rate = currency_year_rates.get((currency_code, year))

        if rate is None:
            stats_usd["no_rate"] += 1
            continue

        # Convert to USD: dim_value (in local currency) / rate (local per USD)
        usd_value = dim_value / rate
        usd_updates.append((usd_value, record_id))

        # Convert to Nov 2025 USD using inflation multiplier
        inflation_mult = get_inflation_multiplier(year, cpi_data)
        if inflation_mult:
            usd_2025_value = usd_value * inflation_mult
            usd_2025_updates.append((usd_2025_value, record_id))

        stats_usd["converted"] += 1

    # Apply USD updates
    if usd_updates:
        cursor.executemany(
            """
            UPDATE expenditures 
            SET total_expenditure_usd = ?
            WHERE id = ?
            """,
            usd_updates,
        )
        conn.commit()
        print(f"\n✓ Updated {len(usd_updates)} records with USD values")

    # Apply USD 2025 updates
    if usd_2025_updates:
        cursor.executemany(
            """
            UPDATE expenditures 
            SET total_expenditure_usd_2025 = ?
            WHERE id = ?
            """,
            usd_2025_updates,
        )
        conn.commit()
        print(f"✓ Updated {len(usd_2025_updates)} records with Nov 2025 USD values")

    print(f"\nUSD Conversion Statistics:")
    print(f"  Successfully converted: {stats_usd['converted']:,}")
    print(f"  No exchange rate found: {stats_usd['no_rate']:,}")
    print(f"  No currency code:       {stats_usd['no_currency']:,}")

    # Step 6: Show sample results
    print("\nSample Results (Top 10 by Nov 2025 USD):")
    print("-" * 100)

    cursor.execute(
        """
        SELECT country, year, national_currency, 
               total_expenditure_all_dim_analysis,
               total_expenditure_usd,
               total_expenditure_usd_2025
        FROM expenditures
        WHERE total_expenditure_usd_2025 IS NOT NULL
        ORDER BY total_expenditure_usd_2025 DESC
        LIMIT 10
        """
    )

    results = cursor.fetchall()
    print(
        f"{'Country':<8} {'Year':<6} {'Currency':<25} {'Local Value':<20} {'USD (Dec Year)':<20} {'USD (Nov 2025)':<20}"
    )
    print("-" * 120)
    for country, year, currency, local_val, usd_val, usd_2025_val in results:
        currency_short = (
            currency[:22] + "..."
            if currency and len(currency) > 25
            else (currency or "N/A")
        )
        print(
            f"{country:<8} {year:<6} {currency_short:<25} {local_val:>18,.0f} {usd_val:>18,.0f} {usd_2025_val:>18,.0f}"
        )

    conn.close()
    print(f"\n✓ Transformation complete! Output saved to: {output_db}")

    # Export to CSV
    csv_path = export_db_to_csv(output_db)

    return str(output_db), csv_path


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

    has_usd = "total_expenditure_usd" in columns
    has_usd_2025 = "total_expenditure_usd_2025" in columns
    print(f"  total_expenditure_usd: {'✓ EXISTS' if has_usd else '✗ MISSING'}")
    print(
        f"  total_expenditure_usd_2025: {'✓ EXISTS' if has_usd_2025 else '✗ MISSING'}"
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

    cursor.execute(
        "SELECT COUNT(*) FROM expenditures WHERE total_expenditure_usd IS NOT NULL"
    )
    usd_count = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM expenditures WHERE total_expenditure_usd_2025 IS NOT NULL"
    )
    usd_2025_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM expenditures")
    total_count = cursor.fetchone()[0]

    print(f"\nData Coverage:")
    print(f"  Total records: {total_count:,}")
    print(
        f"  With unit_of_measure_number: {unit_count:,} ({100*unit_count/total_count:.1f}%)"
    )
    print(f"  With dim_analysis: {dim_count:,} ({100*dim_count/total_count:.1f}%)")
    print(f"  With USD conversion: {usd_count:,} ({100*usd_count/total_count:.1f}%)")
    print(
        f"  With Nov 2025 USD: {usd_2025_count:,} ({100*usd_2025_count/total_count:.1f}%)"
    )

    # Top spenders in Nov 2025 USD
    print(f"\nTop 10 Spenders (Nov 2025 USD):")
    print("-" * 80)
    cursor.execute(
        """
        SELECT country, year, total_expenditure_usd_2025, national_currency
        FROM expenditures
        WHERE total_expenditure_usd_2025 IS NOT NULL
        ORDER BY total_expenditure_usd_2025 DESC
        LIMIT 10
        """
    )

    results = cursor.fetchall()
    for i, (country, year, amount, currency) in enumerate(results, 1):
        print(f"{i:2}. {country} ({year}): ${amount:,.0f} USD (Nov 2025)")

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
