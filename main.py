"""
Main orchestrator script for UN Military Expenditures scraper.
All-in-one entry point for scraping, exporting, and utilities.
"""

import argparse
import sys
import os
import time
import signal
from typing import List, Optional
from datetime import datetime, timedelta


# Check dependencies
def check_dependencies():
    """Check if required dependencies are installed."""
    missing = []
    try:
        import seleniumbase
    except ImportError:
        missing.append("seleniumbase")
    try:
        import pandas
    except ImportError:
        missing.append("pandas")
    try:
        import bs4
    except ImportError:
        missing.append("beautifulsoup4")

    if missing:
        print("ERROR: Missing required dependencies!")
        print(f"   Missing: {', '.join(missing)}")
        print("\nPlease install them with:")
        print("   pip install -r requirements.txt")
        print("\nOr run:")
        print("   python3 -m venv venv")
        print("   source venv/bin/activate  # On macOS/Linux")
        print("   pip install -r requirements.txt")
        sys.exit(1)


check_dependencies()

from database import MilexDatabase
from scraper import MilexScraper
from parser import MilexParser
import config


class MilexOrchestrator:
    """Orchestrates the scraping process."""

    def __init__(self, headless: bool = True, delay: float = 1.0):
        """Initialize orchestrator.

        Args:
            headless: Whether to run browser in headless mode
            delay: Delay between requests in seconds
        """
        self.db = MilexDatabase()
        self.scraper = MilexScraper(headless=headless)
        self.parser = MilexParser()
        self.delay = delay
        self.interrupted = False

    def scrape_all(
        self,
        countries: Optional[List[str]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        resume: bool = True,
    ):
        """Scrape all country-year combinations.

        Args:
            countries: List of country codes to scrape (None for all)
            start_year: Starting year (None for config default)
            end_year: Ending year (None for config default)
            resume: Whether to skip already scraped combinations
        """
        # Determine countries to scrape
        countries_to_scrape = countries or config.COUNTRY_CODES

        # Determine year range
        start = start_year or config.START_YEAR
        end = end_year or config.END_YEAR

        total_combinations = len(countries_to_scrape) * (end - start + 1)
        current = 0

        print(f"Starting scrape of {total_combinations} country-year combinations...")
        print(f"Countries: {len(countries_to_scrape)}")
        print(f"Years: {start} to {end}")
        print(f"Headless mode: {self.scraper.headless}")
        print(f"Resume mode: {resume}")
        print(f"Press Ctrl+C to stop")
        print("-" * 60)

        overall_start_time = datetime.now()
        total_pages_scraped = 0
        total_page_time = 0.0

        try:
            for country in countries_to_scrape:
                if self.interrupted:
                    break

                country_start_time = datetime.now()
                country_pages_scraped = 0

                for year in range(start, end + 1):
                    if self.interrupted:
                        break

                    current += 1

                    # Check if already scraped (if resume mode)
                    if resume:
                        existing = self.db.get_expenditure(country, year)
                        if existing:
                            print(
                                f"[{current}/{total_combinations}] Skipping {country} {year} (already scraped)"
                            )
                            continue

                    print(
                        f"[{current}/{total_combinations}] Scraping {country} {year}...",
                        end=" ",
                        flush=True,
                    )

                    # Update status to in_progress
                    self.db.update_scraping_status(country, year, "in_progress")

                    # Time this page
                    page_start_time = datetime.now()

                    try:
                        # Scrape the page
                        data = self.scraper.scrape_country_year(country, year)

                        page_duration = (
                            datetime.now() - page_start_time
                        ).total_seconds()

                        # Check if page has data
                        if "error" in data:
                            print(f"[ERROR] {data['error']} ({page_duration:.1f}s)")
                            self.db.update_scraping_status(
                                country, year, "failed", data["error"]
                            )
                        elif (
                            not data.get("categories")
                            and data.get("total_expenditure_all") is None
                        ):
                            print(f"No data available ({page_duration:.1f}s)")
                            self.db.update_scraping_status(country, year, "no_data")
                            # Still save the record with NAs
                            self.db.insert_or_update_expenditure(data)
                        else:
                            # Save to database
                            self.db.insert_or_update_expenditure(data)

                            print(
                                f"[SUCCESS] {len(data.get('categories', {}))} categories ({page_duration:.1f}s)"
                            )
                            self.db.update_scraping_status(country, year, "success")

                            country_pages_scraped += 1
                            total_pages_scraped += 1
                            total_page_time += page_duration

                    except KeyboardInterrupt:
                        print("\n\nInterrupt received (Ctrl+C)...")
                        print("Finishing current operation and saving progress...")
                        self.interrupted = True
                        break
                    except Exception as e:
                        page_duration = (
                            datetime.now() - page_start_time
                        ).total_seconds()
                        print(
                            f"[ERROR] Unexpected error: {str(e)} ({page_duration:.1f}s)"
                        )
                        self.db.update_scraping_status(country, year, "failed", str(e))

                    # Delay between requests to be respectful
                    if not self.interrupted:
                        time.sleep(self.delay)

                # Print country summary
                if country_pages_scraped > 0:
                    country_duration = datetime.now() - country_start_time
                    avg_time = country_duration.total_seconds() / country_pages_scraped
                    print(
                        f"  [{country}] Complete: {country_pages_scraped} pages in {self._format_duration(country_duration)} (avg: {avg_time:.1f}s/page)"
                    )

                if self.interrupted:
                    break

        except KeyboardInterrupt:
            print("\n\nInterrupt received (Ctrl+C)...")
            print("Finishing current operation and saving progress...")
            self.interrupted = True

        overall_duration = datetime.now() - overall_start_time

        print("-" * 60)
        if self.interrupted:
            print("WARNING: Scraping interrupted by user")
            print(f"Scraped {total_pages_scraped} pages before interruption")
        else:
            print("Scraping completed successfully!")

        print(f"Total time: {self._format_duration(overall_duration)}")
        if total_pages_scraped > 0:
            avg_page_time = total_page_time / total_pages_scraped
            print(f"Average time per page: {avg_page_time:.1f}s")

        self._print_summary()

    def _format_duration(self, duration: timedelta) -> str:
        """Format a timedelta as a human-readable string.

        Args:
            duration: Time duration

        Returns:
            Formatted string (e.g., "2h 15m 30s")
        """
        total_seconds = int(duration.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0 or not parts:
            parts.append(f"{seconds}s")

        return " ".join(parts)

    def scrape_single(self, country: str, year: int):
        """Scrape a single country-year combination.

        Args:
            country: Country code
            year: Year
        """
        print(f"Scraping {country} {year}...")

        start_time = datetime.now()

        try:
            data = self.scraper.scrape_country_year(country, year)

            duration = (datetime.now() - start_time).total_seconds()

            if "error" in data:
                print(f"Error: {data['error']} ({duration:.1f}s)")
                self.db.update_scraping_status(country, year, "failed", data["error"])
            else:
                # Save to database
                self.db.insert_or_update_expenditure(data)

                print(f"Success! ({duration:.1f}s)")
                print(f"Fields found: {len(data.get('field_data', {}))}")
                print(f"Total expenditure: {data.get('total_expenditure_all')}")

                self.db.update_scraping_status(country, year, "success")

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            print(f"Error: {str(e)} ({duration:.1f}s)")
            self.db.update_scraping_status(country, year, "failed", str(e))

    def _print_summary(self):
        """Print summary of scraping results."""
        print("\nScraping Summary:")
        print("-" * 60)

        progress = self.db.get_scraping_progress()

        if not progress.empty:
            for _, row in progress.iterrows():
                print(f"{row['status']}: {row['count']}")
        else:
            print("No scraping data available")

        print("-" * 60)

    def get_failed_scrapes(self):
        """Display failed scraping attempts."""
        failed = self.db.get_failed_scrapes()

        if failed.empty:
            print("No failed scrapes!")
            return

        print("\nFailed Scrapes:")
        print("-" * 60)
        print(failed.to_string(index=False))

    def close(self):
        """Close database connection."""
        self.db.close()


def export_data(output_file: str = "milex_data.csv", mode: str = "full"):
    """Export database to CSV.

    Args:
        output_file: Output filename
        mode: Export mode ('full', 'summary', 'categories')
    """
    from export import (
        export_to_csv,
        export_summary_stats,
        export_category_pivot,
    )

    if mode == "full":
        export_to_csv(output_file)
    elif mode == "summary":
        export_summary_stats(output_file)
    elif mode == "categories":
        export_category_pivot(output_file)
    elif mode == "all":
        export_to_csv("milex_data.csv")
        export_summary_stats("milex_summary.csv")
        export_category_pivot("milex_categories.csv")


def show_stats():
    """Show database statistics."""
    from utils import show_stats as utils_show_stats

    utils_show_stats()


def view_record(country: str, year: int):
    """View a specific record."""
    from utils import view_record as utils_view_record

    utils_view_record(country, year)


def list_data(data_type: str):
    """List countries, years, or categories."""
    from utils import list_countries, list_years, list_categories

    if data_type == "countries":
        list_countries()
    elif data_type == "years":
        list_years()
    elif data_type == "categories":
        list_categories()


def run_example():
    """Run quick example (Lithuania 2024)."""
    print("=" * 60)
    print("UN MILEX Scraper - Quick Example")
    print("=" * 60)
    print("\nScraping Lithuania 2024 as a test...")
    print()

    orchestrator = MilexOrchestrator(headless=True, delay=1.0)

    try:
        orchestrator.scrape_single("LTU", 2024)
        print("\nExample completed successfully!")
        print("  Check milex_data.db for the results")
        print("\nTo export to CSV:")
        print("  python main.py --export")
    finally:
        orchestrator.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="UN Military Expenditures Database Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run quick example (Lithuania 2024)
  python main.py --example
  
  # Scrape specific countries
  python main.py --country USA,GBR,FRA --start-year 2020
  
  # Scrape all countries (takes hours!)
  python main.py
  
  # Export to CSV
  python main.py --export
  
  # Show database statistics
  python main.py --stats
  
  # View specific record
  python main.py --view LTU 2024
        """,
    )

    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--example", action="store_true", help="Run quick example (Lithuania 2024)"
    )
    mode_group.add_argument(
        "--export", action="store_true", help="Export database to CSV"
    )
    mode_group.add_argument(
        "--stats", action="store_true", help="Show database statistics"
    )
    mode_group.add_argument(
        "--view",
        nargs=2,
        metavar=("COUNTRY", "YEAR"),
        help="View specific record (e.g., --view LTU 2024)",
    )
    mode_group.add_argument(
        "--list",
        choices=["countries", "years", "categories"],
        help="List countries, years, or categories in database",
    )
    mode_group.add_argument(
        "--show-failed", action="store_true", help="Show failed scraping attempts"
    )

    # Scraping options
    parser.add_argument(
        "--country",
        type=str,
        help="Comma-separated list of country codes (e.g., USA,GBR,FRA)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=config.START_YEAR,
        help=f"Starting year (default: {config.START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=config.END_YEAR,
        help=f"Ending year (default: {config.END_YEAR})",
    )
    parser.add_argument("--year", type=int, help="Single year to scrape")
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser in visible mode (for debugging)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between requests in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--no-resume", action="store_true", help="Re-scrape already scraped data"
    )

    # Export options
    parser.add_argument(
        "--output",
        type=str,
        default="milex_data.csv",
        help="Output filename for export (default: milex_data.csv)",
    )
    parser.add_argument(
        "--export-mode",
        choices=["full", "summary", "categories", "all"],
        default="full",
        help="Export mode (default: full)",
    )

    args = parser.parse_args()

    # Handle different modes
    if args.example:
        run_example()
        return

    if args.export:
        export_data(args.output, args.export_mode)
        return

    if args.stats:
        show_stats()
        return

    if args.view:
        view_record(args.view[0].upper(), int(args.view[1]))
        return

    if args.list:
        list_data(args.list)
        return

    # Scraping mode
    headless = not args.visible
    orchestrator = MilexOrchestrator(headless=headless, delay=args.delay)

    try:
        if args.show_failed:
            orchestrator.get_failed_scrapes()
            return

        # Parse countries if provided
        countries = None
        if args.country:
            countries = [c.strip().upper() for c in args.country.split(",")]

        # Handle year argument
        if args.year:
            args.start_year = args.year
            args.end_year = args.year

        # If single country and single year, use scrape_single
        if countries and len(countries) == 1 and args.start_year == args.end_year:
            orchestrator.scrape_single(countries[0], args.start_year)
        else:
            # Run full scrape
            orchestrator.scrape_all(
                countries=countries,
                start_year=args.start_year,
                end_year=args.end_year,
                resume=not args.no_resume,
            )

    finally:
        orchestrator.close()


if __name__ == "__main__":
    main()
