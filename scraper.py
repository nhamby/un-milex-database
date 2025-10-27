"""
Web scraper for UN Military Expenditures Database using SeleniumBase.
"""

import time
import os
from typing import Dict, Any, Optional, List
from seleniumbase import SB
from bs4 import BeautifulSoup
import config


class MilexScraper:
    """Scraper for UN Military Expenditures database."""

    def __init__(self, headless: bool = True):
        """Initialize the scraper.

        Args:
            headless: Whether to run browser in headless mode
        """
        self.headless = headless

    def scrape_country_year(self, country_code: str, year: int) -> Dict[str, Any]:
        """Scrape data for a specific country-year combination.

        Args:
            country_code: ISO 3166-1 alpha-3 country code
            year: Year

        Returns:
            Dictionary containing scraped data
        """
        url = config.get_country_year_url(country_code, year)

        with SB(
            uc=True,
            headless=self.headless,
            chromium_arg="--disable-blink-features=AutomationControlled",
        ) as sb:
            try:
                # Navigate to the page
                sb.open(url)

                # Wait for the report div to load (either with data or nil report)
                # The div.spinner-container should be replaced by div.report.loaded
                try:
                    # Wait up to 10 seconds for either the report div or an error indicator
                    sb.wait_for_element_present("div.report.loaded", timeout=10)
                except Exception:
                    # If div.report.loaded doesn't appear, continue anyway
                    # The page might use a different structure
                    pass

                # Additional wait to ensure all content is loaded
                time.sleep(1)

                # Get page source
                page_source = sb.get_page_source()

                # Parse the page
                data = self._parse_page(page_source, country_code, year, url)

                return data

            except Exception as e:
                print(f"Error scraping {country_code} {year}: {str(e)}")
                return {
                    "country": country_code,
                    "year": year,
                    "page_link": url,
                    "error": str(e),
                }

    def _parse_page(
        self, html: str, country_code: str, year: int, url: str
    ) -> Dict[str, Any]:
        """Parse HTML page to extract expenditure data.

        Args:
            html: HTML content
            country_code: Country code
            year: Year
            url: Page URL

        Returns:
            Dictionary with extracted data
        """
        soup = BeautifulSoup(html, "lxml")

        data = {
            "country": country_code,
            "year": year,
            "page_link": url,
            "national_currency": None,
            "unit_of_measure": None,
            "total_expenditure_all": None,
            "explanatory_remarks": None,
            "nil_report_expenditure": None,
            "field_data": {},
        }

        # Extract National Currency
        # Try dt/dd structure first (older format)
        currency_elem = soup.find("dt", string="National Currency")  # type: ignore
        if currency_elem:
            currency_dd = currency_elem.find_next_sibling("dd")
            if currency_dd:
                data["national_currency"] = currency_dd.get_text(strip=True)

        # Try div.currency-and-unit structure (newer format)
        if not data["national_currency"]:
            currency_divs = soup.find_all("div", class_="currency-and-unit")
            for div in currency_divs:
                ps = div.find_all("p")
                if len(ps) >= 2:
                    label = ps[0].get_text(strip=True)
                    if "national currency" in label.lower():
                        value = ps[1].get_text(strip=True)
                        # Remove leading colon and whitespace
                        value = value.lstrip(":").strip()
                        if value:
                            data["national_currency"] = value
                        break

        # Extract Unit of Measure
        # Try dt/dd structure first (older format)
        unit_elem = soup.find("dt", string="Unit of Measure")  # type: ignore
        if unit_elem:
            unit_dd = unit_elem.find_next_sibling("dd")
            if unit_dd:
                data["unit_of_measure"] = unit_dd.get_text(strip=True)

        # Try div.currency-and-unit structure (newer format)
        if not data["unit_of_measure"]:
            currency_divs = soup.find_all("div", class_="currency-and-unit")
            for div in currency_divs:
                ps = div.find_all("p")
                if len(ps) >= 2:
                    label = ps[0].get_text(strip=True)
                    if "unit of measure" in label.lower():
                        value = ps[1].get_text(strip=True)
                        # Remove leading colon and whitespace
                        value = value.lstrip(":").strip()
                        if value:
                            data["unit_of_measure"] = value
                        break

        # Extract Explanatory Remarks
        remarks_elem = soup.find("dt", string="Explanatory Remarks")  # type: ignore
        if remarks_elem:
            remarks_dd = remarks_elem.find_next_sibling("dd")
            if remarks_dd:
                data["explanatory_remarks"] = remarks_dd.get_text(strip=True)

        # Extract Nil Report Expenditure text
        # Look for <p> tag within div.report.loaded
        nil_report_div = soup.find("div", class_=lambda x: x and "report" in x and "loaded" in x)  # type: ignore
        if nil_report_div:
            nil_report_p = nil_report_div.find("p")
            if nil_report_p:
                nil_text = nil_report_p.get_text(strip=True)
                # Only save if it contains meaningful content (not empty or generic)
                if (
                    nil_text and len(nil_text) > 20
                ):  # Arbitrary threshold to avoid noise
                    data["nil_report_expenditure"] = nil_text

        # Extract Total Expenditure from metadata (not from table)
        # Look for h3 with "Total expenditure" label
        total_h3 = soup.find("h3", string=lambda x: x and "total expenditure" in x.lower())  # type: ignore
        if total_h3:
            # The value is in the next h1 element
            next_h1 = total_h3.find_next("h1")
            if next_h1:
                value_text = next_h1.get_text(strip=True)
                value = config.parse_numeric_value(value_text)
                if value is not None:
                    data["total_expenditure_all"] = value

        # Parse the 11×35 MILEX table structure
        data["field_data"] = self._parse_milex_table(soup)

        return data

    def _parse_milex_table(self, soup: BeautifulSoup) -> Dict[str, float]:
        """Parse the structured 11×35 MILEX expenditure table.

        The table has:
        - Row 0: Force category headers (Strategic, Land, Naval, Air, etc.)
        - Column 0: Expenditure subcategory labels (1. Personnel, 2. Operations, etc.)
        - Data cells contain numeric values

        Args:
            soup: BeautifulSoup object

        Returns:
            Dictionary mapping field names to values
        """
        field_data = {}

        # Find all tables on the page
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")

            if not rows or len(rows) < 2:
                continue

            # Extract force categories from first row (header row)
            # Some tables have a label row before headers, so try first two rows
            header_row = None
            data_start_idx = 1

            for idx in [0, 1]:
                if idx >= len(rows):
                    break

                test_row = rows[idx]
                header_cells = test_row.find_all(["th", "td"])

                # Check if this row contains force categories
                has_categories = False
                for cell in header_cells:
                    cell_text = cell.get_text(strip=True).lower()
                    if any(
                        cat.lower() in cell_text
                        for cat in [
                            "strategic",
                            "land forces",
                            "naval",
                            "air forces",
                            "total expenditure",
                        ]
                    ):
                        has_categories = True
                        break

                if has_categories:
                    header_row = test_row
                    data_start_idx = idx + 1
                    break

            if not header_row:
                continue

            header_cells = header_row.find_all(["th", "td"])

            force_categories = []
            for idx, cell in enumerate(header_cells):
                category_text = cell.get_text(strip=True)
                # Skip the first cell (it's always empty or a label)
                if idx == 0:
                    continue

                if category_text and category_text.lower() not in ["", "cost category"]:
                    # Match to our standardized categories
                    matched = self._match_category(
                        category_text, config.MILEX_FIELDS["categories"]
                    )
                    force_categories.append(matched)
                else:
                    force_categories.append(None)

            if not any(force_categories):
                continue  # No valid categories found in header

            # Parse data rows (skip header rows and label rows)
            for row in rows[data_start_idx:]:
                cells = row.find_all(["td", "th"])

                if not cells or len(cells) < 2:
                    continue

                # First cell is the expenditure subcategory
                subcategory_text = cells[0].get_text(strip=True)

                if not subcategory_text:
                    continue

                # Skip label rows like "Cost category"
                if subcategory_text.lower() in ["cost category", "force groups"]:
                    continue

                # Match to standardized subcategory
                matched_subcategory = self._match_subcategory(
                    subcategory_text, config.MILEX_FIELDS["subcategories"]
                )

                if not matched_subcategory:
                    continue

                # Parse values for each force category column
                for col_idx, cell in enumerate(cells[1:], start=0):
                    if col_idx >= len(force_categories):
                        break

                    category = force_categories[col_idx]
                    if not category:
                        continue

                    value_text = cell.get_text(strip=True)
                    value = config.parse_numeric_value(value_text)

                    if value is not None:
                        # Create standardized field name
                        field_name = f"{category} - {matched_subcategory}"

                        # Verify this field name is in our standardized list
                        if field_name in config.ALL_FIELD_NAMES:
                            field_data[field_name] = value

        return field_data

    def _match_category(self, text: str, known_categories: List[str]) -> Optional[str]:
        """Match extracted text to a known force category.

        Args:
            text: Extracted text from table
            known_categories: List of standardized category names

        Returns:
            Matched category name or None
        """
        text_lower = text.lower().strip()

        # Direct matching
        for category in known_categories:
            if category.lower() in text_lower or text_lower in category.lower():
                return category

        # Fuzzy matching for common variations
        category_variations = {
            "strategic": "Strategic forces",
            "land": "Land forces",
            "naval": "Naval forces",
            "navy": "Naval forces",
            "air": "Air forces",
            "other military": "Other Military Forces",
            "central support": "Central Support Administration and Command",
            "administration": "Central Support Administration and Command",
            "peacekeeping": "UN Peace Keeping",
            "un peace": "UN Peace Keeping",
            "assistance": "Military Assistance and Cooperation",
            "cooperation": "Military Assistance and Cooperation",
            "emergency": "Emergency Aid to Civilians",
            "undistributed": "Undistributed",
            "total": "Total Expenditure",
        }

        for key, category in category_variations.items():
            if key in text_lower:
                return category

        return None

    def _match_subcategory(
        self, text: str, known_subcategories: List[str]
    ) -> Optional[str]:
        """Match extracted text to a known expenditure subcategory.

        Args:
            text: Extracted text from table
            known_subcategories: List of standardized subcategory names

        Returns:
            Matched subcategory name or None
        """
        text_lower = text.lower().strip()

        # Direct matching
        for subcategory in known_subcategories:
            if subcategory.lower() in text_lower or text_lower in subcategory.lower():
                return subcategory

        # Special case: "Totals" should match "5. Total (1+2+3+4)"
        if text_lower == "totals" or text_lower == "total":
            for subcategory in known_subcategories:
                if "5. total" in subcategory.lower():
                    return subcategory

        # Check if text matches the pattern of our subcategories
        # Many subcategories start with numbers like "1.", "1.1", etc.
        for subcategory in known_subcategories:
            # Extract the number pattern at start
            if subcategory[0].isdigit():
                num_part = subcategory.split()[0]  # Get "1." or "1.1" etc.
                if text_lower.startswith(num_part.lower()):
                    return subcategory

        return None

    def check_page_exists(self, country_code: str, year: int) -> bool:
        """Check if a page exists for the given country-year.

        Args:
            country_code: Country code
            year: Year

        Returns:
            True if page exists, False otherwise
        """
        url = config.get_country_year_url(country_code, year)

        with SB(uc=True, headless=True) as sb:
            try:
                sb.open(url)
                time.sleep(1)

                # Check if we got a valid page (not 404 or error)
                page_source = sb.get_page_source()

                # Look for indicators that the page has data
                if "No data available" in page_source or "404" in page_source:
                    return False

                return True

            except Exception:
                return False
