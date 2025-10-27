"""
Parser for UN Military Expenditures data tables.
Handles various table formats and normalizes data.
"""

from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup, Tag
import re
import config


class MilexParser:
    """Parser for military expenditures data from HTML tables."""

    def __init__(self):
        """Initialize parser."""
        self.category_mappings = {}

    def parse_html_tables(self, html: str) -> Dict[str, Any]:
        """Parse all tables from HTML content.

        Args:
            html: HTML content

        Returns:
            Dictionary with parsed data including categories
        """
        soup = BeautifulSoup(html, "lxml")

        result = {"categories": {}, "metadata": {}}

        # Find all tables
        tables = soup.find_all("table")

        for table in tables:
            # Determine table type and parse accordingly
            table_data = self._parse_table(table)

            if table_data:
                result["categories"].update(table_data.get("categories", {}))
                result["metadata"].update(table_data.get("metadata", {}))

        return result

    def _parse_table(self, table: Tag) -> Optional[Dict[str, Any]]:
        """Parse a single table element.

        Args:
            table: BeautifulSoup table element

        Returns:
            Dictionary with parsed data or None
        """
        result = {"categories": {}, "metadata": {}}

        rows = table.find_all("tr")

        if not rows:
            return None

        # Try to identify headers
        headers = self._extract_headers(rows[0])

        # Parse data rows
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])

            if not cells:
                continue

            # Extract row data based on table structure
            row_data = self._parse_row(cells, headers)

            if row_data:
                result["categories"].update(row_data.get("categories", {}))
                result["metadata"].update(row_data.get("metadata", {}))

        return result

    def _extract_headers(self, header_row: Tag) -> List[str]:
        """Extract header names from table header row.

        Args:
            header_row: Table header row element

        Returns:
            List of header names
        """
        headers = []
        cells = header_row.find_all(["th", "td"])

        for cell in cells:
            header_text = cell.get_text(strip=True)
            headers.append(header_text)

        return headers

    def _parse_row(
        self, cells: List[Tag], headers: List[str]
    ) -> Optional[Dict[str, Any]]:
        """Parse a single table row.

        Args:
            cells: List of cell elements
            headers: List of header names

        Returns:
            Dictionary with parsed row data or None
        """
        if not cells:
            return None

        result = {"categories": {}, "metadata": {}}

        # First cell is usually the category/label
        category_text = cells[0].get_text(strip=True)

        if not category_text:
            return None

        # Normalize category name
        category_name = config.normalize_category_name(category_text)

        # Rest of cells contain values
        for i, cell in enumerate(cells[1:], start=1):
            value_text = cell.get_text(strip=True)

            if value_text:
                # Try to parse as numeric value
                value = config.parse_numeric_value(value_text)

                if value is not None:
                    # Use header name if available, otherwise use index
                    if i < len(headers) and headers[i]:
                        full_category = f"{category_name}_{config.normalize_category_name(headers[i])}"
                    else:
                        full_category = category_name

                    result["categories"][full_category] = value
                else:
                    # Non-numeric data - might be metadata
                    if i < len(headers) and headers[i]:
                        key = config.normalize_category_name(headers[i])
                        result["metadata"][f"{category_name}_{key}"] = value_text

        return result

    def parse_expenditure_table(self, table: Tag) -> Dict[str, float]:
        """Parse a standard expenditure table with categories and values.

        Args:
            table: BeautifulSoup table element

        Returns:
            Dictionary mapping category names to values
        """
        categories = {}

        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all(["td", "th"])

            if len(cells) < 2:
                continue

            # First cell is category
            category_text = cells[0].get_text(strip=True)

            # Last cell is typically the value
            value_text = cells[-1].get_text(strip=True)

            if category_text and value_text:
                category_name = config.normalize_category_name(category_text)
                value = config.parse_numeric_value(value_text)

                if value is not None:
                    categories[category_name] = value

        return categories

    def parse_cost_categories(self, html: str) -> Dict[str, float]:
        """Parse cost categories from page HTML.

        Looks for specific patterns like:
        - Land: Personnel, Operations, Procurement, R&D, Construction
        - Naval: Personnel, Operations, Procurement, R&D, Construction
        - Air: Personnel, Operations, Procurement, R&D, Construction
        - Other: Personnel, Operations, Procurement, R&D, Construction

        Args:
            html: HTML content

        Returns:
            Dictionary mapping category names to values
        """
        soup = BeautifulSoup(html, "lxml")
        categories = {}

        # Define category patterns
        force_types = ["Land", "Naval", "Air", "Other"]
        subcategories = [
            "Personnel",
            "Operations",
            "Procurement",
            "R&D",
            "RnD",
            "Construction",
        ]

        # Look for tables or divs containing these categories
        for force_type in force_types:
            for subcat in subcategories:
                # Try various text patterns
                patterns = [
                    f"{force_type}.*{subcat}",
                    f"{subcat}.*{force_type}",
                    f"{force_type}\\s*-\\s*{subcat}",
                ]

                for pattern in patterns:
                    elements = soup.find_all(string=re.compile(pattern, re.IGNORECASE))

                    for elem in elements:
                        # Try to find associated value
                        parent = elem.parent

                        if parent:
                            # Look for numeric value in same row
                            row = parent.find_parent("tr")

                            if row:
                                cells = row.find_all(["td", "th"])

                                for cell in cells:
                                    value_text = cell.get_text(strip=True)
                                    value = config.parse_numeric_value(value_text)

                                    if value is not None:
                                        category_name = (
                                            f"{force_type}_{subcat}".replace(" ", "_")
                                        )
                                        categories[category_name] = value
                                        break

        return categories

    def extract_metadata(self, html: str) -> Dict[str, str]:
        """Extract metadata from page (currency, units, etc.).

        Args:
            html: HTML content

        Returns:
            Dictionary with metadata
        """
        soup = BeautifulSoup(html, "lxml")
        metadata = {}

        # Look for definition lists (dt/dd pairs)
        dt_elements = soup.find_all("dt")

        for dt in dt_elements:
            key = dt.get_text(strip=True)
            dd = dt.find_next_sibling("dd")

            if dd:
                value = dd.get_text(strip=True)
                metadata[key] = value

        return metadata

    def merge_data(self, *data_dicts: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple data dictionaries.

        Args:
            *data_dicts: Variable number of data dictionaries

        Returns:
            Merged dictionary
        """
        merged = {"categories": {}, "metadata": {}}

        for data in data_dicts:
            if "categories" in data:
                merged["categories"].update(data["categories"])

            if "metadata" in data:
                merged["metadata"].update(data["metadata"])

        return merged
