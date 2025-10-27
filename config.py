"""
Configuration and utility functions for UN MILEX scraper.
"""

import os
import json
from typing import List, Optional

# Base URL for the UN Military Expenditures database
BASE_URL = "https://milex-reporting.unoda.org/en/states"

# Year range for scraping (adjust as needed)
START_YEAR = 1998
END_YEAR = 2024


# Load ISO 3166-1 alpha-3 country codes from external file
def _load_country_codes() -> List[str]:
    """Load country codes from country_codes.json file.

    Returns:
        List of ISO 3166-1 alpha-3 country codes
    """
    config_dir = os.path.dirname(os.path.abspath(__file__))
    country_codes_path = os.path.join(config_dir, "country_codes.json")

    try:
        with open(country_codes_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Country codes file not found at {country_codes_path}. "
            "Please ensure country_codes.json exists in the project directory."
        )
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in country_codes.json: {e}")


COUNTRY_CODES = _load_country_codes()


# Load standardized MILEX field structure
def _load_milex_fields():
    """Load the standardized MILEX categories and subcategories.

    Returns:
        Dictionary with categories and subcategories
    """
    config_dir = os.path.dirname(os.path.abspath(__file__))
    fields_path = os.path.join(config_dir, "milex_fields.json")

    try:
        with open(fields_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"MILEX fields file not found at {fields_path}. "
            "Please ensure milex_fields.json exists in the project directory."
        )
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in milex_fields.json: {e}")


MILEX_FIELDS = _load_milex_fields()


def get_all_field_names() -> List[str]:
    """Generate all 385 standardized field names (11 categories × 35 subcategories).

    Returns:
        List of all field names in format "Category - Subcategory"
    """
    field_names = []
    for category in MILEX_FIELDS["categories"]:
        for subcategory in MILEX_FIELDS["subcategories"]:
            field_names.append(f"{category} - {subcategory}")
    return field_names


ALL_FIELD_NAMES = get_all_field_names()


def field_name_to_column_name(field_name: str) -> str:
    """Convert a field name to a SQL column name.

    Args:
        field_name: Field name like "Strategic forces - 1. Personnel"

    Returns:
        Column name like "Strategic_forces_1__Personnel"
    """
    # Replace special characters that are problematic in SQL
    column = field_name.replace(" - ", "_")
    column = column.replace(".", "_")
    column = column.replace(" ", "_")
    column = column.replace(",", "")
    column = column.replace("(", "")
    column = column.replace(")", "")
    column = column.replace("-", "_")
    column = column.replace("/", "_")
    column = column.replace("+", "plus")
    return column


def column_name_to_field_name(column_name: str) -> str:
    """Convert a SQL column name back to a field name.

    Args:
        column_name: Column name like "Strategic_forces_1__Personnel"

    Returns:
        Field name like "Strategic forces - 1. Personnel"
    """
    # This is a reverse mapping - we'll need to match against known field names
    for field_name in ALL_FIELD_NAMES:
        if field_name_to_column_name(field_name) == column_name:
            return field_name
    return column_name  # Return as-is if no match found


# Legacy category names for backward compatibility
KNOWN_CATEGORIES = [
    "Land_Personnel",
    "Land_Operations",
    "Land_Procurement",
    "Land_RnD",
    "Land_Construction",
    "Naval_Personnel",
    "Naval_Operations",
    "Naval_Procurement",
    "Naval_RnD",
    "Naval_Construction",
    "Air_Personnel",
    "Air_Operations",
    "Air_Procurement",
    "Air_RnD",
    "Air_Construction",
    "Other_Personnel",
    "Other_Operations",
    "Other_Procurement",
    "Other_RnD",
    "Other_Construction",
]


def get_country_year_url(country_code: str, year: int) -> str:
    """Generate URL for a specific country-year combination.

    Args:
        country_code: ISO 3166-1 alpha-3 country code
        year: Year

    Returns:
        URL string
    """
    return f"{BASE_URL}/{country_code}/{year}"


def get_all_country_year_combinations() -> List[tuple]:
    """Generate all country-year combinations to scrape.

    Returns:
        List of (country_code, year) tuples
    """
    combinations = []
    for country in COUNTRY_CODES:
        for year in range(START_YEAR, END_YEAR + 1):
            combinations.append((country, year))
    return combinations


def sanitize_filename(filename: str) -> str:
    """Sanitize a filename to remove invalid characters.

    Args:
        filename: Original filename

    Returns:
        Sanitized filename
    """
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "_")
    return filename


def normalize_category_name(category: str) -> str:
    """Normalize category names to consistent format.

    Args:
        category: Raw category name from webpage

    Returns:
        Normalized category name
    """
    # Remove extra whitespace
    category = " ".join(category.split())

    # Replace spaces with underscores
    category = category.replace(" ", "_")

    # Remove special characters except underscores
    category = "".join(c for c in category if c.isalnum() or c == "_")

    return category


def parse_numeric_value(value: str) -> Optional[float]:
    """Parse a numeric value from string, handling various formats.

    Args:
        value: String representation of number

    Returns:
        Float value or None if parsing fails
    """
    if not value or value.strip() == "":
        return None

    try:
        # Remove commas and spaces
        value = value.replace(",", "").replace(" ", "")

        # Handle different decimal separators
        if "." in value and value.count(".") == 1:
            return float(value)

        return float(value)
    except (ValueError, AttributeError):
        return None
