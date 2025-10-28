# UN Military Expenditures Database Scraper

A comprehensive web scraper for the United Nations Office for Disarmament Affairs Military Expenditures Database. Extracts military spending data across all countries and years with a standardized 385-field schema that captures the complete UN MILEX reporting structure.

## Overview

This tool scrapes military expenditure data from <https://milex-reporting.unoda.org/>, creating a structured database with **385 standardized fields** representing the complete 11×35 UN MILEX table format (11 force categories × 35 expenditure subcategories). Each country-year record contains consistent field names for comprehensive analysis and comparison.

**Key Features:**

- **385 Standardized Fields**: Captures complete UN MILEX structure with consistent field names
- **11 Force Categories**: Strategic, Land, Naval, Air, Other Military, Central Support, UN Peacekeeping, Military Assistance, Emergency Aid, Undistributed, Total Expenditure
- **35 Expenditure Subcategories**: Personnel, Operations, Procurement, Construction, R&D with detailed breakdowns
- **Flexible Country Coverage**: Choose between 110 countries (default, faster) or 193 countries (complete)
- **Wide Table Schema**: One row per country-year with 395 columns (10 metadata + 385 data fields)
- **Progress Tracking**: Resume capability, error logging, performance timing
- **Graceful Interrupts**: Ctrl+C handling with progress preservation

## Installation

### Requirements

- Python 3.8+
- Chrome or Chromium browser (for SeleniumBase)

### Setup

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Quick Test

```bash
python main.py --example
```

This scrapes Lithuania 2024 as a test and saves to the database.

## Usage

All functionality is accessed through `main.py`. Run `python main.py --help` for full options.

### Scraping

```bash
# Scrape all countries and years (1998-2024, takes several hours)
python main.py

# Use long country list (193 countries instead of default 110)
python main.py --use-long-list

# Scrape specific countries
python main.py --country USA,GBR,FRA,CHN,RUS

# Scrape year range
python main.py --start-year 2020 --end-year 2024

# Scrape single country-year
python main.py --country LTU --year 2024

# Run with visible browser (debugging)
python main.py --country LTU --year 2024 --visible

# Adjust delay between requests (default: 1.0 seconds)
python main.py --delay 2.0

# Force re-scrape (ignore existing data)
python main.py --country USA --year 2024 --no-resume
```

### Exporting

```bash
# Export all data to CSV
python main.py --export

# Custom output filename
python main.py --export --output my_data.csv

# Export modes
python main.py --export --export-mode full        # All data (default)
python main.py --export --export-mode summary     # Summary statistics by country
```

### Utilities

```bash
# View database statistics
python main.py --stats

# View specific country-year record
python main.py --view USA 2024

# List all countries in database
python main.py --list countries

# List all years in database
python main.py --list years

# Show failed scraping attempts
python main.py --show-failed
```

## Database Schema

### Wide Table Structure

The database uses a **wide table schema** with 395 columns per record, optimized for analysis and export. Each row represents one country-year combination.

#### Metadata Fields (10)

- `id`: Primary key
- `country`: ISO 3166-1 alpha-3 code (e.g., "USA", "GBR")
- `year`: Reporting year
- `national_currency`: Currency used (e.g., "US Dollar")
- `unit_of_measure`: Scale (e.g., "Millions", "Thousands")
- `total_expenditure_all`: Total military expenditure
- `explanatory_remarks`: Optional notes from reporting country
- `nil_report_expenditure`: Text for countries with no military forces (e.g., "The Government of Andorra... possesses neither armed nor military forces")
- `page_link`: Source URL
- `scrape_date`: Timestamp of data collection

#### Data Fields (385)

The 385 standardized fields capture the complete UN MILEX reporting structure:

**Force Categories (11 rows):**

1. Strategic forces
2. Land forces
3. Naval forces
4. Air forces
5. Other Military Forces
6. Central Support Administration and Command
7. UN Peace Keeping
8. Military Assistance and Cooperation
9. Emergency Aid to Civilians
10. Undistributed
11. Total Expenditure

**Expenditure Subcategories (35 columns):**

- **Personnel (6)**: Total, Conscripts, Active Military, Reserves, Civilian, Pensions
- **Operations & Maintenance (5)**: Total, Materials, Maintenance, Services, Other
- **Procurement (12)**: Aircraft, Missiles, Nuclear Warheads, Ships, Armoured Vehicles, Artillery, Weapons, Ammunition, Electronics, Non-Armoured Vehicles, Other
- **Construction (7)**: Total, Air Bases, Naval Bases, Electronics Facilities, Personnel Facilities, Training Facilities, Other
- **Research & Development (3)**: Total, Basic/Applied Research, Development/Testing/Evaluation
- **Totals (2)**: Category Subtotal, Grand Total

#### Field Naming Convention

Fields use the format: `{Force_Category}_{Subcategory_Code}_{Subcategory_Name}`

**Examples:**

- `Strategic_forces_1__Personnel` - Strategic forces total personnel
- `Land_forces_3_1_5_Armoured_Vehicles` - Land forces armoured vehicle procurement
- `Air_forces_2_1_Materials_for_Current_Use` - Air forces current use materials
- `Total_Expenditure_5__Total_1plus2plus3plus4` - Overall expenditure total

### Supporting Tables

#### scraping_metadata

- Progress tracking and error logging
- Fields: `country`, `year`, `status`, `error_message`, `last_attempt`

### CSV Export Format

Wide format with 395 columns, one row per country-year:

```csv
country,year,national_currency,Strategic_forces_1__Personnel,Land_forces_1__Personnel,...
USA,2024,US Dollar,50000,200000,...
GBR,2024,Pound Sterling,15000,82000,...
```

NULL values indicate unreported data. Most countries populate 50-150 of the 385 fields.

## Data Analysis

### Working with Exported Data

**Python (pandas):**

```python
import pandas as pd

# Load exported data
df = pd.read_csv('milex_data.csv')

# View structure
print(df.shape)  # (rows, 395 columns)

# Filter to specific force category
land_cols = [col for col in df.columns if col.startswith('Land_forces_')]
df_land = df[['country', 'year'] + land_cols]

# Analyze personnel across all categories
personnel_cols = [col for col in df.columns if '1__Personnel' in col]
df['total_personnel'] = df[personnel_cols].sum(axis=1, skipna=True)

# Compare procurement spending
procurement_cols = [col for col in df.columns if '3_1_' in col]
df_procurement = df[['country', 'year'] + procurement_cols]

# Time series analysis
usa = df[df['country'] == 'USA'].sort_values('year')
usa.plot(x='year', y='Land_forces_1__Personnel')
```

**R:**

```r
# Load data
df <- read.csv('milex_data.csv')

# Filter to specific category
strategic_cols <- grep('Strategic_forces', names(df), value = TRUE)
df_strategic <- df[, c('country', 'year', strategic_cols)]

# Aggregate by region
personnel <- grep('1__Personnel$', names(df), value = TRUE)
df$total_personnel <- rowSums(df[, personnel], na.rm = TRUE)
```

### Common Analysis Patterns

**Compare countries:**

```bash
# Export and filter to major powers
python main.py --export
# Open milex_data.csv and filter country column
```

**Track trends:**

```bash
# Scrape historical data for one country
python main.py --country USA --start-year 2000
python main.py --export
# Analyze year-over-year changes
```

**Category deep-dive:**

```bash
# Export all data
python main.py --export
# Filter columns by category prefix (e.g., Land_forces_*, Naval_forces_*)
```

## Examples

```bash
# Test single page
python main.py --example

# Scrape P5 countries for last decade
python main.py --country USA,GBR,FRA,RUS,CHN --start-year 2014

# Scrape NATO countries, recent years only
python main.py --country USA,GBR,FRA,DEU,ITA,ESP,CAN,POL --start-year 2020

# Check progress
python main.py --stats

# Export and analyze
python main.py --export
python main.py --view USA 2024
```

## Project Structure

```text
main.py                      # CLI interface for all operations
scraper.py                   # SeleniumBase web scraper with 11×35 table parsing
parser.py                    # HTML table parser (legacy support)
database.py                  # SQLite database manager with wide schema
export.py                    # CSV export functions
utils.py                     # Statistics, view, and list utilities
config.py                    # Configuration and field name generation
milex_fields.json            # 385-field structure definition
country_codes_short.json     # 110 ISO 3166-1 alpha-3 country codes (default)
country_codes_long.json      # 193 ISO 3166-1 alpha-3 country codes (all UN members)
requirements.txt             # Python dependencies
```

## Configuration

### config.py

Core settings and field generation:

```python
START_YEAR = 1998              # Starting year for scraping
END_YEAR = 2024                # Ending year for scraping
BASE_URL = "https://milex-reporting.unoda.org/en/states"

# Country list selection
USE_SHORT_COUNTRY_LIST = True  # Use short list (110 countries) by default
                               # Set to False for long list (193 countries)

# 385 standardized field names generated from milex_fields.json
ALL_FIELD_NAMES = get_all_field_names()  # 11 categories × 35 subcategories
```

### Country Lists

The scraper supports two country code lists:

- **Short List** (`country_codes_short.json`): 110 countries - the default

  - Optimized for faster scraping sessions
  - Includes major economies, UN Security Council members, NATO, and significant regional powers
  - Estimated scrape time: ~2-3 hours for full year range

- **Long List** (`country_codes_long.json`): 193 countries - all UN member states
  - Comprehensive coverage of all countries
  - Use `--use-long-list` flag or change `USE_SHORT_COUNTRY_LIST = False` in config.py
  - Estimated scrape time: ~4-6 hours for full year range

**Switching Between Lists:**

```bash
# Command-line (temporary)
python main.py --use-long-list

# Permanent change (edit config.py)
USE_SHORT_COUNTRY_LIST = False  # Use long list by default
```

### milex_fields.json

Defines the 11 force categories and 35 expenditure subcategories that generate the 385-field structure. Edit this file to modify the standardized schema.

## Technical Notes

### Dependencies

- **seleniumbase** - Automated browser control with undetected-chromedriver mode
- **beautifulsoup4** - HTML parsing for 11×35 table structure
- **pandas** - Data manipulation and CSV export
- **lxml** - Fast XML/HTML processing

### Performance

- **Scraping Speed**: ~7-10 seconds per page with optimizations
- **Total Time**:
  - Short list (110 countries): ~2-3 hours for complete scrape (110 × 27 years)
  - Long list (193 countries): ~4-6 hours for complete scrape (193 × 27 years)
- **Database Size**: ~50-100 MB (estimated for full dataset)
- **Resume Capability**: Automatic skip of existing records
- **Column Count**: 395 (well under SQLite's 2000-column limit)

### Table Parsing

The scraper intelligently parses UN MILEX tables:

1. **Header Extraction**: Identifies 35 expenditure subcategories from column headers
2. **Row Matching**: Matches row labels to 11 force categories using fuzzy matching
3. **Cell Mapping**: Maps each table cell to standardized field name (e.g., "Land forces - 3.1.5 Armoured Vehicles")
4. **Value Parsing**: Extracts numeric values, handles various formats and separators
5. **Field Storage**: Stores in wide table with sanitized column names

### Field Name Sanitization

SQL column names are generated by sanitizing field names:

- Replace `" - "` with `"_"`
- Replace `"."` with `"_"`
- Remove: `","`, `"("`, `")"`
- Replace `"-"` with `"_"`
- Replace `"+"` with `"plus"`

Example: `"Land forces - 3.1.2 Missiles, Including Conventional Warheads"` → `"Land_forces_3_1_2_Missiles_Including_Conventional_Warheads"`

### Data Completeness

- **Typical Coverage**: Most countries report 50-150 of the 385 fields
- **NULL Values**: Unreported fields are stored as NULL (efficient in SQLite)
- **Historical Data**: Older years may have less detailed breakdowns
- **Country Variation**: Smaller militaries report fewer categories

### Error Handling

- **Page Not Found (404)**: Marked as 'no_data', NULL values for all 385 fields
- **Network Errors**: Logged in scraping_metadata, can be retried
- **Parse Failures**: Logged and skipped, scraping continues

### Rate Limiting

Default 1-second delay between requests to respect UN servers. Adjustable via `--delay` flag.

## Troubleshooting

**Import errors after installation:**

```bash
# Ensure virtual environment is activated
source venv/bin/activate
pip install -r requirements.txt
```

**Chrome/Chromium not found:**

```bash
# macOS
brew install --cask google-chrome

# Or download from https://www.google.com/chrome/
```

**Database locked:**

Close any programs accessing `milex_data.db` or create backup:

```bash
mv milex_data.db milex_data_backup.db
```

**Scraping errors:**

Use `--visible` flag to see browser and debug:

```bash
python main.py --country LTU --year 2024 --visible
```

## Data Source

United Nations Office for Disarmament Affairs  
Military Expenditures Database  
<https://milex-reporting.unoda.org/>

## License

See LICENSE file for details.
