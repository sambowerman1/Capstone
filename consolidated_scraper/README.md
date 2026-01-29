# Consolidated Scraper

A unified Python module for scraping person information from three sources:
- **ODMP** (Officer Down Memorial Page) - Fallen officer data
- **Wikidata** - Biographical data (occupation, birth/death dates, etc.)
- **AI Summarizer** - AI-generated summaries from Wikipedia content

## Installation

Install dependencies:
```bash
pip install -r consolidated_scraper/requirements.txt
```

## Quick Start

### Command Line Usage

```bash
# Basic usage with all scrapers (requires state for ODMP)
python -m consolidated_scraper.cli --name "John Smith" --state florida --output result.csv

# Skip ODMP (faster, no Selenium needed)
python -m consolidated_scraper.cli --name "Albert Einstein" --no-odmp --output result.csv

# Skip ODMP and AI (Wikidata only, fastest)
python -m consolidated_scraper.cli --name "John Smith" --no-odmp --no-ai --output result.csv
```

### Python API Usage

```python
from consolidated_scraper import ConsolidatedScraper

# Initialize with state (required for ODMP)
scraper = ConsolidatedScraper(odmp_state='florida')

# Scrape a single person
result = scraper.scrape_person("John Smith")
print(result.wikidata_occupation)
print(result.odmp_cause)
print(result.ai_summary)

# Don't forget to close when done
scraper.close()
```

## CLI Options

| Option | Description |
|--------|-------------|
| `--name NAME` | Single name to scrape |
| `--input FILE` | Text file with names (one per line) |
| `--csv FILE` | CSV file with names |
| `--column COL` | Column name in CSV (required with --csv) |
| `--type TYPE` | Input type: `person`, `highway`, or `auto` (default: auto) |
| `--output FILE` | Output CSV file (required) |
| `--state STATE` | US state for ODMP (required unless --no-odmp) |
| `--threshold N` | ODMP fuzzy match threshold 0-100 (default: 92) |
| `--no-odmp` | Disable ODMP scraper |
| `--no-wikidata` | Disable Wikidata scraper |
| `--no-ai` | Disable AI summarizer |
| `--mistral-key KEY` | Mistral API key (or set `MistralAPIKey` env var) |

## Examples

### Single Name
```bash
python -m consolidated_scraper.cli \
    --name "Martin Luther King Jr" \
    --state georgia \
    --output mlk_result.csv
```

### Highway Name (auto-extracts person name)
```bash
python -m consolidated_scraper.cli \
    --name "John F Kennedy Memorial Highway" \
    --type highway \
    --state florida \
    --output jfk_result.csv
```

### Batch from Text File
```bash
# names.txt should have one name per line
python -m consolidated_scraper.cli \
    --input names.txt \
    --type person \
    --state texas \
    --output batch_results.csv
```

### Batch from CSV
```bash
python -m consolidated_scraper.cli \
    --csv memorial_highways.csv \
    --column DESIGNATIO \
    --type highway \
    --state florida \
    --output highway_results.csv
```

### Fast Mode (Wikidata Only)
```bash
# Skip ODMP and AI for quick results
python -m consolidated_scraper.cli \
    --name "Albert Einstein" \
    --no-odmp \
    --no-ai \
    --output quick_result.csv
```

### With Mistral API Key
```bash
python -m consolidated_scraper.cli \
    --name "John Smith" \
    --no-odmp \
    --mistral-key YOUR_API_KEY \
    --output result_with_ai.csv
```

## Output Fields

The output CSV contains 30 fields from all three sources:

### Input Fields
| Field | Description |
|-------|-------------|
| `input_name` | Original input name |
| `cleaned_name` | Cleaned/extracted person name |
| `input_type` | Detected type (person/highway) |

### ODMP Fields
| Field | Description |
|-------|-------------|
| `odmp_url` | Officer profile URL |
| `odmp_name` | Officer name from ODMP |
| `odmp_bio` | Biographical narrative |
| `odmp_age` | Age at end of watch |
| `odmp_tour` | Years of service |
| `odmp_badge` | Badge number |
| `odmp_cause` | Cause of death |
| `odmp_end_of_watch` | Date of death |
| `odmp_incident_details` | Incident description |
| `odmp_fuzzy_score` | Name match score (0-100) |

### Wikidata Fields
| Field | Description |
|-------|-------------|
| `wikidata_occupation` | Primary occupation |
| `wikidata_race` | Ethnicity |
| `wikidata_sex` | Gender |
| `wikidata_birth_date` | Birth date (YYYY-MM-DD) |
| `wikidata_death_date` | Death date (YYYY-MM-DD) |
| `wikipedia_link` | Wikipedia URL |

### AI Summarizer Fields
| Field | Description |
|-------|-------------|
| `ai_summary` | 4-sentence biography |
| `ai_education` | Educational institutions |
| `ai_dob` | Date of birth |
| `ai_dod` | Date of death |
| `ai_place_of_birth` | Birth location |
| `ai_place_of_death` | Death location |
| `ai_gender` | Gender |
| `ai_involved_in_sports` | yes/no |
| `ai_involved_in_politics` | yes/no |
| `ai_involved_in_military` | yes/no |
| `ai_involved_in_music` | yes/no |

## Python API

### ConsolidatedScraper Class

```python
from consolidated_scraper import ConsolidatedScraper

# Full initialization
scraper = ConsolidatedScraper(
    odmp_state='florida',           # Required if ODMP enabled
    mistral_api_key='YOUR_KEY',     # Optional, uses env var if not set
    enable_odmp=True,               # Default: True
    enable_wikidata=True,           # Default: True
    enable_ai=True,                 # Default: True
    odmp_threshold=92               # Fuzzy match threshold
)

# Scrape single person
result = scraper.scrape_person(
    name="John Smith Memorial Highway",
    input_type='highway'  # or 'person' or 'auto'
)

# Scrape batch
results = scraper.scrape_batch(
    names=["Name 1", "Name 2", "Name 3"],
    input_type='person',
    output_file='results.csv'  # Optional
)

# Scrape from CSV
df = scraper.scrape_from_csv(
    file_path='input.csv',
    name_column='DESIGNATIO',
    input_type='highway',
    output_file='output.csv'
)

# Scrape from text file
results = scraper.scrape_from_text_file(
    file_path='names.txt',
    input_type='person',
    output_file='output.csv'
)

# Always close when done
scraper.close()
```

### PersonRecord Class

```python
from consolidated_scraper import PersonRecord

# Access fields
print(result.input_name)
print(result.wikidata_occupation)
print(result.odmp_cause)
print(result.ai_summary)

# Convert to dictionary
data = result.to_dict()

# Get all field names (for CSV headers)
fields = PersonRecord.get_field_names()
```

## Performance Notes

- **Wikidata**: Fast (API calls only)
- **AI Summarizer**: Moderate (web scraping + API call)
- **ODMP**: Slow (browses entire state's officers via Selenium)

For faster results, use `--no-odmp` to skip ODMP scraping.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MistralAPIKey` | Mistral API key for AI summarizer |

You can also create a `.env.local` file in the project root:
```
MistralAPIKey=your_api_key_here
```

## Troubleshooting

### ODMP Scraper Issues
- Requires Chrome browser installed
- ChromeDriver is auto-installed via webdriver-manager
- If slow, consider using `--no-odmp` for testing

### AI Summarizer Issues
- Requires valid Mistral API key
- Only runs if Wikipedia link is found from Wikidata
- Use `--no-ai` to skip if API key unavailable

### No Results Found
- Check spelling of names
- Try different input types (person vs highway)
- Lower ODMP threshold with `--threshold 80`
