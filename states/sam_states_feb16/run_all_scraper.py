"""
Runner script to extract names from all state data files in sam_states_feb16/
and run the consolidated scraper on each.

Data notes:
- california/california_json.json: GeoJSON, name field = 'Name', state = california
- oklahoma/oklahoma.json: ESRI JSON, name field = 'RouteDesignationName', actual state = Kansas (KDOT)
- missouri/MemorialRoadsAndBridges_*.csv: CSV, name field = 'MEMORIAL_N', actual state = Oklahoma (OKDOT)
- north_carolina_geojson.json: GeoJSON, name field = 'NAME', state = north carolina
"""

import csv
import json
import os
import sys
import logging

# Add the project root to the path so we can import consolidated_scraper
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from consolidated_scraper import ConsolidatedScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def extract_names_from_geojson(filepath, name_key):
    """Extract unique names from a GeoJSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    names = set()
    for feature in data['features']:
        name = feature['properties'].get(name_key)
        if name and name.strip():
            names.add(name.strip())
    return sorted(names)


def extract_names_from_esri_json(filepath, name_key):
    """Extract unique names from an ESRI JSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    names = set()
    for feature in data['features']:
        name = feature['attributes'].get(name_key)
        if name and name.strip():
            names.add(name.strip())
    return sorted(names)


def extract_names_from_csv(filepath, name_key):
    """Extract unique names from a CSV file."""
    names = set()
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get(name_key)
            if name and name.strip():
                names.add(name.strip())
    return sorted(names)


def save_names_csv(names, output_path):
    """Save a list of names to a CSV with a 'name' column."""
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['name'])
        for name in names:
            writer.writerow([name])
    logger.info(f"Saved {len(names)} names to {output_path}")
    return output_path


def run_scraper_on_state(state_name, odmp_state, csv_path, output_path):
    """Run the consolidated scraper on a single state's CSV."""
    logger.info(f"\n{'='*80}")
    logger.info(f"PROCESSING: {state_name} (ODMP state: {odmp_state})")
    logger.info(f"Input: {csv_path}")
    logger.info(f"Output: {output_path}")
    logger.info(f"{'='*80}")

    scraper = ConsolidatedScraper(
        odmp_state=odmp_state,
        enable_odmp=True,
        enable_wikidata=True,
        enable_ai=True,
    )

    try:
        df = scraper.scrape_from_csv(
            csv_path,
            name_column='name',
            input_type='highway',
            output_file=output_path
        )
        logger.info(f"Processed {len(df)} names for {state_name}")
        scraper.print_timing_summary()
        return df
    except KeyboardInterrupt:
        logger.warning(f"Interrupted while processing {state_name}")
        raise
    except Exception as e:
        logger.error(f"Error processing {state_name}: {e}")
        raise
    finally:
        scraper.close()


def main():
    # Define all state configurations
    states = [
        {
            'name': 'California',
            'odmp_state': 'california',
            'extract_func': extract_names_from_geojson,
            'source_file': os.path.join(BASE_DIR, 'california', 'california_json.json'),
            'name_key': 'Name',
            'csv_output': os.path.join(BASE_DIR, 'california', 'california_names.csv'),
            'scraper_output': os.path.join(BASE_DIR, 'california', 'california_scraper_output.csv'),
        },
        {
            'name': 'Kansas (from oklahoma folder)',
            'odmp_state': 'kansas',
            'extract_func': extract_names_from_esri_json,
            'source_file': os.path.join(BASE_DIR, 'oklahoma', 'oklahoma.json'),
            'name_key': 'RouteDesignationName',
            'csv_output': os.path.join(BASE_DIR, 'oklahoma', 'kansas_names.csv'),
            'scraper_output': os.path.join(BASE_DIR, 'oklahoma', 'kansas_scraper_output.csv'),
        },
        {
            'name': 'Oklahoma (from missouri folder)',
            'odmp_state': 'oklahoma',
            'extract_func': extract_names_from_csv,
            'source_file': os.path.join(BASE_DIR, 'missouri', 'MemorialRoadsAndBridges_-9189987098950501562.csv'),
            'name_key': 'MEMORIAL_N',
            'csv_output': os.path.join(BASE_DIR, 'missouri', 'oklahoma_names.csv'),
            'scraper_output': os.path.join(BASE_DIR, 'missouri', 'oklahoma_scraper_output.csv'),
        },
        {
            'name': 'North Carolina',
            'odmp_state': 'north-carolina',
            'extract_func': extract_names_from_geojson,
            'source_file': os.path.join(BASE_DIR, 'north_carolina_geojson.json'),
            'name_key': 'NAME',
            'csv_output': os.path.join(BASE_DIR, 'north_carolina', 'north_carolina_names.csv'),
            'scraper_output': os.path.join(BASE_DIR, 'north_carolina', 'north_carolina_scraper_output.csv'),
        },
    ]

    # Step 1: Extract names from all files into CSVs
    logger.info("=" * 80)
    logger.info("STEP 1: Extracting names from data files")
    logger.info("=" * 80)

    for state in states:
        logger.info(f"\nExtracting names for {state['name']}...")
        names = state['extract_func'](state['source_file'], state['name_key'])
        logger.info(f"  Found {len(names)} unique names")

        # Ensure output directory exists
        os.makedirs(os.path.dirname(state['csv_output']), exist_ok=True)
        save_names_csv(names, state['csv_output'])

    # Step 2: Run scraper on each state
    logger.info("\n" + "=" * 80)
    logger.info("STEP 2: Running consolidated scraper on each state")
    logger.info("=" * 80)

    for state in states:
        try:
            run_scraper_on_state(
                state['name'],
                state['odmp_state'],
                state['csv_output'],
                state['scraper_output']
            )
        except KeyboardInterrupt:
            logger.warning("User interrupted. Stopping.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Failed on {state['name']}: {e}")
            logger.info("Continuing to next state...")
            continue

    logger.info("\n" + "=" * 80)
    logger.info("ALL STATES COMPLETE")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
