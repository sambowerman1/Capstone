#!/usr/bin/env python3
"""
Script to extract person names from Wisconsin commemorative highways,
run the consolidated scraper, and append results to the original CSV.

Includes comprehensive timing and logging to identify performance bottlenecks.
"""

import os
import sys
import re
import time
import logging
from datetime import datetime
import pandas as pd

# Add parent directory to path to import consolidated_scraper
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consolidated_scraper import ConsolidatedScraper
from consolidated_scraper.models import PersonRecord
from consolidated_scraper.timing import Timer, TimingStats


# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# Patterns that indicate a group/category highway rather than an individual person
GROUP_PATTERNS = [
    r'\bveterans?\b',
    r'\bdivision\b',
    r'\bbrigade\b',
    r'\bregiment\b',
    r'\bfirefighters?\b',
    r'\bofficers?\b',
    r'\btechnicians?\b',
    r'\bheritage\b',
    r'\bethnic\b',
    r'\bpow\b',
    r'\bmia\b',
    r'\bstar\b',  # Blue Star
    r'\bcinco de mayo\b',
    r'\bcitizen soldier\b',
    r'\bpeace\b',
    r'\bpolish\b',  # Polish Heritage, Polish Veterans
    r'\bkorean war\b',
    r'\bvietnam war\b',
    r'\bworld war\b',
    r'\biron brigade\b',
    r'\brailsplitters\b',
    r'\bfield artillery\b',
    r'\bairborne\b',
    r'\blaw enforcement\b',
    r'\bgreen bay ethnic\b',
]


def is_person_highway(commemorative_name: str) -> bool:
    """
    Determine if a highway is named after an individual person
    rather than a group or category.
    
    Args:
        commemorative_name: The commemorative name of the highway
        
    Returns:
        True if likely named after an individual, False otherwise
    """
    name_lower = commemorative_name.lower()
    
    # Check for group patterns
    for pattern in GROUP_PATTERNS:
        if re.search(pattern, name_lower):
            return False
    
    return True


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{int(minutes)}m {secs:.1f}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{int(hours)}h {int(minutes)}m {secs:.0f}s"


def main():
    # Start overall timing
    total_start_time = time.perf_counter()
    start_datetime = datetime.now()
    
    logger.info("=" * 70)
    logger.info("WISCONSIN HIGHWAY SCRAPER - STARTED")
    logger.info(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    # Paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'wisconsin_commemorative_highways.csv')
    
    # Read the CSV
    logger.info(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    
    logger.info(f"Total highways: {len(df)}")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Filter for person-named highways
    person_mask = df['Commemorative Name'].apply(is_person_highway)
    person_highways = df[person_mask].copy()
    
    logger.info(f"\nPerson-named highways: {len(person_highways)}")
    for idx, row in person_highways.iterrows():
        logger.info(f"  - {row['Commemorative Name']}")
    
    # Get the commemorative names to process
    names_to_process = person_highways['Commemorative Name'].tolist()
    
    if not names_to_process:
        logger.info("No person-named highways found to process.")
        return
    
    # Initialize the consolidated scraper
    logger.info("\n" + "=" * 70)
    logger.info("INITIALIZING CONSOLIDATED SCRAPER...")
    logger.info("=" * 70)
    
    init_start = time.perf_counter()
    scraper = ConsolidatedScraper(
        odmp_state='wisconsin',
        enable_odmp=True,
        enable_wikidata=True,
        enable_ai=True,
        odmp_threshold=92
    )
    init_time = time.perf_counter() - init_start
    logger.info(f"[TIMING] Scraper initialization: {init_time:.2f}s")
    
    # Track per-person timing
    person_times = []
    
    try:
        # Process each person-named highway
        results = []
        for i, name in enumerate(names_to_process, 1):
            person_start = time.perf_counter()
            
            logger.info("\n" + "-" * 70)
            logger.info(f"[{i}/{len(names_to_process)}] PROCESSING: {name}")
            logger.info(f"Elapsed so far: {format_duration(time.perf_counter() - total_start_time)}")
            logger.info("-" * 70)
            
            try:
                record = scraper.scrape_person(name, input_type='highway')
                results.append(record)
            except Exception as e:
                logger.error(f"Error processing {name}: {e}")
                # Create empty record with just the input name
                record = PersonRecord(input_name=name, cleaned_name=name)
                results.append(record)
            
            person_time = time.perf_counter() - person_start
            person_times.append((name, person_time))
            
            logger.info(f"[TIMING] Person '{name[:30]}...' completed in {format_duration(person_time)}")
            
            # Estimate remaining time
            avg_time = sum(t for _, t in person_times) / len(person_times)
            remaining = len(names_to_process) - i
            est_remaining = avg_time * remaining
            logger.info(f"[ESTIMATE] ~{format_duration(est_remaining)} remaining for {remaining} people")
        
        # Convert results to DataFrame
        results_df = pd.DataFrame([r.to_dict() for r in results])
        
        # Create a mapping from commemorative name to results
        # The input_name in results matches the Commemorative Name
        results_df = results_df.set_index('input_name')
        
        # Get the scraper field names (excluding input fields we'll handle separately)
        scraper_fields = [f for f in PersonRecord.get_field_names() 
                         if f not in ['input_name']]
        
        # Add new columns to the original DataFrame
        for field in scraper_fields:
            df[field] = None
        
        # Fill in the results for person-named highways
        for idx, row in df.iterrows():
            comm_name = row['Commemorative Name']
            if comm_name in results_df.index:
                for field in scraper_fields:
                    df.at[idx, field] = results_df.loc[comm_name, field]
        
        # Save the updated CSV
        df.to_csv(csv_path, index=False)
        
    finally:
        scraper.close()
    
    # Calculate total time
    total_time = time.perf_counter() - total_start_time
    end_datetime = datetime.now()
    
    # Print comprehensive timing summary
    logger.info("\n" + "=" * 70)
    logger.info("TIMING SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total duration: {format_duration(total_time)}")
    logger.info(f"Scraper initialization: {init_time:.2f}s")
    logger.info("")
    
    logger.info("Per-Person Timing:")
    for name, duration in person_times:
        logger.info(f"  {name[:45]:<45} : {format_duration(duration)}")
    
    if person_times:
        total_person_time = sum(t for _, t in person_times)
        avg_person_time = total_person_time / len(person_times)
        logger.info("")
        logger.info(f"Average time per person: {format_duration(avg_person_time)}")
        logger.info(f"Total person processing: {format_duration(total_person_time)}")
    
    # Print scraper's internal timing if available
    scraper.print_timing_summary()
    
    logger.info("\n" + "=" * 70)
    logger.info("RESULTS")
    logger.info("=" * 70)
    logger.info(f"Results saved to: {csv_path}")
    logger.info(f"Processed {len(results)} person-named highways")
    logger.info(f"Added {len(scraper_fields)} new columns")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
