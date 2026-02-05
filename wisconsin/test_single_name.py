#!/usr/bin/env python3
"""
Test script to run the consolidated scraper on a single name
and report detailed timing breakdown for each component.

This helps identify performance bottlenecks without running the full scraper.
"""

import os
import sys
import time
import logging
from datetime import datetime

# Add parent directory to path to import consolidated_scraper
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consolidated_scraper import ConsolidatedScraper
from consolidated_scraper.timing import TimingStats


# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{int(minutes)}m {secs:.1f}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{int(hours)}h {int(minutes)}m {secs:.0f}s"


def run_single_name_test(
    test_name: str = "Frank Lloyd Wright Memorial Highway",
    enable_odmp: bool = True,
    enable_wikidata: bool = True, 
    enable_ai: bool = True
):
    """
    Run the scraper on a single name and report detailed timing.
    
    Args:
        test_name: The highway name to test
        enable_odmp: Whether to enable ODMP scraper
        enable_wikidata: Whether to enable Wikidata scraper
        enable_ai: Whether to enable AI summarizer
    """
    total_start = time.perf_counter()
    start_datetime = datetime.now()
    
    print("\n" + "=" * 70)
    print("SINGLE NAME TEST - TIMING BREAKDOWN")
    print("=" * 70)
    print(f"Test name: {test_name}")
    print(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"ODMP enabled: {enable_odmp}")
    print(f"Wikidata enabled: {enable_wikidata}")
    print(f"AI enabled: {enable_ai}")
    print("=" * 70 + "\n")
    
    # Track individual component times
    timings = {}
    
    # Initialize scraper
    print("[1/4] Initializing scraper...")
    init_start = time.perf_counter()
    
    scraper = ConsolidatedScraper(
        odmp_state='wisconsin',
        enable_odmp=enable_odmp,
        enable_wikidata=enable_wikidata,
        enable_ai=enable_ai,
        odmp_threshold=92
    )
    
    timings['Scraper Initialization'] = time.perf_counter() - init_start
    print(f"    -> {format_duration(timings['Scraper Initialization'])}")
    
    try:
        # Run the scraper
        print(f"\n[2/4] Processing: {test_name}")
        process_start = time.perf_counter()
        
        record = scraper.scrape_person(test_name, input_type='highway')
        
        timings['Total Processing'] = time.perf_counter() - process_start
        print(f"    -> {format_duration(timings['Total Processing'])}")
        
        # Get component timings from the scraper's stats
        for operation, times in scraper.timing_stats.timings.items():
            timings[operation] = sum(times)
        
        # Print results summary
        print(f"\n[3/4] Results retrieved:")
        print(f"    Cleaned name: {record.cleaned_name}")
        print(f"    ODMP match: {'Yes' if record.odmp_name else 'No'}")
        if record.odmp_name:
            print(f"      - Name: {record.odmp_name}")
            print(f"      - Cause: {record.odmp_cause}")
        print(f"    Wikidata found: {'Yes' if record.wikipedia_link else 'No'}")
        if record.wikipedia_link:
            print(f"      - Occupation: {record.wikidata_occupation}")
            print(f"      - Wikipedia: {record.wikipedia_link}")
        print(f"    AI summary: {'Yes' if record.ai_summary else 'No'}")
        if record.ai_summary:
            print(f"      - Summary preview: {record.ai_summary[:100]}...")
        
    finally:
        # Cleanup
        print("\n[4/4] Cleaning up...")
        cleanup_start = time.perf_counter()
        scraper.close()
        timings['Cleanup'] = time.perf_counter() - cleanup_start
        print(f"    -> {format_duration(timings['Cleanup'])}")
    
    # Calculate total
    total_time = time.perf_counter() - total_start
    end_datetime = datetime.now()
    
    # Print detailed timing breakdown
    print("\n" + "=" * 70)
    print("DETAILED TIMING BREAKDOWN")
    print("=" * 70)
    
    # Sort timings by duration (descending)
    sorted_timings = sorted(timings.items(), key=lambda x: x[1], reverse=True)
    
    for operation, duration in sorted_timings:
        percentage = (duration / total_time) * 100
        bar_length = int(percentage / 2)  # Scale to 50 chars max
        bar = "█" * bar_length + "░" * (50 - bar_length)
        print(f"{operation:<30} : {format_duration(duration):>10} ({percentage:5.1f}%) {bar}")
    
    print("-" * 70)
    print(f"{'TOTAL':<30} : {format_duration(total_time):>10} (100.0%)")
    print("=" * 70)
    
    # Print timing summary from scraper
    print("\nSCRAPER INTERNAL TIMING:")
    scraper.print_timing_summary()
    
    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)
    print(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration: {format_duration(total_time)}")
    print("=" * 70)
    
    return timings, total_time


def main():
    """Main entry point with command-line argument support."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Test the consolidated scraper on a single name with detailed timing.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Default test (Frank Lloyd Wright Memorial Highway, all scrapers)
  python test_single_name.py
  
  # Test with a specific name
  python test_single_name.py --name "Gaylord Nelson Highway"
  
  # Test without ODMP (faster, for testing Wikidata/AI only)
  python test_single_name.py --no-odmp
  
  # Test only Wikidata
  python test_single_name.py --no-odmp --no-ai
'''
    )
    
    parser.add_argument(
        '--name', '-n',
        default="Frank Lloyd Wright Memorial Highway",
        help='Highway name to test (default: "Frank Lloyd Wright Memorial Highway")'
    )
    parser.add_argument(
        '--no-odmp',
        action='store_true',
        help='Disable ODMP scraper'
    )
    parser.add_argument(
        '--no-wikidata',
        action='store_true',
        help='Disable Wikidata scraper'
    )
    parser.add_argument(
        '--no-ai',
        action='store_true',
        help='Disable AI summarizer'
    )
    
    args = parser.parse_args()
    
    run_single_name_test(
        test_name=args.name,
        enable_odmp=not args.no_odmp,
        enable_wikidata=not args.no_wikidata,
        enable_ai=not args.no_ai
    )


if __name__ == '__main__':
    main()

