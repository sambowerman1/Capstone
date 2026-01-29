#!/usr/bin/env python3
"""
Command-line interface for the consolidated scraper.
"""

import argparse
import os
import sys

from .scraper import ConsolidatedScraper


def main():
    parser = argparse.ArgumentParser(
        description='Consolidated scraper for ODMP, Wikidata, and AI summarization.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Basic usage (CSV must have a "name" column)
  python -m consolidated_scraper.cli texas /path/to/names.csv

  # With custom column name
  python -m consolidated_scraper.cli texas /path/to/highways.csv --column DESIGNATIO

  # Adjust ODMP fuzzy match threshold
  python -m consolidated_scraper.cli texas /path/to/names.csv --threshold 85

  # Skip ODMP scraper
  python -m consolidated_scraper.cli texas /path/to/names.csv --no-odmp

  # Custom output path
  python -m consolidated_scraper.cli texas /path/to/names.csv --output /other/path/results.csv
'''
    )

    # Positional arguments
    parser.add_argument(
        'state',
        help='US state for ODMP search (e.g., texas, california)'
    )
    parser.add_argument(
        'csv_path',
        help='Path to CSV file with names'
    )

    # Optional overrides
    parser.add_argument(
        '--column',
        default='name',
        help='Column name in CSV file (default: "name")'
    )
    parser.add_argument(
        '--type',
        choices=['person', 'highway', 'auto'],
        default='auto',
        help='Input type: person name, highway designation, or auto-detect (default: auto)'
    )
    parser.add_argument(
        '--output', '-o',
        help='Output CSV file (default: input_output.csv in same directory)'
    )

    # ODMP options
    parser.add_argument(
        '--threshold',
        type=int,
        default=92,
        help='ODMP fuzzy match threshold (0-100, default: 92)'
    )

    # Scraper toggles
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

    # API keys
    parser.add_argument(
        '--mistral-key',
        help='Mistral API key (or set MistralAPIKey env var)'
    )

    args = parser.parse_args()

    # Validate CSV path exists
    if not os.path.isfile(args.csv_path):
        parser.error(f"CSV file not found: {args.csv_path}")

    # Generate output path if not specified
    if args.output:
        output_path = args.output
    else:
        csv_dir = os.path.dirname(args.csv_path) or '.'
        csv_basename = os.path.basename(args.csv_path)
        name_without_ext = os.path.splitext(csv_basename)[0]
        output_path = os.path.join(csv_dir, f"{name_without_ext}_output.csv")

    # Initialize scraper
    try:
        scraper = ConsolidatedScraper(
            odmp_state=args.state,
            mistral_api_key=args.mistral_key,
            enable_odmp=not args.no_odmp,
            enable_wikidata=not args.no_wikidata,
            enable_ai=not args.no_ai,
            odmp_threshold=args.threshold
        )
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    try:
        # Process CSV file
        df = scraper.scrape_from_csv(
            args.csv_path,
            name_column=args.column,
            input_type=args.type,
            output_file=output_path
        )
        print(f"\nProcessed {len(df)} names")
        print(f"Output saved to: {output_path}")

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
    finally:
        scraper.close()


if __name__ == '__main__':
    main()
