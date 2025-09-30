#!/usr/bin/env python3
"""
Demo script that processes the first 10 rows of Memorial_Roadway_Designations.csv
to demonstrate the Wikipedia link finding functionality.
"""

import csv
from wikipedia_link_finder import WikipediaLinkFinder

def demo_processing():
    """Process first 10 rows to demonstrate functionality."""
    
    finder = WikipediaLinkFinder()
    input_file = "Memorial_Roadway_Designations.csv"
    
    print("Memorial Roadway Wikipedia Link Finder - DEMO")
    print("=" * 60)
    print("Processing first 10 rows as demonstration...")
    print()
    
    results = []
    processed_names = set()
    
    try:
        with open(input_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)[:10]  # Only process first 10 rows
        
        for i, row in enumerate(rows, 1):
            designation = str(row.get('DESIGNATIO', ''))
            county = str(row.get('COUNTY', ''))
            
            print(f"{i}. Processing: {designation}")
            
            if not designation or designation == 'nan':
                print("   → No designation found")
                continue
            
            # Extract names
            names = finder.extract_person_names(designation)
            
            if not names:
                print("   → No person names extracted")
                continue
            
            for name in names:
                if name in processed_names:
                    print(f"   → Skipping duplicate: {name}")
                    continue
                
                processed_names.add(name)
                print(f"   → Found name: {name}")
                print(f"   → Searching Wikipedia...")
                
                wikipedia_url = finder.search_wikipedia(name)
                
                result = {
                    'name': name,
                    'original_designation': designation,
                    'county': county,
                    'wikipedia_url': wikipedia_url if wikipedia_url else 'Not Found',
                    'status': 'Found' if wikipedia_url else 'Not Found'
                }
                
                results.append(result)
                print(f"   → Result: {result['status']} - {result['wikipedia_url']}")
            
            print()
        
        # Save demo results
        output_file = "demo_results.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            if results:
                fieldnames = ['name', 'original_designation', 'county', 'wikipedia_url', 'status']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
        
        # Print summary
        found_count = len([r for r in results if r['status'] == 'Found'])
        total_count = len(results)
        
        print("=" * 60)
        print("DEMO RESULTS SUMMARY")
        print("=" * 60)
        print(f"Rows processed: 10")
        print(f"Unique names extracted: {total_count}")
        print(f"Wikipedia pages found: {found_count}")
        print(f"Not found: {total_count - found_count}")
        if total_count > 0:
            print(f"Success rate: {found_count/total_count*100:.1f}%")
        print(f"Demo results saved to: {output_file}")
        
        if results:
            print("\nDetailed Results:")
            for result in results:
                status_icon = "✅" if result['status'] == 'Found' else "❌"
                print(f"{status_icon} {result['name']} - {result['wikipedia_url']}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    demo_processing()
