#!/usr/bin/env python3
"""
Demo script showing how to enhance memorial data with AI summarizer.
This processes just the first 5 Wikipedia entries to demonstrate the functionality.
"""

import csv
import sys
import os

# Add the ai_summarizer directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'ai_summarizer'))

from person_summarizer import Person


def demo_enhancement():
    """Demonstrate the enhancement process with first 5 Wikipedia entries."""
    
    input_file = "memorial_names_with_wikipedia_links.csv"
    
    print("Memorial Data Enhancement Demo")
    print("=" * 60)
    print("Processing first 5 entries with Wikipedia links...\n")
    
    # Read the CSV and find entries with Wikipedia links
    with open(input_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
    
    # Filter to entries with Wikipedia URLs
    wikipedia_entries = [row for row in rows if row.get('status') == 'Found' and row.get('wikipedia_url') != 'Not Found']
    
    # Process first 5 entries
    demo_entries = wikipedia_entries[:5]
    
    enhanced_data = []
    
    for i, entry in enumerate(demo_entries, 1):
        name = entry['name']
        wikipedia_url = entry['wikipedia_url']
        county = entry['county']
        
        print(f"{i}. Processing: {name}")
        print(f"   County: {county}")
        print(f"   Wikipedia: {wikipedia_url}")
        
        try:
            # Create Person object and extract data
            person = Person(wikipedia_url)
            
            summary = person.getSummary()
            education = person.getEducation()
            dob = person.getDOB()
            dod = person.getDOD()
            
            # Display results
            print(f"   ✅ SUCCESS!")
            print(f"   Summary: {summary[:100] if summary else 'None'}...")
            print(f"   Education: {education if education else 'None'}")
            print(f"   DOB: {dob if dob else 'None'}")
            print(f"   DOD: {dod if dod else 'None'}")
            
            # Store enhanced data
            enhanced_entry = {
                'name': name,
                'original_designation': entry['original_designation'],
                'county': county,
                'wikipedia_url': wikipedia_url,
                'status': 'Found',
                'summary': summary or "",
                'education': str(education) if education else "",
                'dob': dob or "",
                'dod': dod or ""
            }
            enhanced_data.append(enhanced_entry)
            
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            
            # Store entry with empty enhanced data
            enhanced_entry = {
                'name': name,
                'original_designation': entry['original_designation'],
                'county': county,
                'wikipedia_url': wikipedia_url,
                'status': 'Found',
                'summary': "",
                'education': "",
                'dob': "",
                'dod': ""
            }
            enhanced_data.append(enhanced_entry)
        
        print("-" * 60)
    
    # Save demo results
    output_file = "demo_enhanced_memorial_data.csv"
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['name', 'original_designation', 'county', 'wikipedia_url', 'status', 
                     'summary', 'education', 'dob', 'dod']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enhanced_data)
    
    print("DEMO COMPLETE")
    print(f"Enhanced data saved to: {output_file}")
    print(f"Processed {len(enhanced_data)} entries")
    
    # Show summary of what was found
    print("\nSUMMARY OF ENHANCED DATA:")
    for entry in enhanced_data:
        status = "✅" if entry['summary'] else "❌"
        print(f"{status} {entry['name']} - Summary: {'Yes' if entry['summary'] else 'No'}, "
              f"DOB: {'Yes' if entry['dob'] else 'No'}, DOD: {'Yes' if entry['dod'] else 'No'}")


if __name__ == "__main__":
    demo_enhancement()
