#!/usr/bin/env python3
"""
Enhance Memorial Data with AI Summarizer

This script reads the memorial_names_with_wikipedia_links.csv file and adds
Summary, DOB, DOD, and Education columns by using the AI summarizer for
each Wikipedia link that was found.
"""

import csv
import sys
import os
from typing import Dict, List, Optional

# Add the ai_summarizer directory to the path so we can import the Person class
sys.path.append(os.path.join(os.path.dirname(__file__), 'ai_summarizer'))

try:
    from person_summarizer import Person
except ImportError as e:
    print(f"Error importing Person class: {e}")
    print("Make sure you're running this from the Capstone directory and that person_summarizer.py exists in ai_summarizer/")
    sys.exit(1)


class MemorialDataEnhancer:
    """A class to enhance memorial data with AI-generated biographical information."""
    
    def __init__(self):
        """Initialize the enhancer."""
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
    
    def process_person(self, wikipedia_url: str) -> Dict[str, Optional[str]]:
        """
        Process a single person's Wikipedia URL to extract biographical data.
        
        Args:
            wikipedia_url: The Wikipedia URL to process
            
        Returns:
            Dictionary with summary, education, dob, dod keys
        """
        result = {
            'summary': None,
            'education': None,
            'dob': None,
            'dod': None
        }
        
        try:
            # Create Person object with the Wikipedia URL
            person = Person(wikipedia_url)
            
            # Extract all biographical data using the new methods
            summary = person.getSummary()
            education = person.getEducation()
            dob = person.getDOB()
            dod = person.getDOD()
            
            # Format the results
            result['summary'] = summary if summary else ""
            result['education'] = str(education) if education else ""
            result['dob'] = dob if dob else ""
            result['dod'] = dod if dod else ""
            
            self.success_count += 1
            
        except Exception as e:
            print(f"Error processing {wikipedia_url}: {e}")
            self.error_count += 1
            
        return result
    
    def enhance_csv(self, input_file: str, output_file: str, max_entries: Optional[int] = None) -> None:
        """
        Enhance the memorial CSV with biographical data.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            max_entries: Maximum number of entries to process (for testing)
        """
        print(f"Reading {input_file}...")
        
        try:
            # Read the existing CSV
            with open(input_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
            
            print(f"Found {len(rows)} memorial entries")
            
            # Filter to only entries with Wikipedia URLs
            entries_with_wikipedia = [row for row in rows if row.get('status') == 'Found' and row.get('wikipedia_url') != 'Not Found']
            print(f"Found {len(entries_with_wikipedia)} entries with Wikipedia links")
            
            if max_entries:
                entries_with_wikipedia = entries_with_wikipedia[:max_entries]
                print(f"Processing first {len(entries_with_wikipedia)} entries for demo")
            
            # Prepare enhanced data
            enhanced_rows = []
            
            for i, row in enumerate(rows, 1):
                # Create enhanced row with new columns
                enhanced_row = row.copy()
                enhanced_row['summary'] = ""
                enhanced_row['education'] = ""
                enhanced_row['dob'] = ""
                enhanced_row['dod'] = ""
                
                # Only process if we have a Wikipedia URL
                if row.get('status') == 'Found' and row.get('wikipedia_url') != 'Not Found':
                    if max_entries and self.processed_count >= max_entries:
                        # If we've hit our limit, just add empty enhanced data
                        enhanced_rows.append(enhanced_row)
                        continue
                    
                    wikipedia_url = row['wikipedia_url']
                    name = row['name']
                    
                    print(f"{i}. Processing: {name}")
                    print(f"   URL: {wikipedia_url}")
                    
                    # Extract biographical data
                    bio_data = self.process_person(wikipedia_url)
                    
                    # Add to enhanced row
                    enhanced_row['summary'] = bio_data['summary']
                    enhanced_row['education'] = bio_data['education']
                    enhanced_row['dob'] = bio_data['dob']
                    enhanced_row['dod'] = bio_data['dod']
                    
                    self.processed_count += 1
                    
                    # Progress update
                    if self.processed_count % 5 == 0:
                        print(f"   Progress: {self.processed_count} processed...")
                    
                else:
                    # No Wikipedia URL, skip processing
                    self.skipped_count += 1
                
                enhanced_rows.append(enhanced_row)
            
            # Write enhanced CSV
            print(f"\nWriting enhanced data to {output_file}...")
            
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                if enhanced_rows:
                    fieldnames = ['name', 'original_designation', 'county', 'wikipedia_url', 'status', 
                                'summary', 'education', 'dob', 'dod']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(enhanced_rows)
            
            # Print summary
            print(f"\n{'='*60}")
            print("ENHANCEMENT COMPLETE")
            print(f"{'='*60}")
            print(f"Total entries: {len(rows)}")
            print(f"Entries with Wikipedia links: {len(entries_with_wikipedia)}")
            print(f"Successfully processed: {self.success_count}")
            print(f"Errors: {self.error_count}")
            print(f"Skipped (no Wikipedia): {self.skipped_count}")
            if self.processed_count > 0:
                print(f"Success rate: {self.success_count/self.processed_count*100:.1f}%")
            print(f"Enhanced data saved to: {output_file}")
            
        except Exception as e:
            print(f"Error processing CSV: {e}")
            raise


def main():
    """Main function to run the memorial data enhancer."""
    input_file = "memorial_names_with_wikipedia_links.csv"
    
    print("Memorial Data Enhancer")
    print("=" * 50)
    print("This script will add Summary, DOB, DOD, and Education columns")
    print("to the memorial CSV using AI summarization.\n")
    
    # Ask user if they want to run a demo or full processing
    choice = input("Run demo (first 10 entries) or full processing? (demo/full): ").lower().strip()
    
    if choice == 'demo':
        output_file = "memorial_data_enhanced_demo.csv"
        max_entries = 10
        print("Running demo with first 10 Wikipedia entries...")
    else:
        output_file = "memorial_data_enhanced_full.csv"
        max_entries = None
        print("Running full processing (this may take 20-30 minutes)...")
        confirm = input("Are you sure? This will make many API calls. (y/n): ").lower().strip()
        if confirm != 'y':
            print("Cancelled.")
            return
    
    enhancer = MemorialDataEnhancer()
    enhancer.enhance_csv(input_file, output_file, max_entries)


if __name__ == "__main__":
    main()
