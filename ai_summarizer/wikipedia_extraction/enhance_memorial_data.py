#!/usr/bin/env python3
"""
Enhance Memorial Data with AI Summarizer

This script reads the memorial_names_with_wikipedia_links.csv file and adds
comprehensive biographical data columns by using the AI summarizer for
each Wikipedia link that was found.

New columns added:
- Summary (4-sentence biographical summary)
- DOB (Date of Birth)
- DOD (Date of Death)
- Education
- Place of Birth
- Place of Death
- Gender
- Involved in Sports (yes/no)
- Involved in Politics (yes/no)
- Involved in Military (yes/no)
- Involved in Music (yes/no)
"""

import csv
import sys
import os
import time
import random
from typing import Dict, List, Optional

# Add the parent directory to the path so we can import the Person class
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from person_summarizer import Person
except ImportError as e:
    print(f"Error importing Person class: {e}")
    print("Make sure you're running this from the Capstone directory and that person_summarizer.py exists in ai_summarizer/")
    sys.exit(1)


class RateLimitHandler:
    """Handles API rate limits with exponential backoff."""
    
    def __init__(self, base_delay: float = 2.0, max_retries: int = 3):
        self.base_delay = base_delay
        self.max_retries = max_retries
        self.last_request_time = 0
    
    def wait_before_request(self):
        """Ensure minimum delay between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.base_delay:
            sleep_time = self.base_delay - time_since_last
            print(f"   Rate limiting: waiting {sleep_time:.1f}s...")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def exponential_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        delay = self.base_delay * (2 ** attempt) + random.uniform(0, 1)
        return min(delay, 60)  # Cap at 60 seconds


class MemorialDataEnhancer:
    """A class to enhance memorial data with AI-generated biographical information."""
    
    def __init__(self):
        """Initialize the enhancer."""
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.rate_limiter = RateLimitHandler()
    
    def process_person_with_retry(self, wikipedia_url: str, name: str) -> Dict[str, Optional[str]]:
        """
        Process a single person's Wikipedia URL with retry logic and rate limiting.
        
        Args:
            wikipedia_url: The Wikipedia URL to process
            name: Person's name for logging
            
        Returns:
            Dictionary with all biographical fields
        """
        result = {
            'summary': "",
            'education': "",
            'dob': "",
            'dod': "",
            'place_of_birth': "",
            'place_of_death': "",
            'gender': "",
            'involved_in_sports': "",
            'involved_in_politics': "",
            'involved_in_military': "",
            'involved_in_music': ""
        }
        
        for attempt in range(self.rate_limiter.max_retries):
            try:
                # Rate limiting before each attempt
                self.rate_limiter.wait_before_request()
                
                # Create Person object with the Wikipedia URL
                person = Person(wikipedia_url)
                
                # Extract all biographical data using the enhanced methods
                summary = person.getSummary()
                education = person.getEducation()
                dob = person.getDOB()
                dod = person.getDOD()
                place_of_birth = person.getPlaceOfBirth()
                place_of_death = person.getPlaceOfDeath()
                gender = person.getGender()
                sports = person.getInvolvedInSports()
                politics = person.getInvolvedInPolitics()
                military = person.getInvolvedInMilitary()
                music = person.getInvolvedInMusic()
                
                # Format the results
                result['summary'] = summary if summary else ""
                result['education'] = str(education) if education else ""
                result['dob'] = dob if dob else ""
                result['dod'] = dod if dod else ""
                result['place_of_birth'] = place_of_birth if place_of_birth else ""
                result['place_of_death'] = place_of_death if place_of_death else ""
                result['gender'] = gender if gender else ""
                result['involved_in_sports'] = sports if sports else ""
                result['involved_in_politics'] = politics if politics else ""
                result['involved_in_military'] = military if military else ""
                result['involved_in_music'] = music if music else ""
                
                self.success_count += 1
                return result
                
            except Exception as e:
                error_msg = str(e).lower()
                
                if "rate limit" in error_msg or "too many requests" in error_msg:
                    if attempt < self.rate_limiter.max_retries - 1:
                        backoff_time = self.rate_limiter.exponential_backoff(attempt)
                        print(f"   Rate limit hit for {name}. Retrying in {backoff_time:.1f}s... (attempt {attempt + 1})")
                        time.sleep(backoff_time)
                        continue
                    else:
                        print(f"   Max retries reached for {name} due to rate limits")
                        self.error_count += 1
                        break
                else:
                    print(f"   Error processing {name}: {e}")
                    self.error_count += 1
                    break
        
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
                # Create enhanced row with all new columns
                enhanced_row = row.copy()
                enhanced_row['summary'] = ""
                enhanced_row['education'] = ""
                enhanced_row['dob'] = ""
                enhanced_row['dod'] = ""
                enhanced_row['place_of_birth'] = ""
                enhanced_row['place_of_death'] = ""
                enhanced_row['gender'] = ""
                enhanced_row['involved_in_sports'] = ""
                enhanced_row['involved_in_politics'] = ""
                enhanced_row['involved_in_military'] = ""
                enhanced_row['involved_in_music'] = ""
                
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
                    
                    # Extract biographical data with retry logic
                    bio_data = self.process_person_with_retry(wikipedia_url, name)
                    
                    # Add all biographical data to enhanced row
                    enhanced_row['summary'] = bio_data['summary']
                    enhanced_row['education'] = bio_data['education']
                    enhanced_row['dob'] = bio_data['dob']
                    enhanced_row['dod'] = bio_data['dod']
                    enhanced_row['place_of_birth'] = bio_data['place_of_birth']
                    enhanced_row['place_of_death'] = bio_data['place_of_death']
                    enhanced_row['gender'] = bio_data['gender']
                    enhanced_row['involved_in_sports'] = bio_data['involved_in_sports']
                    enhanced_row['involved_in_politics'] = bio_data['involved_in_politics']
                    enhanced_row['involved_in_military'] = bio_data['involved_in_military']
                    enhanced_row['involved_in_music'] = bio_data['involved_in_music']
                    
                    self.processed_count += 1
                    
                    # Progress update
                    if self.processed_count % 10 == 0:
                        print(f"   Progress: {self.processed_count} processed, {self.success_count} successful, {self.error_count} errors")
                    elif self.processed_count % 5 == 0:
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
                                'summary', 'education', 'dob', 'dod', 'place_of_birth', 'place_of_death',
                                'gender', 'involved_in_sports', 'involved_in_politics', 'involved_in_military', 
                                'involved_in_music']
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
    input_file = "memorial_enhanced_retry_full.csv"
    
    print("Memorial Data Enhancer - Full Processing")
    print("=" * 60)
    print("This script will add comprehensive biographical columns to the memorial CSV:")
    print("- Summary, DOB, DOD, Education, Place of Birth/Death")
    print("- Gender, Sports/Politics/Military/Music involvement")
    print("All data extracted using AI summarization with rate limiting.\n")
    
    print("Features:")
    print("✅ Rate limit detection and handling")
    print("✅ Exponential backoff retry logic")
    print("✅ Detailed progress tracking")
    print("✅ Graceful error recovery\n")
    
    # Ask user for processing mode
    choice = input("Run demo (first 5 entries) or full processing? (demo/full): ").lower().strip()
    
    if choice == 'demo':
        output_file = "memorial_enhanced_new_fields_demo.csv"
        max_entries = 5
        print("Running demo with first 5 Wikipedia entries...")
    else:
        output_file = "memorial_enhanced_new_fields_full.csv"
        max_entries = None
        print("Running FULL processing with all new biographical fields...")
        print("⚠️  This will process all ~600 entries and may take 2-3 hours")
        print("⚠️  Rate limiting will be applied to respect API limits")
        confirm = input("Continue with full processing? (y/n): ").lower().strip()
        if confirm != 'y':
            print("Cancelled.")
            return
    
    enhancer = MemorialDataEnhancer()
    enhancer.enhance_csv(input_file, output_file, max_entries)


if __name__ == "__main__":
    main()
