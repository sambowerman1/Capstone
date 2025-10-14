#!/usr/bin/env python3
"""
Enhanced Memorial Data with Rate Limit Handling

This script is an improved version that handles API rate limits gracefully
with exponential backoff and retry logic. It extracts comprehensive 
biographical data including all new fields.

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

from person_summarizer import Person
from person_summarizer import PersonSummarizer


class RateLimitHandler:
    """Handles API rate limits with exponential backoff."""
    
    def __init__(self, base_delay: float = 2.0, max_retries: int = 3):
        self.base_delay = base_delay
        self.max_retries = max_retries
        self.last_request_time = 0
    
    def wait_before_request(self):
        """Wait appropriate time before making API request."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.base_delay:
            sleep_time = self.base_delay - time_since_last
            print(f"   → Waiting {sleep_time:.1f}s to respect rate limits...")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def handle_rate_limit_error(self, attempt: int) -> bool:
        """
        Handle rate limit error with exponential backoff.
        
        Returns:
            True if should retry, False if max retries reached
        """
        if attempt >= self.max_retries:
            return False
        
        # Exponential backoff: 5s, 15s, 45s
        wait_time = 5 * (3 ** attempt) + random.uniform(0, 5)
        print(f"   → Rate limit hit! Waiting {wait_time:.1f}s before retry {attempt + 1}/{self.max_retries}...")
        time.sleep(wait_time)
        return True


class ImprovedMemorialEnhancer:
    """Enhanced memorial data processor with rate limit handling."""
    
    def __init__(self):
        self.rate_limiter = RateLimitHandler()
        self.processed_count = 0
        self.success_count = 0
        self.rate_limit_count = 0
        self.error_count = 0
        self.skipped_count = 0
    
    def process_person_with_retry(self, wikipedia_url: str, name: str) -> Dict[str, Optional[str]]:
        """
        Process a person with retry logic for rate limits and extract all biographical fields.
        
        Args:
            wikipedia_url: Wikipedia URL to process
            name: Person's name for logging
            
        Returns:
            Dictionary with all biographical data
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
        
        for attempt in range(self.rate_limiter.max_retries + 1):
            try:
                # Wait before making request
                self.rate_limiter.wait_before_request()
                
                # Create Person object and extract all biographical data
                person = Person(wikipedia_url)
                
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
                
                # Format all results
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
                error_msg = str(e)
                
                # Check if it's a rate limit error
                if "429" in error_msg or "rate" in error_msg.lower() or "capacity exceeded" in error_msg.lower():
                    self.rate_limit_count += 1
                    print(f"   → Rate limit error for {name}")
                    
                    if self.rate_limiter.handle_rate_limit_error(attempt):
                        continue  # Retry
                    else:
                        print(f"   → Max retries reached for {name}")
                        break
                else:
                    # Non-rate limit error
                    print(f"   → Error processing {name}: {error_msg}")
                    break
        
        # If we get here, all retries failed
        self.error_count += 1
        return result
    
    def enhance_csv_with_retry(self, input_file: str, output_file: str, max_entries: Optional[int] = None):
        """Enhanced CSV processing with rate limit handling."""
        
        print(f"Reading {input_file}...")
        
        # Read existing CSV
        with open(input_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
        
        print(f"Found {len(rows)} memorial entries")
        
        # Filter to entries with Wikipedia URLs
        entries_with_wikipedia = [row for row in rows if row.get('status') == 'Found' and row.get('wikipedia_url') != 'Not Found']
        print(f"Found {len(entries_with_wikipedia)} entries with Wikipedia links")
        
        if max_entries:
            entries_with_wikipedia = entries_with_wikipedia[:max_entries]
            print(f"Processing first {len(entries_with_wikipedia)} entries for demo")
        
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
                    enhanced_rows.append(enhanced_row)
                    continue
                
                wikipedia_url = row['wikipedia_url']
                name = row['name']
                
                print(f"{i}. Processing: {name}")
                
                # Process with retry logic
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
                if self.processed_count % 5 == 0:
                    print(f"   📊 Progress: {self.processed_count} processed, {self.success_count} successful, {self.rate_limit_count} rate limited")
            else:
                self.skipped_count += 1
            
            enhanced_rows.append(enhanced_row)
        
        # Write enhanced CSV (ensure final DOB/DOD normalization before write)
        print(f"\nWriting enhanced data to {output_file}...")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            if enhanced_rows:
                fieldnames = ['name', 'original_designation', 'county', 'wikipedia_url', 'status', 
                            'summary', 'education', 'dob', 'dod', 'place_of_birth', 'place_of_death',
                            'gender', 'involved_in_sports', 'involved_in_politics', 'involved_in_military', 
                            'involved_in_music']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                # Final normalization pass for dates to catch any remaining edge-cases
                normalizer = PersonSummarizer(MistralAPIKey=os.getenv('MistralAPIKey'))
                for row in enhanced_rows:
                    row['dob'] = normalizer._normalize_date(row.get('dob')) if row.get('dob') else None
                    row['dod'] = normalizer._normalize_date(row.get('dod')) if row.get('dod') else None
                writer.writerows(enhanced_rows)
        
        # Print final summary
        print(f"\n{'='*60}")
        print("ENHANCEMENT COMPLETE")
        print(f"{'='*60}")
        print(f"Total entries: {len(rows)}")
        print(f"Entries processed: {self.processed_count}")
        print(f"Successfully enhanced: {self.success_count}")
        print(f"Rate limit errors: {self.rate_limit_count}")
        print(f"Other errors: {self.error_count}")
        print(f"Skipped (no Wikipedia): {self.skipped_count}")
        if self.processed_count > 0:
            print(f"Success rate: {self.success_count/self.processed_count*100:.1f}%")
        print(f"Enhanced data saved to: {output_file}")


def main():
    """Main function with improved rate limit handling and all new biographical fields."""
    base_dir = os.path.dirname(__file__)
    input_file = os.path.join(base_dir, "memorial_enhanced_retry_full.csv")
    
    if not os.path.exists(input_file):
        print("\n[ERROR] Input CSV not found.")
        print(f"Expected at: {input_file}")
        print("Run this script from anywhere; paths are now resolved relative to the script.")
        print("If the file name differs, update it in this script or place it alongside the script.")
        return
    
    print("Improved Memorial Data Enhancer - With All New Fields")
    print("=" * 60)
    print("This script adds comprehensive biographical columns with robust error handling:")
    print("- Summary, DOB, DOD, Education, Place of Birth/Death")
    print("- Gender, Sports/Politics/Military/Music involvement")
    print("\nFeatures:")
    print("✅ Rate limit detection and handling")
    print("✅ Exponential backoff retry logic")
    print("✅ Detailed progress tracking")
    print("✅ Graceful error recovery")
    print("✅ All 11 new biographical fields\n")
    
    # Ask user for processing mode
    choice = input("Run demo (10 entries) or full processing? (demo/full): ").lower().strip()
    
    if choice == 'demo':
        output_file = os.path.join(base_dir, "memorial_enhanced_retry_new_fields_demo.csv")
        max_entries = 10
        print("Running demo with rate limit handling and all new fields...")
    else:
        output_file = os.path.join(base_dir, "memorial_enhanced_retry_new_fields_full.csv")
        max_entries = None
        print("Running full processing with rate limit handling and all new fields...")
        print("⚠️  This will process all ~306 Wikipedia entries with comprehensive data")
        print("⚠️  Rate limiting will be applied to respect API limits")
        confirm = input("Continue? (y/n): ").lower().strip()
        if confirm != 'y':
            print("Cancelled.")
            return
    
    enhancer = ImprovedMemorialEnhancer()
    enhancer.enhance_csv_with_retry(input_file, output_file, max_entries)


if __name__ == "__main__":
    main()
