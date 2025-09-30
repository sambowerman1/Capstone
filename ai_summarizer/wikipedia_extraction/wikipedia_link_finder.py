#!/usr/bin/env python3
"""
Wikipedia Link Finder for Memorial Roadway Designations

This script extracts person names from the DESIGNATIO column of Memorial_Roadway_Designations.csv
and searches for corresponding Wikipedia links, outputting results to a new CSV file.
"""

import csv
import re
import requests
import time
from urllib.parse import quote
from typing import List, Dict, Optional, Tuple


class WikipediaLinkFinder:
    """A class to find Wikipedia links for person names from memorial designations."""
    
    def __init__(self):
        """Initialize the Wikipedia link finder."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Memorial-Roadway-Wikipedia-Finder/1.0 (Educational Research)'
        })
        self.delay_between_requests = 1.0  # Be respectful to Wikipedia's servers
    
    def extract_person_names(self, designation: str) -> List[str]:
        """
        Extract person names from memorial designation strings.
        
        Args:
            designation: The designation string from the CSV
            
        Returns:
            List of extracted person names
        """
        names = []
        
        # Skip generic memorial types that don't contain person names
        generic_terms = [
            'Veterans Memorial', 'Gold Star Family Memorial', 'Submarine Veterans Memorial',
            'Hope and Healing Highway', 'County Veterans Memorial'
        ]
        
        if any(term in designation for term in generic_terms):
            return names
        
        # Try multiple patterns in order of specificity
        
        # Pattern 1: Multiple names - "Deputies Name1 and Name2"
        pattern_deputies = r'Deputies\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\s+and\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'
        match_deputies = re.search(pattern_deputies, designation)
        if match_deputies:
            name1 = self._clean_name(match_deputies.group(1).strip())
            name2 = self._clean_name(match_deputies.group(2).strip())
            if name1:
                names.append(name1)
            if name2:
                names.append(name2)
            return names
        
        # Pattern 2: "Name, Jr./Sr. Memorial"
        pattern_jr = r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+),\s+(?:Jr\.?|Sr\.?)'
        match_jr = re.search(pattern_jr, designation)
        if match_jr:
            name = self._clean_name(match_jr.group(1).strip())
            if name:
                names.append(name)
                return names
        
        # Pattern 3: "dedicated to ... Name"
        pattern_dedicated = r'dedicated to (?:U\.S\.\s+Army\s+)?(?:CPL|Sergeant|Lieutenant|Captain|Major|Colonel|General)\s+([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+)'
        match_dedicated = re.search(pattern_dedicated, designation)
        if match_dedicated:
            name = self._clean_name(match_dedicated.group(1).strip())
            if name:
                names.append(name)
                return names
        
        # Pattern 4: General approach - extract everything before road type keywords
        road_keywords = ['Highway', 'Boulevard', 'Bridge', 'Drive', 'Way', 'Street']
        
        for keyword in road_keywords:
            if keyword in designation:
                # Split on the keyword and take the first part
                parts = designation.split(keyword)[0].strip()
                
                # Remove common prefixes
                prefixes_to_remove = [
                    'Staff Sergeant', 'Sergeant', 'Specialist', 'Sheriff', 'Deputy',
                    'Dr.', 'Dr', 'Rev. Dr.', 'Rev Dr', 'Rev.', 'Rev', 'CPL', 'Captain',
                    'Lieutenant', 'Colonel', 'Major', 'General'
                ]
                
                clean_parts = parts
                for prefix in prefixes_to_remove:
                    if clean_parts.startswith(prefix + ' '):
                        clean_parts = clean_parts[len(prefix):].strip()
                        break
                
                # Remove "Memorial" if it's at the end
                if clean_parts.endswith(' Memorial'):
                    clean_parts = clean_parts[:-9].strip()
                
                # Check if what remains looks like a person name
                if self._looks_like_person_name(clean_parts):
                    name = self._clean_name(clean_parts)
                    if name:
                        names.append(name)
                        break
        
        return names
    
    def _looks_like_person_name(self, text: str) -> bool:
        """
        Check if text looks like a person name.
        
        Args:
            text: Text to check
            
        Returns:
            True if it looks like a person name
        """
        if not text or len(text) < 3:
            return False
        
        # Should have at least first and last name
        words = text.split()
        if len(words) < 2:
            return False
        
        # Should not contain numbers
        if re.search(r'\d', text):
            return False
        
        # Should not be generic terms
        generic_terms = ['County', 'Veterans', 'Memorial', 'Family', 'Star', 'Hope', 'Healing']
        if any(term in text for term in generic_terms):
            return False
        
        # Each word should start with capital letter
        for word in words:
            if word and not word[0].isupper():
                return False
        
        return True
    
    def _clean_name(self, name: str) -> str:
        """
        Clean extracted names by removing titles, ranks, and middle initials.
        
        Args:
            name: Raw extracted name
            
        Returns:
            Cleaned name suitable for Wikipedia search
        """
        # Remove common titles and ranks
        titles_to_remove = [
            r'^Dr\.?\s+', r'^Rev\.?\s+Dr\.?\s+', r'^Governor\s+', r'^Sheriff\s+',
            r'^Deputy\s+', r'^Captain\s+', r'^Lieutenant\s+', r'^Colonel\s+',
            r'^Major\s+', r'^General\s+', r'^Staff\s+Sergeant\s+', r'^Sergeant\s+',
            r'^Specialist\s+', r'^CPL\s+', r'^U\.S\.\s+Army\s+'
        ]
        
        for title_pattern in titles_to_remove:
            name = re.sub(title_pattern, '', name, flags=re.IGNORECASE)
        
        # Clean up trailing commas and punctuation
        name = re.sub(r'[,\.]+$', '', name)
        
        # Clean up extra spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Skip if name is too short or contains numbers
        if len(name) < 3 or re.search(r'\d', name):
            return ""
        
        # Skip common non-person names
        non_person_names = ['County', 'Veterans', 'Memorial', 'Highway', 'Bridge', 'Boulevard']
        if any(word in name for word in non_person_names):
            return ""
        
        return name
    
    def search_wikipedia(self, name: str) -> Optional[str]:
        """
        Search for a Wikipedia page for the given name.
        
        Args:
            name: Person name to search for
            
        Returns:
            Wikipedia URL if found, None otherwise
        """
        try:
            # First, try Wikipedia's search API
            search_url = "https://en.wikipedia.org/api/rest_v1/page/title/{}"
            encoded_name = quote(name.replace(' ', '_'))
            
            # Try direct page lookup first
            response = self.session.get(search_url.format(encoded_name))
            if response.status_code == 200:
                return f"https://en.wikipedia.org/wiki/{encoded_name}"
            
            # If direct lookup fails, try search API
            search_api_url = "https://en.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': name,
                'srlimit': 5
            }
            
            time.sleep(self.delay_between_requests)
            response = self.session.get(search_api_url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if 'query' in data and 'search' in data['query']:
                    results = data['query']['search']
                    
                    # Look for exact or close matches
                    for result in results:
                        title = result['title']
                        # Check if this looks like a person page
                        if self._is_likely_person_page(title, name):
                            encoded_title = quote(title.replace(' ', '_'))
                            return f"https://en.wikipedia.org/wiki/{encoded_title}"
            
            return None
            
        except Exception as e:
            print(f"Error searching for {name}: {e}")
            return None
    
    def _is_likely_person_page(self, title: str, original_name: str) -> bool:
        """
        Determine if a Wikipedia page title likely refers to a person.
        
        Args:
            title: Wikipedia page title
            original_name: Original name we searched for
            
        Returns:
            True if likely a person page, False otherwise
        """
        # Skip disambiguation pages
        if '(disambiguation)' in title.lower():
            return False
        
        # Skip pages that are clearly not about people
        non_person_indicators = [
            'highway', 'road', 'bridge', 'building', 'school', 'hospital',
            'company', 'organization', 'band', 'album', 'song', 'movie',
            'book', 'tv series', 'county', 'city', 'state'
        ]
        
        title_lower = title.lower()
        if any(indicator in title_lower for indicator in non_person_indicators):
            return False
        
        # Check if the names match reasonably well
        original_words = set(original_name.lower().split())
        title_words = set(title.lower().split())
        
        # At least half the words should match
        if len(original_words & title_words) >= len(original_words) * 0.5:
            return True
        
        return False
    
    def process_csv(self, input_file: str, output_file: str) -> None:
        """
        Process the memorial designations CSV and create output with Wikipedia links.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
        """
        results = []
        processed_names = set()  # Avoid duplicate searches
        
        print(f"Reading {input_file}...")
        
        try:
            # Read the CSV file using standard library
            with open(input_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
            
            print(f"Found {len(rows)} memorial designations")
            print("Extracting names and searching Wikipedia...")
            
            for row in rows:
                designation = str(row.get('DESIGNATIO', ''))
                county = str(row.get('COUNTY', ''))
                
                if not designation or designation == 'nan':
                    continue
                
                # Extract names from this designation
                names = self.extract_person_names(designation)
                
                for name in names:
                    if name in processed_names:
                        continue  # Skip if we've already processed this name
                    
                    processed_names.add(name)
                    
                    print(f"Searching for: {name}")
                    wikipedia_url = self.search_wikipedia(name)
                    
                    result = {
                        'name': name,
                        'original_designation': designation,
                        'county': county,
                        'wikipedia_url': wikipedia_url if wikipedia_url else 'Not Found',
                        'status': 'Found' if wikipedia_url else 'Not Found'
                    }
                    
                    results.append(result)
                    
                    # Progress indicator
                    if len(results) % 10 == 0:
                        print(f"Processed {len(results)} names...")
            
            # Write output CSV using standard library
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                if results:
                    fieldnames = ['name', 'original_designation', 'county', 'wikipedia_url', 'status']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(results)
            
            # Print summary
            found_count = len([r for r in results if r['status'] == 'Found'])
            total_count = len(results)
            
            print(f"\n{'='*60}")
            print("PROCESSING COMPLETE")
            print(f"{'='*60}")
            print(f"Total unique names extracted: {total_count}")
            print(f"Wikipedia pages found: {found_count}")
            print(f"Not found: {total_count - found_count}")
            print(f"Success rate: {found_count/total_count*100:.1f}%" if total_count > 0 else "No names processed")
            print(f"Results saved to: {output_file}")
            
        except Exception as e:
            print(f"Error processing CSV: {e}")
            raise


def main():
    """Main function to run the Wikipedia link finder."""
    input_file = "Memorial_Roadway_Designations.csv"
    output_file = "memorial_names_with_wikipedia_links.csv"
    
    print("Memorial Roadway Wikipedia Link Finder")
    print("=" * 50)
    
    finder = WikipediaLinkFinder()
    finder.process_csv(input_file, output_file)


if __name__ == "__main__":
    main()
