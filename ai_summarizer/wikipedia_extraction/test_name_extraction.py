#!/usr/bin/env python3
"""
Test script for name extraction from memorial designations.
"""

from wikipedia_link_finder import WikipediaLinkFinder

def test_name_extraction():
    """Test the name extraction functionality with sample designations."""
    
    finder = WikipediaLinkFinder()
    
    # Test cases from the actual CSV data
    test_cases = [
        "Staff Sergeant Michael A. Bock Memorial Highway",
        "Sergeant Jess Thomas Memorial Highway", 
        "Jim Tullis Memorial Boulevard",
        "Allan Bense Highway",
        "Dr. Martin Luther King Jr., Memorial Highway",
        "Dr. Von Mizell Drive",
        "Robert L. Clark Memorial Highway",
        "Sheriff Charles Simeon Dean Highway",
        "James Harold Thompson Highway",
        "CK Steele Memorial Highway",
        "Deputies Tony Forgione and Bill Myers Memorial Bridge",
        "John B. Arnold, Jr., Memorial Highway",
        "Gold Star Family Memorial Bridge, dedicated to U.S. Army CPL Frank R. Gross",
        "Gulf County Veterans Memorial Highway",  # Should extract no names
        "Submarine Veterans Memorial Highway",    # Should extract no names
    ]
    
    print("Testing Name Extraction")
    print("=" * 60)
    
    for designation in test_cases:
        names = finder.extract_person_names(designation)
        print(f"Designation: {designation}")
        print(f"Extracted names: {names}")
        print("-" * 60)

if __name__ == "__main__":
    test_name_extraction()
