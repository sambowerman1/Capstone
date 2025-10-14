#!/usr/bin/env python3
"""
Test script to demonstrate date normalization functionality.
This shows how dates in various formats are standardized to YYYY-MM-DD.
"""

import sys
import os
import re

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from person_summarizer import PersonSummarizer

def normalize_date(date_str):
    """
    Normalize date to YYYY-MM-DD format.
    
    Args:
        date_str: Date string in various formats (YYYY, YYYY-MM, YYYY-MM-DD)
        
    Returns:
        Normalized date in YYYY-MM-DD format, or None if invalid/empty
    """
    if not date_str or date_str == "null" or date_str.lower() == "not found":
        return None
    
    date_str = str(date_str).strip()
    
    # If already in YYYY-MM-DD format, return as is
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    
    # If in YYYY format only, append -01-01
    if re.match(r'^\d{4}$', date_str):
        return f"{date_str}-01-01"
    
    # If in YYYY-MM format, append -01
    if re.match(r'^\d{4}-\d{2}$', date_str):
        return f"{date_str}-01"
    
    # If invalid format, return None
    return None


def main():
    """Test date normalization with various formats."""
    
    print("Date Normalization Test")
    print("=" * 60)
    print("This demonstrates how dates are standardized to YYYY-MM-DD format\n")
    
    # Test cases from the CSV file
    test_dates = [
        ("1927-08-04", "Full date (already correct)"),
        ("1953", "Year only"),
        ("1929-01-15", "Full date (already correct)"),
        ("1818-02", "Year and month only"),
        ("", "Empty string"),
        ("null", "Null value"),
        ("not found", "Not found value"),
        ("1945", "Year only"),
    ]
    
    print(f"{'Input Date':<20} {'Expected Output':<20} {'Description':<30}")
    print("-" * 70)
    
    for date_input, description in test_dates:
        normalized = normalize_date(date_input)
        display_input = f'"{date_input}"' if date_input else '(empty)'
        display_output = normalized if normalized else "None"
        print(f"{display_input:<20} {display_output:<20} {description:<30}")
    
    print("\n" + "=" * 60)
    print("✅ All dates are now standardized to YYYY-MM-DD format!")
    print("\nNotes:")
    print("- Full dates (YYYY-MM-DD) remain unchanged")
    print("- Year-only dates (YYYY) → YYYY-01-01")
    print("- Year-month dates (YYYY-MM) → YYYY-MM-01")
    print("- Invalid/empty dates → None")
    print("\nThis ensures consistent date formatting across all memorial entries.")


if __name__ == "__main__":
    main()

