#!/usr/bin/env python3
"""
Test script to demonstrate Wikipedia search functionality with a few sample names.
"""

from wikipedia_link_finder import WikipediaLinkFinder

def test_wikipedia_search():
    """Test Wikipedia search with a few known names."""
    
    finder = WikipediaLinkFinder()
    
    # Test with some well-known names that should have Wikipedia pages
    test_names = [
        "Martin Luther King Jr",
        "Albert Einstein",  # Should find a page
        "John Smith",       # Common name, might find multiple
        "Michael A. Bock",  # Less likely to find
        "Allan Bense",      # Might find (former Florida politician)
    ]
    
    print("Testing Wikipedia Search")
    print("=" * 60)
    
    for name in test_names:
        print(f"Searching for: {name}")
        url = finder.search_wikipedia(name)
        print(f"Result: {url if url else 'Not Found'}")
        print("-" * 60)

if __name__ == "__main__":
    test_wikipedia_search()
