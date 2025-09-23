#!/usr/bin/env python3
"""
Example usage of the Person Summarizer script.
"""

import asyncio
from person_summarizer import summarize_person_from_url, PersonSummarizer, Person


async def example_single_function():
    """Example using the convenience function."""
    url = "https://en.wikipedia.org/wiki/Albert_Einstein"
    
    try:
        summary = await summarize_person_from_url(url)
        print(f"Summary for {url}:")
        print(summary)
    except Exception as e:
        print(f"Error: {e}")


async def example_person_object():
    """Example using the new Person class - RECOMMENDED APPROACH"""
    urls = [
        "https://en.wikipedia.org/wiki/Marie_Curie",
        "https://en.wikipedia.org/wiki/Albert_Einstein"
    ]
    
    for url in urls:
        try:
            # Create Person object with URL
            person = Person(url)  # Will use MistralAPIKey env var
            print(f"Created: {person}")
            
            # Generate summary
            summary = await person.summarize()
            print(f"\nSummary for {url}:")
            print(summary)
            print("-" * 50)
            
            # Show that summary is cached
            cached = person.get_cached_summary()
            print(f"Cached summary available: {cached is not None}")
            
        except Exception as e:
            print(f"Error processing {url}: {e}")


async def example_class_usage():
    """Example using the PersonSummarizer class directly."""
    # Initialize with API key (or use environment variable MistralAPIKey)
    summarizer = PersonSummarizer()  # Will use MistralAPIKey env var
    
    urls = [
        "https://www.findagrave.com/memorial/1465/albert-einstein"
    ]
    
    for url in urls:
        try:
            summary = await summarizer.summarize_person(url)
            print(f"\nSummary for {url}:")
            print(summary)
            print("-" * 50)
        except Exception as e:
            print(f"Error processing {url}: {e}")


if __name__ == "__main__":
    print("Person Summarizer Examples")
    print("=" * 40)
    
    # Run the examples
    print("1. Using convenience function:")
    asyncio.run(example_single_function())
    
    print("\n" + "=" * 40)
    print("2. Using Person object (RECOMMENDED):")
    asyncio.run(example_person_object())
    
    print("\n" + "=" * 40)
    print("3. Using PersonSummarizer class directly:")
    asyncio.run(example_class_usage())
