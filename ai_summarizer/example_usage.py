#!/usr/bin/env python3
"""
Example usage of the Person Summarizer script.
"""

import asyncio
from person_summarizer import summarize_person_from_url, PersonSummarizer


async def example_single_function():
    """Example using the convenience function."""
    url = "https://en.wikipedia.org/wiki/Albert_Einstein"
    
    try:
        summary = await summarize_person_from_url(url)
        print(f"Summary for {url}:")
        print(summary)
    except Exception as e:
        print(f"Error: {e}")


async def example_class_usage():
    """Example using the PersonSummarizer class directly."""
    # Initialize with API key (or use environment variable MistralAPIKey)
    summarizer = PersonSummarizer()  # Will use MistralAPIKey env var
    
    urls = [
        "https://en.wikipedia.org/wiki/Marie_Curie",
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
    asyncio.run(example_single_function())
    print("\n" + "=" * 40)
    asyncio.run(example_class_usage())
