#!/usr/bin/env python3
"""
Simple example showing the new Person object usage pattern.
"""

import asyncio
from person_summarizer import Person


async def main():
    """Demonstrate the simple Person object usage."""
    
    # Example 1: Create Person object with Wikipedia link
    wikipedia_url = "https://en.wikipedia.org/wiki/Albert_Einstein"
    
    try:
        # Create Person object by passing Wikipedia link
        person = Person(wikipedia_url)
        print(f"Created Person object: {person}")
        
        # Call summarize() to get and save the summary as a string
        summary = await person.summarize()
        print(f"\nSummary saved as string:")
        print(f"'{summary}'")
        
        # Show that it's cached now
        print(f"\nSummary is now cached: {person.get_cached_summary() is not None}")
        
        # You can access the summary again without making another API call
        cached_summary = person.get_cached_summary()
        print(f"Retrieved from cache: {cached_summary == summary}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "="*50)
    
    # Example 2: Create Person object with Find a Grave link
    findagrave_url = "https://www.findagrave.com/memorial/1465/albert-einstein"
    
    try:
        # Create Person object by passing Find a Grave link
        person2 = Person(findagrave_url)
        print(f"Created Person object: {person2}")
        
        # Call summarize() to get and save the summary as a string
        summary2 = await person2.summarize()
        print(f"\nSummary saved as string:")
        print(f"'{summary2}'")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    print("Simple Person Object Usage Example")
    print("=" * 50)
    print("Usage pattern:")
    print("1. person = Person(wikipedia_or_findagrave_url)")
    print("2. summary = await person.summarize()")
    print("3. The summary is saved as a string!")
    print("=" * 50)
    
    asyncio.run(main())
