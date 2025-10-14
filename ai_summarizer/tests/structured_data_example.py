#!/usr/bin/env python3
"""
Example demonstrating the structured data extraction features.

This example shows how to use the enhanced Person class to extract:
- Summary (4-sentence biographical summary)
- Education information
- Date of birth (DOB)
- Date of death (DOD)
- Place of birth
- Place of death
- Gender
- Involvement in sports (yes/no)
- Involvement in politics (yes/no)
- Involvement in military (yes/no)
- Involvement in music (yes/no)
"""

from person_summarizer import Person


def main():
    """Demonstrate the new structured data extraction features."""
    
    # Example with Albert Einstein's Wikipedia page
    url = "https://en.wikipedia.org/wiki/Albert_Einstein"
    
    try:
        print("Creating Person object...")
        person = Person(url)
        print(f"Created: {person}")
        
        print("\n" + "="*60)
        print("EXTRACTING STRUCTURED DATA")
        print("="*60)
        
        # Get summary (this will trigger data extraction for all fields)
        print("\n1. Getting summary...")
        summary = person.summarize_sync()
        print(f"Summary: {summary}")
        
        # Get education information
        print("\n2. Getting education information...")
        education = person.getEducation()
        if education:
            print(f"Education: {education}")
            for i, edu in enumerate(education, 1):
                print(f"   {i}. {edu}")
        else:
            print("Education: Not available")
        
        # Get date of birth
        print("\n3. Getting date of birth...")
        dob = person.getDOB()
        if dob:
            print(f"Date of Birth: {dob}")
        else:
            print("Date of Birth: Not available")
        
        # Get date of death
        print("\n4. Getting date of death...")
        dod = person.getDOD()
        if dod:
            print(f"Date of Death: {dod}")
        else:
            print("Date of Death: Not available (person may be alive)")
        
        # Get new biographical fields
        print("\n5. Getting place of birth...")
        place_of_birth = person.getPlaceOfBirth()
        print(f"Place of Birth: {place_of_birth}")
        
        print("\n6. Getting place of death...")
        place_of_death = person.getPlaceOfDeath()
        print(f"Place of Death: {place_of_death}")
        
        print("\n7. Getting gender...")
        gender = person.getGender()
        print(f"Gender: {gender}")
        
        print("\n8. Getting involvement fields...")
        sports = person.getInvolvedInSports()
        politics = person.getInvolvedInPolitics()
        military = person.getInvolvedInMilitary()
        music = person.getInvolvedInMusic()
        print(f"Involved in Sports: {sports}")
        print(f"Involved in Politics: {politics}")
        print(f"Involved in Military: {military}")
        print(f"Involved in Music: {music}")
        
        print(f"\n9. Person object status: {person}")
        
        print("\n" + "="*60)
        print("DEMONSTRATING CACHING")
        print("="*60)
        
        # Show that subsequent calls use cached data (no API calls)
        print("\nCalling methods again (should use cached data)...")
        print(f"Education (cached): {person.getEducation()}")
        print(f"DOB (cached): {person.getDOB()}")
        print(f"DOD (cached): {person.getDOD()}")
        print(f"Place of Birth (cached): {person.getPlaceOfBirth()}")
        print(f"Gender (cached): {person.getGender()}")
        print(f"Sports (cached): {person.getInvolvedInSports()}")
        
        # Clear cache and show status change
        print("\nClearing cache...")
        person.clear_cache()
        print(f"Person object after cache clear: {person}")
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure you have:")
        print("1. Set the MistralAPIKey environment variable")
        print("2. Installed required dependencies (pip install -r requirements.txt)")
        print("3. A working internet connection")


def demo_multiple_people():
    """Demonstrate with multiple people to show different data scenarios."""
    
    urls = [
        "https://en.wikipedia.org/wiki/Marie_Curie",
        "https://en.wikipedia.org/wiki/Stephen_Hawking",
        "https://en.wikipedia.org/wiki/Albert_Einstein"
    ]
    
    print("\n" + "="*60)
    print("COMPARING MULTIPLE PEOPLE")
    print("="*60)
    
    for i, url in enumerate(urls, 1):
        try:
            print(f"\n{i}. Processing: {url.split('/')[-1].replace('_', ' ')}")
            print("-" * 40)
            
            person = Person(url)
            
            # Extract all data
            summary = person.summarize_sync()
            education = person.getEducation()
            dob = person.getDOB()
            dod = person.getDOD()
            place_of_birth = person.getPlaceOfBirth()
            gender = person.getGender()
            sports = person.getInvolvedInSports()
            politics = person.getInvolvedInPolitics()
            
            print(f"Summary: {summary[:100]}...")
            print(f"Education: {education if education else 'Not available'}")
            print(f"DOB: {dob if dob else 'Not available'}")
            print(f"DOD: {dod if dod else 'Not available/Still alive'}")
            print(f"Place of Birth: {place_of_birth}")
            print(f"Gender: {gender}")
            print(f"Sports: {sports} | Politics: {politics}")
            
        except Exception as e:
            print(f"Error processing {url}: {e}")


if __name__ == "__main__":
    print("Structured Data Extraction Example")
    print("=" * 60)
    print("This example demonstrates the Person class methods:")
    print("- person.getSummary() - Returns 4-sentence biographical summary")
    print("- person.getEducation() - Returns list of education info")
    print("- person.getDOB() - Returns date of birth")
    print("- person.getDOD() - Returns date of death")
    print("- person.getPlaceOfBirth() - Returns place of birth")
    print("- person.getPlaceOfDeath() - Returns place of death")
    print("- person.getGender() - Returns gender")
    print("- person.getInvolvedInSports() - Returns yes/no for sports involvement")
    print("- person.getInvolvedInPolitics() - Returns yes/no for politics involvement")
    print("- person.getInvolvedInMilitary() - Returns yes/no for military involvement")
    print("- person.getInvolvedInMusic() - Returns yes/no for music involvement")
    print("=" * 60)
    
    # Run main example
    main()
    
    # Uncomment the line below to run the multi-person comparison
    # demo_multiple_people()
