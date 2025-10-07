from person_summarizer import Person

# Create Person object
albert_einstein = Person("https://en.wikipedia.org/wiki/Albert_Einstein")

print("Extracting all biographical data in a single API call...")

# Use the new methods to get all data (single API call behind the scenes)
print("\n" + "="*60)
print("COMPLETE BIOGRAPHICAL DATA:")
print("="*60)

# Basic biographical information
summary = albert_einstein.getSummary()
print(f"Summary: {summary}")

# Dates and places
dob = albert_einstein.getDOB()
dod = albert_einstein.getDOD()
place_of_birth = albert_einstein.getPlaceOfBirth()
place_of_death = albert_einstein.getPlaceOfDeath()

print(f"\nDate of Birth: {dob}")
print(f"Date of Death: {dod}")
print(f"Place of Birth: {place_of_birth}")
print(f"Place of Death: {place_of_death}")

# Personal information
gender = albert_einstein.getGender()
education = albert_einstein.getEducation()

print(f"\nGender: {gender}")
print(f"Education: {education}")

# Involvement categories
sports = albert_einstein.getInvolvedInSports()
politics = albert_einstein.getInvolvedInPolitics()
military = albert_einstein.getInvolvedInMilitary()
music = albert_einstein.getInvolvedInMusic()

print(f"\nInvolvement Areas:")
print(f"  Sports: {sports}")
print(f"  Politics: {politics}")
print(f"  Military: {military}")
print(f"  Music: {music}")

print(f"\nPerson object: {albert_einstein}")

print("\n" + "="*60)
print("All data extracted with a single API call!")
print("Subsequent calls to any getter method will use cached data.")
print("="*60)

