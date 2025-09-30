from person_summarizer import Person

# Create Person object
albert_einstein = Person("https://en.wikipedia.org/wiki/Albert_Einstein")

print("Extracting all biographical data in a single API call...")

# Use the new methods to get all data (single API call behind the scenes)
print("\n" + "="*50)
print("BIOGRAPHICAL DATA:")
print("="*50)

summary = albert_einstein.getSummary()
print(f"Summary: {summary}")

education = albert_einstein.getEducation()
print(f"Education: {education}")

dob = albert_einstein.getDOB()
print(f"Date of Birth: {dob}")

dod = albert_einstein.getDOD()
print(f"Date of Death: {dod}")

print(f"\nPerson object: {albert_einstein}")

