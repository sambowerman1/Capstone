import csv
import re

def extract_person_name(highway_name):
    """
    Extract person names from memorial highway names.
    Returns None if no person name is found.
    """
    # Remove common suffixes that aren't part of the name
    suffixes_to_remove = [
        r'\s+Memorial Highway$',
        r'\s+Memorial Bridge$',
        r'\s+Memorial Overpass$',
        r'\s+Memorial Highway$',
        r'\s+Memorial Rest Area$',
        r'\s+Memorial Scenic Highway$',
        r'\s+Heritage Highway$',
        r'\s+Heritage Route$',
        r'\s+Trail$',
        r'\s+Highway$',
        r'\s+Bridge$',
        r'\s+Expressway$',
        r'\s+Byway$',
        r'\s+Corridor$',
        r'\s+Auto Route$',
    ]
    
    # Apply suffix removal
    cleaned_name = highway_name
    for suffix in suffixes_to_remove:
        cleaned_name = re.sub(suffix, '', cleaned_name, flags=re.IGNORECASE)
    
    # List of patterns that indicate it's NOT a person name
    non_person_patterns = [
        r'^Missouri Breaks',
        r'^Garnet',
        r'^Big Sky',
        r'^Canamex',
        r'Official Home',
        r'Veterans Bridge$',
        r'Purple Heart',
        r'Old Forts',
        r'Montana Highway',
        r'Warrior',
        r'Prisoner Of War',
        r'Historic Trail'
    ]
    
    # Check if it matches non-person patterns (except Tom Matsko)
    for pattern in non_person_patterns:
        if re.search(pattern, cleaned_name, flags=re.IGNORECASE):
            if 'Tom Matsko' not in cleaned_name:
                return None
    
    # Handle military ranks and titles
    rank_pattern = r'(?:PFC|Officer|Chief|Senator|Ensign|Captain|Lieutenant|Major|Colonel|General|Highway Patrol)\s+'
    cleaned_name = re.sub(rank_pattern, '', cleaned_name, flags=re.IGNORECASE)
    
    # Remove organizational prefixes
    cleaned_name = re.sub(r'^LCSO\s+\d+-\d+\s+', '', cleaned_name)
    cleaned_name = re.sub(r'\s+US Army$', '', cleaned_name)
    
    # Clean up extra whitespace
    cleaned_name = ' '.join(cleaned_name.split())
    
    # If what remains looks like a person's name (has at least 2 words, starts with capital)
    words = cleaned_name.strip().split()
    if len(words) >= 2 and cleaned_name[0].isupper():
        return cleaned_name.strip()
    
    return None

def main():
    input_file = 'C:/Users/lucas/Data_Science_Capstone/montana/montana_memorial_highways.csv'
    output_file = 'cleaned_names_montana.txt'
    
    person_names = []
    
    # Read the CSV file
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            highway_name = row['Name']
            person_name = extract_person_name(highway_name)
            if person_name:
                person_names.append(person_name)
    
    # Write to output file
    with open(output_file, 'w', encoding='utf-8') as f:
        for name in person_names:
            f.write(name + '\n')
    
    print(f"Extracted {len(person_names)} person names")
    print(f"Output written to: {output_file}")

if __name__ == "__main__":
    main()