import pandas as pd
import re

# -----------------------------
# Load dataset
# -----------------------------
df = pd.read_csv("california_memorial_highways.csv")

# -----------------------------
# Extract person name
# -----------------------------
def extract_person_name(text):
    if pd.isna(text):
        return None

    t = text.lower()

    # Road-related words (strip, do NOT reject)
    road_words = [
        "freeway", "highway", "parkway", "expressway",
        "corridor", "bypass", "route", "memorial"
    ]

    # Titles / roles (strip, keep person)
    titles = [
        "officer", "officers", "detective", "detectives",
        "sheriff", "deputy", "fire", "firefighter",
        "captain", "sergeant", "colonel", "chief",
        "corporal", "specialist", "lieutenant",
        "army", "navy", "marine", "ranger",
        "police", "cdf", "chp", "calfire", "cal",
        "senator", "mayor", "president", "congressman",
        "doctor", "dr", "agent", "pilot", "staff",
        "first", "lady", "probation", "correctional"
    ]

    for w in road_words + titles:
        t = re.sub(rf"\b{w}\b", "", t)

    # Remove filler words
    t = re.sub(r"\b(and|of|the)\b", "", t)

    # Remove punctuation
    t = re.sub(r"[^\w\s]", "", t)

    # Normalize whitespace
    t = re.sub(r"\s+", " ", t).strip()

    # Title case
    t = t.title()

    # -----------------------------
    # Reject non-person entries
    # -----------------------------
    reject_if_contains = [
        # groups / concepts
        "Armed Forces", "Veterans", "Heroes", "Firefighters",
        "Airmen", "Soldiers", "Servicewomen",
        "Medal", "Honor", "Friendship",
        "Generation", "Republic", "Semper",
        "Military Intelligence",

        # geography / places
        "City", "County", "Valley", "River", "Beach",
        "Bay", "Coast", "Lake", "Mountain",
        "Mesa", "Flat", "State", "Prison",
        "Mission", "Delta", "Canyon", "Palms",
        "Yosemite",

        # landmarks / infrastructure
        "Road", "Street", "Boulevard", "Trail", "Path",
        "Bridge", "Turnpike", "Byway", "Extension",

        # regions
        "San ", "Santa ", "Los ", "West ", "East ",
        "North ", "South ",

        # military units / objects
        "Battalion", "Regimental", "Combat Team",
        "Wwii", "Submarine",

        # non-human
        "K9"
    ]

    for bad in reject_if_contains:
        if bad in t:
            return None

    parts = t.split()

    # Must look like a human name
    if len(parts) < 2 or len(parts) > 4:
        return None

    # First token must not be a place/concept
    invalid_first = [
        "California", "United", "Golden", "Historic",
        "Gateway", "Mother", "Trinity", "Pearl",
        "Arroyo", "Avenue"
    ]

    if parts[0] in invalid_first:
        return None

    return t


# -----------------------------
# Split multiple names
# -----------------------------
def split_and_extract(text):
    if pd.isna(text):
        return []
    parts = re.split(r"\band\b|,", text, flags=re.IGNORECASE)
    cleaned = [extract_person_name(p) for p in parts]
    return [c for c in cleaned if c]


# -----------------------------
# Apply pipeline
# -----------------------------
df["person_name"] = df["highway_name"].apply(split_and_extract)
df = df.explode("person_name")

names = (
    df["person_name"]
    .dropna()
    .drop_duplicates()
    .sort_values()
)

# -----------------------------
# FINAL manual exclusions (last 5%)
# -----------------------------
final_exclude = {
    "Armed Forces",
    "Blue Star",
    "Eastwest Blue Star",
    "Northsouth Blue Star",
    "Screaming Eagles",
    "Military Intelligence Service",
    "El Camino Real",
    "El Segundo",
    "Garden Grove",
    "Hansen Way",
    "Arroyo Seco",
    "Skyway Unconstructed",
    "Chiura Obata Great Nature",
    "Ohlone Costanoan Esselen",
    "Ohlone Kallentaruk",
    "Southern California Native American",
    "Viet Dzung Human Rights"
}

names = names[~names.isin(final_exclude)]

# -----------------------------
# Save output
# -----------------------------
names.to_csv("cleaned_names.txt", index=False, header=False)

print(f"Extracted {len(names)} people highways are named after.")