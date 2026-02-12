import pandas as pd
import re

# -----------------------------
# Load dataset
# -----------------------------
df = pd.read_csv("california_memorial_highways.csv")

# -----------------------------
# Person extractor (same logic)
# -----------------------------
def extract_person_name(text):
    if pd.isna(text):
        return None

    t = text.lower()

    road_words = [
        "freeway", "highway", "parkway", "expressway",
        "corridor", "bypass", "route", "memorial"
    ]

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

    t = re.sub(r"\b(and|of|the)\b", "", t)
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = t.title()

    reject_if_contains = [
        "Veterans", "Heroes", "Firefighters", "Airmen",
        "Soldiers", "Servicewomen", "Medal", "Honor",
        "Friendship", "Generation", "Republic", "Semper",
        "Military Intelligence",
        "City", "County", "Valley", "River", "Beach",
        "Bay", "Coast", "Lake", "Mountain", "Canyon",
        "Mesa", "Flat", "State", "Prison", "Mission",
        "Delta", "Yosemite", "Palms",
        "Road", "Street", "Boulevard", "Trail", "Path",
        "Bridge", "Turnpike", "Byway", "Extension",
        "San ", "Santa ", "Los ", "West ", "East ",
        "North ", "South ",
        "Battalion", "Regimental", "Combat Team",
        "Wwii", "Submarine", "K9"
    ]

    for bad in reject_if_contains:
        if bad in t:
            return None

    parts = t.split()
    if len(parts) < 2 or len(parts) > 4:
        return None

    invalid_first = [
        "California", "United", "Golden", "Historic",
        "Gateway", "Mother", "Trinity", "Pearl",
        "Arroyo", "Avenue"
    ]

    if parts[0] in invalid_first:
        return None

    return t


# -----------------------------
# Identify non-person highways
# -----------------------------
def is_person_named_highway(text):
    return extract_person_name(text) is not None


df["is_person_named"] = df["highway_name"].apply(is_person_named_highway)

# Keep ONLY non-person highways
non_person_highways = df.loc[~df["is_person_named"], "highway_name"]

# Drop duplicates & sort
non_person_highways = (
    non_person_highways
    .dropna()
    .drop_duplicates()
    .sort_values()
)

# -----------------------------
# Save output
# -----------------------------
non_person_highways.to_csv(
    "non_person_named_highways.txt",
    index=False,
    header=False
)

print(f"Extracted {len(non_person_highways)} non-person highways.")