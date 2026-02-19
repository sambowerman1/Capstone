import pandas as pd
import re

# -----------------------------
# Load cleaned Michigan names
# -----------------------------
df = pd.read_csv(
    "michigan_cleaned_highway_names.txt",
    header=None,
    names=["highway_name"]
)

# -----------------------------
# Extract person-like names
# -----------------------------
def extract_people(text):
    if pd.isna(text):
        return []

    t = str(text)

    # Normalize quotes
    t = t.replace("’", "'").replace("“", '"').replace("”", '"')

    # Remove road words but KEEP people
    road_words = [
        "Memorial Highway", "Memorial Freeway", "Freeway",
        "Highway", "Parkway", "Bridge", "Bypass", "Trail",
        "Route", "Drive", "Road", "Way"
    ]

    for w in road_words:
        t = re.sub(w, "", t, flags=re.IGNORECASE)

    # Split multiple people
    parts = re.split(r"\band\b|,", t, flags=re.IGNORECASE)

    people = []
    for p in parts:
        p = p.strip()

        # Remove ranks/titles
        p = re.sub(
            r"\b(Officer|Trooper|Deputy|Sheriff|Sergeant|Sgt\.?|Captain|Cpl\.?|Corporal|PFC|SPC|Private|Major|Chief|Staff Sergeant|Lieutenant|Lt\.?|Colonel|Dr\.?)\b",
            "",
            p,
            flags=re.IGNORECASE
        )

        # Remove extra spaces
        p = re.sub(r"\s+", " ", p).strip()

        # Must look like a real name
        words = p.split()
        if 2 <= len(words) <= 5:
            # Reject obvious group names
            bad_keywords = [
                "Veterans", "Airmen", "Legion", "Battalion",
                "Division", "Brigade", "War", "WWII",
                "Medal", "Honor", "Service", "UAW"
            ]
            if not any(b.lower() in p.lower() for b in bad_keywords):
                people.append(p)

    return people

# -----------------------------
# Apply extraction
# -----------------------------
df["people"] = df["highway_name"].apply(extract_people)
df = df.explode("people")

people = (
    df["people"]
    .dropna()
    .drop_duplicates()
    .sort_values()
)

# -----------------------------
# Save PEOPLE ONLY
# -----------------------------
people.to_csv(
    "michigan_people_names.txt",
    index=False,
    header=False
)

print(f"Extracted {len(people)} individual people names.")