import pandas as pd
import re

# -----------------------------
# Load Connecticut dataset
# -----------------------------
df = pd.read_csv("connecticut_memorial_highways.csv")

# Remove quotation marks
df["LSTN_LEG_NAME"] = df["LSTN_LEG_NAME"].str.replace('"', '', regex=False)

# Remove null names
df = df[df["LSTN_LEG_NAME"].notna()]

# -----------------------------
# Extract person name
# -----------------------------
def extract_person_name(text):
    if pd.isna(text):
        return None

    t = text.lower()

    # Remove interstate/state route identifiers
    t = re.sub(r"\bi[-\s]?\d+\b", "", t)
    t = re.sub(r"\bsr\s?\d+\b", "", t)

    # Strip road words
    road_words = [
        "freeway", "highway", "parkway", "expressway",
        "corridor", "bypass", "route", "memorial",
        "way", "bridge", "drive", "trail",
        "interchange", "overpass"
    ]

    # Strip titles/ranks
    titles = [
        "governor", "representative", "senator",
        "col", "colonel", "major",
        "sgt", "sergeant",
        "patrolman", "trooper",
        "hon", "reserve",
        "officer", "captain",
        "chief", "lieutenant",
        "lt", "ensign"
    ]

    for w in road_words + titles:
        t = re.sub(rf"\b{w}\b", "", t)

    # Remove filler words
    t = re.sub(r"\b(and|of|the)\b", "", t)

    # Remove punctuation
    t = re.sub(r"[^\w\s]", "", t)

    # Normalize whitespace
    t = re.sub(r"\s+", " ", t).strip()

    # Convert to title case
    t = t.title()

    # Reject non-person keywords
    reject_words = [
        "Veterans", "Airmen", "Forces",
        "Division", "Regiment", "Battalion",
        "Medal Honor", "Purple Heart",
        "Scenic", "Freedom", "Blue Star",
        "Workers", "Construction",
        "Uss", "Operation"
    ]

    for bad in reject_words:
        if bad in t:
            return None

    parts = t.split()

    # Accept reasonable person name length
    if len(parts) < 2 or len(parts) > 5:
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
df["person_name"] = df["LSTN_LEG_NAME"].apply(split_and_extract)
df = df.explode("person_name")

names = (
    df["person_name"]
    .dropna()
    .drop_duplicates()
    .sort_values()
)

# -----------------------------
# Save output
# -----------------------------
names.to_csv("connecticut_cleaned_names.txt", index=False, header=False)

print(f"\nExtracted {len(names)} individuals highways are named after in Connecticut.\n")
print(names)