import pandas as pd
import re

# -----------------------------
# Load Indiana dataset
# -----------------------------
df = pd.read_excel("indiana_memorial_highways.xlsx")

# Remove quotation marks
df["MEMORIAL HIGHWAY"] = df["MEMORIAL HIGHWAY"].str.replace('"', '', regex=False)

# -----------------------------
# Extract person name
# -----------------------------
def extract_person_name(text):
    if pd.isna(text):
        return None

    t = text.lower()

    # Remove interstate / state route identifiers
    t = re.sub(r"\bi[-\s]?\d+\b", "", t)
    t = re.sub(r"\bsr\s?\d+\b", "", t)

    # Strip road words
    road_words = [
        "freeway", "highway", "parkway", "expressway",
        "corridor", "bypass", "route", "memorial",
        "way", "bridge", "drive", "trail"
    ]

    # Strip titles/ranks
    titles = [
        "governor", "representative", "senator",
        "col", "colonel", "major",
        "patrolman", "trooper", "hon",
        "reserve", "officer", "captain",
        "chief", "lieutenant", "sheriff"
    ]

    for w in road_words + titles:
        t = re.sub(rf"\b{w}\b", "", t)

    # Remove filler words
    t = re.sub(r"\b(and|of|the)\b", "", t)

    # Remove punctuation
    t = re.sub(r"[^\w\s]", "", t)

    # Normalize whitespace
    t = re.sub(r"\s+", " ", t).strip()

    t = t.title()

    # Reject non-person keywords
    reject_words = [
        "Workers", "Purple Heart", "Iron Brigade",
        "Grand Army", "Operation", "Freedom",
        "Vice Presidents", "Medal Honor",
        "National Defense", "Scenic",
        "Uss", "Construction", "Veterans",
        "Airmen", "Forces"
    ]

    for bad in reject_words:
        if bad in t:
            return None

    parts = t.split()

    if len(parts) < 2 or len(parts) > 4:
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
df["person_name"] = df["MEMORIAL HIGHWAY"].apply(split_and_extract)
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
names.to_csv("indiana_cleaned_names.txt", index=False, header=False)

print(f"\nExtracted {len(names)} individuals highways are named after in Indiana.\n")
print(names)