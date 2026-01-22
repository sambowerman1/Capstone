import pandas as pd
import re

# Loading merged dataset
df = pd.read_csv("california_memorial_highways.csv")

# Function to clean names
def clean_person_name(text):
    if pd.isna(text):
        return None

    text = text.lower()

    # Remove common non-person words
    remove_words = [
        "memorial", "highway", "freeway", "corridor", "route",
        "veterans", "veteran", "heroes", "hero", "fallen",
        "honor", "honoring"
    ]

    # Remove titles / ranks
    titles = [
        "officer", "sgt", "sergeant", "captain", "capt", "lt",
        "lieutenant", "deputy", "trooper", "pvt", "cpl", "major",
        "chief"
    ]

    for w in remove_words + titles:
        text = re.sub(rf"\b{w}\b", "", text)

    # Remove punctuation
    text = re.sub(r"[^\w\s]", "", text)

    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Capitalize properly
    text = text.title()

    # Reject group names (no space → likely not a person)
    if text.count(" ") < 1:
        return None

    return text


# -----------------------------
# Apply cleaning
# -----------------------------
df["clean_name"] = df["highway_name"].apply(clean_person_name)

# Drop nulls and duplicates
names = (
    df["clean_name"]
    .dropna()
    .drop_duplicates()
    .sort_values()
)

# -----------------------------
# Save cleaned names
# -----------------------------
names.to_csv("cleaned_names.txt", index=False, header=False)

print(f"Generated {len(names)} cleaned individual names.")