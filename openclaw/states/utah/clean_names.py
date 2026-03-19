import pandas as pd
import re

# -----------------------------
# Load Utah dataset
# -----------------------------
df = pd.read_csv("utah_memorial_highways.csv")

df = df[df["Name"].notna()]

# -----------------------------
# Function to classify
# -----------------------------
def clean_name(text):
    if pd.isna(text):
        return None

    original = text.strip()

    # Remove highway words temporarily to check if it's a person
    temp = re.sub(
        r"(Highway|Memorial|Bridge|Trail|Loop|Boulevard|Area|Legacy|Designated As)",
        "",
        original,
        flags=re.IGNORECASE
    )

    temp = re.sub(r"\bI-\d+\b", "", temp)  # remove route numbers
    temp = re.sub(r"\(.*?\)", "", temp)   # remove things in brackets
    temp = temp.strip()

    # Words that indicate NOT a person
    reject_words = [
        "Veterans", "Division", "Republic",
        "Parks", "Fruitway", "Prehistoric",
        "Heritage", "Trail", "Minuteman",
        "Code Talker", "Purple Heart",
        "National", "Loop", "Boulevard"
    ]

    for word in reject_words:
        if word.lower() in original.lower():
            return original.replace("Highway", "").strip() + " Memorial Highway"

    # If looks like a person (2–5 words)
    if 2 <= len(temp.split()) <= 5:
        return temp.strip()

    return original + " Memorial Highway"


# -----------------------------
# Apply
# -----------------------------
df["cleaned"] = df["Name"].apply(clean_name)

# Save result
df["cleaned"].to_csv("utah_cleaned_names.txt", index=False, header=False)

print(df["cleaned"])