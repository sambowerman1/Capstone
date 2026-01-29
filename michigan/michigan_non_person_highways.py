import pandas as pd
import re

# -----------------------------
# Load dataset
# -----------------------------
df = pd.read_csv("michigan_memorial_highways.csv")

NAME_COL = "MemorialHighwayName"

# -----------------------------
# Basic normalization ONLY
# -----------------------------
def normalize(text):
    if pd.isna(text):
        return None
    t = str(text)
    t = t.replace("’", "'").replace("“", '"').replace("”", '"')
    t = re.sub(r"\s+", " ", t).strip()
    return t

df["clean_name"] = df[NAME_COL].apply(normalize)

# -----------------------------
# Non-person indicators
# (NO deletion, only classification)
# -----------------------------
NON_PERSON_KEYWORDS = [
    "Veterans", "Veteran", "Legion", "Airmen",
    "Division", "Battalion", "Brigade",
    "World War", "WWII", "Korean War", "Vietnam",
    "Purple Heart", "Medal of Honor",
    "Freeway", "Highway", "Parkway", "Bridge",
    "Trail", "Byway", "Route",
    "Memorial Highway", "Memorial Freeway"
]

def looks_non_person(name):
    if not name:
        return False
    return any(k.lower() in name.lower() for k in NON_PERSON_KEYWORDS)

non_person = df[df["clean_name"].apply(looks_non_person)]

# -----------------------------
# Save output
# -----------------------------
non_person[["clean_name"]].drop_duplicates().to_csv(
    "michigan_non_person_highways.txt",
    index=False,
    header=False
)

print(f"Saved {non_person.shape[0]} non-person Michigan memorial highways.")