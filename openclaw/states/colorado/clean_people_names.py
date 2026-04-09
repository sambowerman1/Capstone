import pandas as pd
import re

INPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_people.csv"
OUTPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_people_clean.csv"

BAD_PHRASES = [
    "veterans", "purple heart", "gold star", "fallen", "heroes", "hero",
    "freedom", "honor", "honour", "tribute", "patriot", "patriots",
    "mountain division", "army", "navy", "marines", "air force",
    "pow", "mia", "memorial", "highway", "trail", "route", "road", "bridge"
]

RANKS = [
    "PFC", "SPC", "CPL", "SGT", "SSG", "SFC", "MSG", "1SG",
    "LT", "CPT", "CAPT", "MAJ", "LTC", "COL", "GEN",
    "PRIVATE", "OFFICER", "TROOPER", "DEPUTY",
    "DR", "JR", "SR", "II", "III", "IV"
]

rank_pattern = re.compile(r"\b(" + "|".join(RANKS) + r")\b\.?", re.IGNORECASE)

def clean_name(name):
    if pd.isna(name):
        return ""

    name = str(name).strip()

    # remove rank/title words
    name = rank_pattern.sub("", name)

    # remove punctuation except hyphen/apostrophe
    name = re.sub(r"[^A-Za-z\s'\-]", " ", name)

    # normalize spaces
    name = re.sub(r"\s+", " ", name).strip()

    return name


def bad_name(name):
    if not name:
        return True

    lower = name.lower()

    # generic memorial phrase
    if any(phrase in lower for phrase in BAD_PHRASES):
        return True

    # too short
    if len(name) < 5:
        return True

    # only one word usually weak
    if len(name.split()) < 2:
        return True

    return False


def main():
    df = pd.read_csv(INPUT_FILE)

    df["person_name"] = df["person_name"].fillna("").apply(clean_name)

    df = df[~df["person_name"].apply(bad_name)].copy()

    df = df.drop_duplicates(subset=["person_name", "memorial_name", "route_clean"])

    df = df.sort_values(["person_name", "route_clean"])

    print(df.head(20))
    print(f"\nClean person rows: {len(df)}")

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()