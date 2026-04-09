import pandas as pd
import re

INPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_highways_final.csv"
OUTPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_people.csv"


# words to remove (titles, military ranks, suffixes, noise)
REMOVE_WORDS = [
    "MEMORIAL", "HIGHWAY", "TRAIL", "ROAD", "WAY", "PARKWAY",
    "BRIDGE", "EXPRESSWAY", "BOULEVARD", "ROUTE",
    "PFC", "SGT", "CPL", "LT", "CAPT", "MAJ", "COL",
    "GENERAL", "PRIVATE", "STAFF", "OFFICER",
    "DR", "JR", "SR", "III", "II"
]

REMOVE_PATTERN = re.compile(r"\b(" + "|".join(REMOVE_WORDS) + r")\b", re.IGNORECASE)


# words that indicate NOT a person
NON_PERSON_KEYWORDS = [
    "VETERANS", "PURPLE HEART", "GOLD STAR",
    "FALLEN", "HERO", "HEROES", "PATRIOT",
    "FREEDOM", "HONOR", "TRIBUTE", "MOUNTAIN",
    "DIVISION", "ARMY", "NAVY", "MARINES"
]


def is_person_name(text):
    text_upper = text.upper()
    return not any(k in text_upper for k in NON_PERSON_KEYWORDS)


def extract_name(text):
    if pd.isna(text):
        return ""

    # remove unwanted words
    text = REMOVE_PATTERN.sub("", text)

    # remove extra spaces
    text = re.sub(r"\s+", " ", text).strip()

    # keep only capitalized words (simple heuristic)
    words = text.split()

    # keep words that look like names (start with capital)
    words = [w for w in words if w[0].isupper()]

    return " ".join(words)


def main():
    df = pd.read_csv(INPUT_FILE)

    df["memorial_name"] = df["memorial_name"].astype(str)

    # filter only person-based memorials
    df["is_person"] = df["memorial_name"].apply(is_person_name)
    df_people = df[df["is_person"]].copy()

    # extract names
    df_people["person_name"] = df_people["memorial_name"].apply(extract_name)

    # remove empty or weak names
    df_people = df_people[df_people["person_name"].str.len() > 3]

    # keep only relevant columns
    result = df_people[[
        "person_name",
        "memorial_name",
        "route_clean",
        "COUNTY",
        "CITY"
    ]]

    result = result.drop_duplicates()

    print(result.head(20))
    print(f"\nTotal person-based memorials: {len(result)}")

    result.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()