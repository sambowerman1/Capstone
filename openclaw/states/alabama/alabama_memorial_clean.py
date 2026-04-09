from pathlib import Path
import pandas as pd
import re


STATE_DIR = Path(__file__).resolve().parent
INPUT_FILE = STATE_DIR / "alabama_memorial_highways_from_shapefile.csv"
CLEAN_OUTPUT = STATE_DIR / "alabama_memorial_highways_clean.csv"
PEOPLE_OUTPUT = STATE_DIR / "alabama_memorial_people.csv"


def clean_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def clean_memorial_name(name: str) -> str:
    if not isinstance(name, str):
        return ""

    text = name.strip()

    remove_patterns = [
        r"\bhighway\b", r"\bhwy\b", r"\broute\b",
        r"\broad\b", r"\brd\b", r"\bstreet\b", r"\bst\b",
        r"\bavenue\b", r"\bave\b", r"\bboulevard\b", r"\bblvd\b",
        r"\bdrive\b", r"\bdr\b", r"\blane\b", r"\bln\b",
        r"\bparkway\b", r"\bpkwy\b", r"\bway\b",
        r"\bcircle\b", r"\bcir\b", r"\bcourt\b", r"\bct\b",
        r"\bbridge\b", r"\bexpressway\b", r"\bfreeway\b",
        r"\btrail\b", r"\bgreenway\b", r"\blevee\b",
        r"\baccess road\b",
    ]

    for pattern in remove_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    text = re.sub(r"[^\w\s\.\-']", " ", text)
    return normalize_spaces(text)


def extract_person_like_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return ""

    cleaned = clean_memorial_name(name)

    remove_words = [
        "memorial", "veterans", "veteran", "heroes", "hero", "fallen",
        "patriot", "patriots", "honor", "tribute", "freedom", "remembrance",
        "purple heart", "medal of honor", "gold star",
        "army", "navy", "marine", "marines", "air force", "coast guard",
        "police", "firefighter", "trooper", "officer",
        "sheriff", "deputy", "chief"
    ]

    for word in remove_words:
        cleaned = re.sub(rf"\b{re.escape(word)}\b", "", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"\bI-\d+\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bUS-?\d+\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bAL-?\d+\b", "", cleaned, flags=re.IGNORECASE)

    cleaned = normalize_spaces(cleaned)

    if not cleaned:
        return ""

    words = cleaned.split()

    reject_words = {
        "north", "south", "east", "west", "old", "new",
        "lake", "river", "creek", "hill", "hills", "valley",
        "oak", "pine", "cedar", "maple", "dogwood", "magnolia",
        "spring", "springs", "forest", "grove", "ridge", "meadow",
        "cherokee", "choctaw", "airport", "industrial", "central",
        "county", "state", "union", "church", "college", "academy", "school",
        "park", "village", "place", "greenway", "harbor", "war", "korean",
        "alabama", "access", "northwest", "northeast", "southwest", "southeast",
    }

    if any(word.lower() in reject_words for word in words):
        return ""

    if len(words) < 2:
        return ""

    # reject initials like "B J"
    if all(len(word.replace(".", "")) <= 2 for word in words):
        return ""

    if 2 <= len(words) <= 5:
        return cleaned.title()

    return ""


def normalize_person_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return ""

    n = name.lower().strip()

    # MLK normalization
    if "martin luther king" in n or "mlk" in n:
        return "Martin Luther King Jr"

    # remove titles
    n = re.sub(r"\b(dr|doctor|rev|reverend|mr|mrs|ms)\b\.?", "", n)

    n = re.sub(r"[^\w\s\.]", " ", n)
    n = normalize_spaces(n)

    return n.title()


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    for col in df.columns:
        df[col] = df[col].apply(clean_text)

    if "name" not in df.columns:
        raise ValueError("Expected a 'name' column.")

    df["memorial_name_clean"] = df["name"].apply(clean_memorial_name)

    df["possible_person"] = df["name"].apply(extract_person_like_name)
    df["possible_person"] = df["possible_person"].apply(normalize_person_name)

    # remove empty after normalization
    df = df[df["possible_person"] != ""]

    # deduplicate full dataset
    dedupe_cols = [c for c in ["name", "route", "ref"] if c in df.columns]
    if dedupe_cols:
        df = df.drop_duplicates(subset=dedupe_cols)

    df.to_csv(CLEAN_OUTPUT, index=False)

    people_df = df.drop_duplicates(subset=["possible_person"])

    preferred_cols = [c for c in [
        "possible_person",
        "name",
        "memorial_name_clean",
        "route",
        "ref",
        "fclass",
        "memorial_status",
        "code",
        "osm_id",
        "oneway",
        "maxspeed",
        "layer",
        "bridge",
        "tunnel",
    ] if c in people_df.columns]

    other_cols = [c for c in people_df.columns if c not in preferred_cols]
    people_df = people_df[preferred_cols + other_cols]

    people_df.to_csv(PEOPLE_OUTPUT, index=False)

    print(f"Saved cleaned file: {CLEAN_OUTPUT}")
    print(f"Saved people file: {PEOPLE_OUTPUT}")
    print(f"Total rows after cleaning: {len(df)}")
    print(f"Unique people extracted: {len(people_df)}")


if __name__ == "__main__":
    main()