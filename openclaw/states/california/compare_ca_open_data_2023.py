import re
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent
LOCAL_FILE = ROOT / "california_memorial_highways.csv"
OUTPUT_OPEN_DATA = ROOT / "california_open_data_2023_commemorative.csv"
OUTPUT_MISSING = ROOT / "california_open_data_2023_missing_commemorative.csv"
LOG_DIR = ROOT.parents[1] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

OPEN_DATA_URL = (
    "https://data.ca.gov/dataset/c8d6903b-d151-4b93-a2ad-bcb3fd0b4788/resource/"
    "efdf4627-f20e-42be-b70b-374c50dce024/download/"
    "2023_named-freeways-highways-and-other-routes.csv"
)

COMM_KEYWORDS = [
    "memorial",
    "veteran",
    "veterans",
    "honor",
    "honorary",
    "honoree",
    "remembrance",
    "fallen",
    "gold star",
    "purple heart",
    "blue star",
    "medal of honor",
    "medal of valor",
    "pow",
    "p.o.w",
    "mia",
    "missing in action",
    "vfw",
    "american legion",
    "armed forces",
    "military",
    "army",
    "navy",
    "marine",
    "air force",
    "coast guard",
    "national guard",
    "servicewomen",
    "servicemen",
    "firefighter",
    "firefighters",
    "fire chief",
    "fire captain",
    "police",
    "sheriff",
    "deputy",
    "trooper",
    "patrol",
    "officer",
    "detective",
    "sergeant",
    "lieutenant",
    "captain",
    "chief",
    "colonel",
    "major",
    "general",
    "admiral",
    "dr.",
    "doctor",
    "senator",
    "representative",
    "congressman",
    "president",
    "governor",
    "mayor",
    "judge",
    "agent",
    "pilot",
    "ranger",
    "chaplain",
    "border patrol",
    "k-9",
    "k9",
    "vietnam",
    "korean war",
    "world war",
    "wwii",
    "wwi",
    "pearl harbor",
    "tuskegee",
    "pow/mia",
]


def has_commemorative_keyword(text: str) -> bool:
    if pd.isna(text):
        return False
    t = str(text).lower()
    return any(k in t for k in COMM_KEYWORDS)


# Person-name heuristic adapted from clean_names.py

def extract_person_name(text):
    if pd.isna(text):
        return None

    t = str(text).lower()

    road_words = [
        "freeway",
        "highway",
        "parkway",
        "expressway",
        "corridor",
        "bypass",
        "route",
        "memorial",
    ]

    titles = [
        "officer",
        "officers",
        "detective",
        "detectives",
        "sheriff",
        "deputy",
        "fire",
        "firefighter",
        "captain",
        "sergeant",
        "colonel",
        "chief",
        "corporal",
        "specialist",
        "lieutenant",
        "army",
        "navy",
        "marine",
        "ranger",
        "police",
        "cdf",
        "chp",
        "calfire",
        "cal",
        "senator",
        "mayor",
        "president",
        "congressman",
        "doctor",
        "dr",
        "agent",
        "pilot",
        "staff",
        "first",
        "lady",
        "probation",
        "correctional",
    ]

    for w in road_words + titles:
        t = re.sub(rf"\b{w}\b", "", t)

    t = re.sub(r"\b(and|of|the)\b", "", t)
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = t.title()

    reject_if_contains = [
        "Veterans",
        "Heroes",
        "Firefighters",
        "Airmen",
        "Soldiers",
        "Servicewomen",
        "Medal",
        "Honor",
        "Friendship",
        "Generation",
        "Republic",
        "Semper",
        "Military Intelligence",
        "City",
        "County",
        "Valley",
        "River",
        "Beach",
        "Bay",
        "Coast",
        "Lake",
        "Mountain",
        "Canyon",
        "Mesa",
        "Flat",
        "State",
        "Prison",
        "Mission",
        "Delta",
        "Yosemite",
        "Palms",
        "Road",
        "Street",
        "Boulevard",
        "Trail",
        "Path",
        "Bridge",
        "Turnpike",
        "Byway",
        "Extension",
        "San ",
        "Santa ",
        "Los ",
        "West ",
        "East ",
        "North ",
        "South ",
        "Battalion",
        "Regimental",
        "Combat Team",
        "Wwii",
        "Submarine",
        "K9",
    ]

    for bad in reject_if_contains:
        if bad in t:
            return None

    parts = t.split()
    if len(parts) < 2 or len(parts) > 4:
        return None

    invalid_first = [
        "California",
        "United",
        "Golden",
        "Historic",
        "Gateway",
        "Mother",
        "Trinity",
        "Pearl",
        "Arroyo",
        "Avenue",
    ]

    if parts[0] in invalid_first:
        return None

    return t


def is_person_named(text: str) -> bool:
    return extract_person_name(text) is not None


def normalize_name(text: str) -> str:
    if pd.isna(text):
        return ""
    t = str(text).lower()
    t = t.replace("’", "'").replace("‘", "'")
    t = t.replace("“", '"').replace("”", '"')
    t = re.sub(r"[\"']", "", t)
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def fetch_open_data() -> pd.DataFrame:
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(OPEN_DATA_URL, headers=headers, timeout=30)
    response.raise_for_status()
    text = response.content.decode("utf-8-sig")
    df = pd.read_csv(StringIO(text))
    df = df.rename(
        columns={
            "ï»¿NAME": "highway_name",
            "NAME": "highway_name",
            "ROUTE NO": "route_no",
            "DISTRICT": "district",
            "HOW NAMED": "how_named",
            "FROM": "from_location",
            "TO": "to_location",
        }
    )
    return df


def main():
    if not LOCAL_FILE.exists():
        raise FileNotFoundError(f"Missing local file: {LOCAL_FILE}")

    local_df = pd.read_csv(LOCAL_FILE)
    open_df = fetch_open_data()

    open_df["commemorative_keyword"] = open_df["highway_name"].apply(has_commemorative_keyword)
    open_df["person_named"] = open_df["highway_name"].apply(is_person_named)
    open_df["keep"] = open_df["commemorative_keyword"] | open_df["person_named"]
    open_filtered = open_df[open_df["keep"]].copy()

    local_df["name_norm"] = local_df["highway_name"].apply(normalize_name)
    open_filtered["name_norm"] = open_filtered["highway_name"].apply(normalize_name)

    local_names = set(local_df["name_norm"].dropna().tolist())
    missing = open_filtered[~open_filtered["name_norm"].isin(local_names)].copy()

    open_filtered.to_csv(OUTPUT_OPEN_DATA, index=False)
    missing.to_csv(OUTPUT_MISSING, index=False)

    log_file = LOG_DIR / f"openclaw_agent_{datetime.now().strftime('%Y-%m-%d_%H%M')}.log"
    with log_file.open("w", encoding="utf-8") as handle:
        handle.write("OpenClaw memorial highways task log\n")
        handle.write(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        handle.write("State: California (Open Data 2023 compare)\n\n")
        handle.write(f"Local records: {len(local_df)}\n")
        handle.write(f"Open Data commemorative/person entries: {len(open_filtered)}\n")
        handle.write(f"Missing from local: {len(missing)}\n\n")
        if not missing.empty:
            handle.write("Missing sample:\n")
            for name in missing["highway_name"].head(25).tolist():
                handle.write(f"- {name}\n")

    print(f"Open Data commemorative entries saved to {OUTPUT_OPEN_DATA}")
    print(f"Missing entries saved to {OUTPUT_MISSING}")
    print(f"Log written to {log_file}")


if __name__ == "__main__":
    main()
