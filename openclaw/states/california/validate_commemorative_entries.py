import pandas as pd
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INPUT_FILE = ROOT / "california_memorial_highways.csv"
OUTPUT_FILE = ROOT / "california_commemorative_highways_filtered.csv"
FLAGGED_FILE = ROOT / "california_commemorative_highways_flagged.csv"
LOG_DIR = ROOT.parents[1] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

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


def main():
    df = pd.read_csv(INPUT_FILE)

    df["commemorative_keyword"] = df["highway_name"].apply(has_commemorative_keyword)
    df["person_named"] = df["highway_name"].apply(is_person_named)
    df["keep"] = df["commemorative_keyword"] | df["person_named"]

    kept = df[df["keep"]].copy()
    flagged = df[~df["keep"]].copy()
    flagged["flag_reason"] = "no commemorative keyword or person-name heuristic match"

    kept.to_csv(OUTPUT_FILE, index=False)
    flagged.to_csv(FLAGGED_FILE, index=False)

    log_file = LOG_DIR / f"openclaw_agent_{datetime.now().strftime('%Y-%m-%d_%H%M')}.log"
    with log_file.open("w", encoding="utf-8") as handle:
        handle.write("OpenClaw memorial highways task log\n")
        handle.write(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        handle.write("State: California (validation filter)\n\n")
        handle.write(f"Total records: {len(df)}\n")
        handle.write(f"Kept (commemorative/person): {len(kept)}\n")
        handle.write(f"Flagged (review needed): {len(flagged)}\n\n")
        if not flagged.empty:
            handle.write("Sample flagged entries:\n")
            for name in flagged["highway_name"].head(25).tolist():
                handle.write(f"- {name}\n")

    print(f"Wrote {len(kept)} kept records to {OUTPUT_FILE}")
    print(f"Wrote {len(flagged)} flagged records to {FLAGGED_FILE}")
    print(f"Log written to {log_file}")


if __name__ == "__main__":
    main()
