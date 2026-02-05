import csv
from difflib import SequenceMatcher

# --- CONFIGURATION ---
NAMES_FILE = "Texas/Texas_Names.csv"
CSV_FILE = "C:/Users/lucas/Data_Science_Capstone/Capstone/odmp_scraping/odmp_florida_officers.csv"
OUTPUT_FILE = "matched_officers.csv"
NAME_COLUMN_INDEX = 2  # 0-based index for the name column in CSV
FUZZY_THRESHOLD = 0.85  # how close names must be to be considered a match (0–1)
# ----------------------

def normalize_name(name):
    """Simplify and normalize names for better comparison."""
    return ''.join(ch for ch in name.lower() if ch.isalpha() or ch.isspace()).strip()

def similarity(a, b):
    """Return similarity ratio between two strings."""
    return SequenceMatcher(None, a, b).ratio()

def best_match(target, names):
    """Return the best matching name and its similarity score."""
    best_name, best_score = None, 0
    for n in names:
        score = similarity(target, n)
        if n in target or target in n:  # strong partial match
            score = max(score, 1.0)
        if score > best_score:
            best_name, best_score = n, score
    return best_name, best_score

# --- Load and filter reference names ---
ref_names = []
with open(NAMES_FILE, "r", encoding="utf-8") as f:
    for line in f:
        original = line.strip()
        if not original:
            continue
        if len(original.split()) < 2:  # skip single-word names
            continue
        ref_names.append((original, normalize_name(original)))

# --- Process CSV ---
with open(CSV_FILE, "r", encoding="utf-8", newline='') as infile, \
     open(OUTPUT_FILE, "w", encoding="utf-8", newline='') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    # Read and modify header
    header = next(reader, None)
    if header:
        writer.writerow(["Matched Name"] + header)

    for row in reader:
        if len(row) <= NAME_COLUMN_INDEX:
            continue  # skip malformed rows

        officer_name_raw = row[NAME_COLUMN_INDEX]
        officer_name = normalize_name(officer_name_raw)

        best_match_name, best_score = None, 0
        for original, normalized in ref_names:
            score = similarity(officer_name, normalized)
            if normalized in officer_name or officer_name in normalized:
                score = max(score, 1.0)
            if score > best_score:
                best_match_name, best_score = original, score

        if best_match_name and best_score >= FUZZY_THRESHOLD:
            writer.writerow([best_match_name] + row)

print(f"✅ Matching complete. Filtered rows written to: {OUTPUT_FILE}")