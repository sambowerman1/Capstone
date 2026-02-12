import os
import re
import pandas as pd
from PyPDF2 import PdfReader
from difflib import get_close_matches

# Configuration
CSV_PATH = "Memorial_Roadway_Designations.csv"
PDF_FOLDER = "bill_summaries"

# Helpers
def normalize_name(s: str) -> str:
    """Lowercase, remove punctuation and normalize spaces."""
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[\"'’.,\-]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()

def simplify_title(name):
    """Remove common words like 'memorial' and 'highway' for better fuzzy matching."""
    name = normalize_name(name)
    name = re.sub(r"\bmemorial\b", "", name)
    name = re.sub(r"\bhighway\b", "", name)
    return name.strip()

def extract_designation_paragraphs(pdf_path):
    """Extract both sentence and bullet style designation descriptions."""
    text = ""
    try:
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            txt = page.extract_text() or ""
            text += "\n" + txt
    except Exception as e:
        print(f"⚠️ Could not read {pdf_path}: {e}")
        return []

    text = re.sub(r"\s+", " ", text)

    # Capturing "The bill designates ..." style
    paras = re.findall(
        r"(The bill designates.*?(?:Highway|Bridge|Overpass|Road|Way|Boulevard|Street|Memorial).*?\.)",
        text,
        flags=re.IGNORECASE,
    )

    # 2️⃣ Capture bullet/line format ("I-275 between ... as 'XYZ Highway.'")
    bullets = re.findall(
        r"(?:•||\-)?\s*[A-Z0-9][^:]*?\b(?:Highway|Bridge|Overpass|Road|Way|Boulevard|Street|Memorial).*?\.",
        text,
        flags=re.IGNORECASE,
    )

    # Combine and clean
    all_paras = [re.sub(r"\s+", " ", p).strip() for p in (paras + bullets) if len(p) > 40]
    return list(set(all_paras))

# ---------------- Load main CSV ----------------
print(f"📄 Loading data from {CSV_PATH}")
df = pd.read_csv(CSV_PATH, dtype=str).fillna("")

# Add columns if missing
if "BILL_DESCRIPTION" not in df.columns:
    df["BILL_DESCRIPTION"] = ""
if "BILL_YEAR" not in df.columns:
    df["BILL_YEAR"] = ""

# ---------------- Extract from all PDFs ----------------
all_paras = []
year_map = {}

for file in os.listdir(PDF_FOLDER):
    if not file.lower().endswith(".pdf"):
        continue
    path = os.path.join(PDF_FOLDER, file)
    year = re.findall(r"(20\d{2})", file)
    year = year[0] if year else "Unknown"

    paras = extract_designation_paragraphs(path)
    print(f"📘 {file}: {len(paras)} designations found")

    for p in paras:
        all_paras.append(p)
        year_map[p] = year

print(f"📚 Total extracted: {len(all_paras)}")

# ---------------- Build description map ----------------
desc_map = {}
for para in all_paras:
    name_match = re.search(
        r"(?:as\s+['“\"]?)([A-Z][A-Za-z0-9'’\.\- ]{3,80}?(?:Highway|Bridge|Overpass|Road|Way|Boulevard|Street|Memorial))",
        para,
    )
    if name_match:
        raw = name_match.group(1).strip().replace("”", "").replace("“", "")
        key = normalize_name(raw)
        desc_map.setdefault(key, []).append(para)

print(f"🗺️ Unique designations extracted: {len(desc_map)}")

# ---------------- Match and update CSV ----------------
matches = 0
unmatched = []

for idx, row in df.iterrows():
    local = row.get("Local_Name", "")
    norm_local = simplify_title(local)
    if not norm_local:
        continue

    found = None
    # direct or simplified match
    for key in desc_map.keys():
        simp_key = simplify_title(key)
        if norm_local == simp_key or norm_local in simp_key or simp_key in norm_local:
            found = key
            break

    # fuzzy match with lower cutoff
    if not found:
        close = get_close_matches(norm_local, [simplify_title(k) for k in desc_map.keys()], n=1, cutoff=0.6)
        if close:
            match_key = None
            for k in desc_map.keys():
                if simplify_title(k) == close[0]:
                    match_key = k
                    break
            if match_key:
                found = match_key

    if found:
        combined_desc = " ".join(set(desc_map[found]))
        df.at[idx, "BILL_DESCRIPTION"] = combined_desc
        df.at[idx, "BILL_YEAR"] = year_map.get(desc_map[found][0], "")
        matches += 1
    else:
        unmatched.append(local)

print(f"\n🔗 Added descriptions for {matches} memorials out of {len(df)} rows")
print(f"❌ {len(unmatched)} memorials still unmatched")

if unmatched:
    print("\n🕵️‍♀️ Unmatched memorials (first 25):")
    for u in unmatched[:25]:
        print("  -", u)

# ---------------- Save in place ----------------
df.to_csv(CSV_PATH, index=False)
print(f"💾 Updated file saved in place: {CSV_PATH}")
