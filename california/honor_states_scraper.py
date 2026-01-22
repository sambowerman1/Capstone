import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re
from difflib import SequenceMatcher

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 🔹 Path to California cleaned names file
NAMES_PATH = os.path.join(BASE_DIR, "cleaned_names.txt")

# 🔹 Output file
OUTPUT_CSV = os.path.join(BASE_DIR, "honorstates_ca_allwars_results.csv")

# Honor States California pages (all wars)
BASE_URLS = [
    "https://www.honorstates.org/states/CA/",
    "https://www.honorstates.org/index.php?do=q&state=CA&war=WW1",
    "https://www.honorstates.org/index.php?do=q&state=CA&war=WW2",
    "https://www.honorstates.org/index.php?do=q&state=CA&war=Korea",
    "https://www.honorstates.org/index.php?do=q&state=CA&war=Vietnam",
]

HEADERS = {"User-Agent": "MemorialResearchBot/1.0"}
FUZZY_THRESHOLD = 0.90
REQUEST_DELAY = 0.8
PAGE_DELAY = 0.5

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def normalize(text):
    """Lowercase and remove all non-alphanumeric characters."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


# ---------------------------------------------------------------------
# LOAD NAMES
# ---------------------------------------------------------------------

if not os.path.exists(NAMES_PATH):
    raise FileNotFoundError(f"❌ Cleaned names file not found: {NAMES_PATH}")

with open(NAMES_PATH, "r", encoding="utf-8") as f:
    target_names = [line.strip() for line in f if line.strip()]

normalized_targets = {normalize(n): n for n in target_names}

print(f"📋 Loaded {len(target_names)} California names")
print("🧾 Sample names:", target_names[:8])

# ---------------------------------------------------------------------
# SCRAPER
# ---------------------------------------------------------------------

found_profiles = []

for base_url in BASE_URLS:
    war_name = base_url.split("war=")[-1] if "war=" in base_url else "General"
    print(f"\n🌎 Searching Honor States – {war_name}")

    page = 1
    empty_pages = 0

    while True:
        if "?" in base_url:
            url = f"{base_url}&p={page}"
        else:
            url = f"{base_url}?p={page}"

        print(f"→ Page {page}: {url}")

        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
        except Exception as e:
            print(f"⚠️ Request failed: {e}")
            break

        if res.status_code != 200:
            print("⚠️ Non-200 response, stopping.")
            break

        soup = BeautifulSoup(res.text, "html.parser")
        links = soup.select("a[href*='/profiles/']")

        if not links:
            empty_pages += 1
            if empty_pages >= 2:
                print(f"✅ End of {war_name} pages.")
                break
            page += 1
            continue
        else:
            empty_pages = 0

        for a in links:
            page_name = " ".join(a.stripped_strings)
            if not page_name:
                continue

            norm_page = normalize(page_name)

            best_match = None
            best_score = 0

            for tn_norm, tn_orig in normalized_targets.items():
                score = similarity(norm_page, tn_norm)
                if score > best_score:
                    best_score = score
                    best_match = tn_orig

            if best_score >= FUZZY_THRESHOLD:
                href = a.get("href")
                profile_url = (
                    href if href.startswith("http")
                    else "https://www.honorstates.org" + href
                )

                print(f"      ✅ Match: {page_name} ≈ {best_match} ({best_score:.2f})")

                try:
                    prof_res = requests.get(profile_url, headers=HEADERS, timeout=15)
                    if prof_res.status_code != 200:
                        continue

                    psoup = BeautifulSoup(prof_res.text, "html.parser")

                    data = {
                        "HonorStates_Page": war_name,
                        "Profile_URL": profile_url,
                        "Profile_Name": page_name,
                        "Matched_Name": best_match,
                        "Similarity": round(best_score, 2),
                    }

                    for row in psoup.select("table tr"):
                        cols = row.find_all("td")
                        if len(cols) == 2:
                            key = cols[0].get_text(strip=True)
                            val = cols[1].get_text(strip=True)
                            data[key] = val

                    found_profiles.append(data)
                    time.sleep(REQUEST_DELAY)

                except Exception as e:
                    print(f"         ⚠️ Profile scrape error: {e}")

        page += 1
        time.sleep(PAGE_DELAY)

# ---------------------------------------------------------------------
# SAVE RESULTS
# ---------------------------------------------------------------------

if found_profiles:
    df = pd.DataFrame(found_profiles)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Saved {len(df)} matches → {OUTPUT_CSV}")
else:
    print("\n❌ No Honor States matches found.")

print("\n🎯 Honor States California scraping complete.")