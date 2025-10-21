import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re
from difflib import SequenceMatcher

# Config
base_dir = os.path.dirname(os.path.abspath(__file__))
names_path = os.path.join(base_dir, "..", "names.txt")

# Reading honoree names
with open(names_path, "r", encoding="utf-8") as f:
    target_names = [line.strip() for line in f if line.strip()]

def normalize(text):
    return re.sub(r"[^a-z0-9]", "", text.lower())

normalized_targets = {normalize(n): n for n in target_names}

print(f"📋 Loaded {len(target_names)} names from {names_path}")
print("🧾 Sample names:", target_names[:8])

# Pages to check (all wars + main FL page)
base_urls = [
    "https://www.honorstates.org/states/FL/",
    "https://www.honorstates.org/index.php?do=q&state=FL&war=WW1",
    "https://www.honorstates.org/index.php?do=q&state=FL&war=WW2",
    "https://www.honorstates.org/index.php?do=q&state=FL&war=Korea",
    "https://www.honorstates.org/index.php?do=q&state=FL&war=Vietnam",
]

headers = {"User-Agent": "Mozilla/5.0"}
FUZZY_THRESHOLD = 0.9
found_profiles = []

def similar(a, b): return SequenceMatcher(None, a, b).ratio()

# Scraper loop
for base_url in base_urls:
    war_name = base_url.split("war=")[-1] if "war=" in base_url else "General"
    print(f"\n🌎 Searching {war_name} pages...")

    page = 1
    empty_count = 0
    while True:
        url = f"{base_url}&p={page}" if "?" in base_url else f"{base_url}?p={page}"
        print(f"→ Fetching page {page}: {url}")

        try:
            res = requests.get(url, headers=headers, timeout=15)
        except Exception as e:
            print(f"⚠️ Network error: {e}")
            break

        if res.status_code != 200:
            break

        soup = BeautifulSoup(res.text, "html.parser")
        links = soup.select("a[href*='/profiles/']")
        if not links:
            empty_count += 1
            if empty_count >= 2:
                print(f"✅ Reached end of {war_name} pages.")
                break
            page += 1
            continue
        else:
            empty_count = 0

        page_names = [" ".join(a.stripped_strings) for a in links if "profiles" in a.get("href", "")]
        print(f"   Found {len(page_names)} names. Example:", page_names[:4])

        for a in links:
            name = " ".join(a.stripped_strings)
            if not name:
                continue
            norm_name = normalize(name)

            best_match = None
            best_score = 0
            for tn_norm, tn_orig in normalized_targets.items():
                score = similar(norm_name, tn_norm)
                if score > best_score:
                    best_match, best_score = tn_orig, score

            if best_score >= FUZZY_THRESHOLD:
                href = a.get("href")
                full_link = href if href.startswith("http") else "https://www.honorstates.org" + href
                print(f"      ✅ {name} ≈ {best_match} ({best_score:.2f})")

                # Fetching profile details
                try:
                    prof_res = requests.get(full_link, headers=headers, timeout=15)
                    if prof_res.status_code != 200:
                        continue
                    psoup = BeautifulSoup(prof_res.text, "html.parser")

                    data = {
                        "Page": war_name,
                        "Profile Link": full_link,
                        "Matched Name": name,
                        "From File": best_match,
                        "Similarity": round(best_score, 2)
                    }
                    for row in psoup.select("table tr"):
                        cols = row.find_all("td")
                        if len(cols) == 2:
                            key = cols[0].get_text(strip=True)
                            val = cols[1].get_text(strip=True)
                            data[key] = val

                    found_profiles.append(data)
                    time.sleep(0.8)
                except Exception as e:
                    print(f"         ⚠️ Error reading profile: {e}")

        page += 1
        time.sleep(0.5)

# Saving
out_path = os.path.join(base_dir, "honorstates_allwars_results.csv")
if found_profiles:
    pd.DataFrame(found_profiles).to_csv(out_path, index=False)
    print(f"\n✅ Saved {len(found_profiles)} matches to {out_path}")
else:
    print("\n❌ No matches found in any war pages.")
