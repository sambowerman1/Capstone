import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

NAMES_PATH = os.path.join(BASE_DIR, "cleaned_names.txt")
OUTPUT_CSV = os.path.join(BASE_DIR, "findagrave_ca_results.csv")

BASE_SEARCH_URL = "https://www.findagrave.com/memorial/search"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_DELAY = 3.0  # be respectful

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

# ---------------------------------------------------------------------
# LOAD NAMES
# ---------------------------------------------------------------------

with open(NAMES_PATH, "r", encoding="utf-8") as f:
    names = [line.strip() for line in f if line.strip()]

print(f"📋 Loaded {len(names)} California names")

# ---------------------------------------------------------------------
# SCRAPER
# ---------------------------------------------------------------------

session = requests.Session()
session.headers.update(HEADERS)

results = []

for idx, name in enumerate(names, start=1):
    print(f"\n🔎 ({idx}/{len(names)}) Searching Find a Grave: {name}")

    params = {
        "firstname": "",
        "lastname": "",
        "fullname": name,
        "location": "California",
    }

    try:
        res = session.get(BASE_SEARCH_URL, params=params, timeout=20)
    except Exception as e:
        print(f"⚠️ Request failed: {e}")
        continue

    if res.status_code != 200:
        print("⚠️ Non-200 response")
        continue

    soup = BeautifulSoup(res.text, "html.parser")

    # First result only (best match)
    card = soup.select_one("a.memorial-item")

    if not card:
        print("❌ No memorial found")
        time.sleep(REQUEST_DELAY)
        continue

    memorial_url = "https://www.findagrave.com" + card.get("href", "")

    print(f"   ✅ Found memorial: {memorial_url}")

    # Visit memorial page
    try:
        mem_res = session.get(memorial_url, timeout=20)
        if mem_res.status_code != 200:
            time.sleep(REQUEST_DELAY)
            continue
    except Exception as e:
        print(f"⚠️ Memorial page error: {e}")
        continue

    msoup = BeautifulSoup(mem_res.text, "html.parser")

    def grab(selector):
        el = msoup.select_one(selector)
        return clean_text(el.get_text()) if el else ""

    record = {
        "Queried_Name": name,
        "Memorial_Name": grab("h1.memorial-name"),
        "Birth_Date": grab("span[itemprop='birthDate']"),
        "Death_Date": grab("span[itemprop='deathDate']"),
        "Cemetery": grab("span[itemprop='name']"),
        "Location": grab("span[itemprop='address']"),
        "Military_Service": grab(".military-service"),
        "FindAGrave_URL": memorial_url,
    }

    results.append(record)

    time.sleep(REQUEST_DELAY)

# ---------------------------------------------------------------------
# SAVE
# ---------------------------------------------------------------------

if results:
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Saved {len(df)} Find a Grave records → {OUTPUT_CSV}")
else:
    print("\n❌ No Find a Grave matches found.")

print("\n🎯 Find a Grave scraping complete.")