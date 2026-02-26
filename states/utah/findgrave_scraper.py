"""
findagrave_scraper.py

Improved Find A Grave scraper for Utah memorial highways.
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import urllib.parse
import difflib
import unicodedata
import logging
from typing import List, Tuple, Optional, Dict

# -------------------
# Config
# -------------------

INPUT_FILE = "utah_cleaned_names.txt"
OUTPUT_FILE = "findagrave_results_utah.csv"

DELAY_SECONDS = 1
MAX_CANDIDATES = 3
MIN_SCORE_TO_ACCEPT = 0.25

BASE = "https://www.findagrave.com"
SEARCH_URL = BASE + "/memorial/search"

HEADERS = {
    "User-Agent": "MemorialHighwaysCapstone/1.0 (dhyana@slu.edu)",
    "Accept-Language": "en-US,en;q=0.9"
}

# Prioritize Utah matches
PREFERRED_STATE = "Utah"
PREFERRED_STATE_WEIGHT = 0.15

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

MEMORIAL_RE = re.compile(r"^/memorial/(\d+)")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# -------------------
# Utilities
# -------------------

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFKD", s)
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def name_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, normalize_text(a), normalize_text(b)).ratio()


def extract_years_from_text(text: str) -> Tuple[Optional[int], Optional[int]]:
    if not text:
        return (None, None)
    matches = re.findall(r"(18|19|20)\d{2}", text)
    if not matches:
        return (None, None)
    years = [int(m) for m in matches]
    return (years[0], years[1] if len(years) > 1 else None)


# -------------------
# Find A Grave Search
# -------------------

def search_findagrave(name: str) -> List[str]:
    parts = name.split()
    if not parts:
        return []

    params = {
        "firstname": parts[0],
        "lastname": parts[-1],
        "countryId": "4"  # USA
    }

    try:
        resp = SESSION.get(SEARCH_URL, params=params, timeout=20)
        if resp.status_code != 200:
            return []
    except Exception:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.find_all("a", href=True)

    urls = []
    seen = set()

    for a in anchors:
        m = MEMORIAL_RE.match(a["href"])
        if not m:
            continue
        mem_id = m.group(1)
        if mem_id in seen:
            continue
        seen.add(mem_id)

        full_url = urllib.parse.urljoin(BASE, a["href"])
        urls.append(full_url)

        if len(urls) >= MAX_CANDIDATES:
            break

    return urls


def parse_memorial_page(url: str) -> Dict:
    try:
        r = SESSION.get(url, timeout=20)
        if r.status_code != 200:
            return {}
    except Exception:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")

    data = {
        "url": url,
        "name": None,
        "birth_year": None,
        "death_year": None,
        "place_text": None
    }

    ogt = soup.select_one("meta[property='og:title']")
    if ogt and ogt.get("content"):
        title = ogt["content"]
        data["name"] = title
        b, d = extract_years_from_text(title)
        data["birth_year"] = b
        data["death_year"] = d

    text = soup.get_text(" ", strip=True)
    if "Utah" in text:
        data["place_text"] = "Utah"

    return data


def score_candidate(query_name: str, parsed: Dict) -> float:
    score = 0
    score += name_similarity(query_name, parsed.get("name")) * 0.7

    if parsed.get("place_text") == "Utah":
        score += PREFERRED_STATE_WEIGHT

    return min(score, 1.0)


# -------------------
# Main
# -------------------

def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]

    results = []

    for name in names:
        logging.info(f"Searching: {name}")
        candidates = search_findagrave(name)

        best_score = 0
        best_data = {}

        for url in candidates:
            time.sleep(0.8)
            parsed = parse_memorial_page(url)
            score = score_candidate(name, parsed)

            if score > best_score:
                best_score = score
                best_data = parsed

        results.append({
            "State": "Utah",
            "Name": name,
            "Memorial_URL": best_data.get("url"),
            "Parsed_Name": best_data.get("name"),
            "Birth_Year": best_data.get("birth_year"),
            "Death_Year": best_data.get("death_year"),
            "Match_Score": best_score
        })

        time.sleep(DELAY_SECONDS)

    import pandas as pd
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_FILE, index=False)

    logging.info("Done. Results saved to %s", OUTPUT_FILE)


if __name__ == "__main__":
    main()