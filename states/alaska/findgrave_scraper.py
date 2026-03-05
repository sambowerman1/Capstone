"""
findagrave_scraper_alaska.py

Find A Grave scraper adapted for Alaska memorial highway research
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

INPUT_FILE = "alaska_person_names.txt"
OUTPUT_FILE = "alaska_findagrave_results.csv"

DELAY_SECONDS = 1
MAX_CANDIDATES = 3
MIN_SCORE_TO_ACCEPT = 0.25

BASE = "https://www.findagrave.com"
SEARCH_URL = BASE + "/memorial/search"

HEADERS = {
    "User-Agent": "MemorialHighwaysResearch/1.0 (slu.edu research)",
    "Accept-Language": "en-US,en;q=0.9"
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# prioritize Alaska results
PREFERRED_STATE = "Alaska"
PREFERRED_STATE_WEIGHT = 0.15


# -------------------
# Logging
# -------------------

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
        return 0
    return difflib.SequenceMatcher(None, normalize_text(a), normalize_text(b)).ratio()


def extract_years_from_text(text: str) -> Tuple[Optional[int], Optional[int]]:
    matches = re.findall(r"(18|19|20)\d{2}", text)
    years = [int(m) for m in matches]
    birth = years[0] if len(years) >= 1 else None
    death = years[1] if len(years) >= 2 else None
    return birth, death


# -------------------
# Find memorial links
# -------------------

MEMORIAL_RE = re.compile(r"^/memorial/(\d+)")

def parse_search_results_for_candidates(html, query_name):

    soup = BeautifulSoup(html, "html.parser")

    anchors = soup.find_all("a", href=True)

    candidates = []

    seen = set()

    for a in anchors:

        href = a["href"]

        m = MEMORIAL_RE.match(href)

        if not m:
            continue

        mid = m.group(1)

        if mid in seen:
            continue

        url = urllib.parse.urljoin(BASE, href)

        candidates.append(url)

        seen.add(mid)

        if len(candidates) >= MAX_CANDIDATES:
            break

    return candidates


# -------------------
# Parse memorial page
# -------------------

def parse_memorial_page(html):

    soup = BeautifulSoup(html, "html.parser")

    data = {
        "name": None,
        "birth_year": None,
        "death_year": None,
        "bio": None
    }

    og = soup.select_one("meta[property='og:title']")

    if og and og.get("content"):
        title = og["content"]
        data["name"] = title
        b, d = extract_years_from_text(title)
        data["birth_year"] = b
        data["death_year"] = d

    bio = soup.select_one("div.bio")

    if bio:
        data["bio"] = bio.get_text(" ", strip=True)

    return data


# -------------------
# Score candidate
# -------------------

def score_candidate(query, parsed):

    score = 0

    name_sim = name_similarity(query["name"], parsed.get("name"))

    score += name_sim * 0.7

    if query.get("birth") and parsed.get("birth_year"):

        if int(query["birth"]) == int(parsed["birth_year"]):

            score += 0.2

    if query.get("death") and parsed.get("death_year"):

        if int(query["death"]) == int(parsed["death_year"]):

            score += 0.1

    return min(score, 1)


# -------------------
# Search FindAGrave
# -------------------

def search_findagrave_candidates(name):

    parts = name.split()

    if not parts:
        return []

    params = {
        "firstname": parts[0],
        "lastname": parts[-1],
        "countryId": "4"
    }

    try:

        r = SESSION.get(SEARCH_URL, params=params, timeout=20)

    except Exception as e:

        logging.warning(f"Search failed {name}: {e}")

        return []

    if r.status_code != 200:

        return []

    return parse_search_results_for_candidates(r.text, name)


# -------------------
# Choose best match
# -------------------

def choose_best_memorial(query):

    candidates = search_findagrave_candidates(query["name"])

    best_score = 0

    best_url = None

    best_parsed = {}

    for url in candidates:

        time.sleep(0.8)

        try:

            r = SESSION.get(url, timeout=20)

        except Exception:

            continue

        if r.status_code != 200:
            continue

        parsed = parse_memorial_page(r.text)

        score = score_candidate(query, parsed)

        logging.info(f"{url} score={score:.3f}")

        if score > best_score:

            best_score = score

            best_url = url

            best_parsed = parsed

    if best_score >= MIN_SCORE_TO_ACCEPT:

        return best_url, best_parsed, best_score

    return None, {}, best_score


# -------------------
# Parse input line
# -------------------

def parse_input_line(line):

    parts = [p.strip() for p in line.split("|")]

    data = {"name": parts[0]}

    data["birth"] = parts[1] if len(parts) > 1 and parts[1] else None

    data["death"] = parts[2] if len(parts) > 2 and parts[2] else None

    data["state"] = parts[3] if len(parts) > 3 and parts[3] else None

    return data


# -------------------
# Main
# -------------------

def main():

    with open(INPUT_FILE, encoding="utf-8") as f:

        lines = [l.strip() for l in f if l.strip()]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as out:

        writer = csv.writer(out)

        writer.writerow([
            "query_name",
            "chosen_memorial_url",
            "parsed_name",
            "parsed_birth",
            "parsed_death",
            "match_score"
        ])

        for line in lines:

            query = parse_input_line(line)

            logging.info(f"Searching: {query['name']}")

            url, parsed, score = choose_best_memorial(query)

            writer.writerow([
                query["name"],
                url or "",
                parsed.get("name",""),
                parsed.get("birth_year",""),
                parsed.get("death_year",""),
                score
            ])

            time.sleep(DELAY_SECONDS)


    logging.info("Finished scraping")


if __name__ == "__main__":
    main()