#!/usr/bin/env python3
"""
findagrave_search_first.py

Reads names from names.txt (one per line), searches Find A Grave,
follows the first result, scrapes the bio text, and writes CSV.

Notes:
 - Be polite: this script sleeps between requests (adjust delay if needed).
 - If you want to skip the state filter, set STATE = "".
 - Respect Find A Grave's terms of use and robots.txt.
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import urllib.parse

INPUT_FILE = "names.txt"
OUTPUT_FILE = "findagrave_results.csv"
STATE = "Florida"   # set "" to skip state filter
DELAY_SECONDS = 1.5

BASE = "https://www.findagrave.com"
SEARCH_URL = BASE + "/memorial/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/140.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
}

# mapping of full state name -> postal code (for fallback)
STATE_TO_ABBR = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
    "Colorado":"CO","Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA",
    "Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA",
    "Kansas":"KS","Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD",
    "Massachusetts":"MA","Michigan":"MI","Minnesota":"MN","Mississippi":"MS",
    "Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV","New Hampshire":"NH",
    "New Jersey":"NJ","New Mexico":"NM","New York":"NY","North Carolina":"NC",
    "North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA",
    "Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD","Tennessee":"TN",
    "Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA","Washington":"WA",
    "West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY","District of Columbia":"DC"
}

MEMORIAL_RE = re.compile(r"^/memorial/(\d+)")
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

def first_memorial_from_search_html(html):
    """
    Parse search results page HTML and return first memorial absolute URL (or None).
    Robustly finds links that match /memorial/<id> and skips unrelated anchors.
    """
    soup = BeautifulSoup(html, "html.parser")
    # Strategy: find all anchors with href, pick first /memorial/<id> anchor
    for a in soup.find_all("a", href=True):
        m = MEMORIAL_RE.match(a["href"])
        if m:
            href = a["href"]
            # some hrefs are full URLs already
            if href.startswith("http"):
                return href
            return urllib.parse.urljoin(BASE, href)
    # fallback: look for typical class names (if present)
    fallback = soup.select_one("a.memorial-link, a[href*='/memorial/']")
    if fallback:
        href = fallback.get("href")
        if href:
            return urllib.parse.urljoin(BASE, href)
    return None

def scrape_bio_from_memorial(html):
    """
    Extract the memorial biography text using a few common selectors.
    """
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "div.bio",           # older class
        "section#bio",       # alternative id
        "div.memorial-bio",  # another variant
        "div#bio",
        "div.memorial-content",  # fallback container
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)
    # Try meta description fallback (OG description)
    og = soup.select_one("meta[property='og:description']")
    if og and og.get("content"):
        return og["content"].strip()
    # Nothing found
    return ""

def search_and_get_first(name, state=None):
    """
    Search Find A Grave for name (string) and optional state (full name or abbreviation).
    Returns (memorial_url or None).
    """
    parts = name.split()
    if not parts:
        return None
    first = parts[0]
    last = parts[-1]
    params = {
        "firstname": first,
        "lastname": last,
        "countryId": "4",  # USA
    }
    if state:
        params["state"] = state

    # perform GET
    resp = SESSION.get(SEARCH_URL, params=params, timeout=20)
    if resp.status_code != 200:
        # try without state as fallback
        if state:
            time.sleep(0.5)
            resp = SESSION.get(SEARCH_URL, params={"firstname": first, "lastname": last, "countryId": "4"}, timeout=20)
            if resp.status_code != 200:
                return None
        else:
            return None

    memorial = first_memorial_from_search_html(resp.text)
    return memorial

def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Name", "StateFilter", "MemorialURL", "MemorialID", "BioText"])

        for name in names:
            print(f"[search] {name}")
            memorial_url = None

            # Attempt 1: try full state name (if provided)
            if STATE:
                memorial_url = search_and_get_first(name, state=STATE)
                time.sleep(DELAY_SECONDS)

            # Attempt 2: try state abbreviation (fallback)
            if not memorial_url and STATE:
                abbr = STATE_TO_ABBR.get(STATE)
                if abbr:
                    print(f"  (falling back to abbrev: {abbr})")
                    memorial_url = search_and_get_first(name, state=abbr)
                    time.sleep(DELAY_SECONDS)

            # Attempt 3: try without state filter
            if not memorial_url:
                print("  (searching without state filter)")
                memorial_url = search_and_get_first(name, state=None)
                time.sleep(DELAY_SECONDS)

            memorial_id = ""
            bio_text = ""
            if memorial_url:
                print(f"  -> found: {memorial_url}")
                # fetch memorial page
                try:
                    r = SESSION.get(memorial_url, timeout=20)
                    if r.status_code == 200:
                        bio_text = scrape_bio_from_memorial(r.text)
                        # extract memorial id if present in URL
                        m = re.search(r"/memorial/(\d+)", memorial_url)
                        if m:
                            memorial_id = m.group(1)
                    else:
                        print(f"  ! failed to fetch memorial page: {r.status_code}")
                except Exception as e:
                    print(f"  ! exception fetching memorial: {e}")
            else:
                print("  -> no memorial found")

            writer.writerow([name, STATE, memorial_url or "", memorial_id, bio_text])
            # polite pause before next name
            time.sleep(DELAY_SECONDS)

    print("Done. Results in", OUTPUT_FILE)

if __name__ == "__main__":
    main()
