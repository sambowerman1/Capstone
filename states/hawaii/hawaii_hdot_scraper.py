"""
hawaii_hdot_scraper.py

Scrapes Hawaii DOT (HDOT) Highways Division "State Roads and Highways" pages by island
(from the Visitor hub page) and outputs a cleaned CSV.

Handles all island formatting styles:
- Numbered lists: "1. ...", "2. ..." (Oahu, Kauai, Maui, Lanai)
- Non-numbered route lines: "... (Route 480)" (Molokai)
- Big Island style: "Route 11, ..." (Big Island)

Output: hawaii_state_roads_hdot.csv
Columns: State, Island, Source_URL, Raw, Route, Road_or_Description, Parenthetical
"""

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

VISITOR_URL = "https://hidot.hawaii.gov/highways/visitor/"
HEADERS = {"User-Agent": "MemorialHighwaysCapstone/1.0 (dhyana@slu.edu)"}

# -----------------------------
# Helpers
# -----------------------------

def fetch_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def get_island_links() -> list[dict]:
    """
    Pull island page URLs from the Visitor hub.
    Falls back to hardcoded known URLs if hub parsing fails.
    """
    try:
        soup = fetch_soup(VISITOR_URL)
        islands_needed = {"oahu", "kauai", "maui", "molokai", "lanai", "big island"}

        links = []
        for a in soup.select("a[href]"):
            text = (a.get_text(" ", strip=True) or "").strip()
            href = (a.get("href") or "").strip()
            if not text or not href:
                continue
            if text.lower() in islands_needed:
                links.append({"Island": text, "URL": urljoin(VISITOR_URL, href)})

        # dedupe by URL
        seen = set()
        out = []
        for item in links:
            u = item["URL"].strip()
            if u not in seen:
                seen.add(u)
                out.append({"Island": item["Island"], "URL": u})

        # if we didn't get all, just return what we got (fallback later)
        if out:
            return out

    except Exception:
        pass

    # Fallback list (HDOT site structure sometimes changes)
    return [
        {"Island": "Oahu", "URL": "https://hidot.hawaii.gov/highways/home/oahu/oahu-state-roads-and-highways/"},
        {"Island": "Kauai", "URL": "https://hidot.hawaii.gov/highways/home/kauai/kauai-state-roads-and-highways/"},
        {"Island": "Maui", "URL": "https://hidot.hawaii.gov/highways/home/maui/maui-state-roads-and-highways/"},
        {"Island": "Molokai", "URL": "https://hidot.hawaii.gov/highways/home/maui/molokai-state-roads-and-highways/"},
        {"Island": "Lanai", "URL": "https://hidot.hawaii.gov/highways/home/maui/lanai-state-roads-and-highways/"},
        {"Island": "Big Island", "URL": "https://hidot.hawaii.gov/highways/home/hawaii/state-roads-and-highways/"},
    ]

def extract_lines(soup: BeautifulSoup) -> list[str]:
    """
    Extract road lines from the page.
    Covers:
      1) numbered items: '1. ...'
      2) molokai-style: '... (Route 480)'
      3) big island style: 'Route 11, ...'
    """
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    items = []
    for ln in lines:
        # ignore obvious navigation/footer junk
        if any(bad in ln for bad in [
            "Skip to", "RECENT POSTS", "CATEGORIES", "POLICIES",
            "Powered by", "Copyright", "Search this site",
            "Follow the Department", "Hawaii.gov Government Directory"
        ]):
            continue

        # 1) numbered list
        if re.match(r"^\d+\.\s+", ln):
            items.append(re.sub(r"^\d+\.\s+", "", ln).strip())
            continue

        # 2) non-numbered but has (Route ...)
        if "(Route" in ln and ")" in ln:
            items.append(ln)
            continue

        # 3) Big Island: "Route 11, ..."
        if re.match(r"^Route\s*\d+\b", ln, flags=re.IGNORECASE):
            items.append(ln)
            continue

    # dedupe + clean
    seen = set()
    out = []
    for x in items:
        x = re.sub(r"\s+", " ", x).strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)

    return out

# Parsing patterns:
# - Interstates: "Interstate Route H-2 (Veterans Memorial Freeway)"
# - Many islands: "... (Route 92)" or "... (Routes 99, 7110 & 7101)"
# - Big Island: "Route 11, Kanoelehua Avenue, Mamalahoa Highway, ..."
INTERSTATE_RE = re.compile(r"\bInterstate Route\s+(H-\d+)\b", re.IGNORECASE)
PAREN_LAST_RE = re.compile(r"^(?P<body>.+?)\s*\((?P<paren>[^()]*)\)\s*$")
ROUTE_ONLY_PAREN_RE = re.compile(r"^Route\s+(?P<route>[0-9A-Za-z-]+)$", re.IGNORECASE)
BIG_ISLAND_ROUTE_RE = re.compile(r"^Route\s*(?P<route>\d+)\s*,\s*(?P<body>.+)$", re.IGNORECASE)

def parse_item(raw: str) -> dict:
    raw = re.sub(r"\s+", " ", raw).strip()

    route = None
    road_desc = raw
    parenthetical = None

    # Big Island: "Route 11, ...."
    m_big = BIG_ISLAND_ROUTE_RE.match(raw)
    if m_big:
        route = m_big.group("route")
        road_desc = m_big.group("body").strip()
        parenthetical = None
        return {
            "Raw": raw,
            "Route": route,
            "Road_or_Description": road_desc,
            "Parenthetical": parenthetical
        }

    # Interstates: H-1/H-2/H-3 etc
    m_int = INTERSTATE_RE.search(raw)
    if m_int:
        route = m_int.group(1)

    # If ends with "(...)" capture as parenthetical
    m_paren = PAREN_LAST_RE.match(raw)
    if m_paren:
        road_desc = m_paren.group("body").strip()
        parenthetical = m_paren.group("paren").strip()

        # If parenthetical is "Route 92" store as route
        m_routeparen = re.search(r"\bRoute\s+([0-9A-Za-z-]+)\b", parenthetical, re.IGNORECASE)
        if route is None and m_routeparen:
            route = m_routeparen.group(1)

    return {
        "Raw": raw,
        "Route": route,
        "Road_or_Description": road_desc,
        "Parenthetical": parenthetical
    }

# -----------------------------
# MAIN
# -----------------------------

def main():
    islands = get_island_links()

    rows = []
    for item in islands:
        island = item["Island"].strip()
        url = item["URL"].strip()
        print(f"Scraping {island}: {url}")

        soup = fetch_soup(url)
        raw_items = extract_lines(soup)

        for raw in raw_items:
            parsed = parse_item(raw)
            rows.append({
                "State": "Hawaii",
                "Island": island,
                "Source_URL": url,
                **parsed
            })

    df = pd.DataFrame(rows).drop_duplicates(subset=["Island", "Raw"]).reset_index(drop=True)

    # Optional: quick sanity counts (based on the manual lists you pasted)
    # Expected: Oahu 89, Kauai 13, Maui 27, Lanai 2, Molokai 6, Big Island 13 (Total 150)
    counts = df["Island"].value_counts().to_dict()
    print("\nIsland counts:", counts)
    print("Total rows:", len(df))

    df.to_csv("hawaii_state_roads_hdot.csv", index=False)
    print("\nSaved to hawaii_state_roads_hdot.csv\n")

if __name__ == "__main__":
    main()