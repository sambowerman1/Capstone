
"""
findagrave_scraper_improved.py

Improved Find A Grave scraper:

* Collects multiple candidate memorial links from the search results
* Fetches each candidate memorial and extracts canonical name, birth/death years, place text, etc.
* Scores candidates by name similarity, year matches, and state/place preference
* Supports optional input format: "Full Name" or "Full Name|birth_year|death_year|state"
* Writes detailed CSV with parsed fields and chosen-match score

Requires: requests, beautifulsoup4
Install: pip install requests beautifulsoup4
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

INPUT_FILE = "names.txt"                # lines: "Name" or "Name|birth|death|state"
OUTPUT_FILE = "findagrave_results.csv"
DELAY_SECONDS = 1
MAX_CANDIDATES = 3                      # how many search-result memorial links to evaluate
MIN_SCORE_TO_ACCEPT = 0.25              # threshold to accept best match (tweakable)
BASE = "https://www.findagrave.com"
SEARCH_URL = BASE + "/memorial/search"

HEADERS = {
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
"AppleWebKit/537.36 (KHTML, like Gecko) "
"Chrome/140.0.0.0 Safari/537.36",
"Accept-Language": "en-US,en;q=0.9",
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
}

# Short map for matching/normalization (useful when input uses full state names)

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

# Optional: preferred state to prioritize matches
PREFERRED_STATE = "Florida"   # <-- set to None or "" to disable preference
PREFERRED_STATE_WEIGHT = 0.15 # how much to boost confidence if matched (0.05–0.25 works well)


# logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------

# Utilities

# -------------------

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFKD", s)
    # remove punctuation except keep spaces
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def clean_name_for_search(raw_name: str) -> str:
    """
    Remove military, professional, and honorific titles from a person's name
    before searching Find A Grave.
    """
    if not raw_name:
        return ""
    name = raw_name.strip()

    # Normalize punctuation like commas, periods
    name = re.sub(r"[.,]", "", name)

    # Common titles / ranks / prefixes to remove
    TITLES = [
        "mr", "mrs", "ms", "miss", "dr", "prof", "reverend", "rev",
        "hon", "honorable", "judge",
        "capt", "captain", "lt", "lieutenant", "sgt", "sergeant",
        "pvt", "private", "cpl", "corporal", "spec", "specialist",
        "spc", "trooper", "colonel", "col", "major", "maj", "general", "gen",
        "officer", "chief", "commander", "ensign", "ens",
        "petty officer", "airman", "sailor", "marine", "soldier",
        "staff sergeant", "tech sergeant", "tech sgt", "senior airman",
        "first lieutenant", "second lieutenant", "sgt maj", "sergeant major",
        "po", "ltc", "lt col"
    ]

    # Remove titles at start (case-insensitive)
    pattern = r"^(?:" + "|".join(TITLES) + r")\b\s+"
    name = re.sub(pattern, "", name, flags=re.IGNORECASE)

    # Remove redundant internal commas, extra whitespace
    name = re.sub(r"\s{2,}", " ", name)
    name = name.strip()

    return name


def name_similarity(a: str, b: str) -> float:
    """Return a ratio (0..1) of similarity between names using difflib."""
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, normalize_text(a), normalize_text(b)).ratio()

def extract_years_from_text(text: str) -> Tuple[Optional[int], Optional[int]]:
    """Find the first two 4-digit numbers in the text and interpret as birth/death if found."""
    if not text:
        return (None, None)
    matches = re.findall(r"(18|19|20)\d{2}", text)
    if not matches:
        return (None, None)
    years = [int(m) for m in matches]
    birth = years[0] if len(years) >= 1 else None
    death = years[1] if len(years) >= 2 else None
    return (birth, death)

def canonicalize_state(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if len(s) == 2:
        return s.upper()
    # try full name to abbr
    up = s.title()
    return STATE_TO_ABBR.get(up, s.upper())

# -------------------

# Parsing helpers

# -------------------

def parse_search_results_for_candidates(html: str, query_name: str, max_candidates=6) -> List[str]:
    """
    Return a list of absolute memorial URLs from the results page.
    Heuristics:
    - prefer anchors whose text contains last name or both names
    - prefer anchors inside likely result containers (li, div with 'memorial' or 'result' in class)
    - deduplicate by memorial id
    """
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a", href=True)
    candidates = []
    seen_ids = set()
    qnorm = normalize_text(query_name)
    qparts = qnorm.split()
    prefer_token = qparts[-1] if qparts else ""

    
    for a in anchors:
        href = a["href"]
        m = MEMORIAL_RE.match(href)
        if not m:
            continue
        mem_id = m.group(1)
        if mem_id in seen_ids:
            continue

        # get absolute url
        url = href if href.startswith("http") else urllib.parse.urljoin(BASE, href)
        anchor_text = a.get_text(" ", strip=True) or ""
        textnorm = normalize_text(anchor_text)

        # preference heuristics:
        score = 0
        # 1) anchor text contains last name
        if prefer_token and prefer_token in textnorm:
            score += 2
        # 2) anchor text contains first name
        if qparts and qparts[0] in textnorm:
            score += 1
        # 3) within a "result" container
        parent = a.find_parent()
        if parent:
            cls = " ".join(parent.get("class") or [])
            if "memorial" in cls or "result" in cls or "search" in cls or "card" in cls:
                score += 2

        # keep candidate if it has any decent signal, otherwise still keep a limited number (to not miss)
        if score > 0:
            candidates.append((url, score, anchor_text))
            seen_ids.add(mem_id)
        else:
            # keep some anchors even without anchor text match (low priority) up to max_candidates*2
            if len(candidates) < (max_candidates * 2):
                candidates.append((url, score, anchor_text))
                seen_ids.add(mem_id)

    # sort by heuristic score desc and dedupe
    candidates.sort(key=lambda x: x[1], reverse=True)
    # return URLs, limited
    return [c[0] for c in candidates[:max_candidates]]


def parse_memorial_page(html: str) -> Dict[str, Optional[str]]:
    """
    Attempt to extract canonical name, bio text, meta description, birth/death years, and any place.
    Heuristics:
    - meta[property=og:title] often contains: "John Smith (1900-1970) - Find A Grave Memorial"
    - meta[property=og:description] or page text may include cemetery or place
    """
    soup = BeautifulSoup(html, "html.parser")
    out = {
    "name": None,
    "bio": None,
    "meta_description": None,
    "birth_year": None,
    "death_year": None,
    "place_text": None,
    }

    
    # Try og:title
    ogt = soup.select_one("meta[property='og:title']")
    if ogt and ogt.get("content"):
        title = ogt["content"].strip()
        out["name"] = title
        # attempt to extract years from title
        b, d = extract_years_from_text(title)
        out["birth_year"] = b
        out["death_year"] = d

    # bio selectors
    selectors = ["div.bio", "section#bio", "div.memorial-bio", "div#bio", "div.memorial-content"]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            if txt:
                out["bio"] = txt
                if not out["meta_description"]:
                    out["meta_description"] = txt

    # fallback meta description
    ogdesc = soup.select_one("meta[property='og:description']")
    if ogdesc and ogdesc.get("content"):
        out["meta_description"] = ogdesc["content"].strip()
        # also try to extract years/place from description if missing
        if not out["birth_year"] or not out["death_year"]:
            b, d = extract_years_from_text(out["meta_description"])
            out["birth_year"] = out["birth_year"] or b
            out["death_year"] = out["death_year"] or d

    # place: try find elements which mention cemetery/place
    # common text like "Buried in", "Interred in", "Cemetery"
    text_all = " ".join(filter(None, [out["bio"], out["meta_description"], soup.get_text(" ", strip=True)]))
    place_match = re.search(r"(buried|inter(?:red|ment)|interred|burial|cemetery|grave site)[^.\n]*?([A-Za-z0-9 ,'-]+(?:, [A-Z]{2})?)", text_all, re.IGNORECASE)
    if place_match:
        out["place_text"] = place_match.group(0).strip()
    else:
        # simple heuristic: find a line containing a state abbreviation
        mstate = re.search(r"\b([A-Z]{2})\b", text_all)
        if mstate:
            out["place_text"] = mstate.group(1)

    return out


def score_candidate(query: Dict, parsed: Dict) -> float:
    """
    Score candidate memorial against query data.
    We combine:
    - name similarity (0..1) weighted heavily
    - birth/death matches (binary bonuses)
    - state/place match (partial bonus)
    """
    score = 0.0
    # name similarity
    qname = query.get("name") or ""
    pname = parsed.get("name") or ""
    name_sim = name_similarity(qname, pname)
    score += name_sim * 0.65


    # year matches
    qb = query.get("birth")
    qd = query.get("death")
    pb = parsed.get("birth_year")
    pd = parsed.get("death_year")
    year_bonus = 0.0
    if qb and pb and int(qb) == int(pb):
        year_bonus += 0.18
    if qd and pd and int(qd) == int(pd):
        year_bonus += 0.12
    score += year_bonus

    # state/place soft match
    qstate = canonicalize_state(query.get("state"))
    place_text = parsed.get("place_text") or ""
    if qstate and qstate in (place_text or "").upper():
        score += 0.10
    # also if the query state string appears in page text (case-insensitive)
    if query.get("state") and query["state"].strip().lower() in (place_text or "").lower():
        score += 0.05

    # preferred global state boost (e.g., prioritize Florida)
    if PREFERRED_STATE:
        pref_abbr = canonicalize_state(PREFERRED_STATE)
        if pref_abbr and pref_abbr in (place_text or "").upper():
            score += PREFERRED_STATE_WEIGHT
        elif PREFERRED_STATE.lower() in (place_text or "").lower():
            score += PREFERRED_STATE_WEIGHT * 0.8


    # clamp
    return min(score, 1.0)


# -------------------

# Main search / driver

# -------------------

def search_findagrave_candidates(name: str, state: Optional[str] = None) -> List[str]:
    parts = name.split()
    if not parts:
        return []
    first = parts[0]
    last = parts[-1]
    params = {
        "firstname": first,
        "lastname": last,
        "countryId": "4",  # USA
    }

    abbr = None  # ✅ ensure defined
    if state:
        abbr = canonicalize_state(state)
        if abbr and len(abbr) == 2:
            params["state"] = abbr



    try:
        resp = SESSION.get(SEARCH_URL, params=params, timeout=20)
    except Exception as e:
        logging.warning(f"Search request failed for {name}: {e}")
        return []

    if resp.status_code != 200:
        logging.warning(f"Search returned status {resp.status_code} for {name}")
        return []

    candidates = parse_search_results_for_candidates(resp.text, name, max_candidates=MAX_CANDIDATES)
    logging.info(f"Found {len(candidates)} candidate memorial links for '{name}'")
    return candidates
    

def choose_best_memorial_for_query(query: Dict) -> Tuple[Optional[str], Dict]:
    """
    For a query dict {"name":..., "birth":..., "death":..., "state":...}
    returns chosen memorial URL (or None) and parsed data for that memorial.
    """
    candidates = search_findagrave_candidates(query["name"], query.get("state"))
    best_score = 0.0
    best_url = None
    best_parsed = {}

    
    for url in candidates:
        # polite delay
        time.sleep(0.8)
        try:
            r = SESSION.get(url, timeout=20)
        except Exception as e:
            logging.debug(f"Failed fetching {url}: {e}")
            continue
        if r.status_code != 200:
            logging.debug(f"Non-200 for {url}: {r.status_code}")
            continue

        parsed = parse_memorial_page(r.text)
        score = score_candidate(query, parsed)
        logging.info(f"Candidate {url} -> name='{parsed.get('name')}' years={parsed.get('birth_year')}/{parsed.get('death_year')} score={score:.3f}")
        if score > best_score:
            best_score = score
            best_url = url
            best_parsed = parsed

    if best_score >= MIN_SCORE_TO_ACCEPT:
        logging.info(f"Selected {best_url} with score {best_score:.3f}")
        return best_url, {"score": best_score, **best_parsed}
    else:
        logging.info(f"No candidate reached threshold ({MIN_SCORE_TO_ACCEPT}). Best score was {best_score:.3f}")
        return None, {"score": best_score, **best_parsed}
    

# -------------------

# Input parsing and main loop

# -------------------

def parse_input_line(line: str) -> Dict:
    """
    Accept either:
    - "Full Name"
    - "Full Name|birth_year|death_year|state"  (missing fields can be empty)
    """
    parts = [p.strip() for p in line.split("|")]
    out = {"name": parts[0]}
    if len(parts) >= 2 and parts[1]:
        out["birth"] = parts[1]
    else:
        out["birth"] = None
    if len(parts) >= 3 and parts[2]:
            out["death"] = parts[2]
    else:
        out["death"] = None
    if len(parts) >= 4 and parts[3]:
        out["state"] = parts[3]
    else:
        out["state"] = None
    return out

def main():
    # read names
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]


    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["input_line", "query_name", "query_birth", "query_death", "query_state",
                    "chosen_memorial_url", "chosen_memorial_id", "parsed_name", "parsed_birth", "parsed_death", "parsed_place", "parsed_bio_snippet", "match_score"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for line in lines:
            query = parse_input_line(line)
            query["name"] = clean_name_for_search(query["name"])
            logging.info(f"[search] {query['name']} (birth={query.get('birth')} death={query.get('death')} state={query.get('state')})")
            chosen_url, parsed_info = choose_best_memorial_for_query(query)
            mem_id = ""
            if chosen_url:
                m = re.search(r"/memorial/(\d+)", chosen_url)
                if m:
                    mem_id = m.group(1)

            writer.writerow({
                "input_line": line,
                "query_name": query.get("name"),
                "query_birth": query.get("birth") or "",
                "query_death": query.get("death") or "",
                "query_state": query.get("state") or "",
                "chosen_memorial_url": chosen_url or "",
                "chosen_memorial_id": mem_id,
                "parsed_name": parsed_info.get("name") or "",
                "parsed_birth": parsed_info.get("birth_year") or "",
                "parsed_death": parsed_info.get("death_year") or "",
                "parsed_place": parsed_info.get("place_text") or "",
                "parsed_bio_snippet": (parsed_info.get("bio") or "")[:800],
                "match_score": f"{parsed_info.get('score',0):.3f}"
            })
            # polite rest between queries
            time.sleep(DELAY_SECONDS)

    logging.info("Done. Results in %s", OUTPUT_FILE)


if __name__ == "__main__":
    main()
