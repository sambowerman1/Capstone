import requests
import pandas as pd
import time

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
HEADERS = {"User-Agent": "MemorialHighwaysCapstone/1.0 (dhyana@slu.edu)"}


# -----------------------------
# Search entity
# -----------------------------
def wbsearch_entity(name, language="en", limit=5):
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "search": name,
        "language": language,
        "limit": limit
    }
    r = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json().get("search", [])


# -----------------------------
# Fetch full entity JSON
# -----------------------------
def fetch_entity(entity_id):
    r = requests.get(WIKIDATA_ENTITY_URL.format(entity_id), headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Get property values
# -----------------------------
def get_claim_values(entity_json, pid):
    ent = next(iter(entity_json["entities"].values()))
    claims = ent.get("claims", {}).get(pid, [])
    results = []

    for c in claims:
        dv = c.get("mainsnak", {}).get("datavalue", {}).get("value")
        if not dv:
            continue
        if isinstance(dv, dict) and "id" in dv:
            results.append(dv["id"])
        elif isinstance(dv, dict) and "time" in dv:
            results.append(dv["time"].lstrip("+"))
        elif isinstance(dv, str):
            results.append(dv)

    return results


# -----------------------------
# Ensure entity is a human
# -----------------------------
def is_human(entity_json):
    p31 = get_claim_values(entity_json, "P31")
    return "Q5" in p31  # Q5 = human


# -----------------------------
# Resolve QIDs to labels
# -----------------------------
def resolve_qids(qids):
    if not qids:
        return {}

    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": "|".join(qids),
        "props": "labels",
        "languages": "en"
    }

    r = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json().get("entities", {})

    return {
        qid: ent.get("labels", {}).get("en", {}).get("value", qid)
        for qid, ent in data.items()
    }


# -----------------------------
# Main person extraction
# -----------------------------
def get_person_info(name):
    search_results = wbsearch_entity(name)

    for result in search_results:
        entity_id = result["id"]
        entity_json = fetch_entity(entity_id)

        if not is_human(entity_json):
            continue  # skip non-humans

        ent = next(iter(entity_json["entities"].values()))

        occupations = get_claim_values(entity_json, "P106")
        genders = get_claim_values(entity_json, "P21")
        ethnicities = get_claim_values(entity_json, "P172")
        birth = get_claim_values(entity_json, "P569")
        death = get_claim_values(entity_json, "P570")

        qid_map = resolve_qids(list(set(occupations + genders + ethnicities)))

        def clean_date(d):
            return d[0].split("T")[0] if d else None

        sitelinks = ent.get("sitelinks", {})
        wikipedia_link = sitelinks.get("enwiki", {}).get("url")

        return {
            "State": "Connecticut",
            "Name": name,
            "Primary_Occupation": qid_map.get(occupations[0]) if occupations else None,
            "Sex": qid_map.get(genders[0]) if genders else None,
            "Race": qid_map.get(ethnicities[0]) if ethnicities else None,
            "Birth_Date": clean_date(birth),
            "Death_Date": clean_date(death),
            "Wikipedia_Link": wikipedia_link
        }

    # If no valid human found
    return {
        "State": "Connecticut",
        "Name": name,
        "Primary_Occupation": None,
        "Sex": None,
        "Race": None,
        "Birth_Date": None,
        "Death_Date": None,
        "Wikipedia_Link": None
    }


# -----------------------------
# MAIN
# -----------------------------
with open("connecticut_cleaned_names.txt", encoding="utf-8") as f:
    names = [line.strip() for line in f if line.strip()]

data = []

for name in names:
    print(f"Scraping: {name}")
    data.append(get_person_info(name))
    time.sleep(0.3)  # polite rate limit

df = pd.DataFrame(data)
df.to_csv("wiki_scraper_output_connecticut.csv", index=False)

print(f"\nScraped {len(df)} individuals from Wikidata for Connecticut.\n")