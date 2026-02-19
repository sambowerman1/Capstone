import requests
import pandas as pd

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
HEADERS = {"User-Agent": "MemorialLookupBot/1.0 (nbarnes4@slu.edu)"}


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


def fetch_entity(entity_id):
    r = requests.get(WIKIDATA_ENTITY_URL.format(entity_id), headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()


def get_claim_values(entity_json, pid):
    ent = next(iter(entity_json["entities"].values()))
    claim_list = ent.get("claims", {}).get(pid, [])
    results = []

    for c in claim_list:
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


def get_person_info(name):
    search_results = wbsearch_entity(name)
    if not search_results:
        return {
            "Name": name,
            "Primary Occupation": None,
            "Race": None,
            "Sex": None,
            "Birth Date": None,
            "Death Date": None,
            "Wikipedia Link": None
        }

    entity_id = search_results[0]["id"]
    entity_json = fetch_entity(entity_id)
    ent = next(iter(entity_json["entities"].values()))

    occupations = get_claim_values(entity_json, "P106")
    genders = get_claim_values(entity_json, "P21")
    ethnicities = get_claim_values(entity_json, "P172")
    birth = get_claim_values(entity_json, "P569")
    death = get_claim_values(entity_json, "P570")

    qid_map = resolve_qids(list(set(occupations + genders + ethnicities)))

    def clean_date(dt):
        return dt[0].split("T")[0] if dt else None

    sitelinks = ent.get("sitelinks", {})
    wikipedia_link = sitelinks.get("enwiki", {}).get("url")

    return {
        "Name": name,
        "Primary Occupation": qid_map.get(occupations[0]) if occupations else None,
        "Race": qid_map.get(ethnicities[0]) if ethnicities else None,
        "Sex": qid_map.get(genders[0]) if genders else None,
        "Birth Date": clean_date(birth),
        "Death Date": clean_date(death),
        "Wikipedia Link": wikipedia_link
    }


# -------------------------------
# MAIN SCRIPT (uses output.txt)
# -------------------------------

# Read names from output.txt
with open("C:/Users/lucas/Data_Science_Capstone/Capstone/texas/cleaned_names.txt", encoding="utf-8") as f:

    names = [line.strip() for line in f if line.strip()]

# Extra safety deduplication
names = list(dict.fromkeys(names))

# Run scraper
data = [get_person_info(name) for name in names]

# Save results
df = pd.DataFrame(data)
df.to_csv("Capstone/texas/wiki_scraper_output.csv", index=False)

print(f"Scraped {len(df)} unique names.")
