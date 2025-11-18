import requests
from cleanNames import clean_name
import pandas as pd

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
HEADERS = {"User-Agent": "MemorialLookupBot/1.0 (nbarnes4@slu.edu)"}


def wbsearch_entity(name, language="en", limit=5):
    """Search Wikidata entities for a given name."""
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
    """Fetch full entity JSON for a Wikidata entity ID (e.g. Q937)."""
    r = requests.get(WIKIDATA_ENTITY_URL.format(entity_id), headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()


def get_claim_values(entity_json, pid):
    """Return Q-ids (or times/strings) for a property PID."""
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
            results.append(dv["time"].lstrip("+"))  # ISO time like +1900-01-01T00:00:00Z
        elif isinstance(dv, str):
            results.append(dv)
    return results


def resolve_qids(qids):
    """Resolve Q-ids to English labels."""
    if not qids:
        return {}
    ids_param = "|".join(qids)
    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": ids_param,
        "props": "labels",
        "languages": "en"
    }
    r = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json().get("entities", {})
    return {qid: ent.get("labels", {}).get("en", {}).get("value", qid) for qid, ent in data.items()}


def get_person_info(name):
    """Look up a person in Wikidata and return structured info."""
    search_results = wbsearch_entity(name, limit=5)
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

    entity_id = search_results[0]["id"]  # best match
    entity_json = fetch_entity(entity_id)
    ent = next(iter(entity_json["entities"].values()))

    # Get raw property values
    occupations = get_claim_values(entity_json, "P106")
    citizenships = get_claim_values(entity_json, "P27")
    genders = get_claim_values(entity_json, "P21")
    ethnicities = get_claim_values(entity_json, "P172")
    birth = get_claim_values(entity_json, "P569")
    death = get_claim_values(entity_json, "P570")

    # Resolve Q-ids to English labels
    all_qids = set(occupations + citizenships + genders + ethnicities)
    qid_map = resolve_qids(list(all_qids))

    occ_labels = [qid_map.get(q, q) for q in occupations]
    gender_labels = [qid_map.get(q, q) for q in genders]
    eth_labels = [qid_map.get(q, q) for q in ethnicities]

    # Extract sitelink (Wikipedia link)
    sitelinks = ent.get("sitelinks", {})
    wikipedia_link = None
    if "enwiki" in sitelinks:
        wikipedia_link = sitelinks["enwiki"].get("url")
    elif sitelinks:  # fallback: pick any available wiki
        wikipedia_link = next(iter(sitelinks.values())).get("url")

    # Clean dates
    def clean_date(dt_list):
        if not dt_list:
            return None
        return dt_list[0].split("T")[0]  # "YYYY-MM-DD"

    birth_date = clean_date(birth)
    death_date = clean_date(death)

    return {
        "Name": name,
        "Primary Occupation": occ_labels[0] if occ_labels else None,
        "Race": eth_labels[0] if eth_labels else None,
        "Sex": gender_labels[0] if gender_labels else None,
        "Birth Date": birth_date,
        "Death Date": death_date,
        "Wikipedia Link": wikipedia_link
    }

highways_path = "memorial_highways_test_append.csv"
highways = pd.read_csv(highways_path)
names = [clean_name(i) for i in highways["DESIGNATIO"]]
highways["Name"] = names

highways.to_csv("wikipedia_api_scraper/data_with_names2.csv", index=False)

data = [get_person_info(name) for name in names]
df = pd.DataFrame(data)
df_unique = df.drop_duplicates(subset="Name", keep="first")
df_unique.to_csv("wikipedia_api_scraper/scraper_output.csv", index=False)

merged = pd.merge(highways, df_unique, on="Name", how="left")
merged.to_csv("wikipedia_api_scraper/final_output.csv", index=False)
