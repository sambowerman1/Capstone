import requests
import pandas as pd
import time

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
HEADERS = {"User-Agent": "MemorialHighwaysCapstone/1.0 (dhyana@slu.edu)"}

INPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_people_clean.csv"
OUTPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/wiki_scraper_output_colorado.csv"


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
    try:
        search_results = wbsearch_entity(name)

        if not search_results:
            return {
                "Name": name,
                "Matched_Label": None,
                "Wikidata_ID": None,
                "Description": None,
                "Primary_Occupation": None,
                "Sex": None,
                "Race": None,
                "Birth_Date": None,
                "Death_Date": None,
                "Wikipedia_Link": None
            }

        best_result = search_results[0]
        entity_id = best_result["id"]
        entity_json = fetch_entity(entity_id)
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
        wiki_title = sitelinks.get("enwiki", {}).get("title")
        wikipedia_link = f"https://en.wikipedia.org/wiki/{wiki_title.replace(' ', '_')}" if wiki_title else None

        return {
            "Name": name,
            "Matched_Label": best_result.get("label"),
            "Wikidata_ID": entity_id,
            "Description": best_result.get("description"),
            "Primary_Occupation": qid_map.get(occupations[0]) if occupations else None,
            "Sex": qid_map.get(genders[0]) if genders else None,
            "Race": qid_map.get(ethnicities[0]) if ethnicities else None,
            "Birth_Date": clean_date(birth),
            "Death_Date": clean_date(death),
            "Wikipedia_Link": wikipedia_link
        }

    except Exception as e:
        print(f"Error for {name}: {e}")
        return {
            "Name": name,
            "Matched_Label": None,
            "Wikidata_ID": None,
            "Description": None,
            "Primary_Occupation": None,
            "Sex": None,
            "Race": None,
            "Birth_Date": None,
            "Death_Date": None,
            "Wikipedia_Link": None
        }


def main():
    df_input = pd.read_csv(INPUT_FILE)

    if "person_name" not in df_input.columns:
        raise ValueError("Input file must contain a 'person_name' column.")

    names = (
        df_input["person_name"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    data = []
    for i, name in enumerate(names, start=1):
        print(f"[{i}/{len(names)}] Searching: {name}")
        data.append(get_person_info(name))
        time.sleep(0.3)

    df_info = pd.DataFrame(data)

    final_df = df_input.merge(
        df_info,
        left_on="person_name",
        right_on="Name",
        how="left"
    )

    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nScraped {len(df_info)} individuals from Wikidata.")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()