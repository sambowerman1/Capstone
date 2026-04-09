from pathlib import Path
import pandas as pd
import requests
import time


STATE_DIR = Path(__file__).resolve().parent
INPUT_FILE = STATE_DIR / "alabama_memorial_people.csv"
OUTPUT_FILE = STATE_DIR / "alabama_wikidata_output.csv"

SEARCH_URL = "https://www.wikidata.org/w/api.php"
ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"

HEADERS = {
    "User-Agent": "MemorialHighwaysCapstone/1.0 (dhyana@slu.edu)"
}


def search_entity(name: str):
    """
    Search Wikidata for a name and return the top result info.
    """
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": "en",
        "search": name,
        "limit": 5
    }

    try:
        response = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = data.get("search", [])
        if not results:
            return None

        return results[0]

    except Exception as e:
        print(f"Search error for {name}: {e}")
        return None


def fetch_entity(entity_id: str):
    """
    Fetch full Wikidata entity JSON.
    """
    try:
        url = ENTITY_URL.format(entity_id)
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Fetch error for {entity_id}: {e}")
        return None


def get_claim_values(entity: dict, pid: str):
    """
    Get raw claim values for a Wikidata property.
    """
    claims = entity.get("claims", {})
    if pid not in claims:
        return []

    values = []
    for claim in claims[pid]:
        try:
            mainsnak = claim.get("mainsnak", {})
            datavalue = mainsnak.get("datavalue", {})
            value = datavalue.get("value")
            values.append(value)
        except Exception:
            continue

    return values


def extract_basic_info(entity_json: dict, entity_id: str):
    """
    Extract useful fields from full entity JSON.
    """
    entity = entity_json["entities"][entity_id]

    label = entity.get("labels", {}).get("en", {}).get("value", "")
    description = entity.get("descriptions", {}).get("en", {}).get("value", "")
    aliases = entity.get("aliases", {}).get("en", [])
    alias_list = [a.get("value", "") for a in aliases]

    birth_values = get_claim_values(entity, "P569")   # date of birth
    death_values = get_claim_values(entity, "P570")   # date of death
    occupation_values = get_claim_values(entity, "P106")  # occupation
    country_values = get_claim_values(entity, "P27")  # country of citizenship
    gender_values = get_claim_values(entity, "P21")   # sex or gender

    birth_date = ""
    death_date = ""

    if birth_values:
        birth_date = birth_values[0].get("time", "") if isinstance(birth_values[0], dict) else str(birth_values[0])

    if death_values:
        death_date = death_values[0].get("time", "") if isinstance(death_values[0], dict) else str(death_values[0])

    occupation_ids = []
    for occ in occupation_values:
        if isinstance(occ, dict) and "id" in occ:
            occupation_ids.append(occ["id"])

    country_ids = []
    for c in country_values:
        if isinstance(c, dict) and "id" in c:
            country_ids.append(c["id"])

    gender_ids = []
    for g in gender_values:
        if isinstance(g, dict) and "id" in g:
            gender_ids.append(g["id"])

    return {
        "label": label,
        "description": description,
        "aliases": "; ".join(alias_list),
        "birth_date": birth_date,
        "death_date": death_date,
        "occupation_ids": "; ".join(occupation_ids),
        "country_ids": "; ".join(country_ids),
        "gender_ids": "; ".join(gender_ids),
    }


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    if "possible_person" not in df.columns:
        raise ValueError("Expected column 'possible_person' in alabama_memorial_people.csv")

    names = (
        df["possible_person"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    names = names[names != ""].drop_duplicates().tolist()

    print(f"Total unique names to search: {len(names)}")

    results = []

    for i, name in enumerate(names, start=1):
        print(f"[{i}/{len(names)}] Searching Wikidata for: {name}")

        search_result = search_entity(name)

        if not search_result:
            results.append({
                "input_name": name,
                "wikidata_id": "",
                "matched_label": "",
                "matched_description": "",
                "search_match_label": "",
                "search_match_description": "",
                "aliases": "",
                "birth_date": "",
                "death_date": "",
                "occupation_ids": "",
                "country_ids": "",
                "gender_ids": "",
            })
            time.sleep(0.2)
            continue

        entity_id = search_result.get("id", "")
        search_label = search_result.get("label", "")
        search_description = search_result.get("description", "")

        entity_json = fetch_entity(entity_id)

        if not entity_json:
            results.append({
                "input_name": name,
                "wikidata_id": entity_id,
                "matched_label": "",
                "matched_description": "",
                "search_match_label": search_label,
                "search_match_description": search_description,
                "aliases": "",
                "birth_date": "",
                "death_date": "",
                "occupation_ids": "",
                "country_ids": "",
                "gender_ids": "",
            })
            time.sleep(0.2)
            continue

        info = extract_basic_info(entity_json, entity_id)

        results.append({
            "input_name": name,
            "wikidata_id": entity_id,
            "matched_label": info["label"],
            "matched_description": info["description"],
            "search_match_label": search_label,
            "search_match_description": search_description,
            "aliases": info["aliases"],
            "birth_date": info["birth_date"],
            "death_date": info["death_date"],
            "occupation_ids": info["occupation_ids"],
            "country_ids": info["country_ids"],
            "gender_ids": info["gender_ids"],
        })

        time.sleep(0.2)

    output_df = pd.DataFrame(results)
    output_df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved Wikidata output to: {OUTPUT_FILE}")
    print(f"Total rows saved: {len(output_df)}")


if __name__ == "__main__":
    main()