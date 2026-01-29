"""
Wikidata scraper for person information.

Searches Wikidata for person entities and extracts biographical data.
"""

from typing import Optional, Dict, Any, List
import requests


class WikidataScraper:
    """Scraper for Wikidata person information."""

    WIKIDATA_API = "https://www.wikidata.org/w/api.php"
    WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
    HEADERS = {"User-Agent": "MemorialLookupBot/1.0 (consolidated-scraper)"}

    # Property IDs
    P_OCCUPATION = "P106"
    P_CITIZENSHIP = "P27"
    P_SEX = "P21"
    P_ETHNICITY = "P172"
    P_BIRTH_DATE = "P569"
    P_DEATH_DATE = "P570"

    def __init__(self, timeout: int = 20):
        """
        Initialize the Wikidata scraper.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout

    def _wbsearch_entity(self, name: str, language: str = "en", limit: int = 5) -> List[Dict]:
        """
        Search Wikidata entities for a given name.

        Args:
            name: Person name to search
            language: Search language
            limit: Maximum results to return

        Returns:
            List of search results
        """
        params = {
            "action": "wbsearchentities",
            "format": "json",
            "search": name,
            "language": language,
            "limit": limit
        }
        r = requests.get(
            self.WIKIDATA_API,
            params=params,
            headers=self.HEADERS,
            timeout=self.timeout
        )
        r.raise_for_status()
        return r.json().get("search", [])

    def _fetch_entity(self, entity_id: str) -> Dict:
        """
        Fetch full entity JSON for a Wikidata entity ID.

        Args:
            entity_id: Wikidata entity ID (e.g., Q937)

        Returns:
            Entity JSON data
        """
        r = requests.get(
            self.WIKIDATA_ENTITY_URL.format(entity_id),
            headers=self.HEADERS,
            timeout=self.timeout
        )
        r.raise_for_status()
        return r.json()

    def _get_claim_values(self, entity_json: Dict, pid: str) -> List[str]:
        """
        Return Q-ids (or times/strings) for a property PID.

        Args:
            entity_json: Entity data
            pid: Property ID

        Returns:
            List of values (Q-IDs, times, or strings)
        """
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

    def _resolve_qids(self, qids: List[str]) -> Dict[str, str]:
        """
        Resolve Q-ids to English labels.

        Args:
            qids: List of Q-IDs to resolve

        Returns:
            Dictionary mapping Q-ID to label
        """
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
        r = requests.get(
            self.WIKIDATA_API,
            params=params,
            headers=self.HEADERS,
            timeout=self.timeout
        )
        r.raise_for_status()
        data = r.json().get("entities", {})

        return {
            qid: ent.get("labels", {}).get("en", {}).get("value", qid)
            for qid, ent in data.items()
        }

    @staticmethod
    def _clean_date(dt_list: List[str]) -> Optional[str]:
        """
        Clean date from Wikidata format.

        Args:
            dt_list: List of date strings

        Returns:
            Cleaned date in YYYY-MM-DD format
        """
        if not dt_list:
            return None
        return dt_list[0].split("T")[0]

    def get_person_info(self, name: str) -> Dict[str, Any]:
        """
        Look up a person in Wikidata and return structured info.

        Args:
            name: Person name to search

        Returns:
            Dictionary with person information
        """
        try:
            search_results = self._wbsearch_entity(name, limit=5)
        except requests.RequestException as e:
            print(f"Wikidata search error for '{name}': {e}")
            return self._empty_result(name)

        if not search_results:
            return self._empty_result(name)

        entity_id = search_results[0]["id"]

        try:
            entity_json = self._fetch_entity(entity_id)
        except requests.RequestException as e:
            print(f"Wikidata fetch error for entity {entity_id}: {e}")
            return self._empty_result(name)

        ent = next(iter(entity_json["entities"].values()))

        # Get raw property values
        occupations = self._get_claim_values(entity_json, self.P_OCCUPATION)
        genders = self._get_claim_values(entity_json, self.P_SEX)
        ethnicities = self._get_claim_values(entity_json, self.P_ETHNICITY)
        birth = self._get_claim_values(entity_json, self.P_BIRTH_DATE)
        death = self._get_claim_values(entity_json, self.P_DEATH_DATE)

        # Resolve Q-ids to English labels
        all_qids = set(occupations + genders + ethnicities)
        qid_map = self._resolve_qids(list(all_qids))

        occ_labels = [qid_map.get(q, q) for q in occupations]
        gender_labels = [qid_map.get(q, q) for q in genders]
        eth_labels = [qid_map.get(q, q) for q in ethnicities]

        # Extract sitelink (Wikipedia link)
        sitelinks = ent.get("sitelinks", {})
        wikipedia_link = None
        if "enwiki" in sitelinks:
            wikipedia_link = sitelinks["enwiki"].get("url")
        elif sitelinks:
            wikipedia_link = next(iter(sitelinks.values())).get("url")

        return {
            "Name": name,
            "Primary Occupation": occ_labels[0] if occ_labels else None,
            "Race": eth_labels[0] if eth_labels else None,
            "Sex": gender_labels[0] if gender_labels else None,
            "Birth Date": self._clean_date(birth),
            "Death Date": self._clean_date(death),
            "Wikipedia Link": wikipedia_link
        }

    @staticmethod
    def _empty_result(name: str) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            "Name": name,
            "Primary Occupation": None,
            "Race": None,
            "Sex": None,
            "Birth Date": None,
            "Death Date": None,
            "Wikipedia Link": None
        }
