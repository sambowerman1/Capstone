#!/usr/bin/env python3
"""
Wikipedia Link Finder for Memorial Roadway Designations

This script extracts person names from the DESIGNATIO column of Memorial_Roadway_Designations.csv
and searches for corresponding Wikipedia links, outputting results to a new CSV file.
"""

import csv
import re
import requests
import time
from urllib.parse import quote
from typing import List, Dict, Optional, Tuple
from difflib import SequenceMatcher


class WikipediaLinkFinder:
    """A class to find Wikipedia links for person names from memorial designations."""
    
    def __init__(self):
        """Initialize the Wikipedia link finder."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Memorial-Roadway-Wikipedia-Finder/1.0 (Educational Research)'
        })
        self.delay_between_requests = 1.0  # Be respectful to Wikipedia's servers
    
    def extract_person_names(self, designation: str) -> List[str]:
        """
        Extract person names from memorial designation strings.
        
        Args:
            designation: The designation string from the CSV
            
        Returns:
            List of extracted person names
        """
        names = []
        
        # Skip generic memorial types that don't contain person names
        generic_terms = [
            'Veterans Memorial', 'Gold Star Family Memorial', 'Submarine Veterans Memorial',
            'Hope and Healing Highway', 'County Veterans Memorial'
        ]
        
        if any(term in designation for term in generic_terms):
            return names
        
        # Try multiple patterns in order of specificity
        
        # Pattern 1: Multiple names - "Deputies Name1 and Name2"
        pattern_deputies = r'Deputies\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\s+and\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'
        match_deputies = re.search(pattern_deputies, designation)
        if match_deputies:
            name1 = self._clean_name(match_deputies.group(1).strip())
            name2 = self._clean_name(match_deputies.group(2).strip())
            if name1:
                names.append(name1)
            if name2:
                names.append(name2)
            return names
        
        # Pattern 2: "Name, Jr./Sr. Memorial"
        pattern_jr = r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+),\s+(?:Jr\.?|Sr\.?)'
        match_jr = re.search(pattern_jr, designation)
        if match_jr:
            name = self._clean_name(match_jr.group(1).strip())
            if name:
                names.append(name)
                return names
        
        # Pattern 3: "dedicated to ... Name"
        pattern_dedicated = r'dedicated to (?:U\.S\.\s+Army\s+)?(?:CPL|Sergeant|Lieutenant|Captain|Major|Colonel|General)\s+([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+)'
        match_dedicated = re.search(pattern_dedicated, designation)
        if match_dedicated:
            name = self._clean_name(match_dedicated.group(1).strip())
            if name:
                names.append(name)
                return names
        
        # Pattern 4: General approach - extract everything before road type keywords
        road_keywords = ['Highway', 'Boulevard', 'Bridge', 'Drive', 'Way', 'Street']
        
        for keyword in road_keywords:
            if keyword in designation:
                # Split on the keyword and take the first part
                parts = designation.split(keyword)[0].strip()
                
                # Remove common prefixes
                prefixes_to_remove = [
                    'Staff Sergeant', 'Sergeant', 'Specialist', 'Sheriff', 'Deputy',
                    'Dr.', 'Dr', 'Rev. Dr.', 'Rev Dr', 'Rev.', 'Rev', 'CPL', 'Captain',
                    'Lieutenant', 'Colonel', 'Major', 'General'
                ]
                
                clean_parts = parts
                for prefix in prefixes_to_remove:
                    if clean_parts.startswith(prefix + ' '):
                        clean_parts = clean_parts[len(prefix):].strip()
                        break
                
                # Remove "Memorial" if it's at the end
                if clean_parts.endswith(' Memorial'):
                    clean_parts = clean_parts[:-9].strip()
                
                # Check if what remains looks like a person name
                if self._looks_like_person_name(clean_parts):
                    name = self._clean_name(clean_parts)
                    if name:
                        names.append(name)
                        break
        
        return names
    
    def _looks_like_person_name(self, text: str) -> bool:
        """
        Check if text looks like a person name.
        
        Args:
            text: Text to check
            
        Returns:
            True if it looks like a person name
        """
        if not text or len(text) < 3:
            return False
        
        # Should have at least first and last name
        words = text.split()
        if len(words) < 2:
            return False
        
        # Should not contain numbers
        if re.search(r'\d', text):
            return False
        
        # Should not be generic terms
        generic_terms = ['County', 'Veterans', 'Memorial', 'Family', 'Star', 'Hope', 'Healing']
        if any(term in text for term in generic_terms):
            return False
        
        # Each word should start with capital letter
        for word in words:
            if word and not word[0].isupper():
                return False
        
        return True
    
    def _clean_name(self, name: str) -> str:
        """
        Clean extracted names by removing titles, ranks, and middle initials.
        
        Args:
            name: Raw extracted name
            
        Returns:
            Cleaned name suitable for Wikipedia search
        """
        # Remove common titles and ranks
        titles_to_remove = [
            r'^Dr\.?\s+', r'^Rev\.?\s+Dr\.?\s+', r'^Governor\s+', r'^Sheriff\s+',
            r'^Deputy\s+', r'^Captain\s+', r'^Lieutenant\s+', r'^Colonel\s+',
            r'^Major\s+', r'^General\s+', r'^Staff\s+Sergeant\s+', r'^Sergeant\s+',
            r'^Specialist\s+', r'^CPL\s+', r'^U\.S\.\s+Army\s+'
        ]
        
        for title_pattern in titles_to_remove:
            name = re.sub(title_pattern, '', name, flags=re.IGNORECASE)
        
        # Clean up trailing commas and punctuation
        name = re.sub(r'[,\.]+$', '', name)
        
        # Clean up extra spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Skip if name is too short or contains numbers
        if len(name) < 3 or re.search(r'\d', name):
            return ""
        
        # Skip common non-person names
        non_person_names = ['County', 'Veterans', 'Memorial', 'Highway', 'Bridge', 'Boulevard']
        if any(word in name for word in non_person_names):
            return ""
        
        return name
    
    def search_wikipedia(self, name: str) -> Optional[str]:
        """
        Search for a Wikipedia page for the given name.
        
        Args:
            name: Person name to search for
            
        Returns:
            Wikipedia URL if found, None otherwise
        """
        try:
            # First, try Wikipedia's search API
            search_url = "https://en.wikipedia.org/api/rest_v1/page/title/{}"
            encoded_name = quote(name.replace(' ', '_'))
            
            # Try direct page lookup first
            response = self.session.get(search_url.format(encoded_name))
            if response.status_code == 200:
                return f"https://en.wikipedia.org/wiki/{encoded_name}"
            
            # If direct lookup fails, try search API
            search_api_url = "https://en.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': name,
                'srlimit': 5
            }
            
            time.sleep(self.delay_between_requests)
            response = self.session.get(search_api_url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if 'query' in data and 'search' in data['query']:
                    results = data['query']['search']
                    
                    # Look for exact or close matches
                    for result in results:
                        title = result['title']
                        # Check if this looks like a person page
                        if self._is_likely_person_page(title, name):
                            encoded_title = quote(title.replace(' ', '_'))
                            return f"https://en.wikipedia.org/wiki/{encoded_title}"
            
            return None
            
        except Exception as e:
            print(f"Error searching for {name}: {e}")
            return None

    def _extract_context_from_designation(self, designation: str) -> Dict[str, bool]:
        """Derive expected roles from a roadway designation string."""
        text = designation.lower()
        ctx = {
            'expect_military': any(k in text for k in [
                'u.s. army', 'us army', 'marine', 'air force', 'navy', 'sgt', 'sergeant', 'cpl', 'corporal',
                'pfc', 'trooper', 'lance corporal', 'ssgt', 'staff sergeant', 'colonel', 'major', 'private'
            ]),
            'expect_law_enforcement': any(k in text for k in [
                'deputy', 'sheriff', 'officer', 'patrolman', 'police', 'trooper'
            ]),
            'expect_politician': any(k in text for k in [
                'senator', 'representative', 'congress', 'governor', 'mayor', 'commissioner', 'speaker'
            ]),
            'expect_religious': any(k in text for k in [
                'rev', 'reverend', 'pastor', 'bishop', 'rabbi', 'priest'
            ]),
            'expect_musician': any(k in text for k in [
                'musician', 'singer', 'songwriter', 'band', 'composer'
            ]),
            'expect_athlete': any(k in text for k in [
                'coach', 'olympian', 'athlete', 'football', 'basketball', 'baseball', 'soccer', 'track', 'runner'
            ]),
        }
        return ctx

    def _name_similarity(self, a: str, b: str) -> float:
        """Compute a normalized similarity between two names (0..1), nickname-aware."""
        def clean(n: str) -> str:
            return re.sub(r"[^a-zA-Z\s]", "", n.lower()).strip()
        def split_first_last(n: str) -> tuple:
            parts = [p for p in clean(n).split() if p]
            if not parts:
                return ("", "")
            if len(parts) == 1:
                return (parts[0], "")
            return (parts[0], parts[-1])
        nick_map = {
            'william': {'bill', 'billy', 'will', 'wills', 'liam'},
            'robert': {'bob', 'bobby', 'rob', 'robbie', 'bert'},
            'john': {'jon', 'jack'},
            'margaret': {'peggy', 'maggie', 'meg', 'marge'},
            'elizabeth': {'liz', 'lizzie', 'beth', 'betty', 'eliza', 'liza'},
            'james': {'jim', 'jimmy', 'jamie'},
            'michael': {'mike', 'mikey'},
            'charles': {'charlie', 'chuck'},
            'richard': {'rick', 'ricky', 'rich', 'dick'},
            'thomas': {'tom', 'tommy'},
            'stephen': {'steve', 'steven'},
            'joseph': {'joe', 'joey'},
            'andrew': {'andy', 'drew'},
            'alexander': {'alex', 'sandy'},
            'katherine': {'kate', 'kathy', 'katie', 'cathy', 'kat'},
            'edward': {'ed', 'eddie', 'ted', 'teddy', 'ned'},
            'francis': {'frank', 'frankie'},
        }
        a_clean = clean(a)
        b_clean = clean(b)
        base = SequenceMatcher(None, a_clean, b_clean).ratio()
        af, al = split_first_last(a)
        bf, bl = split_first_last(b)
        # last name mismatch reduces ceiling
        if al and bl and al != bl and al not in b_clean and bl not in a_clean:
            base = min(base, 0.6)
        # try nickname substitutions
        def variants(first: str) -> set:
            v = {first}
            if first in nick_map:
                v |= nick_map[first]
            return v
        best = base
        for av in variants(af):
            for bv in variants(bf):
                a_try = (av + ' ' + al).strip()
                b_try = (bv + ' ' + bl).strip()
                r = SequenceMatcher(None, a_try, b_try).ratio()
                if r > best:
                    best = r
        return best

    def _fetch_page_summary(self, title: str) -> Dict[str, Optional[str]]:
        """Fetch REST summary for a page title."""
        try:
            url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote(title.replace(' ', '_'))}"
            resp = self.session.get(url)
            if resp.status_code == 200:
                j = resp.json()
                return {
                    'extract': j.get('extract') or '',
                    'description': (j.get('description') or ''),
                    'type': j.get('type') or '',
                    'wikibase_item': (j.get('wikibase_item') or j.get('titles', {}).get('wikibase_item') or j.get('pageprops', {}).get('wikibase_item'))
                }
        except Exception:
            pass
        return {'extract': '', 'description': '', 'type': '', 'wikibase_item': None}

    def _fetch_wikidata_traits(self, qid: str) -> Dict[str, any]:
        """Fetch minimal traits from Wikidata: is_human, occupation labels, place labels, country."""
        traits: Dict[str, any] = {
            'is_human': False,
            'occupations': [],
            'place_labels': [],
            'countries': []
        }
        try:
            url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
            resp = self.session.get(url)
            if resp.status_code != 200:
                return traits
            data = resp.json()
            entity = data.get('entities', {}).get(qid, {})
            claims = entity.get('claims', {})

            def _ids(prop: str) -> List[str]:
                ids = []
                for c in claims.get(prop, []):
                    v = c.get('mainsnak', {}).get('datavalue', {}).get('value')
                    if isinstance(v, dict) and 'id' in v:
                        ids.append(v['id'])
                return ids

            # instance of (P31) human Q5
            if 'Q5' in _ids('P31'):
                traits['is_human'] = True

            occ_ids = _ids('P106')
            place_ids = _ids('P19') + _ids('P20')
            country_ids = _ids('P27')

            # Resolve labels (batch resolution is simpler via multiple calls kept minimal here)
            def _labels_for(qids: List[str]) -> List[str]:
                labels: List[str] = []
                for _qid in qids[:6]:  # limit to avoid excess calls
                    try:
                        u = f"https://www.wikidata.org/wiki/Special:EntityData/{_qid}.json"
                        r = self.session.get(u)
                        if r.status_code == 200:
                            ed = r.json().get('entities', {}).get(_qid, {})
                            lab = ed.get('labels', {}).get('en', {}).get('value')
                            if lab:
                                labels.append(lab)
                    except Exception:
                        continue
                return labels

            traits['occupations'] = _labels_for(occ_ids)
            traits['place_labels'] = _labels_for(place_ids)
            traits['countries'] = _labels_for(country_ids)

            return traits
        except Exception:
            return traits

    def _score_candidate(self, title: str, original_name: str, designation: str, county: str) -> Tuple[int, str]:
        """Score a candidate page by multiple signals and return (score, notes)."""
        notes: List[str] = []
        score = 0

        # Quick title/person checks
        if '(disambiguation)' in title.lower():
            notes.append('disambiguation-title')
            score -= 30

        if not self._is_likely_person_page(title, original_name):
            score -= 10

        # Strong penalties for nobility/royalty patterns or territorial titles (e.g., "Lord of Mecklenburg")
        title_l = title.lower()
        nobility_terms = [' lord ', ' duke ', ' duchess ', ' count ', ' earl ', ' prince ', ' princess ', ' marquess ', ' baron ', ' baroness ', ' king ', ' queen ']
        if any(term in f" {title_l} " for term in nobility_terms):
            score -= 40
            notes.append('nobility-title')
        if ' of mecklenburg' in title_l or ', lord of' in title_l or ' of ' in title_l and 'mecklenburg' in title_l:
            score -= 30
            notes.append('territorial-title')
        # Roman numeral immediately after first name usually indicates nobility/numbered rulers
        if re.search(r"^[a-z]+\s+[ivxlcdm]+\b", title_l):
            score -= 25
            notes.append('roman-numeral-after-first-name')

        # Name similarity
        sim = self._name_similarity(original_name, title)
        score += int(sim * 20)
        notes.append(f"name-sim:{sim:.2f}")

        # Wikipedia summary
        summary = self._fetch_page_summary(title)
        extract = (summary.get('extract') or '').lower()
        description = (summary.get('description') or '').lower()
        page_type = (summary.get('type') or '').lower()
        if page_type == 'disambiguation':
            notes.append('disambiguation-summary')
            score -= 30

        # Wikidata traits
        traits = {}
        qid = summary.get('wikibase_item')
        if qid:
            traits = self._fetch_wikidata_traits(qid)
            if traits.get('is_human'):
                score += 40
                notes.append('is-human')
            else:
                notes.append('not-human')

        # Context from designation
        ctx = self._extract_context_from_designation(designation)
        occ_text = ' '.join(traits.get('occupations', [])).lower()
        places_text = ' '.join(traits.get('place_labels', [])).lower()

        def add_if(cond: bool, val: int, tag: str):
            nonlocal score
            if cond:
                score += val
                notes.append(tag)

        # Role matches and gating
        military_hit = any(k in (extract + ' ' + description + ' ' + occ_text) for k in [
            'soldier', 'marine', 'air force', 'navy', 'army', 'military'
        ])
        law_hit = any(k in (extract + ' ' + description + ' ' + occ_text) for k in [
            'police', 'sheriff', 'deputy', 'state trooper', 'law enforcement'
        ])
        add_if(ctx['expect_military'] and military_hit, 15, 'role-military')
        add_if(ctx['expect_law_enforcement'] and law_hit, 15, 'role-law')

        add_if(ctx['expect_politician'] and any(k in (extract + ' ' + description + ' ' + occ_text) for k in [
            'politician', 'senator', 'representative', 'governor', 'mayor', 'legislator'
        ]), 15, 'role-politics')

        add_if(ctx['expect_religious'] and any(k in (extract + ' ' + description + ' ' + occ_text) for k in [
            'pastor', 'priest', 'bishop', 'minister', 'rabbi'
        ]), 15, 'role-religion')

        add_if(ctx['expect_musician'] and any(k in (extract + ' ' + description + ' ' + occ_text) for k in [
            'musician', 'singer', 'songwriter', 'composer'
        ]), 10, 'role-music')

        add_if(ctx['expect_athlete'] and any(k in (extract + ' ' + description + ' ' + occ_text) for k in [
            'football', 'basketball', 'baseball', 'soccer', 'athlete', 'coach', 'olymp'
        ]), 10, 'role-sport')

        # If designation implies modern law enforcement/military, penalize clearly historical figures
        expect_modern_service = ctx['expect_law_enforcement'] or ctx['expect_military']
        if expect_modern_service:
            # Look for years in summary/extract; penalize if pre-1900 context dominates
            years = re.findall(r"(1[5-9]\d{2}|20\d{2}|19\d{2})", extract)
            # If any years exist and max year < 1900, likely historical figure
            try:
                if years:
                    max_year = max(int(y) for y in years)
                    if max_year < 1900:
                        score -= 35
                        notes.append('historical-pre1900')
            except Exception:
                pass

        # Tighten name structure: middle initial mismatches and roman numerals vs middle initials
        # Extract simple first/middle-initial/last from original name
        parts = re.findall(r"[A-Za-z]+|[A-Za-z]\.", original_name)
        if len(parts) >= 2:
            first = parts[0].lower()
            last = parts[-1].lower()
            if last not in title_l:
                score -= 10
                notes.append('last-name-missing')
            if first not in title_l:
                # allow common variants like 'william' vs 'bill' not handled; small penalty
                score -= 5
                notes.append('first-name-missing')
            # If original includes a middle initial, prefer the same letter near the first name
            if len(parts) >= 3 and re.match(r"^[a-zA-Z]\.$", parts[1]):
                mid_initial = parts[1][0].lower()
                # penalize if title shows a different roman numeral right after first name
                if re.search(rf"^{first}\s+[ivxlcdm]+\b", title_l):
                    score -= 30
                    notes.append('roman-vs-middle-initial')
                # penalize if a different single-letter initial appears near first name
                nearby = re.search(rf"{first}\s+([a-z])\.", title_l)
                if nearby and nearby.group(1) != mid_initial:
                    score -= 10
                    notes.append('middle-initial-mismatch')

        # Florida / locality signals
        combined_text = extract + ' ' + description + ' ' + places_text
        add_if('florida' in combined_text, 10, 'mentions-florida')
        add_if(county and county.lower() in combined_text, 8, 'mentions-county')
        # Penalize when Florida/county context is missing (Florida dataset)
        if 'florida' not in combined_text:
            score -= 10
            notes.append('no-florida-penalty')
        if county and county.lower() not in combined_text:
            score -= 8
            notes.append('no-county-penalty')
        add_if('united states' in combined_text or 'american' in combined_text, 5, 'mentions-us')

        # Penalize obvious non-person indicators in summary/description
        non_person_indicators = [
            'highway', 'road', 'bridge', 'building', 'school', 'hospital', 'company', 'organization',
            'band', 'album', 'song', 'movie', 'book', 'tv series', 'park', 'city', 'county', 'river'
        ]
        if any(k in (title.lower() + ' ' + combined_text) for k in non_person_indicators):
            score -= 15
            notes.append('non-person-hint')

        # Enforce role gating: if designation expects LE/military but no hit, apply strong penalty
        if ctx['expect_law_enforcement'] and not law_hit:
            score -= 50
            notes.append('law-gate-fail')
        if ctx['expect_military'] and not military_hit:
            score -= 50
            notes.append('military-gate-fail')

        return score, ','.join(notes)

    def search_wikipedia_with_validation(self, name: str, designation: str, county: str) -> Tuple[Optional[str], int, str, Optional[str]]:
        """Search Wikipedia and validate candidates, returning best URL, confidence, and notes."""
        try:
            candidates: List[str] = []

            # Try direct title
            direct_url = self.search_wikipedia(name)
            if direct_url:
                candidates.append(direct_url)

            # Also enumerate search results for broader scoring
            search_api_url = "https://en.wikipedia.org/w/api.php"
            # Build context-aware query variants
            ctx = self._extract_context_from_designation(designation)
            query_variants: List[str] = [name]
            # Add role/context boosters
            if ctx['expect_law_enforcement']:
                query_variants.extend([
                    f'"{name}" deputy', f'"{name}" sheriff', f'"{name}" trooper',
                    f'"{name}" law enforcement'
                ])
            if ctx['expect_military']:
                query_variants.extend([f'"{name}" soldier', f'"{name}" marine', f'"{name}" army'])
            if county:
                query_variants.append(f'"{name}" {county}')
            query_variants.append(f'"{name}" Florida')

            seen_titles_for_query: set = set()
            for q in query_variants:
                params = {
                    'action': 'query',
                    'format': 'json',
                    'list': 'search',
                    'srsearch': q,
                    'srlimit': 10,
                    'srwhat': 'text',
                }
                time.sleep(self.delay_between_requests)
                response = self.session.get(search_api_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    for result in data.get('query', {}).get('search', []):
                        title = result.get('title')
                        if title and title not in seen_titles_for_query:
                            seen_titles_for_query.add(title)
                            candidates.append(f"https://en.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}")

            best_url = None
            best_score = -999
            best_notes = ''
            best_title: Optional[str] = None
            seen_titles = set()
            for url in candidates:
                try:
                    title = url.split('/wiki/', 1)[1].replace('_', ' ')
                except Exception:
                    continue
                if title in seen_titles:
                    continue
                seen_titles.add(title)

                score, notes = self._score_candidate(title, name, designation, county)
                if score > best_score:
                    best_score = score
                    best_notes = notes
                    best_url = url

            # Raise acceptance threshold to reduce false positives
            accepted_url = best_url if best_score >= 75 else None
            # Keep best title for review
            if best_url:
                try:
                    best_title = best_url.split('/wiki/', 1)[1].replace('_', ' ')
                except Exception:
                    best_title = None
            return accepted_url, max(best_score, 0), best_notes, best_title
        except Exception as e:
            return None, 0, f"error:{e}", None
    
    def _is_likely_person_page(self, title: str, original_name: str) -> bool:
        """
        Determine if a Wikipedia page title likely refers to a person.
        
        Args:
            title: Wikipedia page title
            original_name: Original name we searched for
            
        Returns:
            True if likely a person page, False otherwise
        """
        # Skip disambiguation pages
        if '(disambiguation)' in title.lower():
            return False
        
        # Skip pages that are clearly not about people
        non_person_indicators = [
            'highway', 'road', 'bridge', 'building', 'school', 'hospital',
            'company', 'organization', 'band', 'album', 'song', 'movie',
            'book', 'tv series', 'county', 'city', 'state'
        ]
        
        title_lower = title.lower()
        if any(indicator in title_lower for indicator in non_person_indicators):
            return False
        
        # Check if the names match reasonably well
        original_words = set(original_name.lower().split())
        title_words = set(title.lower().split())
        
        # At least half the words should match
        if len(original_words & title_words) >= len(original_words) * 0.5:
            return True
        
        return False
    
    def process_csv(self, input_file: str, output_file: str) -> None:
        """
        Process the memorial designations CSV and create output with Wikipedia links.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
        """
        results = []
        processed_names = set()  # Avoid duplicate searches
        
        print(f"Reading {input_file}...")
        
        try:
            # Read the CSV file using standard library
            with open(input_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
            
            print(f"Found {len(rows)} memorial designations")
            print("Extracting names and searching Wikipedia...")
            
            review_results = []
            for row in rows:
                designation = str(row.get('DESIGNATIO', ''))
                county = str(row.get('COUNTY', ''))
                
                if not designation or designation == 'nan':
                    continue
                
                # Extract names from this designation
                names = self.extract_person_names(designation)
                
                for name in names:
                    if name in processed_names:
                        continue  # Skip if we've already processed this name
                    
                    processed_names.add(name)
                    print(f"Searching for: {name}")
                    wikipedia_url, conf, notes, best_title = self.search_wikipedia_with_validation(name, designation, county)
                    validated = 'yes' if wikipedia_url else 'no'

                    result = {
                        'name': name,
                        'original_designation': designation,
                        'county': county,
                        'wikipedia_url': wikipedia_url if wikipedia_url else 'Not Found',
                        'status': 'Found' if wikipedia_url else 'Not Found',
                        'match_confidence': conf,
                        'validated': validated,
                        'validation_notes': notes
                    }
                    
                    results.append(result)
                    # Collect low-confidence or rejected candidates for review
                    if (not wikipedia_url and conf > 0) or (wikipedia_url and conf < 85):
                        review_results.append({
                            'name': name,
                            'original_designation': designation,
                            'county': county,
                            'candidate_title': best_title or '',
                            'candidate_url': f"https://en.wikipedia.org/wiki/{quote(best_title.replace(' ', '_'))}" if best_title else '',
                            'match_confidence': conf,
                            'validation_notes': notes
                        })
                    
                    # Progress indicator
                    if len(results) % 10 == 0:
                        print(f"Processed {len(results)} names...")
            
            # Write output CSV using standard library
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                if results:
                    fieldnames = [
                        'name', 'original_designation', 'county', 'wikipedia_url', 'status',
                        'match_confidence', 'validated', 'validation_notes'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(results)

            # Write review CSV for manual curation
            if review_results:
                review_file = output_file.replace('.csv', '_review.csv')
                with open(review_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = [
                        'name', 'original_designation', 'county', 'candidate_title', 'candidate_url',
                        'match_confidence', 'validation_notes'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(review_results)
            
            # Print summary
            found_count = len([r for r in results if r['status'] == 'Found'])
            total_count = len(results)
            
            print(f"\n{'='*60}")
            print("PROCESSING COMPLETE")
            print(f"{'='*60}")
            print(f"Total unique names extracted: {total_count}")
            print(f"Wikipedia pages found: {found_count}")
            print(f"Not found: {total_count - found_count}")
            print(f"Success rate: {found_count/total_count*100:.1f}%" if total_count > 0 else "No names processed")
            print(f"Results saved to: {output_file}")
            
        except Exception as e:
            print(f"Error processing CSV: {e}")
            raise


def main():
    """Main function to run the Wikipedia link finder."""
    input_file = "Memorial_Roadway_Designations.csv"
    output_file = "memorial_names_with_wikipedia_links.csv"
    
    print("Memorial Roadway Wikipedia Link Finder")
    print("=" * 50)
    
    finder = WikipediaLinkFinder()
    finder.process_csv(input_file, output_file)


if __name__ == "__main__":
    main()
