#!/usr/bin/env python3
"""
Enhanced Memorial Data with Rate Limit Handling

This script is an improved version that handles API rate limits gracefully
with exponential backoff and retry logic. It extracts comprehensive 
biographical data including all new fields.

New columns added:
- Summary (4-sentence biographical summary)
- DOB (Date of Birth)
- DOD (Date of Death)
- Education
- Place of Birth
- Place of Death
- Gender
- Involved in Sports (yes/no)
- Involved in Politics (yes/no)
- Involved in Military (yes/no)
- Involved in Music (yes/no)
"""

import csv
import sys
import os
import time
import random
import re
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
try:
    from dotenv import load_dotenv
    _DOTENV_AVAILABLE = True
except Exception:
    _DOTENV_AVAILABLE = False

# Add the parent directory to the path so we can import the Person class
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from person_summarizer import Person
from person_summarizer import PersonSummarizer


class WikipediaResolver:
    """Resolves person names to Wikipedia URLs and extracts names from designations."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Memorial-Roadway-Wikipedia-Finder/1.0 (Educational Research)'
        })
        self.processed_names: Dict[str, Optional[str]] = {}
        # Allow threshold override via env var ACCEPTANCE_THRESHOLD
        try:
            self.acceptance_threshold = int(os.getenv('ACCEPTANCE_THRESHOLD', '0'))
        except Exception:
            self.acceptance_threshold = 0
        # Nickname map for name similarity
        self.nickname_map = {
            'william': {'bill', 'billy', 'will', 'willy'},
            'robert': {'bob', 'bobby', 'rob', 'robbie', 'bert'},
            'richard': {'rick', 'ricky', 'dick', 'rich', 'richie'},
            'johnathan': {'john', 'jon'}, 'jonathan': {'john', 'jon'}, 'john': {'jon'},
            'michael': {'mike', 'mikey'},
            'james': {'jim', 'jimmy', 'jamie'},
            'edward': {'ed', 'eddie', 'ted', 'teddy', 'ned'},
            'charles': {'charlie', 'chuck'},
            'joseph': {'joe', 'joey'},
            'andrew': {'andy', 'drew'},
            'stephen': {'steve', 'stevie'}, 'steven': {'steve', 'stevie'},
            'margaret': {'meg', 'maggie', 'peggy'},
            'elizabeth': {'liz', 'lizzy', 'beth', 'eliza', 'betty', 'betsy'},
        }

    def extract_person_names(self, designation: str) -> List[str]:
        names: List[str] = []

        # Skip generic memorial types
        generic_terms = [
            'Veterans Memorial', 'Gold Star Family Memorial', 'Submarine Veterans Memorial',
            'Hope and Healing Highway', 'County Veterans Memorial'
        ]
        if any(term in designation for term in generic_terms):
            return names

        # Multiple names: "Deputies Name1 and Name2"
        m = re.search(r'Deputies\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\s+and\s+([A-Z][a-z]+\s+[A-Z][a-z]+)', designation)
        if m:
            for g in [1, 2]:
                cleaned = self._clean_name(m.group(g))
                if cleaned:
                    names.append(cleaned)
            if names:
                return names

        # Jr./Sr.
        m = re.search(r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+),\s+(?:Jr\.?|Sr\.?)', designation)
        if m:
            cleaned = self._clean_name(m.group(1))
            if cleaned:
                return [cleaned]

        # dedicated to ... Name
        m = re.search(r'dedicated to (?:U\.S\.\s+Army\s+)?(?:CPL|Sergeant|Lieutenant|Captain|Major|Colonel|General)\s+([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+)', designation)
        if m:
            cleaned = self._clean_name(m.group(1))
            if cleaned:
                return [cleaned]

        # General approach: strip rank/title prefix, take text before road keyword
        road_keywords = ['Highway', 'Boulevard', 'Bridge', 'Drive', 'Way', 'Street']
        for kw in road_keywords:
            if kw in designation:
                left = designation.split(kw)[0].strip()
                prefixes = [
                    'Staff Sergeant', 'Sergeant', 'Specialist', 'Sheriff', 'Deputy',
                    'Dr.', 'Dr', 'Rev. Dr.', 'Rev Dr', 'Rev.', 'Rev', 'CPL', 'Captain',
                    'Lieutenant', 'Colonel', 'Major', 'General'
                ]
                for p in prefixes:
                    if left.startswith(p + ' '):
                        left = left[len(p):].strip()
                        break
                if left.endswith(' Memorial'):
                    left = left[:-9].strip()
                if self._looks_like_person_name(left):
                    cleaned = self._clean_name(left)
                    if cleaned:
                        names.append(cleaned)
                        break
        return names

    def resolve_best(self, name: str, designation: str, county: str) -> Dict[str, Optional[str]]:
        """Return best candidate with scoring, validation, and notes."""
        import difflib

        # Helper: expected roles from designation
        def expected_roles(text: str) -> Dict[str, bool]:
            t = text.lower()
            return {
                'law_enforcement': any(k in t for k in ['sheriff', 'deputy', 'police', 'trooper', 'law enforcement']),
                'military': any(k in t for k in ['sergeant', 'staff sergeant', 'specialist', 'cpl', 'marine', 'navy', 'army', 'air force', 'military', 'soldier']),
                'politician': any(k in t for k in ['governor', 'senator', 'representative', 'speaker', 'politician']),
                'religious': any(k in t for k in ['rev.', 'rev ', 'reverend', 'pastor', 'bishop', 'priest']),
                'musician': any(k in t for k in ['musician', 'singer', 'composer', 'guitarist']),
                'athlete': any(k in t for k in ['athlete', 'football', 'basketball', 'baseball', 'soccer', 'olympic'])
            }

        roles = expected_roles(designation)

        # Build queries: base + context-boosted variants
        queries = [name]
        if county:
            queries.append(f"{name} {county}")
        queries.append(f"{name} Florida")
        if roles['law_enforcement']:
            queries.append(f"{name} sheriff deputy police law enforcement")
        if roles['military']:
            queries.append(f"{name} military army navy air force marine")
        if roles['politician']:
            queries.append(f"{name} politician florida")
        if roles['religious']:
            queries.append(f"{name} reverend pastor bishop")
        if roles['musician']:
            queries.append(f"{name} musician singer")
        if roles['athlete']:
            queries.append(f"{name} athlete sports")

        # Collect candidates from all queries
        candidates: Dict[str, Dict[str, Optional[str]]] = {}

        def add_candidate(title: str, pageid: int, snippet: str):
            if title not in candidates:
                candidates[title] = {'title': title, 'pageid': pageid, 'snippet': snippet, 'extract': None}

        # Direct title attempt
        try:
            encoded = quote(name.replace(' ', '_'))
            resp = self.session.get(f"https://en.wikipedia.org/api/rest_v1/page/title/{encoded}")
            if resp.status_code == 200:
                add_candidate(name, -1, '')
        except Exception:
            pass

        for q in queries:
            try:
                params = {
                    'action': 'query', 'format': 'json', 'list': 'search', 'srsearch': q, 'srlimit': 20
                }
                resp = self.session.get("https://en.wikipedia.org/w/api.php", params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    for r in data.get('query', {}).get('search', []):
                        add_candidate(r.get('title', ''), r.get('pageid', -1), r.get('snippet', ''))
            except Exception:
                continue

        # Also pull in candidates from Wikidata (EntitySearch), then add enwiki sitelinks if present
        try:
            wd = self.session.get(
                "https://www.wikidata.org/w/api.php",
                params={
                    'action': 'wbsearchentities', 'format': 'json', 'language': 'en', 'uselang': 'en',
                    'type': 'item', 'search': name, 'limit': 10
                }, timeout=6
            )
            if wd.status_code == 200:
                for item in wd.json().get('search', []):
                    qid = item.get('id')
                    # Fetch sitelinks to get the enwiki title if any
                    if not qid:
                        continue
                    ent = self.session.get(
                        f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json", timeout=6
                    )
                    if ent.status_code != 200:
                        continue
                    entities = ent.json().get('entities', {})
                    entdata = entities.get(qid, {})
                    sitelinks = entdata.get('sitelinks', {})
                    enwiki = sitelinks.get('enwiki', {})
                    title = enwiki.get('title')
                    if title:
                        add_candidate(title, -1, item.get('description', ''))
        except Exception:
            pass

        # Fetch extracts and wikidata ids for candidates (batch by titles)
        titles = [t for t in candidates if t]
        if titles:
            # Extracts
            try:
                params = {
                    'action': 'query', 'format': 'json', 'prop': 'extracts|pageprops', 'explaintext': 1, 'exintro': 1,
                    'titles': '|'.join(titles)
                }
                resp = self.session.get("https://en.wikipedia.org/w/api.php", params=params)
                if resp.status_code == 200:
                    q = resp.json().get('query', {})
                    pages = q.get('pages', {})
                    for page in pages.values():
                        title = page.get('title')
                        if title in candidates:
                            candidates[title]['extract'] = page.get('extract', '')
                            candidates[title]['wikibase_item'] = page.get('pageprops', {}).get('wikibase_item')
            except Exception:
                pass

        # Wikidata human check
        def is_human_wikidata(qid: Optional[str]) -> bool:
            if not qid:
                return False
            try:
                wd = self.session.get(f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json", timeout=6)
                if wd.status_code != 200:
                    return False
                entities = wd.json().get('entities', {})
                ent = entities.get(qid, {})
                for cl in ent.get('claims', {}).get('P31', []):
                    val = cl.get('mainsnak', {}).get('datavalue', {}).get('value', {})
                    if val.get('id') == 'Q5':
                        return True
            except Exception:
                return False
            return False

        # Name similarity (nickname-aware)
        def name_similarity_points(target: str, candidate_title: str) -> (float, List[str]):
            notes = []
            # strip parentheses and disambiguators
            ct = re.sub(r"\s*\(.*?\)$", "", candidate_title).strip()
            t_norm = re.sub(r"\s+", " ", target).strip()
            ct_norm = re.sub(r"\s+", " ", ct).strip()

            # SequenceMatcher
            sm = difflib.SequenceMatcher(None, t_norm.lower(), ct_norm.lower())
            seq_score = sm.ratio() * 20
            notes.append(f"seq={seq_score:.1f}")

            # Nickname-aware bonus
            def tokens(s: str) -> List[str]:
                return re.findall(r"[A-Za-z]+", s.lower())
            t_tokens = tokens(t_norm)
            c_tokens = tokens(ct_norm)

            bonus = 0.0
            if t_tokens and c_tokens:
                t_first, t_last = t_tokens[0], t_tokens[-1]
                c_first, c_last = c_tokens[0], c_tokens[-1]
                if t_last == c_last:
                    bonus += 6
                else:
                    notes.append("last-mismatch:-4")
                    bonus -= 4
                # nickname map
                nickset = self.nickname_map.get(t_first, set())
                if c_first == t_first or c_first in nickset:
                    bonus += 6
                else:
                    # small penalty
                    bonus -= 2
            total = max(0.0, min(20.0, seq_score + bonus))
            return total, notes

        # Role/context points
        def role_context_points(text: str) -> (float, List[str]):
            notes = []
            tx = (text or '').lower()
            pts = 0.0
            if roles['law_enforcement']:
                if any(k in tx for k in ['police', 'sheriff', 'deputy', 'state trooper', 'law enforcement']):
                    pts += 15; notes.append("le:+15")
                else:
                    pts -= 50; notes.append("le-missing:-50")
            if roles['military']:
                if any(k in tx for k in ['soldier', 'marine', 'army', 'navy', 'air force', 'military']):
                    pts += 15; notes.append("mil:+15")
                else:
                    pts -= 50; notes.append("mil-missing:-50")
            if roles['politician'] and any(k in tx for k in ['politician', 'governor', 'senator', 'representative', 'speaker']):
                pts += 15; notes.append("pol:+15")
            if roles['religious'] and any(k in tx for k in ['reverend', 'pastor', 'bishop', 'priest']):
                pts += 15; notes.append("rel:+15")
            if roles['musician'] and any(k in tx for k in ['musician', 'singer', 'composer', 'guitarist']):
                pts += 10; notes.append("mus:+10")
            if roles['athlete'] and any(k in tx for k in ['athlete', 'football', 'basketball', 'baseball', 'soccer', 'olympic']):
                pts += 10; notes.append("ath:+10")
            return pts, notes

        # Location points
        def location_points(text: str) -> (float, List[str]):
            notes = []
            tx = (text or '').lower()
            pts = 0.0
            if 'florida' in tx:
                pts += 10; notes.append("fl:+10")
            else:
                pts -= 10; notes.append("fl-miss:-10")
            if county and county.lower() in tx:
                pts += 8; notes.append("county:+8")
            else:
                if county:
                    pts -= 8; notes.append("county-miss:-8")
            if any(k in tx for k in ['united states', 'american']):
                pts += 5; notes.append("us:+5")
            return pts, notes

        def penalty_points(title: str, text: str) -> (float, List[str]):
            notes = []
            tl = title.lower()
            tx = (text or '').lower()
            pts = 0.0
            if '(disambiguation)' in tl:
                pts -= 30; notes.append("disamb:-30")
            if any(k in tl for k in ['lord', 'duke', 'earl', 'baron', 'marquess', 'viscount']) or ' of ' in tl:
                pts -= 40; notes.append("nobility:-40")
            if re.search(r'\b[ivxlcdm]{1,4}\b', title.split(' ')[1].lower() if len(title.split(' ')) > 1 else ''):
                pts -= 25; notes.append("roman:-25")
            if any(k in tl for k in ['highway', 'bridge', 'boulevard', 'road', 'building']):
                pts -= 15; notes.append("nonperson:-15")
            # Historical filter: look for years
            years = [int(y) for y in re.findall(r'\b(1[5-9]\d{2}|20\d{2}|19\d{2})\b', tx)]
            if years and max(years) < 1900 and (roles['law_enforcement'] or roles['military']):
                pts -= 35; notes.append("pre1900:-35")
            return pts, notes

        # Score candidates
        best = None
        review_candidates = []
        for title, c in candidates.items():
            snippet = c.get('snippet') or ''
            extract = c.get('extract') or ''
            text = ' '.join([snippet, extract])

            ns, nnotes = name_similarity_points(name, title)
            human = 40.0 if is_human_wikidata(c.get('wikibase_item')) else 0.0
            if human:
                nnotes.append("human:+40")
            rpts, rnotes = role_context_points(text)
            lpts, lnotes = location_points(text)
            ppts, pnotes = penalty_points(title, text)

            score = ns + human + rpts + lpts + ppts
            score = max(0.0, min(100.0, score))
            notes = nnotes + rnotes + lnotes + pnotes

            enc = quote(title.replace(' ', '_'))
            url = f"https://en.wikipedia.org/wiki/{enc}"

            cand_row = {
                'title': title,
                'url': url,
                'score': score,
                'notes': '; '.join(notes)
            }
            review_candidates.append(cand_row)
            if best is None or score > best['score']:
                best = cand_row

        # Decide
        if best and best['score'] >= self.acceptance_threshold:
            res = {
                'wikipedia_url': best['url'],
                'status': 'Found',
                'match_confidence': f"{best['score']:.1f}",
                'validated': 'true',
                'validation_notes': best['notes'],
                'candidate_title': best['title'],
                'review_candidates': review_candidates
            }
        else:
            res = {
                'wikipedia_url': 'Not Found',
                'status': 'Not Found',
                'match_confidence': f"{best['score']:.1f}" if best else "0.0",
                'validated': 'false',
                'validation_notes': best['notes'] if best else 'no-candidates',
                'candidate_title': best['title'] if best else '',
                'review_candidates': review_candidates
            }
        return res

    def _is_likely_person_page(self, title: str, original_name: str) -> bool:
        if '(disambiguation)' in title.lower():
            return False
        non_person = ['highway', 'road', 'bridge', 'building', 'school', 'hospital',
                      'company', 'organization', 'band', 'album', 'song', 'movie',
                      'book', 'tv series', 'county', 'city', 'state']
        tl = title.lower()
        if any(tok in tl for tok in non_person):
            return False
        ow = set(original_name.lower().split())
        tw = set(title.lower().split())
        return len(ow & tw) >= max(1, int(len(ow) * 0.5))

    def _looks_like_person_name(self, text: str) -> bool:
        if not text or len(text) < 3:
            return False
        if re.search(r'\d', text):
            return False
        words = text.split()
        if len(words) < 2:
            return False
        generic = ['County', 'Veterans', 'Memorial', 'Family', 'Star', 'Hope', 'Healing']
        if any(t in text for t in generic):
            return False
        return all(w[0].isupper() for w in words if w)

    def _clean_name(self, name: str) -> str:
        name = re.sub(r'^\s+', '', name)
        name = re.sub(r'[,\.]+$', '', name)
        return re.sub(r'\s+', ' ', name).strip()


class RateLimitHandler:
    """Handles API rate limits with exponential backoff."""
    
    def __init__(self, base_delay: float = 2.0, max_retries: int = 3):
        self.base_delay = base_delay
        self.max_retries = max_retries
        self.last_request_time = 0
    
    def wait_before_request(self):
        """Wait appropriate time before making API request."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.base_delay:
            sleep_time = self.base_delay - time_since_last
            print(f"   → Waiting {sleep_time:.1f}s to respect rate limits...")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def handle_rate_limit_error(self, attempt: int) -> bool:
        """
        Handle rate limit error with exponential backoff.
        
        Returns:
            True if should retry, False if max retries reached
        """
        if attempt >= self.max_retries:
            return False
        
        # Exponential backoff: 5s, 15s, 45s
        wait_time = 5 * (3 ** attempt) + random.uniform(0, 5)
        print(f"   → Rate limit hit! Waiting {wait_time:.1f}s before retry {attempt + 1}/{self.max_retries}...")
        time.sleep(wait_time)
        return True


class ImprovedMemorialEnhancer:
    """Enhanced memorial data processor with rate limit handling."""
    
    def __init__(self):
        self.rate_limiter = RateLimitHandler()
        self.processed_count = 0
        self.success_count = 0
        self.rate_limit_count = 0
        self.error_count = 0
        self.skipped_count = 0
    
    def process_person_with_retry(self, wikipedia_url: str, name: str) -> Dict[str, Optional[str]]:
        """
        Process a person with retry logic for rate limits and extract all biographical fields.
        
        Args:
            wikipedia_url: Wikipedia URL to process
            name: Person's name for logging
            
        Returns:
            Dictionary with all biographical data
        """
        result = {
            'summary': "",
            'education': "",
            'dob': "",
            'dod': "",
            'place_of_birth': "",
            'place_of_death': "",
            'gender': "",
            'involved_in_sports': "",
            'involved_in_politics': "",
            'involved_in_military': "",
            'involved_in_music': ""
        }
        
        for attempt in range(self.rate_limiter.max_retries + 1):
            try:
                # Wait before making request
                self.rate_limiter.wait_before_request()
                
                # Create Person object and extract all biographical data
                person = Person(wikipedia_url)
                
                summary = person.getSummary()
                education = person.getEducation()
                dob = person.getDOB()
                dod = person.getDOD()
                place_of_birth = person.getPlaceOfBirth()
                place_of_death = person.getPlaceOfDeath()
                gender = person.getGender()
                sports = person.getInvolvedInSports()
                politics = person.getInvolvedInPolitics()
                military = person.getInvolvedInMilitary()
                music = person.getInvolvedInMusic()
                
                # Format all results
                result['summary'] = summary if summary else ""
                result['education'] = str(education) if education else ""
                result['dob'] = dob if dob else ""
                result['dod'] = dod if dod else ""
                result['place_of_birth'] = place_of_birth if place_of_birth else ""
                result['place_of_death'] = place_of_death if place_of_death else ""
                result['gender'] = gender if gender else ""
                result['involved_in_sports'] = sports if sports else ""
                result['involved_in_politics'] = politics if politics else ""
                result['involved_in_military'] = military if military else ""
                result['involved_in_music'] = music if music else ""
                
                self.success_count += 1
                return result
                
            except Exception as e:
                error_msg = str(e)
                
                # Check if it's a rate limit error
                if "429" in error_msg or "rate" in error_msg.lower() or "capacity exceeded" in error_msg.lower():
                    self.rate_limit_count += 1
                    print(f"   → Rate limit error for {name}")
                    
                    if self.rate_limiter.handle_rate_limit_error(attempt):
                        continue  # Retry
                    else:
                        print(f"   → Max retries reached for {name}")
                        break
                else:
                    # Non-rate limit error
                    print(f"   → Error processing {name}: {error_msg}")
                    break
        
        # If we get here, all retries failed
        self.error_count += 1
        return result
    
    def enhance_csv_with_retry(self, input_file: str, output_file: str, max_entries: Optional[int] = None):
        """Enhanced CSV processing with inline Wikipedia lookup and rate limit handling."""
        
        print(f"Reading {input_file}...")
        
        # Read input CSV
        with open(input_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            raw_rows = list(reader)
        print(f"Found {len(raw_rows)} input rows")

        resolver = WikipediaResolver()

        # Build person-level rows with fresh Wikipedia lookup
        person_rows: List[Dict[str, str]] = []
        review_rows: List[Dict[str, str]] = []
        for row in raw_rows:
            designation = row.get('DESIGNATIO') or row.get('original_designation') or ''
            county = row.get('COUNTY') or row.get('county') or ''
            # Prefer explicit name if present; else extract from designation
            candidate_names: List[str] = []
            if row.get('name'):
                candidate_names = [row['name']]
            else:
                if designation:
                    candidate_names = resolver.extract_person_names(designation)

            for name in candidate_names:
                resolved = resolver.resolve_best(name, designation, county)
                person_rows.append({
                    'name': name,
                    'original_designation': designation,
                    'county': county,
                    'wikipedia_url': resolved['wikipedia_url'],
                    'status': resolved['status'],
                    'match_confidence': resolved['match_confidence'],
                    'validated': resolved['validated'],
                    'validation_notes': resolved['validation_notes']
                })
                if resolved['status'] == 'Not Found':
                    review_rows.append({
                        'name': name,
                        'original_designation': designation,
                        'county': county,
                        'candidate_title': resolved.get('candidate_title', ''),
                        'match_confidence': resolved['match_confidence'],
                        'validation_notes': resolved['validation_notes']
                    })

        print(f"Built {len(person_rows)} person entries from input")

        # Optionally limit processing
        rows = person_rows
        if max_entries:
            rows = rows[:max_entries]
            print(f"Processing first {len(rows)} entries for demo")
        
        enhanced_rows = []
        
        for i, row in enumerate(rows, 1):
            # Create enhanced row with all new columns
            enhanced_row = row.copy()
            enhanced_row['summary'] = ""
            enhanced_row['education'] = ""
            enhanced_row['dob'] = ""
            enhanced_row['dod'] = ""
            enhanced_row['place_of_birth'] = ""
            enhanced_row['place_of_death'] = ""
            enhanced_row['gender'] = ""
            enhanced_row['involved_in_sports'] = ""
            enhanced_row['involved_in_politics'] = ""
            enhanced_row['involved_in_military'] = ""
            enhanced_row['involved_in_music'] = ""
            # Ensure confidence/validation columns exist from resolver phase
            enhanced_row['match_confidence'] = row.get('match_confidence', "")
            enhanced_row['validated'] = row.get('validated', "")
            enhanced_row['validation_notes'] = row.get('validation_notes', "")
            
            # Only process if we have a Wikipedia URL
            if row.get('status') == 'Found' and row.get('wikipedia_url') != 'Not Found':
                if max_entries and self.processed_count >= max_entries:
                    enhanced_rows.append(enhanced_row)
                    continue
                
                wikipedia_url = row['wikipedia_url']
                name = row['name']
                
                print(f"{i}. Processing: {name}")
                
                # Process with retry logic
                bio_data = self.process_person_with_retry(wikipedia_url, name)
                
                # Add all biographical data to enhanced row
                enhanced_row['summary'] = bio_data['summary']
                enhanced_row['education'] = bio_data['education']
                enhanced_row['dob'] = bio_data['dob']
                enhanced_row['dod'] = bio_data['dod']
                enhanced_row['place_of_birth'] = bio_data['place_of_birth']
                enhanced_row['place_of_death'] = bio_data['place_of_death']
                enhanced_row['gender'] = bio_data['gender']
                enhanced_row['involved_in_sports'] = bio_data['involved_in_sports']
                enhanced_row['involved_in_politics'] = bio_data['involved_in_politics']
                enhanced_row['involved_in_military'] = bio_data['involved_in_military']
                enhanced_row['involved_in_music'] = bio_data['involved_in_music']
                
                self.processed_count += 1
                
                # Progress update
                if self.processed_count % 5 == 0:
                    print(f"   📊 Progress: {self.processed_count} processed, {self.success_count} successful, {self.rate_limit_count} rate limited")
            else:
                self.skipped_count += 1
            
            enhanced_rows.append(enhanced_row)
        
        # Write enhanced CSV (ensure final DOB/DOD normalization before write)
        print(f"\nWriting enhanced data to {output_file}...")
        
        # Local date normalizer (avoids requiring Mistral API key for write-out)
        def _normalize_date_local(date_str: Optional[str]) -> Optional[str]:
            if not date_str:
                return None
            s = str(date_str).strip()
            if s.lower() in {"null", "none", "not found", "unknown", "n/a", "na"}:
                return None
            m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
            if m:
                return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            m = re.match(r'^(\d{4})-(\d{2})$', s)
            if m:
                return f"{m.group(1)}-{m.group(2)}-01"
            m = re.match(r'^(\d{4})$', s)
            if m:
                return f"{m.group(1)}-01-01"
            month_map = {
                'jan': '01', 'january': '01', 'feb': '02', 'february': '02', 'mar': '03', 'march': '03',
                'apr': '04', 'april': '04', 'may': '05', 'jun': '06', 'june': '06', 'jul': '07', 'july': '07',
                'aug': '08', 'august': '08', 'sep': '09', 'sept': '09', 'september': '09', 'oct': '10', 'october': '10',
                'nov': '11', 'november': '11', 'dec': '12', 'december': '12'
            }
            s_clean = re.sub(r'[,.]', '', s).lower()
            m = re.match(r'^(\d{1,2})\s+([a-z]+)\s+(\d{4})$', s_clean)
            if m and m.group(2) in month_map:
                day = int(m.group(1))
                month = month_map[m.group(2)]
                return f"{m.group(3)}-{month}-{day:02d}"
            m = re.match(r'^([a-z]+)\s+(\d{4})$', s_clean)
            if m and m.group(1) in month_map:
                month = month_map[m.group(1)]
                return f"{m.group(2)}-{month}-01"
            m = re.match(r'^(\d{1,2})\s+([a-z]+)\s*(\d{2})?$', s_clean)
            if m and m.group(2) in month_map and m.group(3):
                day = int(m.group(1))
                month = month_map[m.group(2)]
                year = int(m.group(3))
                year = 1900 + year if year >= 50 else 2000 + year
                return f"{year}-{month}-{day:02d}"
            return None

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            if enhanced_rows:
                fieldnames = ['name', 'original_designation', 'county', 'wikipedia_url', 'status',
                              'match_confidence', 'validated', 'validation_notes',
                              'summary', 'education', 'dob', 'dod', 'place_of_birth', 'place_of_death',
                              'gender', 'involved_in_sports', 'involved_in_politics', 'involved_in_military',
                              'involved_in_music']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                # Final normalization pass for dates to catch any remaining edge-cases (no API key required)
                for row in enhanced_rows:
                    row['dob'] = _normalize_date_local(row.get('dob')) if row.get('dob') else None
                    row['dod'] = _normalize_date_local(row.get('dod')) if row.get('dod') else None
                writer.writerows(enhanced_rows)

        # Write review file for low-confidence/not-found
        review_path = os.path.join(os.path.dirname(output_file), 'output_review.csv')
        if review_rows:
            with open(review_path, 'w', newline='', encoding='utf-8') as rf:
                rfields = ['name', 'original_designation', 'county', 'candidate_title', 'match_confidence', 'validation_notes']
                rwriter = csv.DictWriter(rf, fieldnames=rfields)
                rwriter.writeheader()
                rwriter.writerows(review_rows)
        
        # Print final summary
        print(f"\n{'='*60}")
        print("ENHANCEMENT COMPLETE")
        print(f"{'='*60}")
        print(f"Total entries: {len(rows)}")
        print(f"Entries processed: {self.processed_count}")
        print(f"Successfully enhanced: {self.success_count}")
        print(f"Rate limit errors: {self.rate_limit_count}")
        print(f"Other errors: {self.error_count}")
        print(f"Skipped (no Wikipedia): {self.skipped_count}")
        if self.processed_count > 0:
            print(f"Success rate: {self.success_count/self.processed_count*100:.1f}%")
        print(f"Enhanced data saved to: {output_file}")


def main():
    """Main function with inline Wikipedia lookup and full biographical extraction."""
    base_dir = os.path.dirname(__file__)
    project_root = os.path.dirname(os.path.dirname(base_dir))
    # Load .env.local if available so MistralAPIKey from that file is visible
    if _DOTENV_AVAILABLE:
        try:
            load_dotenv(os.path.join(project_root, ".env.local"))
        except Exception:
            pass
    # Default to the original FDOT memorial designations CSV at project root
    input_file = os.path.join(project_root, "Memorial_Roadway_Designations.csv")
    if not os.path.exists(input_file):
        # Fallback: if a preprocessed CSV is present, use it
        alt = os.path.join(base_dir, "memorial_enhanced_retry_full.csv")
        if os.path.exists(alt):
            input_file = alt
        else:
            print("\n[ERROR] Input CSV not found.")
            print(f"Tried: {input_file}")
            print(f"And:   {alt}")
            return
    
    print("Improved Memorial Data Enhancer - With All New Fields")
    print("=" * 60)
    print("This script looks up Wikipedia links inline and adds comprehensive biographical columns with robust error handling:")
    print("- Summary, DOB, DOD, Education, Place of Birth/Death")
    print("- Gender, Sports/Politics/Military/Music involvement")
    print("\nFeatures:")
    print("✅ Rate limit detection and handling")
    print("✅ Exponential backoff retry logic")
    print("✅ Detailed progress tracking")
    print("✅ Graceful error recovery")
    print("✅ All 11 new biographical fields\n")
    
    # Ask user for processing mode
    choice = input("Run demo (10 entries) or full processing? (demo/full): ").lower().strip()
    
    if choice == 'demo':
        output_file = os.path.join(base_dir, "memorial_enhanced_retry_new_fields_demo.csv")
        max_entries = 10
        print("Running demo with rate limit handling and all new fields...")
    else:
        output_file = os.path.join(base_dir, "output.csv")
        max_entries = None
        print("Running full processing with rate limit handling and all new fields...")
        print("⚠️  This will process all ~306 Wikipedia entries with comprehensive data")
        print("⚠️  Rate limiting will be applied to respect API limits")
        confirm = input("Continue? (y/n): ").lower().strip()
        if confirm != 'y':
            print("Cancelled.")
            return
    
    enhancer = ImprovedMemorialEnhancer()
    enhancer.enhance_csv_with_retry(input_file, output_file, max_entries)


if __name__ == "__main__":
    main()
