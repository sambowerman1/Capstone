#!/usr/bin/env python3
"""
Run the AI summarizer pipeline across all state CSVs in openclaw/states/,
producing a single consolidated output CSV with a 'state' column.

Features:
  - Processes states from fewest rows to most rows.
  - Appends to the output CSV one state at a time (not all-or-nothing).
  - On rerun, skips states already present in the output (resume-safe).
  - Detailed progress logging per row, per state, and overall.
"""

import csv
import os
import sys
import time
from typing import List, Dict, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env.local'))
except Exception:
    pass

from person_summarizer import PersonSummarizer, Person
from wikipedia_extraction.enhance_memorial_with_retry import WikipediaResolver

OPENCLAW_DIR = os.path.join(os.path.dirname(__file__), '..', 'openclaw', 'states')
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'openclaw', 'all_states_summarized.csv')

STATE_CSV_CONFIG = {
    'alaska':          {'file': 'alaska_commemorative_highways.csv',               'name_col': 'highway_name'},
    'arkansas':        {'file': 'arkansas_commemorative_highways.csv',             'name_col': 'highway_name'},
    'california':      {'file': 'california_commemorative_highways_filtered.csv',  'name_col': 'highway_name'},
    'connecticut':     {'file': 'connecticut_commemorative_highways.csv',          'name_col': 'highway_name'},
    'delaware':        {'file': 'delaware_commemorative_highways.csv',             'name_col': 'highway_name'},
    'florida':         {'file': 'Memorial_Roadway_Designations.csv',              'name_col': 'DESIGNATIO'},
    'georgia':         {'file': 'georgia_commemorative_highways.csv',              'name_col': 'highway_name'},
    'hawaii':          {'file': 'hawaii_commemorative_highways.csv',               'name_col': 'highway_name'},
    'idaho':           {'file': 'idaho_commemorative_highways.csv',                'name_col': 'highway_name'},
    'illinois':        {'file': 'illinois_commemorative_highways.csv',             'name_col': 'highway_name'},
    'indiana':         {'file': 'indiana_commemorative_highways.csv',              'name_col': 'highway_name'},
    'kansas':          {'file': 'kansas_commemorative_highways.csv',               'name_col': 'highway_name'},
    'louisiana':       {'file': 'louisiana_commemorative_highways.csv',            'name_col': 'highway_name'},
    'maryland':        {'file': 'maryland_commemorative_highways.csv',             'name_col': 'highway_name'},
    'michigan':        {'file': 'michigan_commemorative_highways.csv',             'name_col': 'highway_name'},
    'minnesota':       {'file': 'minnesota_commemorative_highways.csv',            'name_col': 'Highway_Designation'},
    'missouri':        {'file': 'missouri_commemorative_highways.csv',             'name_col': 'highway_name'},
    'montana':         {'file': 'montana_commemorative_highways_curated.csv',      'name_col': 'Name'},
    'nebraska':        {'file': 'nebraska_commemorative_highways.csv',             'name_col': 'highway_name'},
    'nevada':          {'file': 'nevada_commemorative_highways.csv',               'name_col': 'highway_name'},
    'new_hampshire':   {'file': 'new_hampshire_commemorative_highways.csv',        'name_col': 'highway_name'},
    'new_jersey':      {'file': 'new_jersey_commemorative_highways.csv',           'name_col': 'highway_name'},
    'new_york':        {'file': 'new_york_commemorative_highways.csv',             'name_col': 'highway_name'},
    'north_carolina':  {'file': 'north_carolina_commemorative_highways.csv',       'name_col': 'highway_name'},
    'north_dakota':    {'file': 'north_dakota_commemorative_highways.csv',         'name_col': 'highway_name'},
    'ohio':            {'file': 'ohio_commemorative_highways.csv',                 'name_col': 'highway_name'},
    'oklahoma':        {'file': 'oklahoma_commemorative_highways.csv',             'name_col': 'highway_name'},
    'oregon':          {'file': 'oregon_commemorative_highways.csv',               'name_col': 'highway_name'},
    'rhode_island':    {'file': 'rhode_island_commemorative_highways.csv',         'name_col': 'highway_name'},
    'texas':           {'file': 'texas_commemorative_highways.csv',                'name_col': 'highway_name'},
    'utah':            {'file': 'utah_commemorative_highways.csv',                 'name_col': 'highway_name'},
    'washington':      {'file': 'washington_commemorative_highways.csv',           'name_col': 'highway_name'},
    'wisconsin':       {'file': 'wisconsin_commemorative_highways_new.csv',        'name_col': 'highway_name'},
    'wyoming':         {'file': 'wyoming_commemorative_highways.csv',              'name_col': 'highway_name'},
}

OUTPUT_FIELDS = [
    'state', 'highway_name', 'route_no', 'from_location', 'to_location',
    'county', 'person_name', 'wikipedia_url', 'match_confidence',
    'validated', 'validation_notes',
    'summary', 'education', 'dob', 'dod',
    'place_of_birth', 'place_of_death', 'gender',
    'involved_in_sports', 'involved_in_politics',
    'involved_in_military', 'involved_in_music',
]


def state_display_name(folder_name: str) -> str:
    return folder_name.replace('_', ' ').title()


def read_state_rows(state_folder: str, config: dict) -> List[Dict[str, str]]:
    """Read a state CSV and normalize rows to a common shape."""
    csv_path = os.path.join(OPENCLAW_DIR, state_folder, config['file'])
    if not os.path.exists(csv_path):
        return []

    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for raw in reader:
            highway_name = (raw.get(config['name_col']) or '').strip()
            if not highway_name or highway_name == 'nan':
                continue
            rows.append({
                'highway_name': highway_name,
                'route_no': (raw.get('route_no') or raw.get('HWY_NAME') or '').strip(),
                'from_location': (raw.get('from_location') or raw.get('DESCRIPTIO') or '').strip(),
                'to_location': (raw.get('to_location') or '').strip(),
                'county': (raw.get('COUNTY') or raw.get('county') or '').strip(),
            })
    return rows


def load_completed_states() -> Set[str]:
    """Scan the existing output CSV and return the set of state names already present."""
    if not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0:
        return set()
    completed = set()
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            s = (row.get('state') or '').strip()
            if s:
                completed.add(s)
    return completed


def ensure_header():
    """Write the CSV header if the output file doesn't exist or is empty."""
    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        return
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()


def append_rows(rows: List[Dict[str, str]]):
    """Append rows to the output CSV (no header)."""
    if not rows:
        return
    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction='ignore')
        writer.writerows(rows)


def build_state_plan() -> List[Tuple[str, dict, int]]:
    """Pre-read every state to get row counts. Return list sorted ascending by count."""
    plan = []
    for state_folder, config in STATE_CSV_CONFIG.items():
        rows = read_state_rows(state_folder, config)
        plan.append((state_folder, config, len(rows)))
    plan.sort(key=lambda x: x[2])
    return plan


def make_empty_row(state_name: str, row: dict, **overrides) -> dict:
    out = {
        'state': state_name,
        'highway_name': row.get('highway_name', ''),
        'route_no': row.get('route_no', ''),
        'from_location': row.get('from_location', ''),
        'to_location': row.get('to_location', ''),
        'county': row.get('county', ''),
        'person_name': '', 'wikipedia_url': '', 'match_confidence': '',
        'validated': '', 'validation_notes': '',
        'summary': '', 'education': '', 'dob': '', 'dod': '',
        'place_of_birth': '', 'place_of_death': '', 'gender': '',
        'involved_in_sports': '', 'involved_in_politics': '',
        'involved_in_military': '', 'involved_in_music': '',
    }
    out.update(overrides)
    return out


def run_pipeline():
    pipeline_start = time.time()

    api_key = os.getenv('OpenRouterAPIKey')
    if not api_key:
        print("ERROR: OpenRouterAPIKey not set. Set it in .env.local or as an env var.")
        sys.exit(1)

    # --- Pre-pass: count rows per state and sort ascending ---
    print("Scanning state CSVs to determine processing order...")
    state_plan = build_state_plan()
    grand_total_rows = sum(c for _, _, c in state_plan)
    total_states = len(state_plan)

    print(f"\nFound {total_states} states, {grand_total_rows} total highway rows")
    print("Processing order (fewest rows first):")
    for i, (folder, _, count) in enumerate(state_plan, 1):
        print(f"  {i:2d}. {state_display_name(folder):20s} ({count} rows)")
    print()

    # --- Resume logic: detect already-completed states ---
    ensure_header()
    completed_states = load_completed_states()
    if completed_states:
        print(f"Resuming -- {len(completed_states)} state(s) already in output: "
              f"{', '.join(sorted(completed_states))}")
    print(f"Output: {OUTPUT_FILE}\n")

    resolver = WikipediaResolver()
    already_processed: Dict[str, dict] = {}

    global_highways = 0
    global_persons = 0
    global_summaries = 0
    states_processed = 0

    for idx, (state_folder, config, row_count) in enumerate(state_plan, 1):
        state_name = state_display_name(state_folder)

        # --- Skip already-completed states ---
        if state_name in completed_states:
            print(f"[{idx}/{total_states}] SKIP {state_name} (already in output)")
            continue

        state_start = time.time()
        print(f"{'='*60}")
        print(f"[{idx}/{total_states}] {state_name}  ({row_count} rows)")
        print(f"{'='*60}")

        rows = read_state_rows(state_folder, config)
        if not rows:
            print(f"  No rows found, skipping\n")
            continue

        state_rows_out: List[Dict[str, str]] = []
        st_names_extracted = 0
        st_wiki_found = 0
        st_summaries_ok = 0
        st_summaries_fail = 0

        for i, row in enumerate(rows, 1):
            highway_name = row['highway_name']
            names = resolver.extract_person_names(highway_name)

            if not names:
                state_rows_out.append(
                    make_empty_row(state_name, row,
                                   validation_notes='no person name extracted')
                )
                if i % 50 == 0 or i == len(rows):
                    print(f"  [{i}/{len(rows)}] (no person name) {highway_name[:60]}")
                continue

            st_names_extracted += len(names)

            for person_name in names:
                cache_key = f"{person_name}|{state_name}"

                if cache_key in already_processed:
                    cached = already_processed[cache_key]
                    out = dict(cached)
                    out['highway_name'] = highway_name
                    out['route_no'] = row['route_no']
                    out['from_location'] = row['from_location']
                    out['to_location'] = row['to_location']
                    out['county'] = row['county']
                    state_rows_out.append(out)
                    print(f"  [{i}/{len(rows)}] {person_name} -- cached")
                    continue

                print(f"  [{i}/{len(rows)}] {person_name}  <-  {highway_name[:50]}")

                resolved = resolver.resolve_best(
                    person_name, highway_name, row['county'], state=state_name
                )
                wiki_url = resolved.get('wikipedia_url', 'Not Found')
                status = resolved.get('status', 'Not Found')
                confidence = resolved.get('match_confidence', '0')
                validated = resolved.get('validated', 'false')
                notes = resolved.get('validation_notes', '')

                out_row = make_empty_row(
                    state_name, row,
                    person_name=person_name,
                    wikipedia_url=wiki_url if status == 'Found' else '',
                    match_confidence=confidence,
                    validated=validated,
                    validation_notes=notes,
                )

                if status == 'Found' and wiki_url and wiki_url != 'Not Found':
                    st_wiki_found += 1
                    print(f"           -> Wikipedia FOUND (confidence {confidence})")
                    try:
                        person = Person(
                            wiki_url, api_key=api_key,
                            designation=highway_name, state=state_name
                        )
                        summary = person.getSummary()
                        st_summaries_ok += 1

                        out_row['summary'] = summary or ''
                        out_row['education'] = str(person.getEducation() or [])
                        out_row['dob'] = person.getDOB() or ''
                        out_row['dod'] = person.getDOD() or ''
                        out_row['place_of_birth'] = person.getPlaceOfBirth() or ''
                        out_row['place_of_death'] = person.getPlaceOfDeath() or ''
                        out_row['gender'] = person.getGender() or ''
                        out_row['involved_in_sports'] = person.getInvolvedInSports() or ''
                        out_row['involved_in_politics'] = person.getInvolvedInPolitics() or ''
                        out_row['involved_in_military'] = person.getInvolvedInMilitary() or ''
                        out_row['involved_in_music'] = person.getInvolvedInMusic() or ''
                        print(f"           -> Summary OK")

                    except Exception as e:
                        st_summaries_fail += 1
                        print(f"           -> Summary FAILED: {e}")
                        out_row['validation_notes'] = f"{notes}; summarizer error: {e}"
                else:
                    print(f"           -> Wikipedia NOT FOUND")

                already_processed[cache_key] = {
                    k: out_row[k] for k in out_row
                    if k not in ('highway_name', 'route_no', 'from_location',
                                 'to_location', 'county')
                }
                state_rows_out.append(out_row)

        # --- Append this state's results ---
        append_rows(state_rows_out)
        state_elapsed = time.time() - state_start
        states_processed += 1
        global_highways += len(rows)
        global_persons += st_wiki_found
        global_summaries += st_summaries_ok

        print(f"\n  --- {state_name} complete ---")
        print(f"  Rows: {len(rows)}  |  Names extracted: {st_names_extracted}")
        print(f"  Wikipedia found: {st_wiki_found}  |  Summaries OK: {st_summaries_ok}"
              f"  |  Summaries failed: {st_summaries_fail}")
        print(f"  Appended {len(state_rows_out)} rows  |  Elapsed: {state_elapsed:.1f}s\n")

    # --- Global summary ---
    total_elapsed = time.time() - pipeline_start
    print(f"{'='*60}")
    print("ALL STATES COMPLETE")
    print(f"{'='*60}")
    print(f"States processed this run: {states_processed}")
    print(f"Total highways:            {global_highways}")
    print(f"Wikipedia persons found:   {global_persons}")
    print(f"Successful AI summaries:   {global_summaries}")
    print(f"Total elapsed:             {total_elapsed:.1f}s ({total_elapsed/60:.1f}m)")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == '__main__':
    run_pipeline()
