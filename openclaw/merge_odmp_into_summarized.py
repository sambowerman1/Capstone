#!/usr/bin/env python3
import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


ROOT = Path("/Users/sam/Desktop/Capstone")
SUMMARY_PATH = ROOT / "openclaw" / "all_states_summarized.csv"
SOURCE_ROOTS = [
    ("states", ROOT / "states"),
    ("openclaw/states", ROOT / "openclaw" / "states"),
]

ODMP_COLUMNS = [
    "odmp_url",
    "odmp_name",
    "odmp_bio",
    "odmp_age",
    "odmp_tour",
    "odmp_badge",
    "odmp_cause",
    "odmp_end_of_watch",
    "odmp_incident_details",
    "odmp_fuzzy_score",
]

HIGHWAY_KEY_COLUMNS = ["input_name", "highway_name", "Commemorative Name", "matched_input_name"]

ODMP_PREFIX_MAP = {
    "odmp_url": "odmp_url",
    "odmp_name": "odmp_name",
    "odmp_bio": "odmp_bio",
    "odmp_age": "odmp_age",
    "odmp_tour": "odmp_tour",
    "odmp_badge": "odmp_badge",
    "odmp_cause": "odmp_cause",
    "odmp_end_of_watch": "odmp_end_of_watch",
    "odmp_incident_details": "odmp_incident_details",
    "odmp_fuzzy_score": "odmp_fuzzy_score",
}

OFFICER_SCHEMA_MAP = {
    "source_url": "odmp_url",
    "name": "odmp_name",
    "bio": "odmp_bio",
    "age": "odmp_age",
    "tour": "odmp_tour",
    "badge": "odmp_badge",
    "cause": "odmp_cause",
    "end_of_watch": "odmp_end_of_watch",
    "incident_details": "odmp_incident_details",
    "fuzzy_score": "odmp_fuzzy_score",
}


def normalize(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def to_float(value: str) -> float:
    if value is None:
        return -1.0
    text = str(value).strip()
    if not text:
        return -1.0
    try:
        return float(text)
    except ValueError:
        return -1.0


def has_any_odmp_values(odmp_values: Dict[str, str]) -> bool:
    return any((odmp_values.get(col) or "").strip() for col in ODMP_COLUMNS)


def candidate_sort_key(candidate: Dict[str, str]) -> Tuple[int, float]:
    has_url = 1 if (candidate.get("odmp_url") or "").strip() else 0
    score = to_float(candidate.get("odmp_fuzzy_score", ""))
    return has_url, score


def read_summary_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if reader.fieldnames is None:
            raise ValueError(f"No headers found in {path}")
        return rows, reader.fieldnames


def build_summary_index(rows: Iterable[Dict[str, str]]) -> Dict[Tuple[str, str], List[int]]:
    index: Dict[Tuple[str, str], List[int]] = defaultdict(list)
    for i, row in enumerate(rows):
        state_key = normalize(row.get("state", ""))
        highway_key = normalize(row.get("highway_name", ""))
        if state_key and highway_key:
            index[(state_key, highway_key)].append(i)
    return index


def extract_state_from_path(csv_path: Path, source_root: Path) -> Optional[str]:
    rel = csv_path.relative_to(source_root)
    if len(rel.parts) < 2:
        return None
    return rel.parts[0]


def pick_highway_name(row: Dict[str, str], fieldnames: List[str]) -> str:
    for col in HIGHWAY_KEY_COLUMNS:
        if col in fieldnames:
            val = row.get(col, "")
            if (val or "").strip():
                return val
    return ""


def extract_odmp_values(row: Dict[str, str], fieldnames: List[str], mapping: Dict[str, str]) -> Dict[str, str]:
    result = {col: "" for col in ODMP_COLUMNS}
    for src_col, dst_col in mapping.items():
        if src_col in fieldnames:
            value = (row.get(src_col) or "").strip()
            if value and not result[dst_col]:
                result[dst_col] = value
    return result


def get_schema_mapping(fieldnames: List[str]) -> Optional[Dict[str, str]]:
    has_odmp_prefixed = any(col in fieldnames for col in ODMP_PREFIX_MAP.keys())
    if has_odmp_prefixed:
        return ODMP_PREFIX_MAP

    officer_shape = (
        "source_url" in fieldnames
        and "name" in fieldnames
        and "bio" in fieldnames
        and "fuzzy_score" in fieldnames
        and "matched_input_name" in fieldnames
    )
    if officer_shape:
        return OFFICER_SCHEMA_MAP

    return None


def collect_candidates(source_root: Path) -> Dict[Tuple[str, str], Dict[str, str]]:
    best_by_key: Dict[Tuple[str, str], Dict[str, str]] = {}
    for csv_path in sorted(source_root.rglob("*.csv")):
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue
            fieldnames = list(reader.fieldnames)
            schema_mapping = get_schema_mapping(fieldnames)
            has_odmp_shape = schema_mapping is not None
            has_highway_col = any(col in fieldnames for col in HIGHWAY_KEY_COLUMNS)
            if not has_odmp_shape or not has_highway_col:
                continue

            state = extract_state_from_path(csv_path, source_root)
            if not state:
                continue
            state_key = normalize(state.replace("_", " "))

            for row in reader:
                highway_raw = pick_highway_name(row, fieldnames)
                highway_key = normalize(highway_raw)
                if not highway_key:
                    continue

                odmp_values = extract_odmp_values(row, fieldnames, schema_mapping)
                if not has_any_odmp_values(odmp_values):
                    continue

                match_key = (state_key, highway_key)
                existing = best_by_key.get(match_key)
                if existing is None or candidate_sort_key(odmp_values) > candidate_sort_key(existing):
                    best_by_key[match_key] = odmp_values
    return best_by_key


def merge_odmp(summary_rows: List[Dict[str, str]], states_candidates: Dict[Tuple[str, str], Dict[str, str]], openclaw_candidates: Dict[Tuple[str, str], Dict[str, str]]) -> Tuple[int, int, int]:
    rows_changed = 0
    fields_updated = 0
    rows_matched = 0

    for row in summary_rows:
        state_key = normalize(row.get("state", ""))
        highway_key = normalize(row.get("highway_name", ""))
        if not state_key or not highway_key:
            continue

        key = (state_key, highway_key)
        incoming = states_candidates.get(key) or openclaw_candidates.get(key)
        if incoming is None:
            continue
        rows_matched += 1

        row_changed = False
        for col in ODMP_COLUMNS:
            incoming_val = (incoming.get(col) or "").strip()
            if not incoming_val:
                continue
            if row.get(col, "") != incoming_val:
                row[col] = incoming_val
                fields_updated += 1
                row_changed = True
        if row_changed:
            rows_changed += 1
    return rows_changed, fields_updated, rows_matched


def write_summary(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    summary_rows, summary_fieldnames = read_summary_rows(SUMMARY_PATH)
    _ = build_summary_index(summary_rows)

    states_candidates = collect_candidates(SOURCE_ROOTS[0][1])
    openclaw_candidates = collect_candidates(SOURCE_ROOTS[1][1])

    rows_changed, fields_updated, rows_matched = merge_odmp(
        summary_rows=summary_rows,
        states_candidates=states_candidates,
        openclaw_candidates=openclaw_candidates,
    )
    write_summary(SUMMARY_PATH, summary_rows, summary_fieldnames)

    print(f"states candidates: {len(states_candidates)}")
    print(f"openclaw/states candidates: {len(openclaw_candidates)}")
    print(f"summarized rows matched: {rows_matched}")
    print(f"summarized rows changed: {rows_changed}")
    print(f"odmp fields updated: {fields_updated}")


if __name__ == "__main__":
    main()
