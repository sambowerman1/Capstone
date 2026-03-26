"""Phase 1: Normalize & Classify all pending highway rows."""

import json
import re

import db
import utils

# ---------------------------------------------------------------------------
# Route normalization
# ---------------------------------------------------------------------------

_INTERSTATE_RE = re.compile(
    r"(?:Interstate\s+|I[\s-]?)(\d+[A-Za-z]?)", re.IGNORECASE
)
_US_ROUTE_RE = re.compile(
    r"(?:U\.?S\.?\s*(?:Route|Highway|Hwy)?\s*|US[\s-]?)(\d+[A-Za-z]?)", re.IGNORECASE
)
_STATE_ROUTE_PREFIX_RE = re.compile(
    r"(?:State\s+(?:Route|Highway|Road|Hwy)\s+|SR[\s-]?)(\d+[A-Za-z]?)", re.IGNORECASE
)
_STATE_ABBREV_ROUTE_RE = re.compile(
    r"([A-Z]{2})[\s-](\d+[A-Za-z]?)"
)
_IL_ROUTE_RE = re.compile(
    r"(?:IL|IN|ND|SD|WI|MN|IA|MO|NE|KS|OK|OH)\s+(?:Route\s+)?(\d+[A-Za-z]?)",
    re.IGNORECASE,
)
_BARE_NUMBER_RE = re.compile(r"^(\d+)$")


def normalize_route(route_no: str, state: str) -> str:
    """Parse route_no string into a JSON array of route objects.

    Each object: {"type": "interstate"|"us_route"|"state_route"|"unknown", "ref": "..."}
    """
    if not route_no or not route_no.strip():
        return "[]"

    parts = re.split(r"\s*/\s*|\s*,\s*", route_no.strip())
    routes = []

    for part in parts:
        part = part.strip()
        if not part:
            continue

        m = _INTERSTATE_RE.search(part)
        if m:
            routes.append({"type": "interstate", "ref": f"I {m.group(1)}"})
            continue

        m = _US_ROUTE_RE.search(part)
        if m:
            routes.append({"type": "us_route", "ref": f"US {m.group(1)}"})
            continue

        m = _STATE_ROUTE_PREFIX_RE.search(part)
        if m:
            routes.append({"type": "state_route", "ref": f"SR {m.group(1)}"})
            continue

        m = _STATE_ABBREV_ROUTE_RE.search(part)
        if m:
            abbrev = m.group(1).upper()
            num = m.group(2)
            routes.append({"type": "state_route", "ref": f"{abbrev} {num}"})
            continue

        m = _IL_ROUTE_RE.search(part)
        if m:
            routes.append({"type": "state_route", "ref": f"SR {m.group(1)}"})
            continue

        m = _BARE_NUMBER_RE.match(part)
        if m:
            routes.append({"type": "unknown", "ref": m.group(1)})
            continue

        # Couldn't parse — skip (written-out names handled by Phase 1b)

    return json.dumps(routes)


# ---------------------------------------------------------------------------
# Tier classification
# ---------------------------------------------------------------------------

_DESCRIPTION_MARKERS = [
    "designated",
    "that portion",
    "that section",
    "beginning at",
    "commencing at",
]


def _is_description(text: str) -> bool:
    """Return True if the text looks like a legal description rather than a location."""
    if not text:
        return False
    low = text.lower()
    return any(marker in low for marker in _DESCRIPTION_MARKERS)


def classify_tier(row: dict) -> int:
    route = (row.get("route_no") or "").strip()
    frm = (row.get("from_location") or "").strip()
    to = (row.get("to_location") or "").strip()

    has_desc = _is_description(frm) or _is_description(to)

    if route and frm and to and not has_desc:
        return 1
    elif route and frm and not to and not has_desc:
        return 2
    elif has_desc:
        return 3
    elif route and not frm and not to:
        return 4
    elif not route and frm and not has_desc:
        return 5
    else:
        return 6


# ---------------------------------------------------------------------------
# Tier 6 name parsing — look for route refs embedded in the highway name
# ---------------------------------------------------------------------------

_NAME_ROUTE_RE = re.compile(
    r"(?:I[\s-]?\d+|US[\s-]?\d+|SR[\s-]?\d+|"
    r"(?:State\s+)?(?:Route|Highway)\s+\d+|"
    r"[A-Z]{2}[\s-]\d+)"
    r"(?:\b|$)",
    re.IGNORECASE,
)
_NAME_TRAILING_NUM_RE = re.compile(r"\b(\d{1,4})\s*$")


def extract_route_from_name(highway_name: str) -> str | None:
    """Try to find a route reference embedded in the highway name."""
    if not highway_name:
        return None
    m = _NAME_ROUTE_RE.search(highway_name)
    if m:
        return m.group(0).strip()
    m = _NAME_TRAILING_NUM_RE.search(highway_name)
    if m:
        num = m.group(1)
        if int(num) < 1000:
            return num
    return None


# ---------------------------------------------------------------------------
# Main phase runner
# ---------------------------------------------------------------------------

def run():
    rows = db.get_rows_by_status("pending")
    total = len(rows)
    if total == 0:
        print("Phase 1: No pending rows to process.")
        return

    print(f"Phase 1: Normalizing {total} rows...")

    for i, row in enumerate(rows, 1):
        if i % 500 == 0 or i == total:
            print(f"  [{i}/{total}]")

        tier = classify_tier(row)
        route_no = (row.get("route_no") or "").strip()
        state = row["state"]

        # Tier 6: try to extract route from highway name
        if tier == 6:
            found = extract_route_from_name(row["highway_name"])
            if found:
                route_no = found
                tier = 4  # promote: we now have a route

        normalized = normalize_route(route_no, state)

        # Clean from/to locations for non-description tiers
        parsed_from = None
        parsed_to = None
        parsed_county = (row.get("county") or "").strip() or None

        if tier in (1, 2, 5):
            parsed_from = utils.clean_location_text(row.get("from_location") or "")
            parsed_to = utils.clean_location_text(row.get("to_location") or "")
            if not parsed_from:
                parsed_from = None
            if not parsed_to:
                parsed_to = None

        # Tier 3: leave parsed_from/to empty for Phase 1b LLM extraction
        # Tier 4/6: no from/to available

        db.update_row(
            row["id"],
            tier=tier,
            normalized_routes=normalized,
            parsed_from=parsed_from,
            parsed_to=parsed_to,
            parsed_county=parsed_county,
            status="normalized",
        )

    stats = db.get_stats()
    print(f"Phase 1 complete. Tier distribution: {stats['by_tier']}")
