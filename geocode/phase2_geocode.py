"""Phase 2: Geocode from/to endpoints using Photon (primary) and Nominatim (fallback)."""

import asyncio
import json

import config
import db
import utils

# ---------------------------------------------------------------------------
# Geocode a single endpoint string
# ---------------------------------------------------------------------------

async def geocode_endpoint(
    text: str, state: str, route_ref: str | None = None
) -> tuple[float | None, float | None, str | None]:
    """Return (lat, lon, source) for a location string, or (None, None, None)."""
    if not text or not text.strip():
        return None, None, None

    if utils.is_mile_marker(text):
        return None, None, "deferred_to_phase3"

    if utils.is_state_border(text):
        return await _geocode_border(text, state, route_ref)

    clean = utils.clean_location_text(text)

    # Try Photon with full query
    query = f"{clean}, {state}, USA"
    results = await utils.photon_geocode(query)
    hit = _best_in_state(results, state)
    if hit:
        return hit["lat"], hit["lon"], "photon"

    # Fallback to Nominatim
    results = await utils.nominatim_geocode(query)
    hit = _best_in_state(results, state)
    if hit:
        return hit["lat"], hit["lon"], "nominatim"

    # Retry with core place name only
    core = utils.extract_core_place(clean)
    if core != clean:
        results = await utils.photon_geocode(f"{core}, {state}, USA")
        hit = _best_in_state(results, state)
        if hit:
            return hit["lat"], hit["lon"], "photon_retry"

        results = await utils.nominatim_geocode(f"{core}, {state}, USA")
        hit = _best_in_state(results, state)
        if hit:
            return hit["lat"], hit["lon"], "nominatim_retry"

    return None, None, None


async def _geocode_border(
    text: str, state: str, route_ref: str | None
) -> tuple[float | None, float | None, str | None]:
    """Handle state border / state line references."""
    query_parts = []
    if route_ref:
        query_parts.append(route_ref)
    query_parts.append(f"{state} border")
    query = ", ".join(query_parts) + ", USA"

    results = await utils.photon_geocode(query)
    if results:
        return results[0]["lat"], results[0]["lon"], "photon_border"

    results = await utils.nominatim_geocode(query)
    if results:
        return results[0]["lat"], results[0]["lon"], "nominatim_border"

    return None, None, None


def _best_in_state(results: list[dict], state: str) -> dict | None:
    """Pick the best result that falls within the given state."""
    for r in results:
        if utils.result_in_state(r, state):
            return r
    return None


# ---------------------------------------------------------------------------
# Process all rows
# ---------------------------------------------------------------------------

async def _process_all(rows: list[dict]):
    sem = asyncio.Semaphore(config.PHOTON_CONCURRENT)
    total = len(rows)

    async def process_one(row: dict, idx: int):
        async with sem:
            await _geocode_row(row)
        if idx % 100 == 0 or idx == total:
            print(f"  [{idx}/{total}]")

    tasks = [process_one(row, i) for i, row in enumerate(rows, 1)]
    await asyncio.gather(*tasks)


async def _geocode_row(row: dict):
    state = row["state"]
    parsed_from = row.get("parsed_from")
    parsed_to = row.get("parsed_to")

    # Determine route ref for border-handling context
    route_ref = None
    routes_json = row.get("normalized_routes")
    if routes_json:
        try:
            routes = json.loads(routes_json)
            if routes:
                route_ref = routes[0].get("ref")
        except json.JSONDecodeError:
            pass

    log_parts = []

    from_lat, from_lon, from_src = await geocode_endpoint(parsed_from, state, route_ref)
    if parsed_from and not from_lat:
        log_parts.append(f"from '{parsed_from}': no result")

    to_lat, to_lon, to_src = await geocode_endpoint(parsed_to, state, route_ref)
    if parsed_to and not to_lat:
        log_parts.append(f"to '{parsed_to}': no result")

    updates = {
        "from_lat": from_lat,
        "from_lon": from_lon,
        "from_geocode_source": from_src,
        "to_lat": to_lat,
        "to_lon": to_lon,
        "to_geocode_source": to_src,
        "status": "geocoded",
    }
    db.update_row(row["id"], **updates)

    if log_parts:
        db.append_error(row["id"], "phase2: " + " | ".join(log_parts))


def run():
    rows = db.get_rows_by_status("normalized")
    # Only process rows that have something to geocode
    eligible = [
        r for r in rows
        if r.get("parsed_from") or r.get("parsed_to")
    ]
    # Rows with nothing to geocode still advance status
    no_endpoints = [r for r in rows if not r.get("parsed_from") and not r.get("parsed_to")]

    for r in no_endpoints:
        db.update_row(r["id"], status="geocoded")

    if not eligible:
        skipped = len(no_endpoints)
        print(f"Phase 2: No endpoints to geocode ({skipped} rows advanced without geocoding).")
        return

    print(
        f"Phase 2: Geocoding {len(eligible)} rows "
        f"({len(no_endpoints)} have no endpoints — advanced directly)..."
    )

    asyncio.run(_process_all(eligible))
    # Session created inside asyncio.run() is auto-closed with its event loop

    stats = db.get_stats()
    print(f"Phase 2 complete. Status: {stats['by_status']}")
