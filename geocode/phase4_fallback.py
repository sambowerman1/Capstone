"""Phase 4: Centroid fallback + QA checks for all rows."""

import asyncio
import json

from pyproj import Geod
from shapely.geometry import LineString

import db
import utils

_geod = Geod(ellps="WGS84")


def _path_length_miles(coords: list) -> float:
    """Compute geodesic length of a coordinate list in miles."""
    total = 0.0
    for i in range(len(coords) - 1):
        _, _, dist = _geod.inv(
            coords[i][0], coords[i][1],
            coords[i + 1][0], coords[i + 1][1],
        )
        total += dist
    return total * 0.000621371


def _route_midpoint(coords: list) -> tuple[float, float]:
    """Return (lat, lon) at the midpoint along a path."""
    line = LineString(coords)
    mid = line.interpolate(0.5, normalized=True)
    return mid.y, mid.x  # lat, lon


# ---------------------------------------------------------------------------
# Centroid for rows WITH a path
# ---------------------------------------------------------------------------

def _centroid_from_path(row: dict) -> bool:
    """Compute centroid from path_geojson. Returns True if successful."""
    raw = row.get("path_geojson")
    if not raw:
        return False
    try:
        coords = json.loads(raw)
    except json.JSONDecodeError:
        return False

    if len(coords) < 2:
        return False

    lat, lon = _route_midpoint(coords)
    length = _path_length_miles(coords)

    db.update_row(
        row["id"],
        centroid_lat=lat,
        centroid_lon=lon,
        centroid_source="path_midpoint",
        path_length_miles=length,
        status="path_found",
        confidence="high",
    )
    return True


# ---------------------------------------------------------------------------
# Fallback hierarchy for rows WITHOUT a path
# ---------------------------------------------------------------------------

async def _centroid_fallback(row: dict) -> bool:
    """Try the fallback hierarchy. Returns True if a centroid was found."""
    state = row["state"]

    # 1. From-point available
    if row.get("from_lat") is not None and row.get("from_lon") is not None:
        db.update_row(
            row["id"],
            centroid_lat=row["from_lat"],
            centroid_lon=row["from_lon"],
            centroid_source="from_point",
            status="centroid_found",
            confidence="medium",
        )
        return True

    # 2. County geocode
    county = row.get("parsed_county") or row.get("county")
    if county:
        query = f"{county} County, {state}, USA"
        results = await utils.photon_geocode(query)
        hit = next((r for r in results if utils.result_in_state(r, state)), None)
        if hit:
            db.update_row(
                row["id"],
                centroid_lat=hit["lat"],
                centroid_lon=hit["lon"],
                centroid_source="county_geocode",
                status="centroid_found",
                confidence="low",
            )
            return True

    # 3. Route geocode
    routes_json = row.get("normalized_routes", "[]")
    try:
        routes = json.loads(routes_json)
    except json.JSONDecodeError:
        routes = []

    if routes:
        ref = routes[0].get("ref", "")
        if ref:
            query = f"{ref}, {state}, USA"
            results = await utils.photon_geocode(query)
            hit = next((r for r in results if utils.result_in_state(r, state)), None)
            if hit:
                db.update_row(
                    row["id"],
                    centroid_lat=hit["lat"],
                    centroid_lon=hit["lon"],
                    centroid_source="route_geocode",
                    status="centroid_found",
                    confidence="low",
                )
                return True

    # 4. Highway name + state
    name = row.get("highway_name", "")
    if name:
        query = f"{name}, {state}, USA"
        results = await utils.photon_geocode(query)
        hit = next((r for r in results if utils.result_in_state(r, state)), None)
        if hit:
            db.update_row(
                row["id"],
                centroid_lat=hit["lat"],
                centroid_lon=hit["lon"],
                centroid_source="name_geocode",
                status="centroid_found",
                confidence="low",
            )
            return True

    # 5. State geographic center (very low confidence, but mappable)
    centroid = utils.STATE_CENTROIDS.get(state)
    if centroid:
        db.update_row(
            row["id"],
            centroid_lat=centroid[0],
            centroid_lon=centroid[1],
            centroid_source="state_centroid",
            status="centroid_found",
            confidence="very_low",
        )
        return True

    # 6. Nothing works
    db.update_row(row["id"], status="failed")
    db.append_error(row["id"], "phase4: all fallback strategies exhausted")
    return False


# ---------------------------------------------------------------------------
# QA checks
# ---------------------------------------------------------------------------

def _qa_checks(rows: list[dict]):
    """Run sanity checks on all rows after centroid computation.

    QA appends warnings to error_notes but never clears coordinates or
    downgrades status — the centroid is kept for downstream use.
    """
    flagged = 0

    for row in rows:
        notes = []
        lat = row.get("centroid_lat")
        lon = row.get("centroid_lon")
        state = row.get("state", "")

        # Out-of-state check (flag only — keep the centroid)
        if lat is not None and lon is not None:
            bbox = utils.STATE_BBOXES.get(state)
            if bbox:
                min_lat, max_lat, min_lon, max_lon = bbox
                if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                    notes.append("QA: centroid outside state bbox")

        # Implausible path length (flag only)
        length = row.get("path_length_miles")
        if length is not None:
            if length < 0.1:
                notes.append(f"QA: path too short ({length:.2f} mi)")
            elif length > 500:
                notes.append(f"QA: path too long ({length:.2f} mi)")

        # Missing centroid — mark failed only if truly empty
        if lat is None or lon is None:
            if row.get("status") != "failed":
                notes.append("QA: missing centroid")
                db.update_row(row["id"], status="failed")

        if notes:
            flagged += 1
            db.append_error(row["id"], " | ".join(notes))

    if flagged:
        print(f"  QA flagged {flagged} rows (warnings only, coordinates preserved).")


# ---------------------------------------------------------------------------
# Main phase runner
# ---------------------------------------------------------------------------

async def _process_fallbacks(rows: list[dict]):
    """Process rows that need centroid fallback (no path)."""
    sem = asyncio.Semaphore(config.PHOTON_CONCURRENT)
    total = len(rows)

    async def process_one(row: dict, idx: int):
        async with sem:
            await _centroid_fallback(row)
        if idx % 100 == 0 or idx == total:
            print(f"  Fallback: [{idx}/{total}]")

    tasks = [process_one(r, i) for i, r in enumerate(rows, 1)]
    await asyncio.gather(*tasks)


import config  # noqa: E402 (needed for PHOTON_CONCURRENT in _process_fallbacks)


def run():
    all_rows = db.get_all_rows()
    total = len(all_rows)
    if total == 0:
        print("Phase 4: No rows in database.")
        return

    print(f"Phase 4: Computing centroids and running QA on {total} rows...")

    # Step 1: Centroid from path for rows that have one
    path_rows = [r for r in all_rows if r.get("path_geojson")]
    done = 0
    for r in path_rows:
        if _centroid_from_path(r):
            done += 1
    print(f"  Computed {done} centroids from paths.")

    # Step 2: Promote existing endpoint coords to centroid where missing
    all_rows = db.get_all_rows()
    promoted = 0
    for r in all_rows:
        if r.get("centroid_lat") is not None:
            continue
        from_ok = r.get("from_lat") is not None and r.get("from_lon") is not None
        to_ok = r.get("to_lat") is not None and r.get("to_lon") is not None
        if from_ok and to_ok:
            lat = (r["from_lat"] + r["to_lat"]) / 2
            lon = (r["from_lon"] + r["to_lon"]) / 2
            src = "endpoint_midpoint"
        elif from_ok:
            lat, lon, src = r["from_lat"], r["from_lon"], "from_point"
        elif to_ok:
            lat, lon, src = r["to_lat"], r["to_lon"], "to_point"
        else:
            continue
        db.update_row(
            r["id"],
            centroid_lat=lat,
            centroid_lon=lon,
            centroid_source=src,
            status="centroid_found",
            confidence="medium",
        )
        promoted += 1
    print(f"  Promoted {promoted} centroids from existing endpoints.")

    # Step 3: Fallback for rows without a path or centroid
    all_rows = db.get_all_rows()  # re-fetch after updates
    needs_fallback = [
        r for r in all_rows
        if r.get("centroid_lat") is None and r.get("status") != "failed"
    ]

    if needs_fallback:
        print(f"  Running fallback for {len(needs_fallback)} rows...")
        asyncio.run(_process_fallbacks(needs_fallback))

    # Step 4: QA
    print("  Running QA checks...")
    all_rows = db.get_all_rows()
    _qa_checks(all_rows)

    stats = db.get_stats()
    print(f"Phase 4 complete. Final status: {stats['by_status']}")
