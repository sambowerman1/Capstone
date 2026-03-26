"""Phase 3: Route path tracing via Overpass, OSM name search, and OSRM fallback."""

import asyncio
import json
import re

from shapely.geometry import LineString, Point, MultiLineString
from shapely.ops import substring, linemerge

import config
import db
import utils

# ---------------------------------------------------------------------------
# Overpass query builders
# ---------------------------------------------------------------------------

def _overpass_route_query(route_ref: str, state_name: str, route_type: str) -> str:
    """Build an Overpass QL query for a route relation within a state."""
    if route_type == "interstate":
        network_filter = '["network"~"US:I"]'
    elif route_type == "us_route":
        network_filter = '["network"~"US:US"]'
    else:
        network_filter = ""

    # The ref tag in OSM typically uses the numeric part (e.g. "95" for I 95)
    ref = route_ref.split()[-1] if " " in route_ref else route_ref

    return f"""
    [out:json][timeout:30];
    area["name"="{state_name}"]["admin_level"="4"]->.state;
    (
      relation["type"="route"]["route"="road"]["ref"="{ref}"]{network_filter}(area.state);
    );
    out geom;
    """


def _overpass_name_query(highway_name: str, state_name: str) -> str:
    """Build an Overpass QL query searching by the memorial highway name."""
    escaped = re.sub(r'([\\"])', r"\\\1", highway_name)
    return f"""
    [out:json][timeout:30];
    area["name"="{state_name}"]["admin_level"="4"]->.state;
    (
      way["name"~"{escaped}",i](area.state);
      relation["name"~"{escaped}",i](area.state);
    );
    out geom;
    """


# ---------------------------------------------------------------------------
# Geometry extraction from Overpass response
# ---------------------------------------------------------------------------

def _extract_linestring(data: dict) -> LineString | None:
    """Extract a merged LineString from an Overpass response containing ways/relations."""
    all_coords = []

    for element in data.get("elements", []):
        if element["type"] == "way" and "geometry" in element:
            coords = [(n["lon"], n["lat"]) for n in element["geometry"]]
            if len(coords) >= 2:
                all_coords.append(coords)
        elif element["type"] == "relation" and "members" in element:
            for member in element["members"]:
                if member.get("type") == "way" and "geometry" in member:
                    coords = [(n["lon"], n["lat"]) for n in member["geometry"]]
                    if len(coords) >= 2:
                        all_coords.append(coords)

    if not all_coords:
        return None

    if len(all_coords) == 1:
        return LineString(all_coords[0])

    lines = [LineString(c) for c in all_coords]
    merged = linemerge(MultiLineString(lines))
    if isinstance(merged, LineString):
        return merged
    # If linemerge returns a MultiLineString, pick the longest segment
    if hasattr(merged, "geoms"):
        longest = max(merged.geoms, key=lambda g: g.length)
        return longest
    return None


def _clip_route(
    full_line: LineString,
    from_coord: tuple | None,
    to_coord: tuple | None,
) -> LineString:
    """Clip a route LineString between two points. Returns the sub-line."""
    if from_coord is None and to_coord is None:
        return full_line

    if from_coord is not None:
        from_dist = full_line.project(Point(from_coord[1], from_coord[0]))
    else:
        from_dist = 0.0

    if to_coord is not None:
        to_dist = full_line.project(Point(to_coord[1], to_coord[0]))
    else:
        to_dist = full_line.length

    start = min(from_dist, to_dist)
    end = max(from_dist, to_dist)

    if end - start < 1e-9:
        return full_line

    return substring(full_line, start, end)


# ---------------------------------------------------------------------------
# Per-row processing strategies
# ---------------------------------------------------------------------------

async def _try_overpass_route(row: dict) -> tuple[LineString | None, str | None]:
    """Attempt to get route geometry from Overpass using the route ref."""
    routes_json = row.get("normalized_routes", "[]")
    try:
        routes = json.loads(routes_json)
    except json.JSONDecodeError:
        return None, None

    if not routes:
        return None, None

    state_name = row["state"]

    for route_obj in routes:
        ref = route_obj.get("ref", "")
        rtype = route_obj.get("type", "unknown")
        if not ref:
            continue

        ql = _overpass_route_query(ref, state_name, rtype)
        data = await utils.overpass_query(ql)
        if data is None:
            continue

        line = _extract_linestring(data)
        if line is None:
            continue

        # Clip if we have endpoints
        from_coord = None
        to_coord = None
        if row.get("from_lat") is not None and row.get("from_lon") is not None:
            from_coord = (row["from_lat"], row["from_lon"])
        if row.get("to_lat") is not None and row.get("to_lon") is not None:
            to_coord = (row["to_lat"], row["to_lon"])

        clipped = _clip_route(line, from_coord, to_coord)
        return clipped, "overpass"

    return None, None


async def _try_osm_name_search(row: dict) -> tuple[LineString | None, str | None]:
    """Search OSM by the memorial highway name."""
    name = row.get("highway_name", "")
    if not name:
        return None, None

    ql = _overpass_name_query(name, row["state"])
    data = await utils.overpass_query(ql)
    if data is None:
        return None, None

    line = _extract_linestring(data)
    if line is not None:
        return line, "osm_name_search"

    # Try with simplified name (drop "Memorial Highway" suffix)
    simple = re.sub(
        r"\s*(Memorial\s+)?(Highway|Bridge|Expressway|Parkway|Trail|Road)\s*$",
        "",
        name,
        flags=re.IGNORECASE,
    ).strip()
    if simple and simple != name:
        ql = _overpass_name_query(simple, row["state"])
        data = await utils.overpass_query(ql)
        if data:
            line = _extract_linestring(data)
            if line is not None:
                return line, "osm_name_search"

    return None, None


async def _try_osrm_route(row: dict) -> tuple[LineString | None, str | None]:
    """Fall back to OSRM driving route between geocoded endpoints."""
    if (
        row.get("from_lat") is None or row.get("from_lon") is None
        or row.get("to_lat") is None or row.get("to_lon") is None
    ):
        return None, None

    from_coord = (row["from_lat"], row["from_lon"])
    to_coord = (row["to_lat"], row["to_lon"])

    coords = await utils.osrm_route(from_coord, to_coord)
    if coords and len(coords) >= 2:
        return LineString(coords), "osrm"

    return None, None


# ---------------------------------------------------------------------------
# Main phase runner (sequential to respect Overpass rate limits)
# ---------------------------------------------------------------------------

async def _process_all(rows: list[dict]):
    total = len(rows)

    for i, row in enumerate(rows, 1):
        if i % 50 == 0 or i == total or i == 1:
            print(f"  [{i}/{total}]")

        tier = row.get("tier")
        log_parts = []

        # Strategy depends on what data we have
        line = None
        source = None

        # 1. Try Overpass route lookup (for rows with a route ref)
        routes_json = row.get("normalized_routes", "[]")
        has_route = routes_json and routes_json != "[]"

        if has_route:
            line, source = await _try_overpass_route(row)
            if not line:
                log_parts.append("overpass: no route geometry")

        # 2. Try OSM name search (Tier 6, or if Overpass failed)
        if line is None and (tier == 6 or not has_route):
            line, source = await _try_osm_name_search(row)
            if not line:
                log_parts.append("osm_name: no result")

        # 3. OSRM fallback (if we have both endpoints)
        if line is None:
            line, source = await _try_osrm_route(row)
            if not line:
                log_parts.append("osrm: no result or missing endpoints")

        # Save results
        if line is not None:
            geojson_coords = [list(c) for c in line.coords]
            db.update_row(
                row["id"],
                path_geojson=json.dumps(geojson_coords),
                path_source=source,
                status="path_found",
            )
        else:
            # No path found — leave status as geocoded for Phase 4 centroid fallback
            if log_parts:
                db.append_error(row["id"], "phase3: " + " | ".join(log_parts))


def run():
    # Process geocoded rows that don't yet have a path
    rows = db.get_rows_by_status("geocoded")
    total = len(rows)
    if total == 0:
        print("Phase 3: No geocoded rows to process.")
        return

    print(f"Phase 3: Tracing routes for {total} rows (this may take several hours)...")
    asyncio.run(_process_all(rows))

    stats = db.get_stats()
    print(f"Phase 3 complete. Status: {stats['by_status']}")
