"""Export pipeline results to CSV and GeoJSON files."""

import csv
import json
from pathlib import Path

import config
import db

_CSV_COLUMNS = [
    "id", "state", "highway_name", "route_no", "from_location", "to_location",
    "county", "person_name",
    "tier", "normalized_routes", "parsed_from", "parsed_to", "parsed_county",
    "from_lat", "from_lon", "from_geocode_source",
    "to_lat", "to_lon", "to_geocode_source",
    "centroid_lat", "centroid_lon", "centroid_source",
    "status", "confidence", "path_length_miles", "path_source", "error_notes",
]


def _ensure_output_dir():
    Path(config.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def export_csv():
    """Write enriched CSV with all pipeline columns."""
    _ensure_output_dir()
    rows = db.get_all_rows()
    out_path = Path(config.OUTPUT_DIR) / "highways_geocoded.csv"

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"  Wrote {len(rows)} rows to {out_path}")


def export_paths_geojson():
    """Write GeoJSON FeatureCollection of route LineStrings."""
    _ensure_output_dir()
    rows = db.get_all_rows()
    features = []

    for r in rows:
        raw = r.get("path_geojson")
        if not raw:
            continue
        try:
            coords = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if len(coords) < 2:
            continue

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coords,
            },
            "properties": {
                "highway_id": r["id"],
                "state": r["state"],
                "highway_name": r["highway_name"],
                "person_name": r.get("person_name"),
                "confidence": r.get("confidence"),
                "path_source": r.get("path_source"),
                "path_length_miles": r.get("path_length_miles"),
            },
        })

    collection = {"type": "FeatureCollection", "features": features}
    out_path = Path(config.OUTPUT_DIR) / "highways_paths.geojson"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collection, f)

    print(f"  Wrote {len(features)} path features to {out_path}")


def export_centroids_geojson():
    """Write GeoJSON FeatureCollection of centroid Points for all resolved rows."""
    _ensure_output_dir()
    rows = db.get_all_rows()
    features = []

    for r in rows:
        lat = r.get("centroid_lat")
        lon = r.get("centroid_lon")
        if lat is None or lon is None:
            continue

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat],
            },
            "properties": {
                "highway_id": r["id"],
                "state": r["state"],
                "highway_name": r["highway_name"],
                "person_name": r.get("person_name"),
                "status": r.get("status"),
                "confidence": r.get("confidence"),
                "centroid_source": r.get("centroid_source"),
            },
        })

    collection = {"type": "FeatureCollection", "features": features}
    out_path = Path(config.OUTPUT_DIR) / "highways_centroids.geojson"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collection, f)

    print(f"  Wrote {len(features)} centroid features to {out_path}")


def run():
    print("Export: Writing output files...")
    export_csv()
    export_paths_geojson()
    export_centroids_geojson()
    print("Export complete.")
