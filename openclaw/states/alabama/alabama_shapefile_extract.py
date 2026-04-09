from pathlib import Path
import geopandas as gpd
import pandas as pd


STATE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = STATE_DIR / "alabama_memorial_highways_from_shapefile.csv"


def find_roads_shapefile(state_dir: Path) -> Path:
    exact_matches = list(state_dir.rglob("gis_osm_roads_free_1.shp"))
    if exact_matches:
        return exact_matches[0]

    fallback_matches = [p for p in state_dir.rglob("*.shp") if "road" in p.name.lower()]
    if fallback_matches:
        return fallback_matches[0]

    raise FileNotFoundError(f"Could not find a roads shapefile in {state_dir}")


def clean_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def is_memorial_name(name: str) -> bool:
    if not isinstance(name, str):
        return False

    original_name = name.strip()
    name_clean = original_name.lower()

    if not name_clean:
        return False

    strong_keywords = [
        "memorial",
        "veterans",
        "veteran",
        "fallen",
        "purple heart",
        "medal of honor",
        "gold star",
        "martin luther king",
        "mlk",
    ]

    if any(keyword in name_clean for keyword in strong_keywords):
        return True

    return False


def extract_route(row):
    for col in ["ref", "name", "osm_id"]:
        if col in row and pd.notna(row[col]) and str(row[col]).strip():
            return str(row[col]).strip()
    return ""


def main():
    shp_path = find_roads_shapefile(STATE_DIR)
    print(f"Reading shapefile: {shp_path}")

    gdf = gpd.read_file(shp_path)
    print(f"Total road rows in shapefile: {len(gdf)}")
    print("Columns found:", list(gdf.columns))

    useful_cols = [
        col for col in gdf.columns
        if col.lower() in {
            "osm_id", "code", "fclass", "name", "ref", "oneway",
            "maxspeed", "layer", "bridge", "tunnel", "geometry"
        }
    ]
    if useful_cols:
        gdf = gdf[useful_cols]

    for col in gdf.columns:
        if col != "geometry":
            gdf[col] = gdf[col].apply(clean_text)

    if "name" not in gdf.columns:
        raise ValueError("Expected a 'name' column in the roads shapefile.")

    gdf["memorial_status"] = gdf["name"].apply(
        lambda x: "Possible Memorial Designation" if is_memorial_name(x) else ""
    )

    memorial_df = gdf[gdf["memorial_status"] == "Possible Memorial Designation"].copy()
    memorial_df["route"] = memorial_df.apply(extract_route, axis=1)

    # remove empty names
    memorial_df = memorial_df[memorial_df["name"].str.strip() != ""]

    # deduplicate by route + name + ref
    dedupe_cols = [c for c in ["route", "name", "ref"] if c in memorial_df.columns]
    memorial_df = memorial_df.drop_duplicates(subset=dedupe_cols)

    preferred_cols = [c for c in [
        "route", "name", "ref", "fclass", "code", "osm_id", "memorial_status"
    ] if c in memorial_df.columns]

    remaining_cols = [c for c in memorial_df.columns if c not in preferred_cols + ["geometry"]]
    memorial_df = memorial_df[preferred_cols + remaining_cols]

    memorial_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved: {OUTPUT_FILE}")
    print(f"Unique flagged rows: {len(memorial_df)}")


if __name__ == "__main__":
    main()