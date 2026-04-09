import geopandas as gpd
import pandas as pd
import re

INPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/Highways.shp"
OUTPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_highways_from_shapefile.csv"

KEYWORDS = [
    "memorial",
    "veteran", "veterans",
    "purple heart",
    "gold star",
    "fallen",
    "hero", "heroes",
    "patriot",
    "tribute",
    "pow", "mia",
    "freedom",
    "honor", "honour"
]

pattern = re.compile("|".join(re.escape(k) for k in KEYWORDS), re.IGNORECASE)


def safe_col(gdf, col_name):
    if col_name in gdf.columns:
        return gdf[col_name].fillna("").astype(str)
    return pd.Series([""] * len(gdf), index=gdf.index)


def make_route(row):
    sign = str(row["ROUTESIGN"]).strip()
    route = str(row["ROUTE"]).strip()

    route_num = re.sub(r"[A-Z]+$", "", route).lstrip("0")
    if not route_num:
        route_num = route

    if sign == "U.S.":
        return f"US-{route_num}"
    elif sign == "I":
        return f"I-{route_num}"
    elif sign == "SH":
        return f"SH-{route_num}"
    elif sign:
        return f"{sign}-{route_num}"
    else:
        return route


def main():
    print("Reading shapefile...")
    gdf = gpd.read_file(INPUT_FILE)

    gdf["ALIAS"] = safe_col(gdf, "ALIAS")
    gdf["DESCRIPTIO"] = safe_col(gdf, "DESCRIPTIO")
    gdf["ROUTE"] = safe_col(gdf, "ROUTE")
    gdf["ROUTESIGN"] = safe_col(gdf, "ROUTESIGN")
    gdf["COUNTY"] = safe_col(gdf, "COUNTY")
    gdf["CITY"] = safe_col(gdf, "CITY")

    mask = (
        gdf["ALIAS"].str.contains(pattern, na=False) |
        gdf["DESCRIPTIO"].str.contains(pattern, na=False)
    )

    result = gdf.loc[
        mask,
        ["ROUTE", "ROUTESIGN", "ALIAS", "DESCRIPTIO", "COUNTY", "CITY"]
    ].copy()

    result = result.drop_duplicates()

    result["route_clean"] = result.apply(make_route, axis=1)

    result = result.sort_values(
        by=["ALIAS", "route_clean", "COUNTY"],
        na_position="last"
    )

    print(result.head(20))
    print(f"\nTotal memorial-related rows: {len(result)}")

    result.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()