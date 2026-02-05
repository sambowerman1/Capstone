import osmnx as ox
import geopandas as gpd
import os
import json
from mistralai import Mistral
from dotenv import load_dotenv
from shapely.ops import unary_union
from pyrosm import OSM

OSM_PBF = "description_to_county\data\osm\michigan-260204.osm.pbf"

print("Loading Michigan roads (one-time)...")
_osm = OSM(OSM_PBF)
_ROADS_WITH_REF = _osm.get_data_by_custom_criteria(
    custom_filter={"highway": True, "ref": True},
    extra_attributes=["ref", "name"],
    keep_nodes=False
)

print(f"Loaded {_ROADS_WITH_REF.shape[0]} road segments with refs")

load_dotenv()
api_key = os.getenv("MISTRAL_API_KEY")
if not api_key:
    raise RuntimeError("MISTRAL_API_KEY not found. Check your .env file.")

client = Mistral(api_key=api_key)

SYSTEM_PROMPT = """
You extract structured route info from highway descriptions.

Return ONLY valid JSON with this schema:
{
  "country": "US",
  "state": "<2-letter state code if known else null>",
  "highway": "<highway name like I-35, US-50, SR-99 etc>",
  "segment": {
    "from": "<start location or null>",
    "to": "<end location or null>"
  },
  "notes": "<any clarifications or assumptions>"
}
"""

def parse_highway_description(desc: str) -> dict:
    resp = client.chat.complete(
        model="mistral-large-latest",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": desc},
        ],
        temperature=0,
        response_format={"type": "json_object"}
    )
    text = resp.choices[0].message.content
    return json.loads(text)


COUNTY_SHP = "description_to_county/data/tl_2025_us_county/tl_2025_us_county.shp"

def load_counties(state_fips = None) -> gpd.GeoDataFrame:
    counties = gpd.read_file(COUNTY_SHP)
    if state_fips:
        counties = counties[counties["STATEFP"] == state_fips]
    return counties.to_crs(4326)

STATE_ABBR_TO_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10","DC":"11",
    "FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19","KS":"20","KY":"21",
    "LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30",
    "NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38","OH":"39",
    "OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46","TN":"47","TX":"48","UT":"49",
    "VT":"50","VA":"51","WA":"53","WV":"54","WI":"55","WY":"56"
}

def get_highway_geometry_cached(highway_ref: str):
    """
    Return Shapely geometry of a highway from cached Michigan roads
    """
    ref_norm = highway_ref.replace("-", " ").upper()

    roads = _ROADS_WITH_REF.copy()
    roads["ref_norm"] = (
        roads["ref"]
        .astype(str)
        .str.upper()
        .str.replace("-", " ")
    )

    highway = roads[roads["ref_norm"].str.contains(ref_norm, na=False)]

    if highway.empty:
        raise ValueError(f"No geometry found for {highway_ref}")

    return unary_union(highway.geometry.values)

def counties_crossed_by_geometry(counties_gdf: gpd.GeoDataFrame, line_geom) -> list[dict]:
    # intersect counties
    hits = counties_gdf[counties_gdf.intersects(line_geom)].copy()
    hits = hits.sort_values(["STATEFP", "NAME"])
    return [
        {
            "statefp": row["STATEFP"],
            "countyfp": row["COUNTYFP"],
            "name": row["NAME"],
        }
        for _, row in hits.iterrows()
    ]

def highway_description_to_counties(desc: str):
    parsed = parse_highway_description(desc)

    state = parsed.get("state")
    highway = parsed.get("highway")

    if not highway:
        raise ValueError("Could not extract highway name.")

    # normalize common formats
    highway_ref = highway.replace("-", " ").upper()

    state_fips = STATE_ABBR_TO_FIPS.get(state) if state else None
    counties = load_counties(state_fips=state_fips)

    geom = get_highway_geometry_cached(highway_ref=highway_ref)
    crossed = counties_crossed_by_geometry(counties, geom)

    return {
        "parsed": parsed,
        "counties_crossed": crossed,
    }

desc = "The portion of highway M-66 beginning at the intersection with highway M-78 in Calhoun County and extending north to the intersection with highway US-131 in Kalkaska County shall be known as the ""Green Arrow Route""."
result = highway_description_to_counties(desc)


print(result["parsed"])
print("Counties crossed:")
for c in result["counties_crossed"]:
    print("-", c["name"])
