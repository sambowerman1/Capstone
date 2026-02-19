# dhyana_visualizations.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
from datetime import datetime
from collections import Counter
import re
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = BASE_DIR
DATA_FILE = os.path.join(
    BASE_DIR,
    "..",
    "states",
    "florida",
    "memorial_highways_test_append.csv"
)

# Shared utilities
def parse_year(x):
    if pd.isna(x) or x == "":
        return pd.NA
    x = str(x).strip()

    if x.isdigit() and len(x) == 4:
        return int(x)

    for fmt in [
        "%Y/%m/%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]:
        try:
            return datetime.strptime(x, fmt).year
        except Exception:
            continue
    return pd.NA


def normalize_county(name):
    if pd.isna(name):
        return ""

    name = str(name)
    name = re.sub(r"[‐-‒–—―]", "-", name)
    name = name.replace("-", " ")
    name = name.replace("County", "")
    name = name.replace("/", " ")
    name = re.sub(r"\s+", " ", name)
    name = name.strip().title()

    if name == "Dade":
        return "Miami Dade"
    if "Miami" in name and "Dade" in name:
        return "Miami Dade"

    return name

# Data Loader
def load_data():
    df = pd.read_csv(DATA_FILE)
    print("Loaded:", len(df), "rows")

    df["EFFECTIVE_"] = df["EFFECTIVE_"].astype(str).str.strip()
    df["EFFECTIVE_"] = df["EFFECTIVE_"].str.replace(r"\+00$", "+00:00", regex=True)
    df["Year"] = df["EFFECTIVE_"].apply(parse_year)

    df["COUNTY"] = df["COUNTY"].apply(normalize_county)

    return df


# Designations by Year
def plot_designations_by_year(df):
    year_counts = df["Year"].value_counts().sort_index()

    plt.figure(figsize=(12,6))
    year_counts.plot(kind="bar", color="steelblue", edgecolor="black")
    plt.title("Number of Memorial Roadway Designations by Year")
    plt.xlabel("Year")
    plt.ylabel("Number of Designations")
    plt.xticks(rotation=90)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "plot_designations_by_year.png"), dpi=300)
    plt.show()


# Cumulative Growth
def plot_cumulative_growth(df):
    yearly = df.groupby("Year").size().dropna().sort_index()
    cumulative = yearly.cumsum()

    plt.figure(figsize=(10,6))
    plt.plot(cumulative.index, cumulative.values,
             color="#2E8B57", linewidth=2.5, marker="o")
    plt.title("Cumulative Growth of Memorial Roadway Designations")
    plt.xlabel("Year")
    plt.ylabel("Cumulative Total")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "plot_cumulative_designations_over_time.png"), dpi=300)
    plt.show()

# Top Counties
def plot_top_counties(df):
    top = df["COUNTY"].value_counts().head(10)

    plt.figure(figsize=(10,6))
    top.plot(kind="barh", color="#f5b041", edgecolor="black")
    plt.title("Top 10 Counties with Most Designations")
    plt.xlabel("Number of Designations")
    plt.gca().invert_yaxis()
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "plot_top_counties.png"), dpi=300)
    plt.show()

# County Trends Over Time
def plot_county_trends(df):
    top_counties = df["COUNTY"].value_counts().head(6).index
    df_top = df[df["COUNTY"].isin(top_counties)]
    yearly = df_top.groupby(["Year", "COUNTY"]).size().reset_index(name="Count")

    plt.figure(figsize=(10,6))
    sns.lineplot(data=yearly, x="Year", y="Count",
                 hue="COUNTY", marker="o", linewidth=2)
    plt.title("Trends by Top Counties Over Time")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "plot_county_trends_over_time.png"), dpi=300)
    plt.show()

# Designation Types
def plot_designation_types(df):
    df["DESIGNATIO"] = df["DESIGNATIO"].astype(str).str.lower()
    df["DESCRIPTIO"] = df["DESCRIPTIO"].astype(str).str.lower()
    df["combined"] = df["DESIGNATIO"] + " " + df["DESCRIPTIO"]

    keywords = ["highway","bridge","road","drive","boulevard",
                "way","street","trail","avenue","parkway"]

    counts = Counter()
    for text in df["combined"]:
        for word in keywords:
            if re.search(rf"\b{word}\b", text):
                counts[word] += 1

    type_df = pd.DataFrame(counts.items(),
                           columns=["Type","Count"]).sort_values("Count", ascending=False)

    plt.figure(figsize=(10,6))
    plt.barh(type_df["Type"], type_df["Count"],
             color="#FF8C00", edgecolor="black")
    plt.gca().invert_yaxis()
    plt.title("Most Common Designation Types")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "plot_designation_types.png"), dpi=300)
    plt.show()

# Designations by Bill
def plot_designations_by_bill(df):
    df["BILL"] = df["BILL"].astype(str).str.strip().str.upper()
    df["BILL"] = df["BILL"].replace({"NAN": "UNKNOWN", "": "UNKNOWN"})

    top = df["BILL"].value_counts().head(10)

    plt.figure(figsize=(10,6))
    top.plot(kind="barh", color="#4682B4", edgecolor="black")
    plt.title("Top 10 Legislative Bills")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "plot_designations_by_bill.png"), dpi=300)
    plt.show()

# Designations by District
def plot_designations_by_district(df):
    df["District"] = df["District"].astype(str).str.strip()
    df["District"] = df["District"].apply(lambda x: x[0] if x and x[0].isdigit() else x)

    counts = df["District"].value_counts().sort_index()

    plt.figure(figsize=(8,6))
    counts.plot(kind="barh", color="#9370DB", edgecolor="black")
    plt.title("Designations by FDOT District")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "plot_designations_by_district.png"), dpi=300)
    plt.show()

# Florida Map
def plot_florida_map(df):
    county_counts = df["COUNTY"].value_counts().reset_index()
    county_counts.columns = ["COUNTY", "Count"]

    url = "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip"
    gdf = gpd.read_file(url)
    florida = gdf[gdf["STATEFP"] == "12"].copy()
    florida["COUNTY"] = florida["NAME"].apply(normalize_county)

    merged = florida.merge(county_counts, on="COUNTY", how="left").fillna(0)

    fig, ax = plt.subplots(figsize=(8,10))
    merged.plot(column="Count", cmap="YlOrRd",
                edgecolor="black", legend=True, ax=ax)
    ax.set_title("Florida Memorial Designations by County")
    ax.axis("off")

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "plot_florida_map_designations.png"), dpi=300)
    plt.show()

# Main Execution
def main():
    df = load_data()

    plot_designations_by_year(df)
    plot_cumulative_growth(df)
    plot_top_counties(df)
    plot_county_trends(df)
    plot_designation_types(df)
    plot_designations_by_bill(df)
    plot_designations_by_district(df)
    plot_florida_map(df)


if __name__ == "__main__":
    main()