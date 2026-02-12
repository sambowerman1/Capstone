"""
Fetch ACS 5-year (recommended) indicators for all Florida counties and write a single CSV.
Requires: requests, pandas. Get a Census API key (free) and set CENSUS_KEY as env var or paste below.

Outputs: florida_counties_demographics.csv
"""

import os
import sys
import pandas as pd
import requests

YEAR = os.environ.get("ACS_YEAR", "2023")  # 5-year endpoint "acs/acs5"
DATASET = f"https://api.census.gov/data/{YEAR}/acs/acs5"
PROFILE = f"https://api.census.gov/data/{YEAR}/acs/acs5/profile"
SUBJECT = f"https://api.census.gov/data/{YEAR}/acs/acs5/subject"
STATE_FIPS = "12"  # Florida
API_KEY = os.environ.get("CENSUS_KEY", "")  # optional for small pulls

# ---- Variables ----
# Median age (years)
VAR_MEDIAN_AGE = "DP05_0018E"  # ACS DP05 (profile): Median age
# Median household income (dollars)
VAR_MEDIAN_HH_INC = "S1901_C01_012E"  # Subject table S1901
# Median value of owner-occupied housing units (dollars)
VAR_MEDIAN_HOME_VALUE = "DP04_0089E"  # ACS DP04 (profile)

# Educational attainment (age 25+): percent high school grad or higher; percent bachelor's or higher
VAR_ED_HSPLUS = "S1501_C02_015E"      # % High school graduate or higher (25 years+)
VAR_ED_BACHPLUS = "S1501_C02_014E"    # % Bachelor's degree or higher (25 years+)

# Unemployment Rate:
VAR_UNEMPLOYMENT_RATE = "S2301_C04_001E"

#Poverty Rate:
VAR_POVERTY_RATE = "S1701_C03_001E"


# Race / Ethnicity (percent). You can choose either "alone" or "alone or in combination".
# Here we use "alone" categories from DP05 (% of total population).
# NOTE: Verify codes for your chosen YEAR at:
#   {PROFILE}/groups/DP05.html
VAR_PCT_WHITE = "DP05_0077PE"     # White alone (%)
VAR_PCT_BLACK = "DP05_0078PE"     # Black or African American alone (%)
VAR_PCT_AIAN  = "DP05_0079PE"     # American Indian and Alaska Native alone (%)
VAR_PCT_ASIAN = "DP05_0080PE"     # Asian alone (%)
VAR_PCT_NHPI  = "DP05_0081PE"     # Native Hawaiian and Other Pacific Islander alone (%)
VAR_PCT_OTHER = "DP05_0082PE"     # Some other race alone (%)
VAR_PCT_TWO   = "DP05_0083PE"     # Two or more races (%)
VAR_PCT_HISP  = "DP05_0071PE"     # Hispanic or Latino (of any race) (%)

def pull(endpoint, vars_):
    params = {
        "get": ",".join(["NAME"] + vars_),
        "for": "county:*",
        "in": f"state:{STATE_FIPS}",
    }
    if API_KEY:
        params["key"] = API_KEY
    r = requests.get(endpoint, params=params, timeout=60)
    r.raise_for_status()
    rows = r.json()
    cols = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=cols)
    # Make GEOID for stable joins
    df["GEOID"] = df["state"] + df["county"]
    return df


def main():
    # Profile: median age + home value + race/ethnicity percents
    prof_vars = [
        VAR_MEDIAN_AGE, VAR_MEDIAN_HOME_VALUE,
        VAR_PCT_WHITE, VAR_PCT_BLACK, VAR_PCT_ASIAN, VAR_PCT_AIAN, VAR_PCT_NHPI, VAR_PCT_OTHER, VAR_PCT_TWO, VAR_PCT_HISP
    ]
    df_profile = pull(PROFILE, prof_vars)

    # Subject: income + education
    subj_vars = [VAR_MEDIAN_HH_INC, VAR_ED_HSPLUS, VAR_ED_BACHPLUS, VAR_UNEMPLOYMENT_RATE,
    VAR_POVERTY_RATE]
    df_subject = pull(SUBJECT, subj_vars)

    # Merge
    df = df_profile.merge(df_subject[["GEOID"] + subj_vars], on="GEOID", how="left")

    # Clean and rename
    rename_map = {
        "NAME": "County",
        VAR_MEDIAN_AGE: "Median_Age",
        VAR_MEDIAN_HH_INC: "Median_Household_Income",
        VAR_MEDIAN_HOME_VALUE: "Median_Home_Value",
        VAR_PCT_WHITE: "Pct_White_Alone",
        VAR_PCT_BLACK: "Pct_Black_Alone",
        VAR_PCT_ASIAN: "Pct_Asian_Alone",
        VAR_PCT_AIAN: "Pct_AIAN_Alone",
        VAR_PCT_NHPI: "Pct_NHPI_Alone",
        VAR_PCT_OTHER: "Pct_SomeOther_Alone",
        VAR_PCT_TWO: "Pct_TwoOrMore",
        VAR_PCT_HISP: "Pct_Hispanic",
        VAR_ED_HSPLUS: "HS_Grad_or_Higher",
        VAR_ED_BACHPLUS: "Bachelors_or_Higher",
        VAR_UNEMPLOYMENT_RATE: "Unemployment_Rate",
        VAR_POVERTY_RATE: "Pct_Below_Poverty_Level"
    }
    df = df.rename(columns=rename_map)

    # Keep useful columns and cast numeric
    keep = ["GEOID", "state", "county", "County",
            "Median_Age", "Median_Household_Income", "Median_Home_Value",
            "Pct_White_Alone","Pct_Black_Alone","Pct_Asian_Alone","Pct_AIAN_Alone","Pct_NHPI_Alone","Pct_SomeOther_Alone","Pct_TwoOrMore","Pct_Hispanic",
            "HS_Grad_or_Higher","Bachelors_or_Higher","Unemployment_Rate","Pct_Below_Poverty_Level"]
    df = df[keep].copy()
    df = df.assign(
        StateFIPS=df["state"],
        CountyFIPS=df["county"]
    )
    df = df.drop(columns=["state", "county"])

    # Convert to numeric where appropriate
    num_cols = [c for c in df.columns if c not in ("GEOID","County","StateFIPS","CountyFIPS")]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Sort by county name
    df = df.sort_values("County").reset_index(drop=True)

    out_path = "florida_counties_demographics.csv"
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(df)} rows.")

    df_acs = pd.read_csv("florida_counties_demographics.csv")

    # --- Load Florida voter registration spreadsheet ---
    df_reg = pd.read_excel("party-affiliation-by-county-2024.xlsx", skiprows=3)

    # Clean up columns
    df_reg = df_reg.rename(columns={
        "County": "County",
        "Republican Party Of Florida": "Registered_Republican",
        "Florida Democratic Party": "Registered_Democrat",
        "Minor Party": "Registered_Minor",
        "No Party Affiliation": "Registered_NPA",
        "Totals": "Registered_Total"
    })


    # Standardize county names to match ACS dataset
    df_reg["County"] = df_reg["County"].str.title().str.strip() + " County, Florida"

    df_reg.at[12, "County"] = "DeSoto County, Florida"

    # --- Merge on county name ---
    df_merged = df_acs.merge(df_reg, on="County", how="left")

    # Read in election data
    election_df = pd.read_excel("florida_2024_presidential_votes_full.xlsx")

    # Select the first three columns (county names, trump votes, harris votes)
    election_subset = election_df.iloc[:, :3]  # First three columns

    #rename counties to match other dataset
    election_subset["County"] = df_reg["County"].str.title().str.strip()

    election_subset.at[12, "County"] = "DeSoto County, Florida"
    # Merge
    df_merged_election = df_merged.merge(election_subset, on='County', how='left')

    # --- Save merged dataset ---
    df_merged_election.to_csv("florida_counties_demographics_with_voterreg.csv", index=False)

    print(f"Wrote merged dataset with {len(df_merged)} rows")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print("HTTPError:", e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error:", e, file=sys.stderr)
        sys.exit(2)
