"""
Fetch ACS 5-year (recommended) indicators for all Texas counties and write a single CSV.
Requires: requests, pandas. Get a Census API key (free) and set CENSUS_KEY as env var or paste below.

Outputs: texas_counties_demographics.csv
"""

import os
import sys
import pandas as pd
import requests

YEAR = os.environ.get("ACS_YEAR", "2023")  # 5-year endpoint "acs/acs5"
DATASET = f"https://api.census.gov/data/{YEAR}/acs/acs5"
PROFILE = f"https://api.census.gov/data/{YEAR}/acs/acs5/profile"
SUBJECT = f"https://api.census.gov/data/{YEAR}/acs/acs5/subject"
STATE_FIPS = "48"  # Texas
API_KEY = os.environ.get("CENSUS_KEY", "")  # optional for small pulls

# ---- Variables ----
VAR_MEDIAN_AGE = "DP05_0018E"
VAR_MEDIAN_HH_INC = "S1901_C01_012E"
VAR_MEDIAN_HOME_VALUE = "DP04_0089E"
VAR_ED_HSPLUS = "S1501_C02_015E"
VAR_ED_BACHPLUS = "S1501_C02_014E"
VAR_UNEMPLOYMENT_RATE = "S2301_C04_001E"
VAR_POVERTY_RATE = "S1701_C03_001E"
VAR_PCT_WHITE = "DP05_0077PE"
VAR_PCT_BLACK = "DP05_0078PE"
VAR_PCT_AIAN  = "DP05_0079PE"
VAR_PCT_ASIAN = "DP05_0080PE"
VAR_PCT_NHPI  = "DP05_0081PE"
VAR_PCT_OTHER = "DP05_0082PE"
VAR_PCT_TWO   = "DP05_0083PE"
VAR_PCT_HISP  = "DP05_0071PE"

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
    subj_vars = [VAR_MEDIAN_HH_INC, VAR_ED_HSPLUS, VAR_ED_BACHPLUS, VAR_UNEMPLOYMENT_RATE, VAR_POVERTY_RATE]
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

    # Convert to numeric
    num_cols = [c for c in df.columns if c not in ("GEOID","County","StateFIPS","CountyFIPS")]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("County").reset_index(drop=True)

    out_path = "texas_counties_demographics.csv"
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(df)} rows.")

    # If you have Texas voter registration/election files, adjust these paths and column names accordingly
    # Example placeholders:
    # df_reg = pd.read_excel("texas_party_affiliation_2024.xlsx")
    # df_election = pd.read_excel("texas_2024_presidential_votes_full.xlsx")
    # df_merged = df.merge(df_reg, on="County", how="left").merge(df_election, on="County", how="left")
    # df_merged.to_csv("texas_counties_demographics_with_voterreg.csv", index=False)

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print("HTTPError:", e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error:", e, file=sys.stderr)
        sys.exit(2)
