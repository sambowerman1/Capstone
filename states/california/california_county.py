"""
Fetch ACS 5-year indicators for all California counties and write a single CSV.
Requires: requests, pandas.
Outputs: california_counties_demographics.csv
"""

import os
import sys
import pandas as pd
import requests

YEAR = os.environ.get("ACS_YEAR", "2023")
PROFILE = f"https://api.census.gov/data/{YEAR}/acs/acs5/profile"
SUBJECT = f"https://api.census.gov/data/{YEAR}/acs/acs5/subject"
STATE_FIPS = "06"  # California
API_KEY = os.environ.get("CENSUS_KEY", "")  # optional

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
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df["GEOID"] = df["state"] + df["county"]
    return df

def main():
    # Profile table
    prof_vars = [
        VAR_MEDIAN_AGE, VAR_MEDIAN_HOME_VALUE,
        VAR_PCT_WHITE, VAR_PCT_BLACK, VAR_PCT_ASIAN,
        VAR_PCT_AIAN, VAR_PCT_NHPI, VAR_PCT_OTHER,
        VAR_PCT_TWO, VAR_PCT_HISP
    ]
    df_profile = pull(PROFILE, prof_vars)

    # Subject table
    subj_vars = [
        VAR_MEDIAN_HH_INC, VAR_ED_HSPLUS,
        VAR_ED_BACHPLUS, VAR_UNEMPLOYMENT_RATE,
        VAR_POVERTY_RATE
    ]
    df_subject = pull(SUBJECT, subj_vars)

    # Merge
    df = df_profile.merge(
        df_subject[["GEOID"] + subj_vars],
        on="GEOID",
        how="left"
    )

    # Rename columns
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

    # Keep useful columns
    keep = [
        "GEOID", "County",
        "Median_Age", "Median_Household_Income", "Median_Home_Value",
        "Pct_White_Alone","Pct_Black_Alone","Pct_Asian_Alone",
        "Pct_AIAN_Alone","Pct_NHPI_Alone","Pct_SomeOther_Alone",
        "Pct_TwoOrMore","Pct_Hispanic",
        "HS_Grad_or_Higher","Bachelors_or_Higher",
        "Unemployment_Rate","Pct_Below_Poverty_Level"
    ]
    df = df[keep].copy()

    # Convert numeric
    for col in df.columns:
        if col not in ("GEOID", "County"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("County").reset_index(drop=True)

    out_path = "california_counties_demographics.csv"
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(df)} rows.")

if __name__ == "__main__":
    main()