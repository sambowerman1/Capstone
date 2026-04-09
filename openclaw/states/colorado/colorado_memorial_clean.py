import pandas as pd

INPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_highways_from_shapefile.csv"
OUTPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_highways_clean.csv"


def clean_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def main():
    df = pd.read_csv(INPUT_FILE)

    for col in ["ALIAS", "DESCRIPTIO", "route_clean", "COUNTY", "CITY", "ROUTE", "ROUTESIGN"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
        else:
            df[col] = ""

    # pick best memorial name
    df["memorial_name"] = df["ALIAS"]
    df.loc[df["memorial_name"] == "", "memorial_name"] = df["DESCRIPTIO"]

    # remove blanks
    df = df[df["memorial_name"] != ""]

    # sort before grouping
    df = df.sort_values(["memorial_name", "route_clean", "COUNTY", "CITY"])

    # group repeated segments into one row per memorial name + route
    grouped = (
        df.groupby(["memorial_name", "route_clean"], dropna=False)
        .agg({
            "COUNTY": lambda x: "; ".join(sorted(set(v for v in x if v))),
            "CITY": lambda x: "; ".join(sorted(set(v for v in x if v))),
            "ROUTE": lambda x: "; ".join(sorted(set(v for v in x if v))),
            "ROUTESIGN": lambda x: "; ".join(sorted(set(v for v in x if v))),
            "DESCRIPTIO": lambda x: "; ".join(sorted(set(v for v in x if v))),
            "ALIAS": lambda x: "; ".join(sorted(set(v for v in x if v))),
        })
        .reset_index()
    )

    grouped = grouped.sort_values(["memorial_name", "route_clean"])

    print(grouped.head(20))
    print(f"\nTotal clean memorial highways: {len(grouped)}")

    grouped.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()