import pandas as pd

INPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_highways_clean.csv"
OUTPUT_FILE = "/Users/d/Capstone/openclaw/states/colorado/colorado_memorial_highways_final.csv"


def main():
    df = pd.read_csv(INPUT_FILE)

    # clean columns
    df["memorial_name"] = df["memorial_name"].str.title().str.strip()
    df["route_clean"] = df["route_clean"].str.strip()

    # remove very generic or weak entries (optional)
    df = df[df["memorial_name"].str.len() > 5]

    # remove duplicates again (safety)
    df = df.drop_duplicates(subset=["memorial_name", "route_clean"])

    # sort nicely
    df = df.sort_values(["memorial_name", "route_clean"])

    print(df.head(20))
    print(f"\nFinal memorial highways count: {len(df)}")

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved final file to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()