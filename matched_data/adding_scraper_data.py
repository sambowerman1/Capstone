import pandas as pd

df = pd.read_csv("matched_data/matched_data_with_odmp.csv")
columns = ['DESIGNATIO','Primary Occupation','Race','Sex','Birth Date','Death Date','Wikipedia Link']
scraper = pd.read_csv("wikipedia_api_scraper/merged_output.csv")[columns]
merged = pd.merge(df, scraper, on="DESIGNATIO", how="left")
merged = merged.drop_duplicates(subset=["DESIGNATIO"], keep="first")

merged.to_csv("matched_data/full_data.csv", index=False)