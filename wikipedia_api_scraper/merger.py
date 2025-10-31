import pandas as pd


scraper = pd.read_csv("wikipedia_api_scraper/scraper_merged.csv")
df2 = pd.read_csv("matched_data/matched_data_with_odmp.csv")

total_merged = pd.merge(df2, scraper, on="DESIGNATIO", how="left")

total_merged.to_csv("matched_data/sam_noah_odmp.csv", index=False)