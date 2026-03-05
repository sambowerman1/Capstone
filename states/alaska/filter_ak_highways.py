import pandas as pd

df = pd.read_csv("alaska_state_highways.csv")

# keep only Alaska highways (ref starts with AK)
ak = df[df["ref"].str.startswith("AK", na=False)].copy()

# remove duplicates
ak = ak.drop_duplicates()

ak.to_csv("alaska_ak_highways.csv", index=False)

print("Saved alaska_ak_highways.csv")
print(ak)