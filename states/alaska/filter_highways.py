import pandas as pd

df = pd.read_csv("alaska_highways.csv")

# keep rows that have a route number
df = df[df["ref"].notna()]

df.to_csv("alaska_state_highways.csv", index=False)

print("Saved alaska_state_highways.csv")