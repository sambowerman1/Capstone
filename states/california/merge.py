import pandas as pd

# Loading the CSV files
df_1 = pd.read_csv("blue_star_memorial_highways.csv")
df_2 = pd.read_csv("freehighway_and_other.csv")

rename_map = {
    "NAME": "highway_name",
    "ROUTE NO": "route_no",
    "DISTRICT": "district",
    "FROM": "from_location",
    "TO": "to_location",
    "HOW NAMED": "how_named"
}

df_1 = df_1.rename(columns=rename_map)
df_2 = df_2.rename(columns=rename_map)


final_df = pd.concat(
    [df_1, df_2],
    ignore_index=True,
    sort=False
)

# Merged dataset
final_df.to_csv(
    "california_memorial_highways.csv",
    index=False
)

# -------------------------
# 5. Quick sanity check
# -------------------------
final_df.shape, final_df.columns