import pandas as pd
import re

# -----------------------------
# Load dataset
# -----------------------------
df = pd.read_csv("michigan_memorial_highways.csv")

# -----------------------------
# Column to use
# -----------------------------
NAME_COL = "MemorialHighwayName"

if NAME_COL not in df.columns:
    raise ValueError(f"{NAME_COL} not found. Columns: {df.columns.tolist()}")

# -----------------------------
# Normalize text (NO REMOVALS)
# -----------------------------
def normalize_text(text):
    if pd.isna(text):
        return None

    t = str(text)

    # Normalize quotes
    t = t.replace("’", "'").replace("“", '"').replace("”", '"')

    # Remove extra whitespace
    t = re.sub(r"\s+", " ", t)

    # Strip leading/trailing spaces
    t = t.strip()

    return t

# -----------------------------
# Apply cleaning
# -----------------------------
df["clean_name"] = df[NAME_COL].apply(normalize_text)

# -----------------------------
# Save cleaned output
# -----------------------------
df[["clean_name"]].to_csv(
    "michigan_cleaned_highway_names.txt",
    index=False,
    header=False
)

print(f"Cleaned {df.shape[0]} Michigan memorial highway names.")