import pandas as pd
import numpy as np
from pathlib import Path

# ---------- Config ----------
path_A = "Capstone/memorial_highways_test_append.csv"
path_B = "Capstone/odmp_scraping/matched_officers.csv"
name_col_A = "DESIGNATIO"   # change if different
name_col_B = "Matched Name"   # change if different
output_path = "Capstone/matched_data/matched_data_with_odmp.csv"
# ----------------------------

# Load
A = pd.read_csv(path_A, dtype=str)  # read as strings to avoid dtype surprises
B = pd.read_csv(path_B, dtype=str)

# Ensure name columns exist
if name_col_A not in A.columns:
    raise KeyError(f"'{name_col_A}' not found in datasetA columns: {A.columns.tolist()}")
if name_col_B not in B.columns:
    raise KeyError(f"'{name_col_B}' not found in datasetB columns: {B.columns.tolist()}")

# Convert NaN -> empty string for matching purposes, keep originals for output
A[name_col_A] = A[name_col_A].fillna("").astype(str)
B[name_col_B] = B[name_col_B].fillna("").astype(str)

# If B has any columns that collide with A, rename B's columns by appending _B
collision_cols = [c for c in B.columns if c in A.columns]
B_renamed = B.rename(columns={c: f"{c}_B" for c in collision_cols})
# Keep track of original-to-new names for later
b_col_map = {c: (f"{c}_B" if c in collision_cols else c) for c in B.columns}

# Final column order: all A columns (original order) then all B columns (in B_renamed order)
final_B_cols = [b_col_map[c] for c in B.columns]
final_columns = list(A.columns) + final_B_cols

# Build merged rows explicitly
out_rows = []  # list of dicts (one dict per output row)

names_B = B_renamed[name_col_B].tolist()

# create list of B dicts for quicker access
B_dicts = B_renamed.to_dict(orient="records")

for idx_a, row_a in A.reset_index().iterrows():
    nameA = str(row_a[name_col_A])
    # find all indices j in B such that B.name is contained in A.name (case-insensitive)
    matches = []
    lower_nameA = nameA.lower()
    for j, nameB in enumerate(names_B):
        if nameB and nameB.lower() in lower_nameA:
            matches.append(j)
            print(j)

    if not matches:
        # no matches -> keep one row with A fields and NaN for B fields
        out = {}
        # A columns (use values from row_a)
        for c in A.columns:
            out[c] = row_a[c]
        # B columns -> NaN
        for c in final_B_cols:
            out[c] = np.nan
        out_rows.append(out)
    else:
        # for each matching B, produce one output row (duplicate A data)
        for j in matches:
            out = {}
            for c in A.columns:
                out[c] = row_a[c]
            # add B columns
            b_row = B_dicts[j]
            for orig_b_col in B.columns:
                out_col = b_col_map[orig_b_col]
                out[out_col] = b_row.get(b_col_map[orig_b_col], np.nan)
            out_rows.append(out)

# Create DataFrame and ensure column order
merged_df = pd.DataFrame(out_rows, columns=final_columns)

# Save
merged_df.to_csv(output_path, index=False)
print(f"✅ Done. Output saved to: {Path(output_path).absolute()}")
print(f"Rows in A: {len(A)}, Rows in output: {len(merged_df)} (duplicates when multiple B matches)")
