import csv

# --- CONFIGURATION ---
INPUT_FILE = "honorstates_allwars_results.csv"
OUTPUT_FILE = "honorstates_profiles_cleaned.csv"
REMOVE_NAMES = {"Willie Miller", "James D Pruitt", "John W Smith", "John V Smith"}
# ----------------------

with open(INPUT_FILE, "r", encoding="utf-8") as infile, \
     open(OUTPUT_FILE, "w", encoding="utf-8", newline='') as outfile:
    
    reader = csv.DictReader(infile)
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
    writer.writeheader()
    
    removed = 0
    kept = 0
    for row in reader:
        if row.get("Matched Name") in REMOVE_NAMES:
            removed += 1
            continue
        writer.writerow(row)
        kept += 1

print(f"Done. {kept} rows kept, {removed} rows removed.")
print(f"Clean file saved as: {OUTPUT_FILE}")