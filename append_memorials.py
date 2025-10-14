import pandas as pd
from datetime import datetime
import re
import shutil

CSV_PATH = "Memorial_Roadway_Designations.csv"
BACKUP_PATH = "Memorial_Roadway_Designations.backup.csv"

# -------- New memorials (Name, County string as you listed) --------
RAW_ENTRIES = [
    # 2025
    ("Heroes Memorial Overpass", "Bradford County"),
    ("Sergeant Elio Diaz Memorial Highway", "Charlotte County"),
    ("PBSO Motorman Highway", "Palm Beach County"),
    ("Staff Sergeant Matthew Sitton Memorial Highway", "Pinellas County"),
    ("Sheriff Gary S. Borders Memorial Highway", "Lake County"),
    ("Master Deputy Bradley Link Memorial Highway", "Lake County"),
    ("Sergeant Karl Strohsal Memorial Highway", "Seminole County"),
    ("SPC Daniel J. Agami Bridge", "Broward County"),
    ("Deputy William May Memorial Highway", "Walton County"),
    ("Manolo Reyes Boulevard", "Miami-Dade County"),
    ("Master Patrol Officer Jesse Madsen Memorial Highway", "Hillsborough County"),
    ("Geraldine Thompson Way", "Orange County"),
    ("Harris Rosen Way", "Orange County"),
    ("Harry Frisch Street", "Duval County"),
    ("Senator James A. Sebesta Memorial Highway", "Hillsborough and Pinellas Counties"),
    ("Congressman Lincoln Diaz-Balart Memorial Highway", "Miami-Dade County"),
    ("Jose Wejebe Bridge", "Monroe County"),
    ("Celia Cruz Way", "Miami-Dade County"),
    ("President Donald J. Trump Boulevard", "Palm Beach County"),
    ("Sonia Castro Way", "Miami-Dade County"),
    ("Deputy William Gentry, Jr., Memorial Highway", "Highlands County"),
    # 2024
    ("Jimmy Buffett Memorial Highway", ""),
    # 2023
    ("Deputy Sheriff Michael Hartwick Memorial Highway", "Pinellas County"),
    ("Sgt. Maj. Thomas Richard ‘Ric’ Landreth Memorial Highway", "Santa Rosa County"),
    ("SPC Zachary L. Shannon Memorial Highway", "Pinellas County"),
    ("Officer Scott Eric Bell Highway", "Duval County"),
    ("Officer Christopher Michael Kane Highway", "Duval County"),
    ("Coach Gwendolyn Maxwell Bridge", "Duval County"),
    ("Dr. Sally Ride Memorial Bridge", "Brevard County"),
    ("Corporal James McWhorter Memorial Highway", "Nassau County"),
    ("Rush Limbaugh Way", "Hernando County"),
    ("Senior Inspector Rita Jane Hall Memorial Highway", "Jefferson County"),
    ("Michael Scott Williams Parkway", "Taylor County"),
    ("Officer Kevin Valencia Memorial Highway", "Orange County"),
    ("Deputy Sheriff Eugene ‘Stetson’ Gregory Memorial Highway", "Seminole County and Brevard County"),
    ("Kyle Lee Patterson Memorial Way", "St. Lucie County"),
    ("Deputy Sheriff Barbara Ann Pill Memorial Highway", "Brevard County"),
    ("Mama Elsa Street", "Miami-Dade County"),
    ("Deputy Sheriff Morris Fish Memorial Intersection", "Baker County"),
    ("Christa McAuliffe Bridge", "Brevard County"),
    ("Archbishop Edward A. McCarthy High School Way", "Broward County"),
    ("SSgt. Carl Philippe Enis Memorial Highway", "Franklin County"),
    ("Lewis Bear, Jr., Memorial Highway", "Escambia County"),
    ("Lois D. Martin Way", "Palm Beach County"),
    ("Armand and Perry Lovell Memorial Highway", "Marion County"),
    # 2022
    ("98 Points of Light Way", "Miami-Dade County"),
    # 2021
    ("Deputy Michael J. Magli Memorial Road", "Pinellas County"),
    ("Sergeant Brian LaVigne Road", "Hillsborough County"),
    ("Officer Jesse Madsen Memorial Highway", "Hillsborough County"),
]

# -------- Helpers --------
def normalize_counties_to_names(val: str):
    if not isinstance(val, str) or not val.strip():
        return [""]
    s = val.strip().replace("Counties", "County")
    s = re.sub(r"\s+and\s+", ",", s)
    parts = [p.strip() for p in re.split(r",|;", s) if p.strip()]
    names = []
    for p in parts:
        if p.endswith(" County"):
            p = p[:-7]
        p = " ".join(p.split())
        names.append(p)
    return names or [""]

def clean_text(x: str):
    if not isinstance(x, str):
        return x
    x = x.replace("Conunty", "County").replace("Seminle", "Seminole").replace("St.Lucie", "St. Lucie")
    return " ".join(x.split())

# -------- Load CSV & backup --------
df = pd.read_csv(CSV_PATH, dtype=str)
shutil.copyfile(CSV_PATH, BACKUP_PATH)

required_cols = ["OBJECTID","COUNTY","COUNTY_ID","DESIGNATIO","DESCRIPTIO","HWY_NAME",
                 "BILL","DEDICATED","EFFECTIVE_","District","Local_Name","BILL_URL",
                 "CreationDa","Creator","EditDate","Editor","MemId","Shape_Leng",
                 "LOCAL_RES","GlobalID","Shape__Length"]

for c in required_cols:
    if c not in df.columns:
        df[c] = ""

def to_int_safe(v):
    try: return int(float(v))
    except: return None

max_obj = max([to_int_safe(v) for v in df["OBJECTID"]] + [0])
next_obj = (max_obj or 0) + 1

today = datetime.today().strftime("%Y-%m-%d")

new_rows = []
for name, county_field in RAW_ENTRIES:
    for county_name in normalize_counties_to_names(county_field):
        row = {c: "" for c in required_cols}
        row["OBJECTID"]   = str(next_obj); next_obj += 1
        row["COUNTY"]     = clean_text(county_name)
        row["DESCRIPTIO"] = clean_text(name)
        row["Local_Name"] = clean_text(name)
        row["CreationDa"] = today
        row["Creator"]    = "append_memorials.py"
        row["EditDate"]   = today
        row["Editor"]     = "append_memorials.py"
        new_rows.append(row)

new_df = pd.DataFrame(new_rows, columns=required_cols)
combo = pd.concat([df[required_cols], new_df], ignore_index=True)

for col in ["COUNTY", "Local_Name", "DESCRIPTIO"]:
    combo[col] = combo[col].apply(clean_text)

combo = combo.drop_duplicates(subset=["COUNTY","Local_Name"], keep="first")

combo.to_csv(CSV_PATH, index=False)
print(f"✅ Added {len(new_df)} new rows (expanded by county name only).")
print(f"💾 Updated: {CSV_PATH}")
