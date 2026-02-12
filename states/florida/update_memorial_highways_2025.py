# ===========================================================
# update_memorial_highways_2025.py
# -----------------------------------------------------------
# Purpose : Append 2023–2025 highway/bridge/street designations
#           into a NEW CSV file (does NOT modify the original).
# Author  : Dhyana
# Date    : [Today's Date]
# ===========================================================

import pandas as pd
import uuid
import os

# --------------------------------------------------
# STEP 1: Load the original data safely
# --------------------------------------------------
main_csv_path = "Memorial_Roadway_Designations.csv"

if not os.path.exists(main_csv_path):
    print("❌ File not found:", main_csv_path)
    exit()

print("📂 Reading:", main_csv_path)
main_df = pd.read_csv(main_csv_path)
print("✅ Loaded successfully:", len(main_df), "rows found")

# --------------------------------------------------
# STEP 2: Add new 2023–2025 designations
# --------------------------------------------------
new_data = [
    # ==================== 2023 - CS/CS/HB 21 ====================
    ["Pinellas", "Deputy Sheriff Michael Hartwick Memorial Highway",
     "That portion of I-275 between mile markers 30 and 31 in Pinellas County is designated as 'Deputy Sheriff Michael Hartwick Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Santa Rosa", "Sgt. Maj. Thomas Richard 'Ric' Landreth Memorial Highway",
     "That portion of S.R. 87 between E. Bay Boulevard and Bob Tolbert Road in Santa Rosa County is designated as 'Sgt. Maj. Thomas Richard Landreth Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Pinellas", "SPC Zachary L. Shannon Memorial Highway",
     "Alternate U.S. 19/Bayshore Boulevard between Orange Street and Michigan Boulevard in Pinellas County is designated as 'SPC Zachary L. Shannon Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Duval", "Officer Scott Eric Bell Highway",
     "That portion of S.R. 105/Heckscher Drive between New Berlin Road East and Orahood Lane in Duval County is designated as 'Officer Scott Eric Bell Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Duval", "Officer Christopher Michael Kane Highway",
     "That portion of S.R. 9A/East Beltway 295 between Gate Parkway and Baymeadows Road in Duval County is designated as 'Officer Christopher Michael Kane Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Duval", "Coach Gwendolyn Maxwell Bridge to Ribault",
     "The bridge on Howell Drive over the Ribault River in Duval County is designated as 'Coach Gwendolyn Maxwell Bridge to Ribault.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Brevard", "Dr. Sally Ride Memorial Bridge",
     "Upon completion of construction, the NASA Causeway Bridge on S.R. 405 over the Indian River in Brevard County is designated as 'Dr. Sally Ride Memorial Bridge.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Nassau", "Corporal James McWhorter Memorial Highway",
     "That portion of I-95 between mile markers 380 and 381 in Nassau County is designated as 'Corporal James McWhorter Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Hernando", "Rush Limbaugh Way",
     "Cortez Boulevard between U.S. 41 and S.R. 50/50A in Hernando County is designated as 'Rush Limbaugh Way.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Jefferson", "Senior Inspector Rita Jane Hall Memorial Highway",
     "That portion of I-10 between mile markers 222 and 228 in Jefferson County is designated as 'Senior Inspector Rita Jane Hall Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Taylor", "Michael Scott Williams Parkway",
     "That portion of U.S. 19 between C.R. 361 and C.R. 30 in Taylor County is designated as 'Michael Scott Williams Parkway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Orange", "Officer Kevin Valencia Memorial Highway",
     "That portion of S.R. 435 between Conroy Road and Vineland Road in Orange County is designated as 'Officer Kevin Valencia Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Seminole", "Deputy Sheriff Eugene 'Stetson' Gregory Memorial Highway",
     "That portion of S.R. 46 between East Lake Mary Boulevard and the Brevard County line is designated as 'Deputy Sheriff Eugene Gregory Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["St. Lucie", "Kyle Lee Patterson Memorial Way",
     "That portion of S.R. 70/Okeechobee Road between Ideal Holding Road and C.R. 613 in St. Lucie County is designated as 'Kyle Lee Patterson Memorial Way.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Brevard", "Deputy Sheriff Barbara Ann Pill Memorial Highway",
     "That portion of S.R. 518/Eau Gallie Boulevard between Wickham Road and John Rodes Boulevard in Brevard County is designated as 'Deputy Sheriff Barbara Ann Pill Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Miami-Dade", "Mama Elsa Street",
     "That portion of S.W. 22nd Avenue between Kirk Street and Tigertail Avenue in Miami-Dade County is designated as 'Mama Elsa Street.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Baker", "Deputy Sheriff Morris Fish Memorial Intersection",
     "The intersection at S.R. 121 North and C.R. 23D in Baker County is designated as 'Deputy Sheriff Morris Fish Memorial Intersection.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Brevard", "Christa McAuliffe Bridge",
     "The bridge on S.R. 3 over the Canaveral Barge Canal in Brevard County is designated as 'Christa McAuliffe Bridge.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Broward", "Archbishop Edward A. McCarthy High School Way",
     "That portion of S.R. 823/South Flamingo Road between S.W. 52nd Street and S.W. 55th Street in Broward County is designated as 'Archbishop Edward A. McCarthy High School Way.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Wakulla/Franklin", "SSgt. Carl Philippe Enis Memorial Highway",
     "That portion of U.S. 98 between Tarpine Drive in Wakulla County and Alligator Drive in Franklin County is designated as 'SSgt. Carl Philippe Enis Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Escambia", "Lewis Bear, Jr. Memorial Highway",
     "That portion of S.R. 289/North Ninth Avenue between Bayfront Parkway and U.S. 90/East Cervantes Street in Escambia County is designated as 'Lewis Bear, Jr. Memorial Highway.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],
    ["Palm Beach", "Lois D. Martin Way",
     "Glades Road between Dixie Highway and Federal Highway in Boca Raton is designated as 'Lois D. Martin Way.'",
     "", "CS/CS/HB 21", "2023/07/01 05:00:00+00", "2023"],

    # ==================== 2024 - HB 91 ====================
    ["Multi-County", "Jimmy Buffett Memorial Highway",
     "The entire length of S.R. A1A from the Georgia state line to Key West is designated as 'Jimmy Buffett Memorial Highway.'",
     "", "HB 91", "2024/08/30 05:00:00+00", "2024"],

    # ==================== 2025 - CS/CS/HB 987 ====================
    ["Bradford", "Heroes Memorial Overpass", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Charlotte", "Sergeant Elio Diaz Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Palm Beach", "PBSO Motorman Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Pinellas", "Staff Sergeant Matthew Sitton Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Lake", "Sheriff Gary S. Borders Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Lake", "Master Deputy Bradley Link Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Seminole", "Sergeant Karl Strohsal Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Broward", "SPC Daniel J. Agami Bridge", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Walton", "Deputy William May Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Miami-Dade", "Manolo Reyes Boulevard", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Hillsborough", "Master Patrol Officer Jesse Madsen Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Orange", "Geraldine Thompson Way", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Orange", "Harris Rosen Way", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Duval", "Harry Frisch Street", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Hillsborough/Pinellas", "Senator James A. Sebesta Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Miami-Dade", "Congressman Lincoln Diaz-Balart Memorial Highway", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Monroe", "Jose Wejebe Bridge", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Miami-Dade", "Celia Cruz Way", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Palm Beach", "President Donald J. Trump Boulevard", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"],
    ["Miami-Dade", "Sonia Castro Way", "", "", "CS/CS/HB 987", "2025/07/01 05:00:00+00", "2025"]
]

print("🧾 Total new records in list:", len(new_data))

# --------------------------------------------------
# STEP 3: Prepare new DataFrame
# --------------------------------------------------
cols = ["COUNTY", "DESIGNATIO", "DESCRIPTIO", "HWY_NAME", "BILL", "EFFECTIVE_", "Year"]
new_df = pd.DataFrame(new_data, columns=cols)

# Fill additional fields as in the original file
new_df["OBJECTID"] = range(main_df["OBJECTID"].max() + 1,
                            main_df["OBJECTID"].max() + 1 + len(new_df))
new_df["COUNTY_ID"] = ""
new_df["DEDICATED"] = ""
new_df["District"] = ""
new_df["Local_Name"] = ""
new_df["BILL_URL"] = ""
new_df["CreationDa"] = ""
new_df["Creator"] = ""
new_df["EditDate"] = ""
new_df["Editor"] = ""
new_df["MemId"] = [str(i) for i in range(1, len(new_df)+1)]
new_df["Shape_Leng"] = ""
new_df["LOCAL_RES"] = ""
new_df["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(new_df))]
new_df["Shape__Length"] = ""

# Reorder columns to match the main CSV
new_df = new_df[
    ["OBJECTID","COUNTY","COUNTY_ID","DESIGNATIO","DESCRIPTIO","HWY_NAME",
     "BILL","DEDICATED","EFFECTIVE_","District","Local_Name","BILL_URL",
     "CreationDa","Creator","EditDate","Editor","MemId","Shape_Leng",
     "LOCAL_RES","GlobalID","Shape__Length"]
]

# --------------------------------------------------
# STEP 4: Merge & Save into a NEW FILE ONLY
# --------------------------------------------------
final_df = pd.concat([main_df, new_df], ignore_index=True)
output_file = "memorial_highways_test_append.csv"
final_df.to_csv(output_file, index=False)

print(f"💾 Created new file: {output_file}")
print(f"🧾 Original file remains unchanged ({len(main_df)} rows).")
print(f"✅ New file now has {len(final_df)} total rows (added {len(new_df)}).")