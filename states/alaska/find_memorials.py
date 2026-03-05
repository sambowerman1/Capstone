import pandas as pd
import re

df = pd.read_csv("alaska_state_highways.csv")

def looks_like_person(name):
    if pd.isna(name):
        return False

    name = name.strip()

    # match patterns like "George Parks Highway"
    pattern_full = r"^[A-Z][a-z]+ [A-Z][a-z]+ (Highway|Parkway|Expressway|Boulevard|Trail)$"

    # match patterns like "Richardson Highway"
    pattern_last = r"^[A-Z][a-z]+ (Highway|Parkway|Expressway|Boulevard|Trail)$"

    return bool(re.match(pattern_full, name) or re.match(pattern_last, name))


memorial = df[df["name"].apply(looks_like_person)].copy()

memorial["route"] = memorial["ref"].str.replace(" ", "-")

memorial = memorial[["route", "name"]]

memorial.to_csv("alaska_person_memorial_highways.csv", index=False)

print("Saved alaska_person_memorial_highways.csv")
print(memorial)