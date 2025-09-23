import pandas as pd
import re

def clean_name(road_name):
    # Remove common suffixes like Highway, Boulevard, Way, Parkway, etc.
    cleaned = re.sub(r"\s+(Memorial\s+|Historic\s+)?(Trail|GreeneWay|Overpass|Beltway|Roadway|Interchange|Corridor|Expressway|Intersection|Road|Causeway|Street|Drive|Highway|Boulevard|Way|Parkway|Bridge|Avenue|Lane)$", "", road_name, flags=re.IGNORECASE)
    return cleaned.strip()

data = pd.read_csv("Memorial_Roadway_Designations.csv")
names = [clean_name(n) for n in data["DESIGNATIO"]]

with open("names.txt", "w", encoding="utf-8") as f:
    for name in names:
        print(name)
        f.write(name + "\n")