import pandas as pd
import re

def clean_name(road_name):
    # Remove common suffixes like Highway, Boulevard, Way, Parkway, etc.
    cleaned = re.sub(r"\s+(Memorial\s+|Historic\s+)?(Trail|GreeneWay|Overpass|Beltway|Roadway|Interchange|Corridor|Expressway|Intersection|Road|Causeway|Street|Drive|Highway|Boulevard|Way|Parkway|Bridge|Avenue|Lane)$", "", road_name, flags=re.IGNORECASE)
    punc_removed = re.sub(r'[^\w\s]', '', cleaned)
    return punc_removed.strip()