import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# ---------------------------------
# Load your Colorado dataset
# ---------------------------------

file_path = "/Users/d/Capstone/states/colorado/colorado_memorial_highways.csv"

df = pd.read_csv(file_path)

# ---------------------------------
# Extract unique highway names
# ---------------------------------

routes = df["ALIAS"].dropna().unique()

print("Total unique routes:", len(routes))

# ---------------------------------
# Function to check memorial name
# ---------------------------------

def check_memorial(route):

    try:
        url = f"https://en.wikipedia.org/wiki/{route}"

        headers = {"User-Agent": "Mozilla/5.0"}

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        text = soup.get_text().lower()

        if "memorial highway" in text or "memorial" in text:
            return "Possible Memorial Designation"

        return None

    except:
        return None


# ---------------------------------
# Run scraper
# ---------------------------------

results = []

for route in routes:

    print("Checking:", route)

    memorial = check_memorial(route)

    results.append({
        "route": route,
        "memorial_status": memorial
    })

    time.sleep(1)


# ---------------------------------
# Save results
# ---------------------------------

results_df = pd.DataFrame(results)

output_file = "/Users/d/Capstone/states/colorado/colorado_memorial_results.csv"

results_df.to_csv(output_file, index=False)

print("Results saved to:", output_file)