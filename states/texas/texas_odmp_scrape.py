import csv
import os
import re
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def clean_text(text):
    """Normalize multiline text into a single-line, collapse whitespace."""
    if text is None:
        return ""
    text = str(text)
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

# ---------------------------------------------------------------------
# Initialize Chrome Driver
# ---------------------------------------------------------------------
def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument("--headless=new")  # uncomment for headless
    driver = webdriver.Chrome(options=options)
    return driver

# ---------------------------------------------------------------------
# Handle pop-up modal or cookie banner (robust)
# ---------------------------------------------------------------------
def close_popup(driver):
    """Try to close cookie or info modals if they appear."""
    try:
        time.sleep(1)
        popup_selectors = [
            "button.cc-dismiss",
            "a.cc-btn",
            ".cc-window button",
            ".close",
            "button[aria-label='Close']",
            "button.optanon-alert-box-close",
            ".modal button",
        ]
        for selector in popup_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for el in elements:
                    try:
                        if el.is_displayed():
                            try:
                                el.click()
                            except Exception:
                                driver.execute_script("arguments[0].click();", el)
                            print(f"✅ Closed popup using selector: {selector}")
                            time.sleep(1)
                            return
                    except Exception:
                        continue
            except Exception:
                continue
        from selenium.webdriver.common.keys import Keys
        webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
    except Exception as e:
        print(f"⚠️ Popup close attempt failed (continuing): {e}")

# ---------------------------------------------------------------------
# Search for officer by name
# ---------------------------------------------------------------------
def search_officer(driver, name):
    """Search for an officer by name in Texas and return officer URLs."""
    search_url = "https://www.odmp.org/search/browse/texas"
    driver.get(search_url)
    close_popup(driver)
    wait = WebDriverWait(driver, 10)

    try:
        search_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input#q"))
        )
        search_input.clear()
        search_input.send_keys(name)
        search_input.send_keys(u'\ue007')  # Press Enter

        # Wait for search results
        wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href*='/officer/']"))
        )

        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/officer/']")
        officer_links = list({a.get_attribute("href") for a in anchors})
        print(f"🔍 Found {len(officer_links)} results for '{name}'")
        return officer_links
    except TimeoutException:
        print(f"⚠️ No results found or page took too long for '{name}'")
        return []

# ---------------------------------------------------------------------
# Scrape officer data
# ---------------------------------------------------------------------
def scrape_person(driver, person_url):
    driver.get(person_url)
    close_popup(driver)
    wait = WebDriverWait(driver, 15)

    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1, .officer-bio, .officer-incident-description")))
    except TimeoutException:
        print(f"⚠️ Timeout loading {person_url}")
        return None

    data = {"url": person_url}

    try:
        data["name"] = driver.find_element(By.CSS_SELECTOR, "h1").text.strip()
    except NoSuchElementException:
        data["name"] = ""

    try:
        data["incident_description"] = driver.find_element(By.CSS_SELECTOR, ".officer-incident-description").text.strip()
    except NoSuchElementException:
        data["incident_description"] = ""

    try:
        bio_section = driver.find_element(By.CSS_SELECTOR, ".officer-bio").text.strip()
    except NoSuchElementException:
        bio_section = ""
    data["bio_section_raw"] = bio_section

    def extract_field(label):
        for line in bio_section.split("\n"):
            if label in line:
                return line.split(label)[-1].strip(": ").strip()
        return ""

    data["age"] = extract_field("Age")
    data["tour"] = extract_field("Tour")
    data["badge"] = extract_field("Badge")

    try:
        details_section = driver.find_element(By.CSS_SELECTOR, ".incident-details").text.strip()
    except NoSuchElementException:
        details_section = ""
    data["incident_details_raw"] = details_section

    def extract_detail(label):
        for line in details_section.split("\n"):
            if label in line:
                return line.split(label)[-1].strip(": ").strip()
        return ""

    data["cause"] = extract_detail("Cause")
    data["weapon"] = extract_detail("Weapon")

    for k, v in list(data.items()):
        data[k] = clean_text(v)

    print(f"      ✅ Scraped: {data.get('name', '')}")
    return data

# ---------------------------------------------------------------------
# Save to CSV safely (appends, adds header if necessary)
# ---------------------------------------------------------------------
def write_to_csv(filename, fieldnames, row):
    file_exists = os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    output_file = "odmp_texas_officers.csv"
    driver = init_driver()

    # Replace with your new target names
    target_names = ["Barbara Fenley"]

    fieldnames = [
        "url", "name",
        "age", "tour", "badge",
        "cause", "weapon",
        "incident_description",
        "bio_section_raw", "incident_details_raw"
    ]

    # Resume support
    scraped_urls = set()
    if os.path.exists(output_file):
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if "url" in row and row["url"]:
                        scraped_urls.add(row["url"])
            print(f"🔁 Loaded {len(scraped_urls)} already scraped officers")
        except Exception as e:
            print(f"⚠️ Could not read existing CSV for resume: {e}")

    # Iterate through target names
    for name in target_names:
        print(f"\n🔎 Searching for: {name}")
        officer_links = search_officer(driver, name)

        for officer_index, p_url in enumerate(officer_links, start=1):
            if p_url in scraped_urls:
                print(f"⏩ Skipping already scraped officer: {p_url}")
                continue

            record = scrape_person(driver, p_url)
            if record:
                row = {fn: record.get(fn, "") for fn in fieldnames}
                write_to_csv(output_file, fieldnames, row)
                scraped_urls.add(p_url)
                print(f"      ✅ Saved record for {record['name']}")

            time.sleep(random.uniform(1.5, 3.5))

    driver.quit()
    print("\n✅ Done! Data saved to:", os.path.abspath(output_file))

# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
