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
    if text is None:
        return ""
    text = str(text)
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument("--headless=new")  # optional

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# ---------------------------------------------------------------------
# Close popups
# ---------------------------------------------------------------------
def close_popup(driver):
    time.sleep(1)
    selectors = [
        "button.cc-dismiss",
        "a.cc-btn",
        "button[aria-label='Close']",
        "button.optanon-alert-box-close",
        ".modal button"
    ]
    for selector in selectors:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, selector):
                if el.is_displayed():
                    driver.execute_script("arguments[0].click();", el)
                    time.sleep(1)
                    return
        except Exception:
            continue

# ---------------------------------------------------------------------
# Search officer (California)
# ---------------------------------------------------------------------
def search_officer(driver, name):
    search_url = "https://www.odmp.org/search/browse/california"
    driver.get(search_url)
    close_popup(driver)

    wait = WebDriverWait(driver, 10)

    try:
        search_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input#q"))
        )
        search_input.clear()
        search_input.send_keys(name)
        search_input.send_keys(u'\ue007')

        wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href*='/officer/']"))
        )

        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/officer/']")
        links = list({a.get_attribute("href") for a in anchors})
        print(f"🔍 Found {len(links)} ODMP results for '{name}'")
        return links

    except TimeoutException:
        print(f"⚠️ No ODMP results for '{name}'")
        return []

# ---------------------------------------------------------------------
# Scrape officer profile
# ---------------------------------------------------------------------
def scrape_person(driver, url):
    driver.get(url)
    close_popup(driver)

    wait = WebDriverWait(driver, 15)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1")))
    except TimeoutException:
        return None

    data = {"url": url}

    def safe_find(selector):
        try:
            return driver.find_element(By.CSS_SELECTOR, selector).text
        except NoSuchElementException:
            return ""

    data["name"] = safe_find("h1")
    data["incident_description"] = safe_find(".officer-incident-description")
    data["bio_section_raw"] = safe_find(".officer-bio")
    data["incident_details_raw"] = safe_find(".incident-details")

    def extract(label, block):
        for line in block.split("\n"):
            if label in line:
                return line.split(label)[-1].strip(": ").strip()
        return ""

    data["age"] = extract("Age", data["bio_section_raw"])
    data["tour"] = extract("Tour", data["bio_section_raw"])
    data["badge"] = extract("Badge", data["bio_section_raw"])
    data["cause"] = extract("Cause", data["incident_details_raw"])
    data["weapon"] = extract("Weapon", data["incident_details_raw"])

    return {k: clean_text(v) for k, v in data.items()}

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    output_file = "odmp_california_officers.csv"
    driver = init_driver()

    with open("cleaned_names.txt", encoding="utf-8") as f:
        target_names = [line.strip() for line in f if line.strip()]

    fieldnames = [
        "url", "name", "age", "tour", "badge",
        "cause", "weapon",
        "incident_description",
        "bio_section_raw", "incident_details_raw"
    ]

    scraped_urls = set()
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                scraped_urls.add(row.get("url"))

    for name in target_names:
        print(f"\n🔎 Searching ODMP for: {name}")
        links = search_officer(driver, name)

        for link in links:
            if link in scraped_urls:
                continue

            record = scrape_person(driver, link)
            if record:
                with open(output_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if f.tell() == 0:
                        writer.writeheader()
                    writer.writerow(record)
                scraped_urls.add(link)
                print(f"✅ Saved: {record['name']}")

            time.sleep(random.uniform(1.5, 3.5))

    driver.quit()
    print("\n✅ Finished California ODMP scraping")

if __name__ == "__main__":
    main()