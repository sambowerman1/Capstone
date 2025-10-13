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
    # Ensure it's a string
    text = str(text)
    # Replace newlines/tabs with single space
    text = re.sub(r'[\r\n\t]+', ' ', text)
    # Collapse multiple spaces to one
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
        # short sleep to allow any animated popups to appear
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
                    # interact only with visible elements
                    try:
                        if el.is_displayed():
                            try:
                                el.click()
                            except Exception:
                                # fallback to JS click
                                driver.execute_script("arguments[0].click();", el)
                            print(f"✅ Closed popup using selector: {selector}")
                            time.sleep(1)
                            return
                    except Exception:
                        continue
            except Exception:
                continue

        # Last resort: send ESCAPE key to dismiss focus-trapping popups
        from selenium.webdriver.common.keys import Keys
        webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
    except Exception as e:
        # don't let popup troubles stop the scraper
        print(f"⚠️ Popup close attempt failed (continuing): {e}")


# ---------------------------------------------------------------------
# Get department links for Florida
# ---------------------------------------------------------------------
def get_department_links(driver, root_url):
    driver.get(root_url)
    close_popup(driver)
    wait = WebDriverWait(driver, 20)

    try:
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "a")))
    except TimeoutException:
        print("⚠️ Could not find any links on the root page.")
        return []

    dept_anchors = driver.find_elements(By.TAG_NAME, "a")
    dept_links = list({
        a.get_attribute("href")
        for a in dept_anchors
        if a.get_attribute("href") and "/agency/" in a.get_attribute("href")
    })
    print(f"🔗 Found {len(dept_links)} department links")
    return dept_links


# ---------------------------------------------------------------------
# Get officer links for a department
# ---------------------------------------------------------------------
def get_person_links(driver, dept_url):
    driver.get(dept_url)
    close_popup(driver)
    # small pause for dynamic content
    time.sleep(1.5)

    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/officer/']")
    person_links = list({
        a.get_attribute("href")
        for a in anchors
        if a.get_attribute("href") and "/officer/" in a.get_attribute("href")
    })

    print(f"   👮 Found {len(person_links)} officers at {dept_url}")
    return person_links


# ---------------------------------------------------------------------
# Scrape officer data
# ---------------------------------------------------------------------
def scrape_person(driver, person_url):
    driver.get(person_url)
    close_popup(driver)
    wait = WebDriverWait(driver, 15)

    try:
        # wait for a stable element (h1 or officer-bio)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1, .officer-bio, .officer-incident-description")))
    except TimeoutException:
        print(f"⚠️ Timeout loading {person_url}")
        return None

    data = {"url": person_url}

    # Name
    try:
        data["name"] = driver.find_element(By.CSS_SELECTOR, "h1").text.strip()
    except NoSuchElementException:
        data["name"] = ""

    # Incident Description
    try:
        data["incident_description"] = driver.find_element(By.CSS_SELECTOR, ".officer-incident-description").text.strip()
    except NoSuchElementException:
        data["incident_description"] = ""

    # Bio section (Age, Tour, Badge)
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

    # Incident details (Cause, Weapon)
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

    # Clean all scraped fields to single-line form
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
    root_url = "https://www.odmp.org/search/browse?state=FL"
    output_file = "odmp_florida_officers.csv"
    driver = init_driver()

    fieldnames = [
        "dept_url", "url", "name",
        "age", "tour", "badge",
        "cause", "weapon",
        "incident_description",
        "bio_section_raw", "incident_details_raw"
    ]

    # Resume support: load already scraped urls (if file exists)
    scraped_urls = set()
    if os.path.exists(output_file):
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    # handle missing url gracefully
                    if "url" in row and row["url"]:
                        scraped_urls.add(row["url"])
            print(f"🔁 Loaded {len(scraped_urls)} already scraped officers")
        except Exception as e:
            print(f"⚠️ Could not read existing CSV for resume: {e}")

    departments = get_department_links(driver, root_url)

    # iterate departments
    i = 0
    for dept_index, dept_url in enumerate(departments, start=1):
        i += 1
        print(f"\n🏛 Department {dept_index}/{len(departments)}: {dept_url}")
        try:
            person_links = get_person_links(driver, dept_url)
        except Exception as e:
            print(f"❌ Error fetching officers for {dept_url}: {e}")
            continue

        # iterate officers
        for officer_index, p_url in enumerate(person_links, start=1):
            if p_url in scraped_urls:
                print(f"⏩ Skipping already scraped officer ({officer_index}/{len(person_links)}): {p_url}")
                continue

            print(f"   👮 Officer {officer_index}/{len(person_links)}: {p_url}")
            try:
                record = scrape_person(driver, p_url)
                if record:
                    record["dept_url"] = dept_url
                    # ensure record contains all fieldnames (fill missing keys with empty)
                    row = {fn: record.get(fn, "") for fn in fieldnames}
                    write_to_csv(output_file, fieldnames, row)
                    scraped_urls.add(p_url)
                    print("      ✅ Saved record.")
            except Exception as e:
                print(f"❌ Error scraping {p_url}: {e}")

            # polite randomized delay
            time.sleep(random.uniform(1.5, 3.5))
        if i == 1:
            break

    driver.quit()
    print("\n✅ Done! Data saved to:", os.path.abspath(output_file))


# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
