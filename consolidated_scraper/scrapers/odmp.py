"""
ODMP (Officer Down Memorial Page) scraper.

Searches for fallen officers by name and extracts biographical information.
"""

import time
import logging
from typing import Optional, Dict, Any, List

from bs4 import BeautifulSoup, NavigableString
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from rapidfuzz import fuzz

from ..timing import Timer, TimingStats


# Configure logger
logger = logging.getLogger(__name__)


class ODMPScraper:
    """Scraper for the Officer Down Memorial Page (odmp.org)."""

    BASE_URL = "https://www.odmp.org"
    DEFAULT_THRESHOLD = 92

    def __init__(self, state: str, threshold: int = DEFAULT_THRESHOLD):
        """
        Initialize the ODMP scraper.

        Args:
            state: US state to search (e.g., 'texas', 'california')
            threshold: Fuzzy match threshold (0-100), default 92
        """
        self.state = state.lower()
        self.threshold = threshold
        self.driver = None
        self._officer_urls_cache = None
        
        # Timing stats for performance tracking
        self.timing_stats = TimingStats("ODMP Scraper")
        self.url_collection_time: float = 0.0
        self.profile_scrape_times: List[float] = []

    def _get_driver(self) -> webdriver.Chrome:
        """Create and return a headless Chrome WebDriver."""
        start = time.perf_counter()
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        elapsed = time.perf_counter() - start
        logger.info(f"[TIMING] ODMP Driver Initialization: {elapsed:.2f}s")
        return driver

    def _ensure_driver(self):
        """Ensure driver is initialized."""
        if self.driver is None:
            self.driver = self._get_driver()

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize name for comparison."""
        return " ".join(name.lower().split())

    def _is_fuzzy_match(self, name_from_site: str, target_name: str) -> tuple:
        """
        Check if two names are a fuzzy match.

        Args:
            name_from_site: Name found on ODMP
            target_name: Name we're searching for

        Returns:
            Tuple of (is_match, score)
        """
        n1 = self._normalize_name(name_from_site)
        n2 = self._normalize_name(target_name)

        # Multiple scoring strategies for reliability
        scores = [
            fuzz.token_sort_ratio(n1, n2),
            fuzz.token_set_ratio(n1, n2),
            fuzz.partial_ratio(n1, n2),
        ]

        best_score = max(scores)
        return best_score >= self.threshold, best_score

    def _click_next_page(self, wait: WebDriverWait) -> bool:
        """
        Click the 'Next' button in pagination.

        Returns:
            True if clicked successfully, False if no more pages
        """
        try:
            buttons = self.driver.find_elements(
                By.CSS_SELECTOR, "button.mat-paginator-navigation-next"
            )

            if not buttons:
                return False

            next_button = buttons[0]

            if "mat-button-disabled" in next_button.get_attribute("class"):
                return False

            self.driver.execute_script("arguments[0].scrollIntoView();", next_button)
            self.driver.execute_script("arguments[0].click();", next_button)

            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "mat-card")))
            time.sleep(2)
            return True

        except Exception as e:
            logger.warning(f"Pagination error: {e}")
            return False

    def _collect_officer_urls(self) -> List[str]:
        """
        Collect all officer profile URLs from the state browse page.

        Returns:
            List of officer profile URLs
        """
        if self._officer_urls_cache is not None:
            logger.info("[TIMING] ODMP URL Collection: 0.00s (cached)")
            return self._officer_urls_cache

        start_time = time.perf_counter()
        
        self._ensure_driver()
        wait = WebDriverWait(self.driver, 10)

        start_url = f"{self.BASE_URL}/search/browse/{self.state}"
        self.driver.get(start_url)

        officer_links = set()
        page_number = 1

        while True:
            logger.info(f"Scanning browse page {page_number}...")

            try:
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "mat-card")))
            except Exception:
                break

            cards = self.driver.find_elements(By.CSS_SELECTOR, "mat-card")

            for i in range(len(cards)):
                try:
                    cards = self.driver.find_elements(By.CSS_SELECTOR, "mat-card")
                    if i >= len(cards):
                        break
                    card = cards[i]

                    self.driver.execute_script("arguments[0].scrollIntoView();", card)
                    time.sleep(1)

                    self.driver.execute_script("arguments[0].click();", card)

                    wait.until(EC.url_contains("/officer/"))
                    officer_links.add(self.driver.current_url)
                    logger.info(f"  Found: {self.driver.current_url}")

                    self.driver.back()
                    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "mat-card")))
                    time.sleep(2)

                except Exception as e:
                    logger.warning(f"  Skipping card due to error: {e}")

            if self._click_next_page(wait):
                page_number += 1
            else:
                break

        self._officer_urls_cache = sorted(officer_links)
        
        self.url_collection_time = time.perf_counter() - start_time
        logger.info(f"[TIMING] ODMP URL Collection: {self.url_collection_time:.2f}s ({len(self._officer_urls_cache)} officers found)")
        
        return self._officer_urls_cache

    def _scrape_officer_profile(self, url: str) -> Dict[str, Any]:
        """
        Scrape biographical data from an officer profile page.

        Args:
            url: Officer profile URL

        Returns:
            Dictionary with officer data
        """
        start_time = time.perf_counter()
        
        self._ensure_driver()
        wait = WebDriverWait(self.driver, 10)
        self.driver.get(url)

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1")))
        soup = BeautifulSoup(self.driver.page_source, "lxml")

        data = {"source_url": url}

        # Extract name
        h1 = soup.find("h1")
        data["name"] = h1.get_text(strip=True) if h1 else None

        def get_value_after_strong(label: str) -> Optional[str]:
            strong_tags = soup.find_all(
                "strong", string=lambda s: s and label.lower() in s.lower()
            )
            for tag in strong_tags:
                next_node = tag.next_sibling
                if isinstance(next_node, NavigableString):
                    value = next_node.strip()
                    if value:
                        return value
            return None

        data["age"] = get_value_after_strong("Age:")
        data["tour"] = get_value_after_strong("Tour:")
        data["badge"] = get_value_after_strong("Badge:")
        data["cause"] = get_value_after_strong("Cause:")

        # End of watch
        eow = soup.find(string=lambda s: s and "End of Watch:" in s)
        if eow:
            data["end_of_watch"] = eow.replace("End of Watch:", "").strip()
        else:
            data["end_of_watch"] = None

        # Bio (main memorial narrative)
        bio_blocks = soup.find_all("p")
        long_paragraphs = [
            p.get_text(" ", strip=True) for p in bio_blocks
            if len(p.get_text(strip=True)) > 200
        ]
        data["bio"] = long_paragraphs[0] if long_paragraphs else None

        # Incident details
        incident_header = soup.find("h2", string=lambda s: s and "Incident Details" in s)
        incident_text = []

        if incident_header:
            for sib in incident_header.find_next_siblings():
                if sib.name == "h2":
                    break
                if sib.name == "p":
                    incident_text.append(sib.get_text(" ", strip=True))

        data["incident_details"] = " ".join(incident_text) if incident_text else None

        elapsed = time.perf_counter() - start_time
        self.profile_scrape_times.append(elapsed)
        logger.info(f"Scraped: {data['name']} ({elapsed:.2f}s)")
        
        return data

    def search_officer(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Search for an officer by name.

        Args:
            name: Person name to search for

        Returns:
            Dictionary with officer data if found, None otherwise
        """
        search_start = time.perf_counter()
        
        logger.info(f"\nSearching ODMP for: {name}")
        officer_urls = self._collect_officer_urls()

        profiles_checked = 0
        for url in officer_urls:
            try:
                officer = self._scrape_officer_profile(url)
                profiles_checked += 1

                if officer["name"]:
                    match, score = self._is_fuzzy_match(officer["name"], name)

                    if match:
                        officer["fuzzy_score"] = score
                        elapsed = time.perf_counter() - search_start
                        logger.info(f"[TIMING] ODMP Search Total: {elapsed:.2f}s (found match after {profiles_checked} profiles)")
                        logger.info(f"Match found ({score}): {officer['name']}")
                        return officer

                time.sleep(1)
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")

        elapsed = time.perf_counter() - search_start
        logger.info(f"[TIMING] ODMP Search Total: {elapsed:.2f}s (no match, checked {profiles_checked} profiles)")
        logger.info(f"No ODMP match found for: {name}")
        return None

    def get_timing_summary(self) -> str:
        """Get a summary of ODMP scraper timing."""
        lines = [
            "",
            "-" * 40,
            "ODMP Scraper Timing Breakdown:",
            f"  URL Collection: {self.url_collection_time:.2f}s",
        ]
        
        if self.profile_scrape_times:
            total_profile_time = sum(self.profile_scrape_times)
            avg_profile_time = total_profile_time / len(self.profile_scrape_times)
            lines.append(f"  Profile Scrapes: {total_profile_time:.2f}s total")
            lines.append(f"    - Count: {len(self.profile_scrape_times)} profiles")
            lines.append(f"    - Average: {avg_profile_time:.2f}s per profile")
        
        lines.append("-" * 40)
        return "\n".join(lines)

    def close(self):
        """Close the WebDriver."""
        if self.driver:
            self.driver.quit()
            self.driver = None
