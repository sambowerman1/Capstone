"""
Main ConsolidatedScraper class that orchestrates all three scrapers.
"""

import csv
from typing import Optional, List, Union
import pandas as pd

from .models import PersonRecord
from .name_cleaner import process_name
from .scrapers.odmp import ODMPScraper
from .scrapers.wikidata import WikidataScraper
from .scrapers.ai_summarizer import AISummarizer


class ConsolidatedScraper:
    """
    Consolidated scraper that combines ODMP, Wikidata, and AI summarization.

    Example usage:
        scraper = ConsolidatedScraper(odmp_state='texas')
        result = scraper.scrape_person("John Smith Memorial Highway", input_type='highway')
        print(result.odmp_cause)
        print(result.wikidata_sex)
        print(result.ai_summary)
        scraper.close()
    """

    def __init__(
        self,
        odmp_state: Optional[str] = None,
        mistral_api_key: Optional[str] = None,
        enable_odmp: bool = True,
        enable_wikidata: bool = True,
        enable_ai: bool = True,
        odmp_threshold: int = 92
    ):
        """
        Initialize the consolidated scraper.

        Args:
            odmp_state: US state for ODMP search (required if enable_odmp=True)
            mistral_api_key: Mistral API key for AI summarizer
            enable_odmp: Enable ODMP scraper
            enable_wikidata: Enable Wikidata scraper
            enable_ai: Enable AI summarizer
            odmp_threshold: Fuzzy match threshold for ODMP (0-100)
        """
        self.enable_odmp = enable_odmp
        self.enable_wikidata = enable_wikidata
        self.enable_ai = enable_ai

        # Initialize ODMP scraper
        self._odmp = None
        if enable_odmp:
            if not odmp_state:
                raise ValueError("odmp_state is required when ODMP scraper is enabled")
            self._odmp = ODMPScraper(state=odmp_state, threshold=odmp_threshold)

        # Initialize Wikidata scraper
        self._wikidata = WikidataScraper() if enable_wikidata else None

        # Initialize AI summarizer
        self._ai = None
        if enable_ai:
            self._ai = AISummarizer(mistral_api_key=mistral_api_key)

    def scrape_person(
        self,
        name: str,
        input_type: str = 'auto'
    ) -> PersonRecord:
        """
        Scrape a single person through all enabled sources.

        Args:
            name: Person name or highway designation
            input_type: 'highway', 'person', or 'auto' for auto-detection

        Returns:
            PersonRecord with combined data from all sources
        """
        # Process the name
        cleaned_name, detected_type = process_name(name, input_type)

        print(f"\n{'='*60}")
        print(f"Scraping: {name}")
        print(f"Cleaned name: {cleaned_name}")
        print(f"Input type: {detected_type}")
        print(f"{'='*60}")

        # Create record
        record = PersonRecord(
            input_name=name,
            cleaned_name=cleaned_name,
            input_type=detected_type
        )

        # Run ODMP scraper
        if self.enable_odmp and self._odmp:
            odmp_data = self._odmp.search_officer(cleaned_name)
            if odmp_data:
                record.odmp_url = odmp_data.get("source_url")
                record.odmp_name = odmp_data.get("name")
                record.odmp_bio = odmp_data.get("bio")
                record.odmp_age = odmp_data.get("age")
                record.odmp_tour = odmp_data.get("tour")
                record.odmp_badge = odmp_data.get("badge")
                record.odmp_cause = odmp_data.get("cause")
                record.odmp_end_of_watch = odmp_data.get("end_of_watch")
                record.odmp_incident_details = odmp_data.get("incident_details")
                record.odmp_fuzzy_score = odmp_data.get("fuzzy_score")

        # Run Wikidata scraper
        if self.enable_wikidata and self._wikidata:
            print(f"\nSearching Wikidata for: {cleaned_name}")
            wiki_data = self._wikidata.get_person_info(cleaned_name)
            record.wikidata_occupation = wiki_data.get("Primary Occupation")
            record.wikidata_race = wiki_data.get("Race")
            record.wikidata_sex = wiki_data.get("Sex")
            record.wikidata_birth_date = wiki_data.get("Birth Date")
            record.wikidata_death_date = wiki_data.get("Death Date")
            record.wikipedia_link = wiki_data.get("Wikipedia Link")

        # Run AI summarizer if Wikipedia link is available
        if self.enable_ai and self._ai and record.wikipedia_link:
            print(f"\nRunning AI summarizer on: {record.wikipedia_link}")
            ai_data = self._ai.extract_data(record.wikipedia_link)
            record.ai_summary = ai_data.get("summary")
            record.ai_education = ai_data.get("education", [])
            record.ai_dob = ai_data.get("dob")
            record.ai_dod = ai_data.get("dod")
            record.ai_place_of_birth = ai_data.get("place_of_birth")
            record.ai_place_of_death = ai_data.get("place_of_death")
            record.ai_gender = ai_data.get("gender")
            record.ai_involved_in_sports = ai_data.get("involved_in_sports")
            record.ai_involved_in_politics = ai_data.get("involved_in_politics")
            record.ai_involved_in_military = ai_data.get("involved_in_military")
            record.ai_involved_in_music = ai_data.get("involved_in_music")

        return record

    def scrape_batch(
        self,
        names: List[str],
        input_type: str = 'auto',
        output_file: Optional[str] = None
    ) -> List[PersonRecord]:
        """
        Scrape multiple names through all enabled sources.

        Args:
            names: List of names or highway designations
            input_type: 'highway', 'person', or 'auto' for auto-detection
            output_file: Optional CSV file to save results

        Returns:
            List of PersonRecord objects
        """
        results = []

        for i, name in enumerate(names, 1):
            print(f"\n[{i}/{len(names)}] Processing: {name}")
            try:
                record = self.scrape_person(name, input_type)
                results.append(record)
            except Exception as e:
                print(f"Error processing {name}: {e}")
                # Create empty record with just the input name
                record = PersonRecord(input_name=name, cleaned_name=name)
                results.append(record)

        if output_file:
            self._save_to_csv(results, output_file)

        return results

    def scrape_from_csv(
        self,
        file_path: str,
        name_column: str,
        input_type: str = 'auto',
        output_file: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Process names from a CSV file.

        Args:
            file_path: Path to input CSV file
            name_column: Column name containing names
            input_type: 'highway', 'person', or 'auto'
            output_file: Optional output CSV file

        Returns:
            DataFrame with results
        """
        df = pd.read_csv(file_path)

        if name_column not in df.columns:
            raise ValueError(f"Column '{name_column}' not found in CSV. Available: {list(df.columns)}")

        names = df[name_column].dropna().tolist()
        records = self.scrape_batch(names, input_type, output_file)

        # Convert to DataFrame
        result_df = pd.DataFrame([r.to_dict() for r in records])
        return result_df

    def scrape_from_text_file(
        self,
        file_path: str,
        input_type: str = 'auto',
        output_file: Optional[str] = None
    ) -> List[PersonRecord]:
        """
        Process names from a text file (one name per line).

        Args:
            file_path: Path to input text file
            input_type: 'highway', 'person', or 'auto'
            output_file: Optional output CSV file

        Returns:
            List of PersonRecord objects
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            names = [line.strip() for line in f if line.strip()]

        return self.scrape_batch(names, input_type, output_file)

    def _save_to_csv(self, records: List[PersonRecord], output_file: str):
        """Save records to CSV file."""
        if not records:
            print("No records to save")
            return

        fieldnames = PersonRecord.get_field_names()

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                writer.writerow(record.to_dict())

        print(f"\nSaved {len(records)} records to {output_file}")

    def close(self):
        """Close all scrapers and release resources."""
        if self._odmp:
            self._odmp.close()
