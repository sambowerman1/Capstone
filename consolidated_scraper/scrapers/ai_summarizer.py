"""
AI Summarizer wrapper for Wikipedia content.

Uses the existing PersonSummarizer to extract biographical data from Wikipedia URLs.
"""

import sys
import os
import time
import logging
from typing import Optional, Dict, Any

from ..timing import Timer

# Configure logger
logger = logging.getLogger(__name__)

# Add parent directory to path to import person_summarizer
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'ai_summarizer'))

try:
    from person_summarizer import Person, PersonSummarizer
    AI_SUMMARIZER_AVAILABLE = True
except ImportError:
    AI_SUMMARIZER_AVAILABLE = False
    Person = None
    PersonSummarizer = None


class AISummarizer:
    """
    Wrapper for the AI summarizer that extracts biographical data from Wikipedia.
    """

    def __init__(self, api_key: Optional[str] = None, mistral_api_key: Optional[str] = None):
        """
        Initialize the AI summarizer.

        Args:
            api_key: Xiaomi MiMo API key. If not provided, will use environment variable.
            mistral_api_key: Deprecated alias for api_key, kept for backward compatibility.
        """
        self.api_key = api_key or mistral_api_key
        self._summarizer = None
        
        self.last_extraction_time: float = 0.0
        self.extraction_times: list = []

        if not AI_SUMMARIZER_AVAILABLE:
            logger.warning("Warning: AI summarizer not available. Install required dependencies.")

    def _get_summarizer(self) -> Optional['PersonSummarizer']:
        """Get or create the PersonSummarizer instance."""
        if not AI_SUMMARIZER_AVAILABLE:
            return None

        if self._summarizer is None:
            try:
                self._summarizer = PersonSummarizer(api_key=self.api_key)
            except Exception as e:
                logger.error(f"Failed to initialize PersonSummarizer: {e}")
                return None

        return self._summarizer

    def extract_data(self, wikipedia_url: str, designation: Optional[str] = None,
                     state: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract biographical data from a Wikipedia URL.

        Args:
            wikipedia_url: Wikipedia URL to process
            designation: Highway designation string for page validation
            state: State name for page validation

        Returns:
            Dictionary with extracted data including summary, dates, places, and flags.
        """
        empty_result = self._empty_result()

        if not wikipedia_url:
            return empty_result

        if not AI_SUMMARIZER_AVAILABLE:
            logger.warning("AI summarizer not available")
            return empty_result

        start_time = time.perf_counter()
        
        try:
            logger.info(f"[AI] Creating Person object for: {wikipedia_url}")
            person_start = time.perf_counter()
            person = Person(wikipedia_url, api_key=self.api_key,
                            designation=designation, state=state)
            person_init_time = time.perf_counter() - person_start
            logger.info(f"[TIMING] AI Person Init: {person_init_time:.2f}s")

            summary_start = time.perf_counter()
            summary = person.getSummary()
            summary_time = time.perf_counter() - summary_start
            logger.info(f"[TIMING] AI Data Extraction (crawl + API): {summary_time:.2f}s")

            result = {
                "summary": summary,
                "education": person.getEducation() or [],
                "dob": person.getDOB(),
                "dod": person.getDOD(),
                "place_of_birth": person.getPlaceOfBirth(),
                "place_of_death": person.getPlaceOfDeath(),
                "gender": person.getGender(),
                "involved_in_sports": person.getInvolvedInSports(),
                "involved_in_politics": person.getInvolvedInPolitics(),
                "involved_in_military": person.getInvolvedInMilitary(),
                "involved_in_music": person.getInvolvedInMusic()
            }
            
            self.last_extraction_time = time.perf_counter() - start_time
            self.extraction_times.append(self.last_extraction_time)
            logger.info(f"[TIMING] AI Total Extraction: {self.last_extraction_time:.2f}s")
            
            return result

        except ValueError as e:
            elapsed = time.perf_counter() - start_time
            logger.error(f"Invalid URL ({elapsed:.2f}s): {e}")
            return empty_result
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            logger.error(f"Error extracting AI data from {wikipedia_url} ({elapsed:.2f}s): {e}")
            return empty_result

    def get_timing_summary(self) -> str:
        """Get a summary of AI summarizer timing."""
        lines = [
            "",
            "-" * 40,
            "AI Summarizer Timing Breakdown:",
        ]
        
        if self.extraction_times:
            total_time = sum(self.extraction_times)
            avg_time = total_time / len(self.extraction_times)
            lines.append(f"  Extractions: {total_time:.2f}s total")
            lines.append(f"    - Count: {len(self.extraction_times)}")
            lines.append(f"    - Average: {avg_time:.2f}s per extraction")
        else:
            lines.append("  No extractions performed")
        
        lines.append("-" * 40)
        return "\n".join(lines)

    @staticmethod
    def _empty_result() -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            "summary": None,
            "education": [],
            "dob": None,
            "dod": None,
            "place_of_birth": None,
            "place_of_death": None,
            "gender": None,
            "involved_in_sports": None,
            "involved_in_politics": None,
            "involved_in_military": None,
            "involved_in_music": None
        }

    @staticmethod
    def is_available() -> bool:
        """Check if the AI summarizer is available."""
        return AI_SUMMARIZER_AVAILABLE
