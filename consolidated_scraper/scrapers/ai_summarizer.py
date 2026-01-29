"""
AI Summarizer wrapper for Wikipedia content.

Uses the existing PersonSummarizer to extract biographical data from Wikipedia URLs.
"""

import sys
import os
from typing import Optional, Dict, Any

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

    def __init__(self, mistral_api_key: Optional[str] = None):
        """
        Initialize the AI summarizer.

        Args:
            mistral_api_key: Mistral API key. If not provided, will use environment variable.
        """
        self.mistral_api_key = mistral_api_key
        self._summarizer = None

        if not AI_SUMMARIZER_AVAILABLE:
            print("Warning: AI summarizer not available. Install required dependencies.")

    def _get_summarizer(self) -> Optional['PersonSummarizer']:
        """Get or create the PersonSummarizer instance."""
        if not AI_SUMMARIZER_AVAILABLE:
            return None

        if self._summarizer is None:
            try:
                self._summarizer = PersonSummarizer(MistralAPIKey=self.mistral_api_key)
            except Exception as e:
                print(f"Failed to initialize PersonSummarizer: {e}")
                return None

        return self._summarizer

    def extract_data(self, wikipedia_url: str) -> Dict[str, Any]:
        """
        Extract biographical data from a Wikipedia URL.

        Args:
            wikipedia_url: Wikipedia URL to process

        Returns:
            Dictionary with extracted data:
            - summary: 4-sentence biography
            - education: List of educational institutions
            - dob: Date of birth (YYYY-MM-DD)
            - dod: Date of death (YYYY-MM-DD)
            - place_of_birth: Birth location
            - place_of_death: Death location
            - gender: male/female/not found
            - involved_in_sports: yes/no
            - involved_in_politics: yes/no
            - involved_in_military: yes/no
            - involved_in_music: yes/no
        """
        empty_result = self._empty_result()

        if not wikipedia_url:
            return empty_result

        if not AI_SUMMARIZER_AVAILABLE:
            print("AI summarizer not available")
            return empty_result

        try:
            # Create Person object and extract data
            person = Person(wikipedia_url, MistralAPIKey=self.mistral_api_key)

            # This triggers data extraction
            summary = person.getSummary()

            return {
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

        except ValueError as e:
            print(f"Invalid URL: {e}")
            return empty_result
        except Exception as e:
            print(f"Error extracting AI data from {wikipedia_url}: {e}")
            return empty_result

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
