"""
Data models for the consolidated scraper.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, List


@dataclass
class PersonRecord:
    """
    Combined data record for a person from all three scrapers.

    Fields are prefixed by source:
    - odmp_*: Officer Down Memorial Page data
    - wikidata_*: Wikidata/Wikipedia data
    - ai_*: AI-generated summary data from Wikipedia content
    """

    # Input name (original and cleaned)
    input_name: str = ""
    cleaned_name: str = ""
    input_type: str = ""  # 'highway' or 'person'

    # ODMP fields
    odmp_url: Optional[str] = None
    odmp_name: Optional[str] = None
    odmp_bio: Optional[str] = None
    odmp_age: Optional[str] = None
    odmp_tour: Optional[str] = None
    odmp_badge: Optional[str] = None
    odmp_cause: Optional[str] = None
    odmp_end_of_watch: Optional[str] = None
    odmp_incident_details: Optional[str] = None
    odmp_fuzzy_score: Optional[float] = None

    # Wikidata fields
    wikidata_occupation: Optional[str] = None
    wikidata_race: Optional[str] = None
    wikidata_sex: Optional[str] = None
    wikidata_birth_date: Optional[str] = None
    wikidata_death_date: Optional[str] = None
    wikipedia_link: Optional[str] = None

    # AI summarizer fields
    ai_summary: Optional[str] = None
    ai_education: Optional[List[str]] = field(default_factory=list)
    ai_dob: Optional[str] = None
    ai_dod: Optional[str] = None
    ai_place_of_birth: Optional[str] = None
    ai_place_of_death: Optional[str] = None
    ai_gender: Optional[str] = None
    ai_involved_in_sports: Optional[str] = None
    ai_involved_in_politics: Optional[str] = None
    ai_involved_in_military: Optional[str] = None
    ai_involved_in_music: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert record to dictionary."""
        result = asdict(self)
        # Convert education list to comma-separated string for CSV compatibility
        if result.get('ai_education'):
            result['ai_education'] = ', '.join(result['ai_education'])
        return result

    @classmethod
    def get_field_names(cls) -> List[str]:
        """Get list of all field names for CSV headers."""
        return [
            'input_name', 'cleaned_name', 'input_type',
            'odmp_url', 'odmp_name', 'odmp_bio', 'odmp_age', 'odmp_tour',
            'odmp_badge', 'odmp_cause', 'odmp_end_of_watch', 'odmp_incident_details',
            'odmp_fuzzy_score',
            'wikidata_occupation', 'wikidata_race', 'wikidata_sex',
            'wikidata_birth_date', 'wikidata_death_date', 'wikipedia_link',
            'ai_summary', 'ai_education', 'ai_dob', 'ai_dod',
            'ai_place_of_birth', 'ai_place_of_death', 'ai_gender',
            'ai_involved_in_sports', 'ai_involved_in_politics',
            'ai_involved_in_military', 'ai_involved_in_music'
        ]
