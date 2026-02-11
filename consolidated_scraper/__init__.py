"""
Consolidated Scraper Package

A unified interface for scraping person information from ODMP, Wikidata, and Wikipedia/AI summarization.
"""

from .scraper import ConsolidatedScraper
from .models import PersonRecord
from .name_cleaner import clean_name, detect_input_type

__all__ = ['ConsolidatedScraper', 'PersonRecord', 'clean_name', 'detect_input_type']
__version__ = '1.0.0'
