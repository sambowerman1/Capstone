"""
Individual scrapers for ODMP, Wikidata, and AI summarization.
"""

from .odmp import ODMPScraper
from .wikidata import WikidataScraper
from .ai_summarizer import AISummarizer

__all__ = ['ODMPScraper', 'WikidataScraper', 'AISummarizer']
