#!/usr/bin/env python3
"""
Person Summarizer Script

This script takes Wikipedia or Find a Grave links, extracts content using crawl4ai,
and generates 4-sentence summaries using Xiaomi MiMo v2 Flash (Xiaomi OpenAI-compatible API).
"""

import re
import os
import json
from typing import Optional, Dict, Any
import asyncio
from crawl4ai import AsyncWebCrawler
try:
    from dotenv import load_dotenv
    _DOTENV_AVAILABLE = True
except Exception:
    _DOTENV_AVAILABLE = False
from openai import OpenAI

# Xiaomi MiMo platform (OpenAI-compatible). Override via env if Xiaomi changes endpoints.
DEFAULT_XIAOMI_MIMO_BASE_URL = "https://api.xiaomimimo.com/v1"
# Model id from Xiaomi MiMo API (lowercase slug; see platform docs). Override with XIAOMI_MIMO_MODEL.
DEFAULT_XIAOMI_MIMO_MODEL = "mimo-v2-flash"


class PersonSummarizer:
    """A class to handle web scraping and summarization of person profiles."""
    
    def __init__(self, api_key: Optional[str] = None, MistralAPIKey: Optional[str] = None):
        """
        Initialize the PersonSummarizer.
        
        Args:
            api_key: Xiaomi MiMo API key. If not provided, uses XiaomiAIKey or XIAOMI_MIMO_API_KEY.
            MistralAPIKey: Deprecated alias for api_key, kept for backward compatibility.
        """
        if _DOTENV_AVAILABLE:
            try:
                load_dotenv(".env.local")
            except Exception:
                pass

        resolved_key = (
            api_key
            or MistralAPIKey
            or os.getenv("XiaomiAIKey")
            or os.getenv("XIAOMI_MIMO_API_KEY")
        )
        if not resolved_key:
            raise ValueError(
                "Xiaomi MiMo API key must be provided or set as XiaomiAIKey or XIAOMI_MIMO_API_KEY "
                "(OpenRouter keys are not accepted on Xiaomi's endpoint)"
            )

        base_url = os.getenv("XIAOMI_MIMO_BASE_URL", DEFAULT_XIAOMI_MIMO_BASE_URL).rstrip("/")
        self._model = os.getenv("XIAOMI_MIMO_MODEL", DEFAULT_XIAOMI_MIMO_MODEL)
        self._client = OpenAI(api_key=resolved_key, base_url=base_url)

    def _mimo_chat(
        self,
        messages: list,
        *,
        max_tokens: int,
        temperature: float,
    ):
        """Call Xiaomi MiMo chat completions (OpenAI-compatible)."""
        return self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
    
    def _is_valid_url(self, url: str) -> bool:
        """
        Validate if the URL is from Wikipedia or Find a Grave.
        
        Args:
            url: The URL to validate
            
        Returns:
            True if valid Wikipedia or Find a Grave URL, False otherwise
        """
        url = url.lower().strip()
        
        # Check for Wikipedia URLs
        wikipedia_pattern = r'https?://([\w-]+\.)?wikipedia\.org/'
        
        # Check for Find a Grave URLs
        findagrave_pattern = r'https?://(www\.)?findagrave\.com/'
        
        return bool(re.match(wikipedia_pattern, url) or re.match(findagrave_pattern, url))
    
    def _clean_markdown(self, text: str) -> str:
        """
        Remove markdown formatting from text.
        
        Args:
            text: Text that may contain markdown formatting
            
        Returns:
            Clean text without markdown formatting
        """
        # Remove bold formatting
        text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
        
        # Remove italic formatting
        text = re.sub(r'\*(.*?)\*', r'\1', text)
        
        # Remove any remaining asterisks
        text = re.sub(r'\*', '', text)
        
        # Clean up any double spaces that might result
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        Normalize date to YYYY-MM-DD.
        Handles: YYYY, YYYY-MM, YYYY-MM-DD, common natural formats (e.g., 'Jan 5, 1980', 'March 1980').
        Returns None if invalid/empty.
        """
        if not date_str:
            return None
        s = str(date_str).strip()
        if s.lower() in {"null", "none", "not found", "unknown", "n/a", "na"}:
            return None

        # ISO forms
        m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        m = re.match(r'^(\d{4})-(\d{2})$', s)
        if m:
            return f"{m.group(1)}-{m.group(2)}-01"
        m = re.match(r'^(\d{4})$', s)
        if m:
            return f"{m.group(1)}-01-01"

        # Natural language months
        month_map = {
            'jan': '01', 'january': '01',
            'feb': '02', 'february': '02',
            'mar': '03', 'march': '03',
            'apr': '04', 'april': '04',
            'may': '05',
            'jun': '06', 'june': '06',
            'jul': '07', 'july': '07',
            'aug': '08', 'august': '08',
            'sep': '09', 'sept': '09', 'september': '09',
            'oct': '10', 'october': '10',
            'nov': '11', 'november': '11',
            'dec': '12', 'december': '12',
        }
        s_clean = re.sub(r'[,\.]', '', s).lower()
        # Patterns: "jan 5 1980", "january 1980", "5 january 1980"
        m = re.match(r'^(\d{1,2})\s+([a-z]+)\s+(\d{4})$', s_clean)
        if m and m.group(2) in month_map:
            day = int(m.group(1))
            mon = month_map[m.group(2)]
            return f"{m.group(3)}-{mon}-{day:02d}"
        m = re.match(r'^([a-z]+)\s+(\d{1,2})\s+(\d{4})$', s_clean)
        if m and m.group(1) in month_map:
            day = int(m.group(2))
            mon = month_map[m.group(1)]
            return f"{m.group(3)}-{mon}-{day:02d}"
        m = re.match(r'^([a-z]+)\s+(\d{4})$', s_clean)
        if m and m.group(1) in month_map:
            mon = month_map[m.group(1)]
            return f"{m.group(2)}-{mon}-01"

        # Fallback: extract a 4-digit year if present
        m = re.search(r'(\d{4})', s_clean)
        if m:
            return f"{m.group(1)}-01-01"

        return None
    
    async def _extract_content(self, url: str) -> str:
        """
        Extract content from the URL and convert to markdown.
        
        Args:
            url: The URL to scrape
            
        Returns:
            Markdown content of the page
            
        Raises:
            Exception: If content extraction fails
        """
        try:
            async with AsyncWebCrawler(verbose=True) as crawler:
                result = await crawler.arun(
                    url=url,
                    word_count_threshold=10,
                    extraction_strategy="CosineStrategy",
                    chunking_strategy="RegexChunking",
                    bypass_cache=True
                )
                
                if result.success:
                    return result.markdown
                else:
                    raise Exception(f"Failed to crawl URL: {result.error_message}")
                    
        except Exception as e:
            raise Exception(f"Error extracting content from {url}: {str(e)}")
    
    def _generate_summary(self, markdown_content: str) -> str:
        """
        Generate a 4-sentence summary using the LLM.
        
        Args:
            markdown_content: The markdown content to summarize
            
        Returns:
            A 4-sentence summary of the person
            
        Raises:
            Exception: If summary generation fails
        """
        try:
            prompt = f"""
Please create a concise 4-sentence biographical summary of the person described in the following content. 
Focus on the most important biographical details such as their birth/death dates, notable achievements, 
profession, and historical significance. Each sentence should contain meaningful information.

Content:
{markdown_content[:4000]}

Requirements:
- Exactly 4 sentences
- Focus on biographical facts
- Include birth/death information if available
- Highlight major achievements or significance
- Use PLAIN TEXT ONLY - no markdown formatting, no bold (**), no italics (*), no special formatting
- Write in simple, clean prose without any markup
"""

            messages = [
                {"role": "user", "content": prompt}
            ]
            
            response = self._mimo_chat(
                messages,
                max_tokens=200,
                temperature=0.3,
            )
            
            summary = response.choices[0].message.content.strip()
            
            summary = self._clean_markdown(summary)
            return summary
            
        except Exception as e:
            raise Exception(f"Error generating summary: {str(e)}")
    
    def _extract_complete_data(self, markdown_content: str) -> Dict[str, Any]:
        """
        Extract both summary and structured biographical data in a single API call.
        
        Args:
            markdown_content: The markdown content to analyze
            
        Returns:
            Dictionary containing summary and structured biographical data
            
        Raises:
            Exception: If data extraction fails
        """
        try:
            prompt = f"""
Please analyze the following biographical content and return a complete JSON object with both a summary and structured data.

Content:
{markdown_content[:4000]}

Return ONLY a valid JSON object in this EXACT format:
{{
    "summary": "A concise 4-sentence biographical summary focusing on birth/death dates, notable achievements, profession, and historical significance. Use PLAIN TEXT ONLY - no markdown formatting.",
    "education": ["institution1", "degree1", "institution2"],
    "date_of_birth": "YYYY-MM-DD",
    "date_of_death": "YYYY-MM-DD",
    "place_of_birth": "City, State, Country",
    "place_of_death": "City, State, Country",
    "involved_in_sports": "yes",
    "involved_in_politics": "yes",
    "involved_in_military": "yes", 
    "involved_in_music": "yes",
    "gender": "male"
}}

Requirements:
- summary: Exactly 4 sentences, plain text, include birth/death info if available
- education: Array of educational institutions/degrees, empty array [] if none found
- date_of_birth: STRICTLY use YYYY-MM-DD format. If only year known, use YYYY-01-01. If year and month known, use YYYY-MM-01. Use null if not found.
- date_of_death: STRICTLY use YYYY-MM-DD format. If only year known, use YYYY-01-01. If year and month known, use YYYY-MM-01. Use null if not found or still alive.
- place_of_birth: Birth location as "City, State, Country" format, "not found" if not available
- place_of_death: Death location as "City, State, Country" format, "not found" if not available
- involved_in_sports: "yes" if person was involved in sports, "no" if not found or no involvement
- involved_in_politics: "yes" if person was involved in politics, "no" if not found or no involvement
- involved_in_military: "yes" if person was involved in military, "no" if not found or no involvement
- involved_in_music: "yes" if person was involved in music, "no" if not found or no involvement
- gender: "male", "female", or "not found" if cannot be determined

IMPORTANT: All dates MUST be in YYYY-MM-DD format. Never return just YYYY or YYYY-MM.

Return ONLY the JSON object, no other text.
"""

            messages = [
                {"role": "user", "content": prompt}
            ]
            
            response = self._mimo_chat(
                messages,
                max_tokens=500,
                temperature=0.1,
            )
            
            json_text = response.choices[0].message.content.strip()
            
            # Clean up potential markdown formatting around JSON
            if json_text.startswith("```json"):
                json_text = json_text.replace("```json", "").replace("```", "").strip()
            elif json_text.startswith("```"):
                json_text = json_text.replace("```", "").strip()
            
            # Try to parse the JSON response
            try:
                complete_data = json.loads(json_text)
                
                # Validate and ensure all required keys exist
                required_keys = {"summary", "education", "date_of_birth", "date_of_death", 
                               "place_of_birth", "place_of_death", "involved_in_sports", 
                               "involved_in_politics", "involved_in_military", "involved_in_music", "gender"}
                for key in required_keys:
                    if key not in complete_data:
                        if key == "summary":
                            complete_data[key] = "Summary not available."
                        elif key == "education":
                            complete_data[key] = []
                        elif key in ["involved_in_sports", "involved_in_politics", "involved_in_military", "involved_in_music"]:
                            complete_data[key] = "no"
                        elif key in ["place_of_birth", "place_of_death", "gender"]:
                            complete_data[key] = "not found"
                        else:
                            complete_data[key] = None
                
                # Clean summary of any markdown formatting
                if complete_data["summary"]:
                    complete_data["summary"] = self._clean_markdown(complete_data["summary"])
                
                # Normalize dates to ensure consistent YYYY-MM-DD format
                complete_data["date_of_birth"] = self._normalize_date(complete_data.get("date_of_birth"))
                complete_data["date_of_death"] = self._normalize_date(complete_data.get("date_of_death"))
                
                return complete_data
                
            except json.JSONDecodeError as e:
                # If JSON parsing fails, return default structure
                print(f"Warning: Failed to parse JSON response: {e}")
                print(f"Raw response: {json_text[:200]}...")
                return {
                    "summary": "Summary extraction failed.",
                    "education": [],
                    "date_of_birth": None,
                    "date_of_death": None,
                    "place_of_birth": "not found",
                    "place_of_death": "not found",
                    "involved_in_sports": "no",
                    "involved_in_politics": "no",
                    "involved_in_military": "no",
                    "involved_in_music": "no",
                    "gender": "not found"
                }
            
        except Exception as e:
            raise Exception(f"Error extracting complete data: {str(e)}")
    
    def _validate_page_relevance(self, markdown_content: str, person_name: str,
                                  designation: Optional[str] = None,
                                  state: Optional[str] = None) -> Dict[str, Any]:
        """
        Use the LLM to check whether a fetched Wikipedia page is actually about
        the person the highway was named after, not a different person with the
        same name.
        
        Args:
            markdown_content: The page content (markdown)
            person_name: The expected person name extracted from the designation
            designation: The full highway designation string
            state: The state the highway is in
            
        Returns:
            Dict with is_correct_person (bool), confidence (float), reason (str)
        """
        designation_ctx = designation or "unknown"
        state_ctx = state or "unknown"
        try:
            prompt = f"""You are validating whether a Wikipedia article is about the correct person.

A highway or memorial road has this designation: "{designation_ctx}"
State: {state_ctx}
The person the highway is believed to be named after: "{person_name}"

Here is the beginning of the Wikipedia article that was found for that person name:
---
{markdown_content[:2000]}
---

Determine whether this Wikipedia article is actually about the person this highway/memorial road was named after.
Consider: Does the article's subject match the name? Is the person connected to the correct state? 
Does the person's profession/background make sense for a highway memorial (e.g. law enforcement, military, politician, local figure)?

Return ONLY a valid JSON object:
{{
    "is_correct_person": true,
    "confidence": 0.85,
    "reason": "Brief explanation of why this is or is not the correct person"
}}"""

            messages = [{"role": "user", "content": prompt}]
            response = self._mimo_chat(
                messages,
                max_tokens=200,
                temperature=0.1,
            )
            json_text = response.choices[0].message.content.strip()
            if json_text.startswith("```"):
                json_text = re.sub(r"^```(?:json)?\s*", "", json_text)
                json_text = re.sub(r"\s*```$", "", json_text)
            result = json.loads(json_text)
            return {
                "is_correct_person": bool(result.get("is_correct_person", False)),
                "confidence": float(result.get("confidence", 0.0)),
                "reason": str(result.get("reason", ""))
            }
        except Exception as e:
            print(f"Warning: Page validation failed, assuming page is correct: {e}")
            return {"is_correct_person": True, "confidence": 0.0, "reason": f"Validation error: {e}"}
    
    async def summarize_person(self, url: str, designation: Optional[str] = None,
                                state: Optional[str] = None) -> str:
        """
        Main function to process a URL and return a summary.
        
        Args:
            url: Wikipedia or Find a Grave URL
            designation: Highway designation string for page validation
            state: State name for page validation
            
        Returns:
            A 4-sentence summary of the person
            
        Raises:
            ValueError: If URL is not from Wikipedia or Find a Grave
            Exception: If processing fails
        """
        if not self._is_valid_url(url):
            raise ValueError("URL must be from Wikipedia or Find a Grave")
        
        try:
            print(f"Extracting content from: {url}")
            markdown_content = await self._extract_content(url)
            
            if not markdown_content or len(markdown_content.strip()) < 100:
                raise Exception("Insufficient content extracted from the page")
            
            if designation:
                person_name = url.split('/wiki/')[-1].replace('_', ' ') if '/wiki/' in url else ""
                validation = self._validate_page_relevance(
                    markdown_content, person_name, designation, state
                )
                if not validation["is_correct_person"]:
                    print(f"WARNING: Page validation failed - {validation['reason']}")
                    return f"[UNVERIFIED - wrong person likely] {validation['reason']}"
            
            print("Generating summary...")
            summary = self._generate_summary(markdown_content)
            
            return summary
            
        except Exception as e:
            raise Exception(f"Failed to process {url}: {str(e)}")
    
    async def extract_person_data(self, url: str, designation: Optional[str] = None,
                                   state: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract both summary and structured data for a person in a single API call.
        
        Args:
            url: Wikipedia or Find a Grave URL
            designation: Highway designation string for page validation
            state: State name for page validation
            
        Returns:
            Dictionary containing summary, structured biographical data, and validation info
            
        Raises:
            ValueError: If URL is not from Wikipedia or Find a Grave
            Exception: If processing fails
        """
        if not self._is_valid_url(url):
            raise ValueError("URL must be from Wikipedia or Find a Grave")
        
        try:
            print(f"Extracting content from: {url}")
            markdown_content = await self._extract_content(url)
            
            if not markdown_content or len(markdown_content.strip()) < 100:
                raise Exception("Insufficient content extracted from the page")
            
            validation = None
            if designation:
                person_name = url.split('/wiki/')[-1].replace('_', ' ') if '/wiki/' in url else ""
                print("Validating Wikipedia page relevance...")
                validation = self._validate_page_relevance(
                    markdown_content, person_name, designation, state
                )
                if not validation["is_correct_person"]:
                    print(f"WARNING: Page validation failed - {validation['reason']}")
                    return {
                        "summary": f"[UNVERIFIED] {validation['reason']}",
                        "structured_data": {
                            "education": [], "date_of_birth": None, "date_of_death": None,
                            "place_of_birth": "not found", "place_of_death": "not found",
                            "involved_in_sports": "no", "involved_in_politics": "no",
                            "involved_in_military": "no", "involved_in_music": "no",
                            "gender": "not found"
                        },
                        "validation": validation
                    }
            
            print("Extracting complete biographical data...")
            complete_data = self._extract_complete_data(markdown_content)
            
            return {
                "summary": complete_data["summary"],
                "structured_data": {
                    "education": complete_data["education"],
                    "date_of_birth": complete_data["date_of_birth"],
                    "date_of_death": complete_data["date_of_death"],
                    "place_of_birth": complete_data["place_of_birth"],
                    "place_of_death": complete_data["place_of_death"],
                    "involved_in_sports": complete_data["involved_in_sports"],
                    "involved_in_politics": complete_data["involved_in_politics"],
                    "involved_in_military": complete_data["involved_in_military"],
                    "involved_in_music": complete_data["involved_in_music"],
                    "gender": complete_data["gender"]
                },
                "validation": validation
            }
            
        except Exception as e:
            raise Exception(f"Failed to process {url}: {str(e)}")


# Person class for object-oriented usage
class Person:
    """A class representing a person from Wikipedia or Find a Grave with summarization capabilities."""
    
    def __init__(self, url: str, api_key: Optional[str] = None,
                 designation: Optional[str] = None, state: Optional[str] = None,
                 MistralAPIKey: Optional[str] = None):
        """
        Initialize a Person object with a URL.
        
        Args:
            url: Wikipedia or Find a Grave URL
            api_key: Xiaomi MiMo API key (optional if set as environment variable)
            designation: Highway designation string, used for page validation
            state: State name, used for page validation
            MistralAPIKey: Deprecated alias for api_key, kept for backward compatibility
            
        Raises:
            ValueError: If URL is not from Wikipedia or Find a Grave
        """
        self.url = url
        self.designation = designation
        self.state = state
        self.summary = None
        self._structured_data = None
        self._data_extracted = False
        self._validation_result = None
        self._summarizer = PersonSummarizer(api_key=api_key, MistralAPIKey=MistralAPIKey)
        
        if not self._summarizer._is_valid_url(url):
            raise ValueError("URL must be from Wikipedia or Find a Grave")
    
    async def _extract_all_data(self):
        """
        Internal method to extract both summary and structured data.
        This is called automatically when any data is requested.
        """
        if not self._data_extracted:
            data = await self._summarizer.extract_person_data(
                self.url, designation=self.designation, state=self.state
            )
            self.summary = data["summary"]
            self._structured_data = data["structured_data"]
            self._validation_result = data.get("validation")
            self._data_extracted = True
    
    async def summarize(self) -> str:
        """
        Generate and return a 4-sentence summary of the person.
        
        Returns:
            A 4-sentence summary of the person
            
        Note:
            The summary is cached after the first call for efficiency.
        """
        await self._extract_all_data()
        return self.summary
    
    def summarize_sync(self) -> str:
        """
        Synchronous version of summarize() for easier usage.
        
        Returns:
            A 4-sentence summary of the person
            
        Note:
            This method handles the async/await internally using asyncio.run()
            The summary is cached after the first call for efficiency.
        """
        import asyncio
        
        if not self._data_extracted:
            # Run the async data extraction method
            asyncio.run(self._extract_all_data())
        return self.summary
    
    def get_cached_summary(self) -> Optional[str]:
        """
        Get the cached summary without making a new API call.
        
        Returns:
            The cached summary if available, None otherwise
        """
        return self.summary
    
    def getSummary(self) -> Optional[str]:
        """
        Get the person's 4-sentence biographical summary.
        
        Returns:
            4-sentence summary as string, or None if not available
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        return self.summary
    
    def getEducation(self) -> Optional[list]:
        """
        Get the person's education information.
        
        Returns:
            List of educational institutions/degrees, or None if not available
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("education")
        return None
    
    def getDOB(self) -> Optional[str]:
        """
        Get the person's date of birth.
        
        Returns:
            Date of birth as string (YYYY-MM-DD or YYYY format), or None if not available
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("date_of_birth")
        return None
    
    def getDOD(self) -> Optional[str]:
        """
        Get the person's date of death.
        
        Returns:
            Date of death as string (YYYY-MM-DD or YYYY format), or None if not available/still alive
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("date_of_death")
        return None
    
    def getPlaceOfBirth(self) -> Optional[str]:
        """
        Get the person's place of birth.
        
        Returns:
            Place of birth as string, "not found" if not available, or None if data not extracted
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("place_of_birth")
        return None
    
    def getPlaceOfDeath(self) -> Optional[str]:
        """
        Get the person's place of death.
        
        Returns:
            Place of death as string, "not found" if not available, or None if data not extracted
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("place_of_death")
        return None
    
    def getInvolvedInSports(self) -> Optional[str]:
        """
        Get whether the person was involved in sports.
        
        Returns:
            "yes" if involved in sports, "no" if not involved or not found, or None if data not extracted
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("involved_in_sports")
        return None
    
    def getInvolvedInPolitics(self) -> Optional[str]:
        """
        Get whether the person was involved in politics.
        
        Returns:
            "yes" if involved in politics, "no" if not involved or not found, or None if data not extracted
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("involved_in_politics")
        return None
    
    def getInvolvedInMilitary(self) -> Optional[str]:
        """
        Get whether the person was involved in military.
        
        Returns:
            "yes" if involved in military, "no" if not involved or not found, or None if data not extracted
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("involved_in_military")
        return None
    
    def getInvolvedInMusic(self) -> Optional[str]:
        """
        Get whether the person was involved in music.
        
        Returns:
            "yes" if involved in music, "no" if not involved or not found, or None if data not extracted
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("involved_in_music")
        return None
    
    def getGender(self) -> Optional[str]:
        """
        Get the person's gender.
        
        Returns:
            "male", "female", "not found" if cannot be determined, or None if data not extracted
            
        Note:
            This method will automatically extract data if not already done.
        """
        import asyncio
        
        if not self._data_extracted:
            asyncio.run(self._extract_all_data())
        
        if self._structured_data:
            return self._structured_data.get("gender")
        return None
    
    def getLlmPageCorrectPerson(self) -> Optional[bool]:
        """
        Whether post-fetch LLM validation accepted the Wikipedia article as the highway's honoree.

        Returns:
            True if validation passed, False if the model rejected the page (wrong person / bad content),
            None if validation was not run (e.g. no designation) or extraction did not complete.
        """
        import asyncio

        if not self._data_extracted:
            asyncio.run(self._extract_all_data())

        if self._validation_result is None:
            return None
        return bool(self._validation_result.get("is_correct_person"))
    
    def clear_cache(self):
        """Clear the cached summary and structured data to force regeneration on next call."""
        self.summary = None
        self._structured_data = None
        self._data_extracted = False
        self._validation_result = None
    
    def __str__(self) -> str:
        """String representation of the Person object."""
        return f"Person(url='{self.url}', data_extracted={self._data_extracted})"
    
    def __repr__(self) -> str:
        """Detailed representation of the Person object."""
        return self.__str__()


# Convenience function for easy usage
async def summarize_person_from_url(url: str, api_key: Optional[str] = None) -> str:
    """
    Convenience function to summarize a person from a Wikipedia or Find a Grave URL.
    
    Args:
        url: Wikipedia or Find a Grave URL
        api_key: Xiaomi MiMo API key (optional if set as environment variable)
        
    Returns:
        A 4-sentence summary of the person
    """
    summarizer = PersonSummarizer(api_key=api_key)
    return await summarizer.summarize_person(url)


# Example usage
async def main():
    """Example usage of the PersonSummarizer."""
    # Example URLs (replace with actual URLs for testing)
    test_urls = [
        "https://en.wikipedia.org/wiki/Albert_Einstein",
        "https://www.findagrave.com/memorial/1465/albert-einstein"
    ]
    
    try:
        for url in test_urls:
            print(f"\n{'='*50}")
            print(f"Processing: {url}")
            print(f"{'='*50}")
            
            summary = await summarize_person_from_url(url)
            print(f"\nSummary:\n{summary}")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
