#!/usr/bin/env python3
"""
Person Summarizer Script

This script takes Wikipedia or Find a Grave links, extracts content using crawl4ai,
and generates 4-sentence summaries using the Mistral API.
"""

import re
import os
import json
from typing import Optional, Dict, Any
import asyncio
from crawl4ai import AsyncWebCrawler
from mistralai import Mistral


class PersonSummarizer:
    """A class to handle web scraping and summarization of person profiles."""
    
    def __init__(self, MistralAPIKey: Optional[str] = None):
        """
        Initialize the PersonSummarizer.
        
        Args:
            MistralAPIKey: Mistral API key. If not provided, will look for MistralAPIKey env var.
        """
        self.MistralAPIKey = MistralAPIKey or os.getenv('MistralAPIKey')
        if not self.MistralAPIKey:
            raise ValueError("Mistral API key must be provided or set as MistralAPIKey environment variable")
        
        self.mistral_client = Mistral(api_key=self.MistralAPIKey)
    
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
        Generate a 4-sentence summary using Mistral API.
        
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
{markdown_content[:4000]}  # Limit content to avoid token limits

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
            
            response = self.mistral_client.chat.complete(
                model="mistral-medium-latest",
                messages=messages,
                max_tokens=200,
                temperature=0.3
            )
            
            summary = response.choices[0].message.content.strip()
            
            # Remove any remaining markdown formatting as backup
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
- date_of_birth: Date in YYYY-MM-DD format (or YYYY if only year known), null if not found
- date_of_death: Date in YYYY-MM-DD format (or YYYY if only year known), null if not found or still alive
- place_of_birth: Birth location as "City, State, Country" format, "not found" if not available
- place_of_death: Death location as "City, State, Country" format, "not found" if not available
- involved_in_sports: "yes" if person was involved in sports, "no" if not found or no involvement
- involved_in_politics: "yes" if person was involved in politics, "no" if not found or no involvement
- involved_in_military: "yes" if person was involved in military, "no" if not found or no involvement
- involved_in_music: "yes" if person was involved in music, "no" if not found or no involvement
- gender: "male", "female", or "not found" if cannot be determined

Return ONLY the JSON object, no other text.
"""

            messages = [
                {"role": "user", "content": prompt}
            ]
            
            response = self.mistral_client.chat.complete(
                model="mistral-medium-latest",
                messages=messages,
                max_tokens=500,
                temperature=0.1  # Lower temperature for more consistent JSON output
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
    
    async def summarize_person(self, url: str) -> str:
        """
        Main function to process a URL and return a summary.
        
        Args:
            url: Wikipedia or Find a Grave URL
            
        Returns:
            A 4-sentence summary of the person
            
        Raises:
            ValueError: If URL is not from Wikipedia or Find a Grave
            Exception: If processing fails
        """
        # Validate URL
        if not self._is_valid_url(url):
            raise ValueError("URL must be from Wikipedia or Find a Grave")
        
        try:
            # Extract content
            print(f"Extracting content from: {url}")
            markdown_content = await self._extract_content(url)
            
            if not markdown_content or len(markdown_content.strip()) < 100:
                raise Exception("Insufficient content extracted from the page")
            
            # Generate summary
            print("Generating summary...")
            summary = self._generate_summary(markdown_content)
            
            return summary
            
        except Exception as e:
            raise Exception(f"Failed to process {url}: {str(e)}")
    
    async def extract_person_data(self, url: str) -> Dict[str, Any]:
        """
        Extract both summary and structured data for a person in a single API call.
        
        Args:
            url: Wikipedia or Find a Grave URL
            
        Returns:
            Dictionary containing summary and structured biographical data
            
        Raises:
            ValueError: If URL is not from Wikipedia or Find a Grave
            Exception: If processing fails
        """
        # Validate URL
        if not self._is_valid_url(url):
            raise ValueError("URL must be from Wikipedia or Find a Grave")
        
        try:
            # Extract content
            print(f"Extracting content from: {url}")
            markdown_content = await self._extract_content(url)
            
            if not markdown_content or len(markdown_content.strip()) < 100:
                raise Exception("Insufficient content extracted from the page")
            
            # Extract all data in a single API call
            print("Extracting complete biographical data...")
            complete_data = self._extract_complete_data(markdown_content)
            
            # Restructure to match expected format
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
                }
            }
            
        except Exception as e:
            raise Exception(f"Failed to process {url}: {str(e)}")


# Person class for object-oriented usage
class Person:
    """A class representing a person from Wikipedia or Find a Grave with summarization capabilities."""
    
    def __init__(self, url: str, MistralAPIKey: Optional[str] = None):
        """
        Initialize a Person object with a URL.
        
        Args:
            url: Wikipedia or Find a Grave URL
            MistralAPIKey: Mistral API key (optional if set as environment variable)
            
        Raises:
            ValueError: If URL is not from Wikipedia or Find a Grave
        """
        self.url = url
        self.summary = None
        self._structured_data = None
        self._data_extracted = False
        self._summarizer = PersonSummarizer(MistralAPIKey)
        
        # Validate URL immediately
        if not self._summarizer._is_valid_url(url):
            raise ValueError("URL must be from Wikipedia or Find a Grave")
    
    async def _extract_all_data(self):
        """
        Internal method to extract both summary and structured data.
        This is called automatically when any data is requested.
        """
        if not self._data_extracted:
            data = await self._summarizer.extract_person_data(self.url)
            self.summary = data["summary"]
            self._structured_data = data["structured_data"]
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
    
    def clear_cache(self):
        """Clear the cached summary and structured data to force regeneration on next call."""
        self.summary = None
        self._structured_data = None
        self._data_extracted = False
    
    def __str__(self) -> str:
        """String representation of the Person object."""
        return f"Person(url='{self.url}', data_extracted={self._data_extracted})"
    
    def __repr__(self) -> str:
        """Detailed representation of the Person object."""
        return self.__str__()


# Convenience function for easy usage
async def summarize_person_from_url(url: str, MistralAPIKey: Optional[str] = None) -> str:
    """
    Convenience function to summarize a person from a Wikipedia or Find a Grave URL.
    
    Args:
        url: Wikipedia or Find a Grave URL
        MistralAPIKey: Mistral API key (optional if set as environment variable)
        
    Returns:
        A 4-sentence summary of the person
    """
    summarizer = PersonSummarizer(MistralAPIKey)
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
