#!/usr/bin/env python3
"""
Person Summarizer Script

This script takes Wikipedia or Find a Grave links, extracts content using crawl4ai,
and generates 4-sentence summaries using the Mistral API.
"""

import re
import os
from typing import Optional
import asyncio
from crawl4ai import AsyncWebCrawler
from mistralai import Mistral


class PersonSummarizer:
    """A class to handle web scraping and summarization of person profiles."""
    
    def __init__(self, mistral_api_key: Optional[str] = None):
        """
        Initialize the PersonSummarizer.
        
        Args:
            mistral_api_key: Mistral API key. If not provided, will look for MistralAPIKey env var.
        """
        self.mistral_api_key = mistral_api_key or os.getenv('MistralAPIKey')
        if not self.mistral_api_key:
            raise ValueError("Mistral API key must be provided or set as MistralAPIKey environment variable")
        
        self.mistral_client = Mistral(api_key=self.mistral_api_key)
    
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


# Person class for object-oriented usage
class Person:
    """A class representing a person from Wikipedia or Find a Grave with summarization capabilities."""
    
    def __init__(self, url: str, mistral_api_key: Optional[str] = None):
        """
        Initialize a Person object with a URL.
        
        Args:
            url: Wikipedia or Find a Grave URL
            mistral_api_key: Mistral API key (optional if set as environment variable)
            
        Raises:
            ValueError: If URL is not from Wikipedia or Find a Grave
        """
        self.url = url
        self.summary = None
        self._summarizer = PersonSummarizer(mistral_api_key)
        
        # Validate URL immediately
        if not self._summarizer._is_valid_url(url):
            raise ValueError("URL must be from Wikipedia or Find a Grave")
    
    async def summarize(self) -> str:
        """
        Generate and return a 4-sentence summary of the person.
        
        Returns:
            A 4-sentence summary of the person
            
        Note:
            The summary is cached after the first call for efficiency.
        """
        if self.summary is None:
            self.summary = await self._summarizer.summarize_person(self.url)
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
        
        if self.summary is None:
            # Run the async summarize method
            self.summary = asyncio.run(self._summarizer.summarize_person(self.url))
        return self.summary
    
    def get_cached_summary(self) -> Optional[str]:
        """
        Get the cached summary without making a new API call.
        
        Returns:
            The cached summary if available, None otherwise
        """
        return self.summary
    
    def clear_cache(self):
        """Clear the cached summary to force regeneration on next summarize() call."""
        self.summary = None
    
    def __str__(self) -> str:
        """String representation of the Person object."""
        return f"Person(url='{self.url}', summary_cached={self.summary is not None})"
    
    def __repr__(self) -> str:
        """Detailed representation of the Person object."""
        return self.__str__()


# Convenience function for easy usage
async def summarize_person_from_url(url: str, mistral_api_key: Optional[str] = None) -> str:
    """
    Convenience function to summarize a person from a Wikipedia or Find a Grave URL.
    
    Args:
        url: Wikipedia or Find a Grave URL
        mistral_api_key: Mistral API key (optional if set as environment variable)
        
    Returns:
        A 4-sentence summary of the person
    """
    summarizer = PersonSummarizer(mistral_api_key)
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
