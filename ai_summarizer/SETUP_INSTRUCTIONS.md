# Person Summarizer Setup Instructions

This script extracts content from Wikipedia or Find a Grave links and generates 4-sentence summaries using the Mistral API.

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your Mistral API key:
```bash
export MistralAPIKey="your_mistral_api_key_here"
```

Alternatively, you can pass the API key directly when creating the PersonSummarizer instance.

## Usage

### Option 1: Using the convenience function

```python
import asyncio
from person_summarizer import summarize_person_from_url

async def main():
    url = "https://en.wikipedia.org/wiki/Albert_Einstein"
    summary = await summarize_person_from_url(url)
    print(summary)

asyncio.run(main())
```

### Option 2: Using the PersonSummarizer class

```python
import asyncio
from person_summarizer import PersonSummarizer

async def main():
    # Initialize with API key (optional if using environment variable)
    summarizer = PersonSummarizer(mistral_api_key="your_key_here")
    
    url = "https://en.wikipedia.org/wiki/Marie_Curie"
    summary = await summarizer.summarize_person(url)
    print(summary)

asyncio.run(main())
```

## Supported URLs

- Wikipedia: `https://*.wikipedia.org/*`
- Find a Grave: `https://www.findagrave.com/*`

## Features

- ✅ Validates Wikipedia and Find a Grave URLs
- ✅ Extracts content using crawl4ai and converts to markdown
- ✅ Generates 4-sentence summaries using Mistral Medium
- ✅ Comprehensive error handling
- ✅ Async support for better performance

## Example Output

```
Albert Einstein was a German-born theoretical physicist who is widely regarded as one of the greatest and most influential scientists of all time. Born on March 14, 1879, in Ulm, Germany, he died on April 18, 1955, in Princeton, New Jersey. He is best known for developing the theory of relativity and his mass-energy equivalence formula E=mc², which revolutionized modern physics. Einstein was awarded the Nobel Prize in Physics in 1921 for his explanation of the photoelectric effect and his contributions to theoretical physics.
```

## Error Handling

The script includes comprehensive error handling for:
- Invalid URLs (not from Wikipedia or Find a Grave)
- Network issues during content extraction
- API failures
- Insufficient content extraction
- Token limit issues

Run `python example_usage.py` to see working examples!
