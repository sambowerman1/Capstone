"""
Name cleaning and input type detection utilities.
"""

import re
from typing import Tuple


# Highway/road suffixes to remove
HIGHWAY_SUFFIXES = (
    r"(Memorial\s+|Historic\s+)?(Trail|GreeneWay|Overpass|Beltway|Roadway|"
    r"Interchange|Corridor|Expressway|Intersection|Road|Causeway|Street|"
    r"Drive|Highway|Boulevard|Way|Parkway|Bridge|Avenue|Lane)"
)


def clean_name(road_name: str) -> str:
    """
    Remove common highway/road suffixes and extract the person's name.

    Args:
        road_name: Full highway designation (e.g., "John Smith Memorial Highway")

    Returns:
        Cleaned person name (e.g., "John Smith")
    """
    # Remove common suffixes like Highway, Boulevard, Way, Parkway, etc.
    cleaned = re.sub(
        rf"\s+({HIGHWAY_SUFFIXES})$",
        "",
        road_name,
        flags=re.IGNORECASE
    )
    # Remove punctuation
    punc_removed = re.sub(r'[^\w\s]', '', cleaned)
    return punc_removed.strip()


def detect_input_type(name: str) -> str:
    """
    Auto-detect whether input is a highway designation or person name.

    Args:
        name: Input string to classify

    Returns:
        'highway' if the name contains road-related keywords, 'person' otherwise
    """
    highway_keywords = [
        'highway', 'boulevard', 'street', 'road', 'avenue', 'lane',
        'drive', 'parkway', 'way', 'bridge', 'trail', 'expressway',
        'beltway', 'corridor', 'interchange', 'causeway', 'overpass',
        'memorial highway', 'memorial bridge', 'memorial road'
    ]

    name_lower = name.lower()

    for keyword in highway_keywords:
        if keyword in name_lower:
            return 'highway'

    return 'person'


def process_name(name: str, input_type: str = 'auto') -> Tuple[str, str]:
    """
    Process input name based on type.

    Args:
        name: Input name (highway designation or person name)
        input_type: 'highway', 'person', or 'auto' for auto-detection

    Returns:
        Tuple of (cleaned_name, detected_type)
    """
    if input_type == 'auto':
        input_type = detect_input_type(name)

    if input_type == 'highway':
        cleaned = clean_name(name)
    else:
        # For person names, just clean up whitespace and punctuation
        cleaned = re.sub(r'[^\w\s]', '', name).strip()
        cleaned = ' '.join(cleaned.split())

    return cleaned, input_type
