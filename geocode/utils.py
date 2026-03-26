import asyncio
import re
import time
import aiohttp

import config

# ---------------------------------------------------------------------------
# State name <-> abbreviation mappings
# ---------------------------------------------------------------------------

STATE_NAMES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR",
    "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "District of Columbia": "DC",
}

ABBREV_TO_STATE = {v: k for k, v in STATE_NAMES.items()}

# Approximate bounding boxes (min_lat, max_lat, min_lon, max_lon)
STATE_BBOXES = {
    "Alabama": (30.2, 35.0, -88.5, -84.9),
    "Alaska": (51.2, 71.4, -179.1, -129.9),
    "Arizona": (31.3, 37.0, -114.8, -109.0),
    "Arkansas": (33.0, 36.5, -94.6, -89.6),
    "California": (32.5, 42.0, -124.4, -114.1),
    "Colorado": (37.0, 41.0, -109.1, -102.0),
    "Connecticut": (41.0, 42.1, -73.7, -71.8),
    "Delaware": (38.5, 39.8, -75.8, -75.0),
    "Florida": (24.5, 31.0, -87.6, -80.0),
    "Georgia": (30.4, 35.0, -85.6, -80.8),
    "Hawaii": (18.9, 22.2, -160.2, -154.8),
    "Idaho": (42.0, 49.0, -117.2, -111.0),
    "Illinois": (37.0, 42.5, -91.5, -87.0),
    "Indiana": (37.8, 41.8, -88.1, -84.8),
    "Iowa": (40.4, 43.5, -96.6, -90.1),
    "Kansas": (37.0, 40.0, -102.1, -94.6),
    "Kentucky": (36.5, 39.1, -89.6, -81.9),
    "Louisiana": (29.0, 33.0, -94.0, -89.0),
    "Maine": (43.1, 47.5, -71.1, -66.9),
    "Maryland": (37.9, 39.7, -79.5, -75.0),
    "Massachusetts": (41.2, 42.9, -73.5, -69.9),
    "Michigan": (41.7, 48.3, -90.4, -82.4),
    "Minnesota": (43.5, 49.4, -97.2, -89.5),
    "Mississippi": (30.2, 35.0, -91.7, -88.1),
    "Missouri": (36.0, 40.6, -95.8, -89.1),
    "Montana": (44.4, 49.0, -116.0, -104.0),
    "Nebraska": (40.0, 43.0, -104.1, -95.3),
    "Nevada": (35.0, 42.0, -120.0, -114.0),
    "New Hampshire": (42.7, 45.3, -72.6, -70.7),
    "New Jersey": (38.9, 41.4, -75.6, -73.9),
    "New Mexico": (31.3, 37.0, -109.1, -103.0),
    "New York": (40.5, 45.0, -79.8, -71.9),
    "North Carolina": (33.8, 36.6, -84.3, -75.5),
    "North Dakota": (45.9, 49.0, -104.0, -96.6),
    "Ohio": (38.4, 42.0, -84.8, -80.5),
    "Oklahoma": (33.6, 37.0, -103.0, -94.4),
    "Oregon": (42.0, 46.3, -124.6, -116.5),
    "Pennsylvania": (39.7, 42.3, -80.5, -74.7),
    "Rhode Island": (41.1, 42.0, -71.9, -71.1),
    "South Carolina": (32.0, 35.2, -83.4, -78.6),
    "South Dakota": (42.5, 46.0, -104.1, -96.4),
    "Tennessee": (35.0, 36.7, -90.3, -81.6),
    "Texas": (25.8, 36.5, -106.6, -93.5),
    "Utah": (37.0, 42.0, -114.1, -109.0),
    "Vermont": (42.7, 45.0, -73.4, -71.5),
    "Virginia": (36.5, 39.5, -83.7, -75.2),
    "Washington": (45.5, 49.0, -124.8, -116.9),
    "West Virginia": (37.2, 40.6, -82.6, -77.7),
    "Wisconsin": (42.5, 47.1, -92.9, -86.8),
    "Wyoming": (41.0, 45.0, -111.1, -104.1),
    "District of Columbia": (38.8, 39.0, -77.1, -76.9),
}


def state_abbrev(full_name: str) -> str | None:
    return STATE_NAMES.get(full_name)


def state_full_name(abbrev: str) -> str | None:
    return ABBREV_TO_STATE.get(abbrev.upper())


# ---------------------------------------------------------------------------
# Location text helpers
# ---------------------------------------------------------------------------

_JUNCTION_RE = re.compile(
    r"^(junction\s+(with|of)\s+)", re.IGNORECASE
)
_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*")
_CITY_LIMITS_RE = re.compile(
    r"\s*(city\s+limit(?:s)?|city\s+boundar(?:y|ies))\s*", re.IGNORECASE
)
_BOUNDARY_IN_RE = re.compile(r"\bboundary\s+in\s+", re.IGNORECASE)
_MILE_MARKER_RE = re.compile(
    r"\b(mile\s+marker|mile\s+post|milepost|MP)\s+[\d.]+", re.IGNORECASE
)
_STATE_BORDER_RE = re.compile(
    r"(border|state\s+line|boundary\s+line|boundary)", re.IGNORECASE
)


def clean_location_text(text: str) -> str:
    """Strip noise from a from/to location string before geocoding."""
    if not text:
        return ""
    t = text.strip()
    t = _JUNCTION_RE.sub("", t)
    t = _PAREN_RE.sub(" ", t)
    t = _CITY_LIMITS_RE.sub("", t)
    t = _BOUNDARY_IN_RE.sub("", t)
    return t.strip()


def extract_core_place(text: str) -> str:
    """Try to extract just the city/town name from a complex description."""
    clean = clean_location_text(text)
    # "SR 5 in Seattle" -> "Seattle"
    m = re.search(r"\bin\s+(.+)$", clean, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # "north of Castle Rock" -> "Castle Rock"
    m = re.search(r"\b(?:north|south|east|west)\s+of\s+(.+)$", clean, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # "near Fernley" -> "Fernley"
    m = re.search(r"\bnear\s+(.+)$", clean, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return clean


def is_state_border(text: str) -> bool:
    if not text:
        return False
    return bool(_STATE_BORDER_RE.search(text))


def is_mile_marker(text: str) -> bool:
    if not text:
        return False
    return bool(_MILE_MARKER_RE.search(text))


def result_in_state(result: dict, state_name: str) -> bool:
    """Check whether a geocode result falls within the given state's bounding box."""
    bbox = STATE_BBOXES.get(state_name)
    if not bbox:
        return True  # can't verify, assume okay
    lat = result.get("lat")
    lon = result.get("lon")
    if lat is None or lon is None:
        return False
    min_lat, max_lat, min_lon, max_lon = bbox
    pad = 0.15  # small tolerance for edge cases
    return (min_lat - pad <= lat <= max_lat + pad and
            min_lon - pad <= lon <= max_lon + pad)


# ---------------------------------------------------------------------------
# Rate limiters
# ---------------------------------------------------------------------------

class RateLimiter:
    """Simple async rate limiter that enforces a minimum delay between calls."""

    def __init__(self, delay: float):
        self._delay = delay
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            wait = self._delay - (now - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()


_nominatim_limiter = RateLimiter(config.NOMINATIM_DELAY)
_overpass_limiter = RateLimiter(config.OVERPASS_DELAY)


# ---------------------------------------------------------------------------
# Async HTTP helpers
# ---------------------------------------------------------------------------

async def _get_session() -> aiohttp.ClientSession:
    """Return a module-level session, creating one if needed.

    Callers that use these helpers inside an async context should call
    close_session() when finished.
    """
    # We intentionally create one session per event loop. This is safe because
    # main.py runs a single asyncio.run() per phase.
    global _session
    try:
        if _session is not None and not _session.closed:
            return _session
    except NameError:
        pass
    _session = aiohttp.ClientSession(
        headers={"User-Agent": config.USER_AGENT},
        timeout=aiohttp.ClientTimeout(total=60),
    )
    return _session

_session: aiohttp.ClientSession | None = None


async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()
    _session = None


async def photon_geocode(query: str, limit: int = 5) -> list[dict]:
    """Query Photon and return a list of {lat, lon, name, state, ...} dicts."""
    session = await _get_session()
    params = {
        "q": query,
        "limit": limit,
        "lang": "en",
        "lat": 39.8,
        "lon": -98.5,
    }
    try:
        async with session.get(config.PHOTON_URL, params=params) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
    except Exception:
        return []

    results = []
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        coords = feat.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            results.append({
                "lat": coords[1],
                "lon": coords[0],
                "name": props.get("name", ""),
                "state": props.get("state", ""),
                "city": props.get("city", ""),
                "county": props.get("county", ""),
            })
    return results


async def nominatim_geocode(query: str, limit: int = 5) -> list[dict]:
    """Query Nominatim (respecting rate limit) and return results."""
    await _nominatim_limiter.acquire()
    session = await _get_session()
    params = {
        "q": query,
        "format": "json",
        "countrycodes": "us",
        "limit": limit,
        "addressdetails": 1,
    }
    try:
        async with session.get(config.NOMINATIM_URL, params=params) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
    except Exception:
        return []

    results = []
    for item in data:
        results.append({
            "lat": float(item.get("lat", 0)),
            "lon": float(item.get("lon", 0)),
            "name": item.get("display_name", ""),
            "state": item.get("address", {}).get("state", ""),
            "importance": item.get("importance", 0),
        })
    return results


async def overpass_query(ql: str) -> dict | None:
    """Run an Overpass QL query, returning the parsed JSON response."""
    await _overpass_limiter.acquire()
    session = await _get_session()
    retries = 3
    backoff = 5.0
    for attempt in range(retries):
        try:
            async with session.post(
                config.OVERPASS_URL,
                data={"data": ql},
                timeout=aiohttp.ClientTimeout(total=90),
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429 or resp.status >= 500:
                    await asyncio.sleep(backoff * (2 ** attempt))
                    continue
                return None
        except asyncio.TimeoutError:
            await asyncio.sleep(backoff * (2 ** attempt))
        except Exception:
            return None
    return None


async def osrm_route(from_coord: tuple, to_coord: tuple) -> list | None:
    """Get a driving route between two (lat, lon) pairs via OSRM.

    Returns a list of [lon, lat] coordinate pairs (GeoJSON order) or None.
    """
    session = await _get_session()
    from_lon, from_lat = from_coord[1], from_coord[0]
    to_lon, to_lat = to_coord[1], to_coord[0]
    url = f"{config.OSRM_URL}/{from_lon},{from_lat};{to_lon},{to_lat}"
    params = {"overview": "full", "geometries": "geojson"}
    try:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    routes = data.get("routes", [])
    if not routes:
        return None
    return routes[0].get("geometry", {}).get("coordinates")
