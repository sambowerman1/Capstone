import os
from pathlib import Path
from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent.parent / ".env.local"
load_dotenv(_env_path)

# Xiaomi MiMo API
MIMO_API_URL = "https://api.mimo-v2.com/v1/chat/completions"
MIMO_API_KEY = os.environ.get("XiaomiAIKey", "")
MIMO_MODEL = "mimo-v2-flash"

# Geocoding APIs (all free, no keys needed)
PHOTON_URL = "https://photon.komoot.io/api"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OSRM_URL = "http://router.project-osrm.org/route/v1/driving"

# Rate limits
NOMINATIM_DELAY = 1.1
OVERPASS_DELAY = 2.0
PHOTON_CONCURRENT = 10
OSRM_CONCURRENT = 5
MIMO_CONCURRENT = 10

# Paths (relative to this file's directory)
_BASE = Path(__file__).resolve().parent
INPUT_CSV = _BASE / "data" / "all_states_summarized.csv"
DB_PATH = _BASE / "highway_geocoder.db"
OUTPUT_DIR = _BASE / "output"

# HTTP
USER_AGENT = "memorial-highway-geocoder/1.0 (research project)"
