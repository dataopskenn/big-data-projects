# ----------------------------------------------------------------------------------
# FILE: config.py
# Contains configuration variables to avoid magic constants and promote Do-not Repeat Yourself (DRY) code.
# ----------------------------------------------------------------------------------

import os
from pathlib import Path

# Allow override from environment variable, else fallback to project-relative path
BASE_DIR = Path(os.getenv("DATA_PATH", Path(__file__).resolve().parent.parent / "data"))

RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed" / "yellow_tripdata"

# NYC Taxi data base URL
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
