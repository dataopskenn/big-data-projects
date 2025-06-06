# ----------------------------------------------------------------------------------
# FILE: config.py
# Contains configuration variables to avoid magic constants and promote DRY code.
# ----------------------------------------------------------------------------------

from pathlib import Path

# Base directory paths (absolute or relative to container volume if in Docker)
BASE_DIR = Path("/data")
RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed" / "yellow_tripdata"

# NYC Taxi data base URL
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
