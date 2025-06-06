# ----------------------------------------------------------------------------------
# FILE: downloader.py
# Responsible solely for downloading files. Follows Single Responsibility Principle (SRP).
# ----------------------------------------------------------------------------------

import os
import urllib.request
from typing import Optional
from config import RAW_DIR, BASE_URL


def download_data(year: int, month: int) -> Optional[str]:
    """
    Downloads the NYC Yellow Taxi data Parquet file for a given year and month.
    If file already exists locally, the download is skipped.

    This adheres to the SRP principle and avoids duplicated downloads to save I/O.
    """
    filename = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    local_path = RAW_DIR / filename

    if local_path.exists():
        print(f"File already exists: {local_path}")
        return str(local_path)

    try:
        os.makedirs(RAW_DIR, exist_ok=True)  # Ensure raw directory exists
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, str(local_path))
        print(f"Downloaded to {local_path}")
        return str(local_path)
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None