# nyc_etl.py — Production-grade ETL with partitioned Parquet output and full comments

import os
import urllib.request
import polars as pl
from pyarrow import dataset as ds
from pyarrow import parquet as pq
from typing import Optional

# ---------------------------------------------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------------------------------------------

RAW_DIR = "/data/raw"  # Directory to store raw downloaded parquet files
PROCESSED_DIR = f"/data/processed/yellow_tripdata"  # Directory for cleaned, partitioned output
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"  # NYC Taxi data source

# ---------------------------------------------------------------------------------------------------------------
# STEP 1: DOWNLOAD RAW FILES
# ---------------------------------------------------------------------------------------------------------------

def download_data(year: int, month: int) -> Optional[str]:
    """
    Downloads one month's yellow taxi data as Parquet.
    Returns the local file path if successful, else None.
    """
    filename = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    local_path = os.path.join(RAW_DIR, filename)

    # Skip if file already downloaded
    if os.path.exists(local_path):
        print(f"File already exists: {local_path}")
        return local_path

    try:
        os.makedirs(RAW_DIR, exist_ok=True)  # Ensure directory exists
        print(f"Downloading {url}")
        urllib.request.urlretrieve(url, local_path)  # Download file
        print(f"Downloaded: {local_path}")
        return local_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

# ---------------------------------------------------------------------------------------------------------------
# STEP 2: CLEANING LOGIC
# ---------------------------------------------------------------------------------------------------------------

def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans and prepares taxi data:
    - Removes nulls
    - Ensures datetime parsing
    - Extracts partition columns (year/month)
    """
    print("🧹 Cleaning data...")

    # Filter out rows with nulls in key columns
    df = df.filter(
        (pl.col("passenger_count").is_not_null()) &
        (pl.col("trip_distance").is_not_null()) &
        (pl.col("total_amount").is_not_null())
    )

    # Handle datetime types dynamically
    if df.schema["tpep_pickup_datetime"] != pl.Datetime:
        df = df.with_columns(
            pl.col("tpep_pickup_datetime").str.strptime(pl.Datetime, strict=False)
        )
    if df.schema["tpep_dropoff_datetime"] != pl.Datetime:
        df = df.with_columns(
            pl.col("tpep_dropoff_datetime").str.strptime(pl.Datetime, strict=False)
        )

    # Drop rows with invalid timestamps
    df = df.filter(
        (pl.col("tpep_pickup_datetime").is_not_null()) &
        (pl.col("tpep_dropoff_datetime").is_not_null())
    )

    # Extract partition columns
    df = df.with_columns([
        pl.col("tpep_pickup_datetime").dt.year().alias("year"),
        pl.col("tpep_pickup_datetime").dt.month().alias("month")
    ])

    print(f"Cleaned {df.shape[0]} rows")
    return df

# ---------------------------------------------------------------------------------------------------------------
# STEP 3: WRITE TO PARTITIONED FORMAT
# ---------------------------------------------------------------------------------------------------------------

def write_partitioned_parquet(df: pl.DataFrame, output_dir: str):
    """
    Writes a Polars DataFrame to partitioned Parquet using pyarrow.dataset.
    Partitions by 'year' and 'month'.
    """
    print(f"Writing to: {output_dir}")

    # Convert to Arrow table
    arrow_table = df.to_arrow()

    # Use pyarrow to write partitioned dataset
    ds.write_dataset(
        data=arrow_table,
        base_dir=output_dir,
        format="parquet",
        partitioning=["year", "month"],
        existing_data_behavior="delete_matching"  # Deletes only matching partitions
        # file_options=pq.ParquetWriterOptions(compression="snappy")
    )

    print(f"Partitioned parquet written by year/month")

# ---------------------------------------------------------------------------------------------------------------
# STEP 4: FULL ETL FOR A MONTH
# ---------------------------------------------------------------------------------------------------------------

def run_etl(year: int, month: int):
    """
    Complete ETL pipeline: download, clean, and save partitioned parquet.
    """
    print(f"\n ETL for {year}-{month:02d}")

    file_path = download_data(year, month)
    if not file_path:
        print(f" Skipping {year}-{month:02d} due to download error")
        return

    try:
        df_raw = pl.read_parquet(file_path)  # Load downloaded Parquet
        df_clean = clean_data(df_raw)        # Clean it

        # Filter out bad timestamps before writing
        df_clean = df_clean.filter(
        (pl.col("year") == year) & 
        (pl.col("month") == month)
        )

        write_partitioned_parquet(df_clean, PROCESSED_DIR)  # Save it
    except Exception as e:
        print(f"ETL failed for {year}-{month:02d}: {e}")

# ---------------------------------------------------------------------------------------------------------------
# ENTRYPOINT: CLI HANDLING
# ---------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi ETL — Parquet Partitioned")
    parser.add_argument("--year", type=int, default=2023, help="Year to process")
    parser.add_argument("--month", type=int, help="Month to process (1-12)")
    parser.add_argument("--all-months", action="store_true", help="Process entire year")

    args = parser.parse_args()

    if args.all_months:
        for m in range(1, 13):
            run_etl(args.year, m)
    elif args.month:
        run_etl(args.year, args.month)
    else:
        print(" Please provide either --month <1-12> or --all-months")
