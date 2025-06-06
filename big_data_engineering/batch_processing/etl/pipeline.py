# ----------------------------------------------------------------------------------
# FILE: pipeline.py
# The high-level orchestration function calling download → clean → write.
# This is designed to be invoked by CLI or Airflow, ensuring decoupled execution.
# ----------------------------------------------------------------------------------

# pipeline.py
import polars as pl
from downloader import download_data
from transformer import clean_data
from writer import write_partitioned_parquet
import os


def run_etl(year: int, month: int):
    """
    Runs the full ETL pipeline for a given month:
    - Downloads data
    - Cleans it
    - Writes it to partitioned parquet

    Follows Single Responsibility and Dependency Inversion principles for easy orchestration.
    """
    print(f"Starting ETL for {year}-{month:02d}")
    file_path = download_data(year, month)

    print("Exists:", os.path.exists(file_path))

    if not file_path:
        print("Skipping due to failed download.")
        return

    df = pl.read_parquet(file_path)
    df_clean = clean_data(df)

    # Final filter for timestamp-based partitioning validation
    df_clean = df_clean.filter(
        (pl.col("year") == year) & (pl.col("month") == month)
    )

    write_partitioned_parquet(df_clean)