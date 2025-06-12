# ----------------------------------------------------------------------------------
# FILE: transformer.py
# Responsible solely for cleaning and transforming the data.
# Uses Polars for fast in-memory transformations.
# Follows Single Responsibility Principle (SRP).
# ----------------------------------------------------------------------------------

# transformer.py
import polars as pl

def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the input DataFrame by:
    - Removing nulls from critical columns
    - Parsing pickup and dropoff as Datetime with explicit format
    - Extracting year and month from pickup timestamp
    """
    # 1. Filter out rows missing critical numeric fields
    df = df.filter(
        (pl.col("passenger_count").is_not_null()) &
        (pl.col("trip_distance").is_not_null()) &
        (pl.col("total_amount").is_not_null())
    )

    # 2. Cast timestamps to string (Utf8) before parsing
    df = df.with_columns([
        pl.col("tpep_pickup_datetime").cast(pl.Utf8),
        pl.col("tpep_dropoff_datetime").cast(pl.Utf8),
    ])

    # 3. Parse strings to Datetime with exact format and non-strict mode
    df = df.with_columns([
        pl.col("tpep_pickup_datetime")
          .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False),
        pl.col("tpep_dropoff_datetime")
          .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False),
    ])

    # 4. Drop any rows where parsing failed (null timestamps)
    df = df.filter(
        pl.col("tpep_pickup_datetime").is_not_null() &
        pl.col("tpep_dropoff_datetime").is_not_null()
    )

    # 5. Add year and month columns for partitioning
    df = df.with_columns([
        pl.col("tpep_pickup_datetime").dt.year().alias("year"),
        pl.col("tpep_pickup_datetime").dt.month().alias("month"),
    ])

    return df