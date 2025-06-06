# ----------------------------------------------------------------------------------
# FILE: transformer.py
# Responsible solely for cleaning and transforming the data.
# Uses Polars for fast in-memory transformations.
# ----------------------------------------------------------------------------------

import polars as pl


def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the input DataFrame by:
    - Removing nulls from critical columns
    - Ensuring datetime format on pickup and dropoff columns
    - Extracting year and month from pickup timestamp for partitioning

    Separation of concerns is applied here so that transformation logic is testable independently.
    """
    print("Cleaning data...")

    # Filter nulls from critical fields
    df = df.filter(
        (pl.col("passenger_count").is_not_null()) &
        (pl.col("trip_distance").is_not_null()) &
        (pl.col("total_amount").is_not_null())
    )

    # Convert to datetime if needed
    if df.schema.get("tpep_pickup_datetime") != pl.Datetime:
        df = df.with_columns(
            pl.col("tpep_pickup_datetime").str.strptime(pl.Datetime, strict=False)
        )
    if df.schema.get("tpep_dropoff_datetime") != pl.Datetime:
        df = df.with_columns(
            pl.col("tpep_dropoff_datetime").str.strptime(pl.Datetime, strict=False)
        )

    # Filter invalid timestamps
    df = df.filter(
        (pl.col("tpep_pickup_datetime").is_not_null()) &
        (pl.col("tpep_dropoff_datetime").is_not_null())
    )

    # Extract year and month for partitioning
    df = df.with_columns([
        pl.col("tpep_pickup_datetime").dt.year().alias("year"),
        pl.col("tpep_pickup_datetime").dt.month().alias("month")
    ])

    print(f"Cleaned {df.shape[0]} rows.")
    return df