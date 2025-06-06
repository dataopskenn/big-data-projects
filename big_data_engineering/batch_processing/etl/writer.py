# ----------------------------------------------------------------------------------
# FILE: writer.py
# Responsible for writing cleaned data to disk using partitioning.
# Follows Single Responsibility Principle (SRP) and is separated for easier testing and logging.
# ----------------------------------------------------------------------------------

import pyarrow.dataset as ds
import polars as pl
from config import PROCESSED_DIR


def write_partitioned_parquet(df: pl.DataFrame, output_dir: str = str(PROCESSED_DIR)) -> str:
    """
    Converts the Polars DataFrame to an Arrow Table and writes it as partitioned Parquet files.
    Partitioning is done by 'year' and 'month'.

    This function is designed to be idempotent and safe for repeated runs.
    """
    print(f"Writing partitioned data to: {output_dir}")
    arrow_table = df.to_arrow()

    ds.write_dataset(
        data=arrow_table,
        base_dir=output_dir,
        format="parquet",
        partitioning=["year", "month"],
        existing_data_behavior="delete_matching"
    )

    print("Write complete.")
    return output_dir