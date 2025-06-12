# tests/test_transformer.py

import polars as pl
import pytest

from etl.transformer import clean_data

@pytest.fixture
def raw_df():
    """
    Arrange: Create a raw DataFrame containing a mix of valid and invalid rows:
      - Row 0: fully valid
      - Row 1: null passenger_count
      - Row 2: invalid pickup timestamp
      - Row 3: null dropoff datetime
      - Row 4: valid, but negative trip distance (edge case)
    """
    return pl.DataFrame({
        "passenger_count": [1, None, 2, 1, 3],
        "trip_distance": [0.5, 1.0, 2.0, 3.0, -1.0],
        "total_amount": [10.0, 20.0, 30.0, 40.0, 50.0],
        "tpep_pickup_datetime": [
            "2025-01-01 08:00:00",  # valid
            "2025-01-02 09:00:00",  # valid but dropped by null passenger_count
            "not-a-date",           # invalid format
            "2025-01-04 11:00:00",  # valid but will drop due to null dropoff
            "2025-01-05 12:00:00"   # valid
        ],
        "tpep_dropoff_datetime": [
            "2025-01-01 08:15:00",  # valid
            "2025-01-02 09:15:00",  # valid
            "2025-01-03 10:00:00",  # valid but pickup invalid
            None,                   # null dropoff
            "2025-01-05 12:30:00"   # valid
        ]
    })

def test_clean_data_basic_filtering(raw_df):
    """
    Act: Run the cleaning function.
    Assert:
      - Only rows with no nulls in critical columns remain.
      - Invalid timestamp rows are dropped.
      - Extracted 'year' and 'month' match the pickup datetime.
    """
    cleaned = clean_data(raw_df)

    # We expect only rows 0 and 4 to survive:
    # row 1 dropped (null passenger_count), row 2 dropped (invalid timestamp), row 3 dropped (null dropoff)
    assert cleaned.height == 2, f"Expected 2 rows after cleaning, got {cleaned.height}"

    # Check that year/month columns exist and are correct
    years = cleaned["year"].to_list()
    months = cleaned["month"].to_list()
    assert years == [2025, 2025], f"Year column values wrong: {years}"
    assert months == [1, 1], f"Month column values wrong: {months}"

def test_clean_data_negative_distance_kept(raw_df):
    """
    Act: Run cleaning.
    Assert:
      - Rows with negative trip_distance are not explicitly dropped by clean_data,
        because clean_data only filters nulls and invalid timestamps.
    """
    cleaned = clean_data(raw_df)

    # The row with trip_distance = -1.0 is row index 4 in raw_df and should still be present
    distances = cleaned["trip_distance"].to_list()
    assert -1.0 in distances, "clean_data should not drop rows based on negative trip_distance"

@pytest.mark.parametrize(
    "pickup, dropoff, keep",
    [
        ("2025-02-01 10:00:00", "2025-02-01 10:15:00", True),
        ("bad-date",            "2025-02-02 11:00:00", False),
        ("2025-02-03 12:00:00", None,                       False),
    ]
)
def test_clean_data_timestamps_parametrized(pickup, dropoff, keep):
    """
    Parametrized test for timestamp validation:
      - Valid pickup & dropoff -> row kept
      - Invalid pickup -> dropped
      - Null dropoff -> dropped
    """
    df = pl.DataFrame({
        "passenger_count": [1],
        "trip_distance": [1.0],
        "total_amount": [5.0],
        "tpep_pickup_datetime": [pickup],
        "tpep_dropoff_datetime": [dropoff]
    })

    cleaned = clean_data(df)
    if keep:
        assert cleaned.height == 1, f"Expected row kept for pickup={pickup}, dropoff={dropoff}"
    else:
        assert cleaned.height == 0, f"Expected row dropped for pickup={pickup}, dropoff={dropoff}"

def test_clean_data_type_conversion(raw_df):
    """
    Act: Run cleaning.
    Assert:
      - After cleaning, pickup and dropoff columns are Polars Datetime type.
    """
    cleaned = clean_data(raw_df)
    # Polars dtype check
    dtype_pickup = cleaned.schema["tpep_pickup_datetime"]
    dtype_dropoff = cleaned.schema["tpep_dropoff_datetime"]
    assert dtype_pickup == pl.Datetime, f"Expected pickup datetime type, got {dtype_pickup}"
    assert dtype_dropoff == pl.Datetime, f"Expected dropoff datetime type, got {dtype_dropoff}"
