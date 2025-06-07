# Building the NYC Taxi Data Batch ETL Pipeline

## A Detailed, Practical Guide on Code, Design, and Production Engineering

---

## What Am I Doing and Why?
I am building a **batch ETL** pipeline to process NYC Yellow Taxi trip data - a large-scale, messy, real-world dataset. The goal isn’t just to make it work but to engineer it well so that it is:

- Reliable: Handles failures gracefully, can be rerun without issues, and produces accurate data.

- Maintainable: Easy for me or others to read, debug, and extend later without confusion.

- Performant: Efficient in CPU, memory, and disk usage to scale as data grows.

- Portable: Runs consistently anywhere - my laptop, a server, or cloud - without surprises.

- Observable: Gives clear visibility into what it’s doing, so issues are caught early.

- Tested: Code correctness and data integrity are verified automatically.

- Automated: Runs on schedule without manual intervention, with alerts on failure.

If I come back in two years, I want to understand not just “what” the code does but “why” it was written that way and how it supports these critical properties.

--- 

## Modular Code Design - Single Responsibility Principle (SRP)

**What is it?**

Each module or function has exactly one reason to change. This keeps the codebase clean and reduces accidental breakage.

**Why is this important?**

If downloading, cleaning, and writing all happened in one giant function, a small change in the cleaning logic risks breaking downloads or writing. It’s also hard to test or reuse parts independently.

**How I applied it here:**

I separated the code into modules:

- `downloader.py` only downloads files, ensuring idempotency by skipping files already saved.

- `transformer.py` only cleans and prepares the data.

- `writer.py` only writes cleaned data to disk with partitioning.

- `pipeline.py` orchestrates these steps.

- `config.py` centralizes constants and paths.

Example from `downloader.py`:

```python
def download_data(year: int, month: int) -> Optional[str]:
    filename = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    local_path = RAW_DIR / filename

    if local_path.exists():
        print(f"File already exists: {local_path}")
        return str(local_path)

    try:
        os.makedirs(RAW_DIR, exist_ok=True)  # Ensure directory exists
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, str(local_path))
        print(f"Downloaded to {local_path}")
        return str(local_path)
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None
```

Notice how I check if the file already exists and skip downloading if so. This makes the function idempotent - a critical property in data engineering to make pipelines rerunnable without side effects.

---

## Configuration Management - Avoiding Hardcoded Paths

**What is it?**

Centralizing constants like file paths, URLs, and environment variables in a dedicated module or `.env` file rather than sprinkling them throughout the code.

Why is this important?

Hardcoded paths break portability. For example, absolute paths like `/data/raw` behave differently on Windows vs Linux, and they are not relative to the project directory.

**How I applied it here:**

```python
import os
from pathlib import Path

BASE_DIR = Path(os.getenv("DATA_PATH", Path(__file__).resolve().parent.parent / "data"))
RAW_DIR = BASE_DIR / "raw_data"
PROCESSED_DIR = BASE_DIR / "processed_data" / "yellow_tripdata"
```

This approach supports:

- Local development without Docker.

- Running inside Docker where `/data` is volume-mounted.

- CI/CD pipelines with environment overrides.

I can now change data paths without modifying the source code, which improves flexibility and reduces bugs related to environment differences.

---

## Data Cleaning with Polars - Handling Real-World Messy Data

**What is it?**

Data is often incomplete, inconsistent, or incorrectly typed. Cleaning involves removing or imputing missing values, parsing dates, and validating ranges.

**Why is this important?**

Garbage in, garbage out. Without cleaning, downstream analytics will be inaccurate or fail.

**How I applied it here:**

```python
df = df.filter(
    (pl.col("passenger_count").is_not_null()) &
    (pl.col("trip_distance").is_not_null()) &
    (pl.col("total_amount").is_not_null())
)

if df.schema.get("tpep_pickup_datetime") != pl.Datetime:
    df = df.with_columns(
        pl.col("tpep_pickup_datetime").str.strptime(pl.Datetime, strict=False)
    )
```

I can avoid unnecessary computation overhead by checking types before parsing. Polars offers speed and expressiveness, and if needed, I can scale further using its lazy API.

---

## Writing Partitioned Parquet - Efficient Storage and Querying

**What is it?**

Partitioning data by key columns (here `year` and `month)` physically separates data, enabling queries to scan only relevant data subsets.

**Why is this important?**

This is essential for scale: scanning the entire dataset on every query is expensive.

**How I applied it:**

```python
ds.write_dataset(
    data=arrow_table,
    base_dir=output_dir,
    format="parquet",
    partitioning=["year", "month"],
    existing_data_behavior="delete_matching",
)
```

Key points:

- Partitioning on year and month supports efficient data pruning.

- `existing_data_behavior="delete_matching"` prevents errors when overwriting partitions.

- Writing compressed Parquet files saves storage and improves I/O.

---

## Avoiding Double Execution - `if __name__ == "__main__":`

**What is it?**

This Python idiom ensures scripts only run code when executed directly, not when imported as modules.

**Why is this important?**

In orchestrators like Airflow or during testing, I import modules without wanting side-effects like starting ETL runs.

**How I applied it:**

```python
if __name__ == "__main__":
    main()
```

This enables reuse and prevents unexpected pipeline executions during imports.

---

## Containerization with Docker - Consistency Everywhere

**What is it?**

Packaging the entire runtime environment (Python version, dependencies, OS) and code inside containers.

**Why is this important?**

Prevents "works on my machine" syndrome, simplifies deployment, and standardizes the environment.

**How I applied it:**

- A single Dockerfile installs dependencies and copies source code.

- `docker-compose.yml` orchestrates ETL, DuckDB, Airflow, and optionally Jupyter notebook containers.

- Data directories are volume-mounted for persistence.

This guarantees the pipeline runs identically across environments and supports smooth team onboarding.

---

### Querying with DuckDB —-SQL Over Parquet Files

**What is it?**

DuckDB is an embedded analytical SQL engine that can query Parquet files directly without loading everything into memory.

**Why is this important?**

It bridges raw data and business intelligence without heavy infrastructure, enabling fast, flexible analysis.

**How I applied it:**

```sql
SELECT vendor_id, COUNT(*) as trip_count
FROM 'data/processed_data/yellow_tripdata/'
WHERE year = 2024 AND month = 3
GROUP BY vendor_id;
```

This approach supports easy validation and empowers analysts with familiar SQL tools.

---

## Automated Testing with pytest — Building Confidence, Building With Integrity

**What is it?**

Automated tests check functionality and data quality without manual intervention.

**Why is this important?**

Tests catch bugs early and enable safe refactoring.

**How I applied it:**

- Tests cover downloading, cleaning, and writing modules independently.

- I use mocks for external dependencies to keep tests fast.

- Reduces risks and improves developer confidence in the quality of output.

---

## Orchestration with Apache Airflow - Automation and Monitoring

**What is it?**

Airflow schedules, monitors, and manages workflows as DAGs, handling dependencies and failures.

**Why is this important?**

Manual runs don’t scale, and errors need to be caught and retried automatically.

**How I applied it:**

- Created Airflow DAGs that run monthly ETL jobs.

- Tasks correspond to download, clean, write steps.

- Airflow handles retries, logs, and alerts.

This elevates the pipeline from manual scripts to production-ready automation.

---

## Summary: Why This Design Matters

If I neglect these principles:

- Code becomes brittle and hard to maintain.

- Pipelines fail silently or produce wrong data.

- Scaling to bigger data or teams becomes impossible.

By applying these best practices, I ensure the pipeline is:

- Modular and testable

- Performant and scalable

- Portable and reproducible

- Observable and automatable

---

## References

- VanderPlas, Jake. Python Data Science Handbook, O’Reilly, 2016 - foundational for Python data tools.

- Polars Documentation, https://pola-rs.github.io/polars-book/ - performance-oriented dataframe library.

- Apache Arrow Project, https://arrow.apache.org/ - for Parquet storage and efficient I/O.

- Docker Docs, https://docs.docker.com/ - all documentation for docker and it's components.

- DuckDB Official Site, https://duckdb.org/ - embedded SQL for analytics.

- Apache Airflow Documentation, https://airflow.apache.org/ - orchestration best practices.

- DataTalks.Club, https://datatalks.club/  
  [Datatalks.Club GitHub](https://github.com/DataTalksClub/)  
  [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)  