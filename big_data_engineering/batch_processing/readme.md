# NYC Taxi Data ETL Pipeline — Documentation & Lessons Learned

## Project Scope
This project aims to build a **Batch ETL pipeline** that processes NYC Yellow Taxi Trip data from public sources. The pipeline:

- **Downloads** raw `.parquet` files for each month
- **Cleans** the data
- **Partitions** it by year and month
- **Stores** the results in a well-structured and efficient **Parquet dataset**
- Prepares the data to be queried later using tools like **DuckDB**, or visualized with **Power BI**

---

## Why This Project?
Understanding how to set up an ETL pipeline is foundational for becoming a Data Engineer.
This project serves as a stepping stone for working with **big data, analytics pipelines, and enterprise-level data processing**.

---

## Tech Stack

| Tool           | Role                          | Why?                                          | Alternatives          |
|----------------|-------------------------------|-----------------------------------------------|------------------------|
| **Python (3.12+)** | Core ETL scripting         | Industry standard, readable, powerful         | Scala, Java            |
| **Polars**     | High-performance DataFrame ops | MUCH faster than pandas, uses Apache Arrow   | pandas (slower), Dask  |
| **PyArrow**    | Write partitioned Parquet datasets | Supports proper partitioning + DuckDB/Spark compatibility | fastparquet           |
| **Docker**     | Containerization              | Ensures consistency and reproducibility       | Podman                 |
| **Power BI**   | BI layer (planned)            | Easy-to-use, powerful visual analytics        | Tableau, Superset      |
| **DuckDB**     | Query Parquet files directly  | Fast, lightweight SQL on files                | Trino, Presto          |

---

## Workflow Overview

```
Raw Source (NYC Open Data Parquet URL)
        ↓
Download → Clean → Partition → Save to Disk
        ↓           ↓            ↓
Polars  → PyArrow  → Parquet (year/month)
```

Output folder structure:

```
/data/processed_data/yellow_tripdata/
└── year=2024/
    └── month=03/
        └── part-0.parquet
```

---

## Docker-Based Development
To keep everything reproducible, I've used Docker.

### Benefits:
- Same Python and library versions on every run.
- Environment is reproducible on any machine, so chances of failure is greatly reduced.

---

## Lessons Learned & Challenges

### 1. PyArrow Partitioning Pitfall
- **Problem:** PyArrow refused to write data because the output directory already existed.
- **Fix:** We used `existing_data_behavior="delete_matching"` and wrote the url of the directory as f-string so it is processed at runtime and not interpreted as a constant value.
- **Tip:** Never write directly to folders that contain stray files. Partition folders must be clean.

### 2. ParquetWriterOptions Error
- **Problem:** Code crashed with `AttributeError: module 'pyarrow.parquet' has no attribute 'ParquetWriterOptions'`.
- **Cause:** I used a PyArrow version **< 14.0.0** where this attribute was not available.
- **Fix:** Either:
  - Upgrade `pyarrow` in `requirements.txt` to `>=14.0.0`.
  - Or remove `file_options=...` completely.

### 3. Bad Timestamps in Data
- **Problem:** ETL output showed folders like `year=2002`, `year=2010` etc., even when processing only 2024.
- **Cause:** Some rows had corrupted or strange timestamps.
- **Fix:** Added a filter:

```python
# Strict filtering
pl.col("year") == year & pl.col("month") == month
```

### 4. Orphan Docker Containers
- **Problem:** Running many ETL jobs created containers like `etl-run-xxxx`, which didn’t stop automatically.
- **Fix:**

```bash
docker-compose down --remove-orphans
```

---

## Testing the ETL Script
Use this inside the Docker container:

```bash
python nyc_etl.py --year 2024 --month 3
```

Or process a full year:

```bash
python nyc_etl.py --year 2024 --all-months
```

---

## Clean-Up
To stop everything:

```bash
docker-compose down --remove-orphans
```

To kill all running containers:

```bash
docker kill $(docker ps -q)
```

To reset volumes:

```bash
docker system prune -a --volumes
```

---

## Key Python Concepts Used
- **Polars filtering and date parsing**.
- **Downloading files with `urllib.request`**.
- **CLI argument parsing with `argparse`**.
- **Partitioned file saving with PyArrow**.

---

## What's Next
- Query this data using DuckDB or Trino.
- Build a Power BI dashboard via ODBC connector to DuckDB.
- Add orchestration using **Airflow** or **Prefect** or **Mage** or all as different solutions for different use-cases.
