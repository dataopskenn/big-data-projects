
# NYC Yellow Taxi Data ETL Pipeline – Improvement Plan and Next Steps

## Project Status

This project already includes:
- A functioning ETL pipeline written in `nyc_etl.py`
- Docker containerization using `docker-compose.yml`
- Data transformation using Polars and PyArrow
- CLI interface for processing by year or all months

## Purpose of this Document

This markdown outlines what has been completed, what needs improvement, and the specific steps I will take to modularize, test, orchestrate, and document the project in a production-grade manner.

---

## Review Summary

### 1. nyc_etl.py

**What is done well:**
- Clear modular functions (e.g., `download_data`, `clean_data`, `run_etl`)
- Proper use of Polars and PyArrow for speed and Parquet compatibility
- Partitioning logic is implemented
- Basic CLI interface via argparse

**What will improve:**
| Issue | Reason | Solution |
|-------|--------|----------|
| Monolithic structure | Hard to test or extend | Split into modules: downloader, transformer, writer, config |
| Hardcoded paths | Not portable | Move to config.py or dotenv file |
| Use of print() | Not suitable for production | Replace with Python logging |
| No formal test hooks | Unscalable for CI | Modularize functions for unit tests |
| No return info from writers | No observability | Return written paths or row counts |
| No schema validation | Risk of bad data | Introduce validation and filtering |

---

### 2. Dockerfile and docker-compose.yml

**Strengths:**
- Mounts volumes for persistent data
- Named containers for clarity

**What will improve:**
| Issue | Reason | Solution |
|-------|--------|----------|
| Dockerfile not validated | Unknown base image | Use minimal Python base image with requirements.txt |
| Single container | Not scalable | Add services for DuckDB and Airflow |
| No environment support | Hard to configure | Introduce .env file and volume mappings |

---

### 3. requirements.txt

**What is good:**
- Required libraries listed
- PyArrow version pinned for compatibility

**What will improve:**
| Issue | Reason | Solution |
|-------|--------|----------|
| Unused pandas import | Wastes space | Remove unless needed |
| Missing orchestration/testing tools | Not CI/CD-ready | Add pytest, airflow, dotenv, etc. |

---

## Improvement Plan by Stage

### A. Refactor into Modular Components

- downloader.py: Data acquisition
- transformer.py: Cleaning and date parsing
- writer.py: Partitioned saving
- config.py: Base paths and constants
- logger.py: Unified logging
- main.py: CLI and orchestration hook

### B. Create DuckDB Script or Notebook

- Read Parquet folders using DuckDB SQL
- Explore metrics like trip count, fare averages, vendor analysis
- Output aggregates to CSV for Power BI

### C. Power BI Integration

- Option 1: Connect Power BI to DuckDB via ODBC
- Option 2: Import CSV aggregates from DuckDB
- Document both methods and fields

### D. Add Testing using pytest

- Add unit tests for each module
- Create sample datasets under /tests/fixtures
- Test schema, null handling, timestamp filtering

### E. Extend Docker Compose

- Add containers:
  - ETL engine (already present)
  - DuckDB server or CLI
  - Airflow scheduler + UI
- Use volumes and networks for integration

### F. Add Airflow DAGs

- Use PythonOperator to modularize task calls
- Run DAGs by month or year
- Log task status and failures
- Deploy using Docker Compose

### G. Finalize README and Documentation

- Step-by-step usage
- How to run tests
- Visual architecture and folder structure
- Sample queries and Power BI screenshots

---

## Final Project Structure (Planned)

```
batch_processing/
├── etl/
│   ├── downloader.py
│   ├── transformer.py
│   ├── writer.py
│   └── config.py
├── dags/
│   └── nyc_etl_dag.py
├── tests/
│   ├── test_downloader.py
│   ├── test_transformer.py
│   └── test_writer.py
├── notebooks/
│   └── explore_duckdb.ipynb
├── scripts/
│   └── duck_query_runner.py
├── docker/
│   └── Dockerfile
│   └── docker-compose.yml
├── .env
├── README.md
└── data/
    ├── raw/
    └── processed/
```

---