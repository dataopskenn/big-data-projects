# Big Data • ML Engineering • MLOps — Local First Projects

Welcome to my open, ongoing collection of **Big Data Engineering**, **Machine Learning Engineering**, and **MLOps** projects — developed entirely using **local-first, reproducible, and containerized environments**.

This repository is more than a showcase of code — it's an evolving workspace, my learning hub/experience, and demonstration of how complex data systems and ML applications can be developed, tested, and deployed **locally at first, then replicated on the cloud**. All of this is to document my learning and pour out my thoughts and findings to both help myself and whoever comes in here in the future.

---

## Objectives

- Design, build, and ship projects that simulate production-grade analytics and ML workflows
- Rely exclusively on **open-source tools** and **local resources** (no cloud services)
- Maintain clean, reproducible environments using **Docker**
- Practice end-to-end engineering: from ingestion and processing to deployment and monitoring
- Document learnings and technical choices for each project

---

## Guiding Principles

- **Local-first**: All work is performed using Docker-based environments, with no tools installed globally on the host OS.
- **Modular Projects**: Each project lives in its own directory with a consistent structure and is containerized to run independently.
- **Reproducibility**: Every environment can be rebuilt with a single command using version-pinned `Dockerfile`s and `requirements.txt`.
- **Continuous Learning**: This repo will grow over time as I explore new architectures, tools, and ideas — with full transparency in how I’m learning and improving.

---

## What's Coming

This repository is split into broad categories:
- **Big Data Engineering** (ETL, batch/stream pipelines, data lakes, warehousing)
- **Machine Learning Engineering** (training pipelines, APIs, experiment tracking)
- **MLOps** (automation, model drift detection, CI/CD pipelines, version control)
- **LLM/RAG Prototypes** (LLM deployment, prompt tracking, vector search, LangChain)

Each category will contain several independent projects designed to be self-contained, modular, and well-documented.

---

## Technologies

Some of the tools and frameworks you’ll see across the stack:

- **Languages**: Python, SQL, Bash
- **Data Tools**: Apache Spark, Iceberg, DuckDB, Kafka, PostgreSQL, TimescaleDB
- **ML Libraries**: Scikit-learn, XGBoost, Transformers (HuggingFace), LangChain
- **APIs & Dashboards**: FastAPI, Flask, Streamlit, JupyterLab
- **Ops & Monitoring**: MLflow, DVC, EvidentlyAI, Great Expectations
- **Environment Management**: Docker, Docker Compose, Makefiles, Terraform (Plan to for quick cloud architecture build)

---

## How I'll Work

- All development will happen inside Docker containers.
- Each container will expose only what’s needed (e.g. JupyterLab, REST APIs).
- I'll use `make` and `docker-compose` to manage services, run tests, and launch interfaces.
- All data and code are tracked for reproducibility and version control.
- Documentation is written as I go, with blog posts or markdown summaries included. I'll try.

---

## Learning Resources & Credit

I'll build these projects with inspiration and guidance from some incredible educators, engineers, and open-source communities:

- [Zach Wilson / DataExpert.io](https://www.dataexpert.io)  
  GitHub: [https://github.com/EcZachly](https://github.com/EcZachly)  
  Zach is one of the best educators and data engineers around.

- [DataTalks.Club](https://datatalks.club/)
- [Made With ML by Goku Mohandas](https://madewithml.com/)
- [HuggingFace Transformers Course](https://huggingface.co/course)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [LangChain Documentation](https://docs.langchain.com/)
- [EvidentlyAI Docs](https://docs.evidentlyai.com/)
- [Apache Spark Official Docs](https://spark.apache.org/docs/latest/)
- [Docker Documentation](https://docs.docker.com/)
