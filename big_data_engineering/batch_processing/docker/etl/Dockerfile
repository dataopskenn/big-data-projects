# Use the latest Python 3.13 slim image
FROM python:3.13-slim

# Prevent Python from writing .pyc files and forcing stdout flush
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory inside container
WORKDIR /app

# Install system packages (curl for troubleshooting, optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Install required Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ETL script into the container
COPY nyc_etl.py .

# Default command (can be overridden in docker-compose)
CMD ["python", "nyc_etl.py"]
