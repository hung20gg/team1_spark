# Dockerfile
FROM apache/airflow:3.0.6


# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
