# =============================================================================
# EPL STATS PIPELINE - DOCKERFILE
# ============================================================================

FROM python:3.10-slim

#set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    curl \\
    postgresql-client \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Set environment variables
ENV PYTHONPATH=/app
ENV AIRFLOW_HOME=/opt/airflow

# Expose ports
EXPOSE 8080 8000

# Default command
CMD ["bash"]