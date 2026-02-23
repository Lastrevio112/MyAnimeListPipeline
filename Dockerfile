# Start from a slim Python 3.11 image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/workspace/dbt

# Install system dependencies needed by some Python packages
RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /workspace

# Copy requirements first (Docker caches this layer if requirements.txt hasn't changed)
COPY requirements.txt .

# Install all Python packages including DBT and DuckDB
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# NodeJS required for DudckDB SQLTools in VSCode
RUN apt-get update && apt-get install -y nodejs npm

# also to make sql tools work
RUN npm install -g duckdb-async@0.10.2

# Install DuckDB CLI
RUN curl -fsSL https://github.com/duckdb/duckdb/releases/download/v1.4.0/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip \
    && apt-get install -y unzip \
    && unzip /tmp/duckdb.zip -d /tmp/ \
    && mv /tmp/duckdb /usr/local/bin/duckdb \
    && chmod +x /usr/local/bin/duckdb \
    && rm /tmp/duckdb.zip

# Install DBT Fusion CLI (the newer Rust-based DBT runner)
# We install both classic dbt-core and dbt-fusion for maximum VS Code compatibility
RUN curl -fsSL https://dbt-fusion.sh/install.sh | bash || true

# Expose Jupyter port
EXPOSE 8888

# Default command: start bash (Dev Containers override this anyway)
CMD ["/bin/bash"]