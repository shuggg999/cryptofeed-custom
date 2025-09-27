# Cryptofeed Monitoring System Docker Image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install additional dependencies for ClickHouse and Cython compilation
RUN pip install clickhouse-connect httpx Cython

# Copy project files
COPY . .

# Try to use Cython extensions or fallback to pure Python
RUN cd /app && \
    (echo "Attempting to import cryptofeed.types..." && \
     python -c "import cryptofeed.types; print('cryptofeed.types already available')" || \
     (echo "Building Cython extensions..." && \
      timeout 300 python setup.py build_ext --inplace && \
      python -c "import cryptofeed.types; print('Cython build successful')" || \
      (echo "Cython build failed, using pure Python fallback..." && \
       cp /app/cryptofeed/types_fallback.py /app/cryptofeed/types.py && \
       python -c "import cryptofeed.types; print('Using pure Python fallback')"))) && \
    echo "Types module setup complete."

# Create necessary directories
RUN mkdir -p logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose ports
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health')" || exit 1

# Default command
CMD ["python", "-m", "cryptofeed_api.monitor.main"]