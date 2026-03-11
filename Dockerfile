FROM python:3.13-slim

# Avoid Python writing .pyc files and force unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# App directory
WORKDIR /app

# Install Python dependencies first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Create directory for SQLite database
VOLUME ["/app/db"]

# Default database path
ENV DB_PATH=/app/db/market_data.db

# Run the application
CMD ["python", "src/financedb/main.py"]
