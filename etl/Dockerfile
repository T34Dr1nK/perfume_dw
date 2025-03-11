# Use a lightweight Python image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Install PostgreSQL dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy only the necessary files
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire ETL script directory
COPY . .

# Ensure logs directory exists
RUN mkdir -p logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "etl_script.py"]
