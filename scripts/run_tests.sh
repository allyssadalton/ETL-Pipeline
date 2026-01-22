#!/bin/bash

# Test runner script for ETL Pipeline

echo "Running ETL Pipeline Tests..."
echo "============================="

# Install dependencies if needed
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage (optional)
# pytest tests/ --cov=ingestion --cov=transformation --cov=validation --cov=storage --cov=analytics --cov-report=html

echo "Test run complete."