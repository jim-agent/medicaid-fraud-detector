#!/bin/bash
#
# Medicaid Fraud Signal Detection Engine - Run Script
#
# Usage: ./run.sh [options]
#
# Options are passed through to the Python script:
#   --output FILE     Output JSON file (default: fraud_signals.json)
#   --data-dir DIR    Data directory (default: ./data)
#   --no-gpu          Disable GPU acceleration
#   --memory-limit    DuckDB memory limit (default: 8GB)
#   --verbose         Enable verbose output

set -e

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Error: Virtual environment not found."
    echo "Please run setup.sh first."
    exit 1
fi

# Activate virtual environment
source .venv/bin/activate

# Check if data files exist
if [ ! -f "data/medicaid-provider-spending.parquet" ]; then
    echo "Error: Medicaid spending data not found."
    echo "Please run setup.sh first to download data."
    exit 1
fi

# Run the fraud detection engine
echo "Starting Medicaid Fraud Signal Detection Engine..."
echo ""

python -m src.main "$@"
