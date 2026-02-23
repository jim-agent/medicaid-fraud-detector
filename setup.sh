#!/bin/bash
#
# Medicaid Fraud Signal Detection Engine - Setup Script
#
# This script:
# 1. Creates a Python virtual environment
# 2. Installs dependencies
# 3. Downloads required data files
#
# Tested on: Ubuntu 22.04+, macOS 14+ (Apple Silicon)
# Requires: Python 3.11+, curl, unzip

set -e

echo "=============================================="
echo "Medicaid Fraud Signal Detection Engine Setup"
echo "=============================================="
echo ""

# Detect OS
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macOS"
    PYTHON_CMD="python3"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="Linux"
    PYTHON_CMD="python3"
else
    echo "Warning: Untested OS ($OSTYPE), proceeding anyway..."
    PYTHON_CMD="python3"
fi

echo "Detected OS: $OS"
echo ""

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | cut -d' ' -f2)
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d'.' -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d'.' -f2)

if [[ $PYTHON_MAJOR -lt 3 ]] || [[ $PYTHON_MAJOR -eq 3 && $PYTHON_MINOR -lt 11 ]]; then
    echo "Error: Python 3.11+ required. Found: $PYTHON_VERSION"
    exit 1
fi
echo "Python version: $PYTHON_VERSION ✓"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
if [ -d ".venv" ]; then
    echo "  Virtual environment already exists"
else
    $PYTHON_CMD -m venv .venv
    echo "  Created .venv/"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate
echo "  Activated"
echo ""

# Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "  Dependencies installed ✓"
echo ""

# Create data directory
echo "Creating data directory..."
mkdir -p data
echo "  data/ created"
echo ""

# Download data files
echo "Downloading data files..."
echo "  (This may take 10-30 minutes depending on connection speed)"
echo ""

# File 1: LEIE Exclusion List (~15MB)
if [ -f "data/UPDATED.csv" ]; then
    echo "  [1/3] LEIE exclusion list: already downloaded ✓"
else
    echo "  [1/3] Downloading LEIE exclusion list (~15MB)..."
    curl -L -# -o data/UPDATED.csv \
        "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
    echo "       Downloaded ✓"
fi

# File 2: HHS Medicaid Provider Spending (~2.9GB)
if [ -f "data/medicaid-provider-spending.parquet" ]; then
    echo "  [2/3] Medicaid spending data: already downloaded ✓"
else
    echo "  [2/3] Downloading Medicaid spending data (~2.9GB)..."
    curl -L -# -o data/medicaid-provider-spending.parquet \
        "https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
    echo "       Downloaded ✓"
fi

# File 3: NPPES NPI Registry (~1GB zipped)
if ls data/npidata_pfile_*.csv 1> /dev/null 2>&1; then
    echo "  [3/3] NPPES NPI registry: already extracted ✓"
elif [ -f "data/nppes.zip" ]; then
    echo "  [3/3] Extracting NPPES NPI registry..."
    cd data
    unzip -o -q nppes.zip "npidata_pfile_*.csv"
    cd ..
    echo "       Extracted ✓"
else
    echo "  [3/3] Downloading NPPES NPI registry (~1GB)..."
    curl -L -# -o data/nppes.zip \
        "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip"
    echo "       Extracting..."
    cd data
    unzip -o -q nppes.zip "npidata_pfile_*.csv"
    cd ..
    echo "       Downloaded and extracted ✓"
fi

echo ""
echo "=============================================="
echo "Setup complete!"
echo "=============================================="
echo ""
echo "Data files:"
ls -lh data/*.parquet data/*.csv 2>/dev/null | head -5
echo ""
echo "To run the fraud detection engine:"
echo "  ./run.sh"
echo ""
echo "Or manually:"
echo "  source .venv/bin/activate"
echo "  python -m src.main --output fraud_signals.json"
echo ""
