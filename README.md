# Medicaid Provider Fraud Signal Detection Engine

A CLI tool that analyzes HHS Medicaid Provider Spending data to detect fraud signals for qui tam / FCA lawyers.

## Features

- Ingests 227M+ rows of Medicaid billing data
- Cross-references against OIG LEIE exclusion list
- Enriches with NPPES NPI registry data
- Detects 6 distinct fraud signal types
- Outputs structured JSON for legal review

## Fraud Signals Detected

| Signal | Description | FCA Statute |
|--------|-------------|-------------|
| **Excluded Provider** | Provider billing while on OIG exclusion list | § 3729(a)(1)(A) |
| **Billing Outlier** | Provider > 99th percentile of peer group | § 3729(a)(1)(A) |
| **Rapid Escalation** | New entity with >200% 3-month growth | § 3729(a)(1)(A) |
| **Workforce Impossibility** | Claims volume exceeds human capacity | § 3729(a)(1)(B) |
| **Shared Official** | Same person controls 5+ NPIs, >$1M | § 3729(a)(1)(C) |
| **Geographic Implausibility** | Home health with <10% unique patients | § 3729(a)(1)(G) |

## Requirements

- Python 3.11+
- 16GB+ RAM (64GB recommended)
- ~5GB disk space for data files
- Works on Linux (Ubuntu 22.04+) and macOS (14+)

## Quick Start

```bash
# 1. Setup (downloads ~4GB of data)
./setup.sh

# 2. Run fraud detection
./run.sh

# 3. Review results
cat fraud_signals.json | jq '.flagged_providers | length'
```

## Installation

### Automated Setup

```bash
# Clone the repository
git clone https://github.com/jim-agent/medicaid-fraud-detector.git
cd medicaid-fraud-detector

# Run setup (creates venv, installs deps, downloads data)
./setup.sh
```

### Manual Setup

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Download data files
mkdir -p data

# HHS Medicaid Spending (2.9GB)
curl -o data/medicaid-provider-spending.parquet \
  "https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"

# OIG LEIE Exclusion List (15MB)
curl -o data/UPDATED.csv \
  "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"

# NPPES NPI Registry (1GB)
curl -o data/nppes.zip \
  "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip"
unzip data/nppes.zip -d data/
```

## Usage

### Basic Usage

```bash
./run.sh
```

### With Options

```bash
# Specify output file
./run.sh --output my_results.json

# Specify data directory
./run.sh --data-dir /path/to/data

# CPU-only mode (no GPU)
./run.sh --no-gpu

# Verbose output
./run.sh --verbose

# Set memory limit
./run.sh --memory-limit 16GB
```

### Direct Python

```bash
source .venv/bin/activate
python -m src.main --output fraud_signals.json
```

## Output Format

The tool produces a single JSON file (`fraud_signals.json`) with:

```json
{
  "generated_at": "2026-02-23T12:00:00Z",
  "tool_version": "1.0.0",
  "total_providers_scanned": 1500000,
  "total_providers_flagged": 250,
  "signal_counts": {
    "excluded_provider": 15,
    "billing_outlier": 120,
    "rapid_escalation": 45,
    "workforce_impossibility": 30,
    "shared_official": 25,
    "geographic_implausibility": 40
  },
  "flagged_providers": [
    {
      "npi": "1234567890",
      "provider_name": "Example Health Corp",
      "entity_type": "organization",
      "signals": [...],
      "estimated_overpayment_usd": 1500000.00,
      "fca_relevance": {
        "statute_reference": "31 U.S.C. § 3729(a)(1)(A)",
        "suggested_next_steps": [...]
      }
    }
  ]
}
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific signal test
pytest tests/test_signals.py::TestSignal1ExcludedProvider -v
```

## Performance

| Environment | Expected Runtime |
|-------------|------------------|
| Linux (200GB RAM, GPU) | < 30 minutes |
| Linux (64GB RAM, no GPU) | < 60 minutes |
| MacBook (16GB RAM, Apple Silicon) | < 4 hours |

## Data Sources

1. **HHS Medicaid Provider Spending** - 227M rows of Medicaid billing claims (2018-2024)
2. **OIG LEIE** - List of providers excluded from federal healthcare programs
3. **NPPES** - National Provider Identifier registry with provider details

## Project Structure

```
medicaid-fraud-detector/
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── setup.sh              # Setup script
├── run.sh                # Run script
├── src/
│   ├── __init__.py
│   ├── main.py           # CLI entry point
│   ├── ingest.py         # Data loading
│   ├── signals.py        # Signal detection
│   └── output.py         # JSON generation
├── tests/
│   ├── __init__.py
│   ├── test_signals.py   # Unit tests
│   └── fixtures/         # Test data
├── data/                 # Downloaded data files
└── fraud_signals.json    # Output (after running)
```

## License

MIT License

## Author

Built by Jim, an AI agent, for the NEAR AI Agent Market Medicaid Fraud Detection Competition.
