#!/usr/bin/env python3
"""
Medicaid Provider Fraud Signal Detection Engine

CLI tool that analyzes HHS Medicaid spending data to detect fraud signals.
"""

import argparse
import logging
import sys
import os
import resource
import platform
from pathlib import Path
from datetime import datetime

from .ingest import DataIngestor
from .signals import SignalDetector
from .output import ReportGenerator


def get_memory_usage_mb() -> float:
    """Get current memory usage in MB."""
    if platform.system() == 'Linux':
        # More accurate on Linux
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) / 1024  # KB to MB
    # Fallback using resource module
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_maxrss / 1024  # KB to MB on Linux


def get_peak_memory_mb() -> float:
    """Get peak memory usage in MB."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # ru_maxrss is in KB on Linux, bytes on macOS
    if platform.system() == 'Darwin':
        return usage.ru_maxrss / (1024 * 1024)
    return usage.ru_maxrss / 1024

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Medicaid Provider Fraud Signal Detection Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main --output fraud_signals.json
  python -m src.main --data-dir ./data --output results.json
  python -m src.main --no-gpu --output fraud_signals.json
        """
    )
    
    parser.add_argument(
        '--data-dir', '-d',
        type=str,
        default='./data',
        help='Directory containing input data files (default: ./data)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='fraud_signals.json',
        help='Output JSON file path (default: fraud_signals.json)'
    )
    
    parser.add_argument(
        '--no-gpu',
        action='store_true',
        help='Disable GPU acceleration (CPU-only mode)'
    )
    
    parser.add_argument(
        '--memory-limit',
        type=str,
        default='8GB',
        help='DuckDB memory limit (default: 8GB)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        logger.error("Run setup.sh first to download required data files")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Medicaid Provider Fraud Signal Detection Engine")
    logger.info("=" * 60)
    logger.info(f"Data directory: {data_dir.absolute()}")
    logger.info(f"Output file: {args.output}")
    logger.info(f"GPU disabled: {args.no_gpu}")
    logger.info(f"Memory limit: {args.memory_limit}")
    logger.info("")
    
    start_time = datetime.now()
    
    try:
        # Phase 1: Data Ingestion
        logger.info("PHASE 1: Loading data sources...")
        ingestor = DataIngestor(data_dir)
        ingestor.load_all()
        conn = ingestor.get_connection()
        
        # Phase 2: Signal Detection
        logger.info("")
        logger.info("PHASE 2: Detecting fraud signals...")
        detector = SignalDetector(conn)
        signals = detector.detect_all_signals()
        
        # Phase 3: Report Generation
        logger.info("")
        logger.info("PHASE 3: Generating report...")
        generator = ReportGenerator(conn)
        report = generator.generate_report(signals, args.output)
        
        # Collect resource metrics
        elapsed = datetime.now() - start_time
        peak_memory_mb = get_peak_memory_mb()
        current_memory_mb = get_memory_usage_mb()
        
        # Add resource metrics to report
        report['execution_metrics'] = {
            'total_runtime_seconds': round(elapsed.total_seconds(), 2),
            'total_runtime_human': str(elapsed),
            'peak_memory_mb': round(peak_memory_mb, 2),
            'final_memory_mb': round(current_memory_mb, 2),
            'platform': platform.system(),
            'python_version': platform.python_version(),
            'cpu_count': os.cpu_count(),
        }
        
        # Re-write report with metrics
        generator.write_report(report, args.output)
        
        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total runtime: {elapsed}")
        logger.info(f"Peak memory: {peak_memory_mb:.2f} MB")
        logger.info(f"Providers scanned: {report['total_providers_scanned']:,}")
        logger.info(f"Providers flagged: {report['total_providers_flagged']:,}")
        logger.info("")
        logger.info("Signal counts:")
        for signal_type, count in report['signal_counts'].items():
            logger.info(f"  {signal_type}: {count:,}")
        logger.info("")
        logger.info(f"Output written to: {args.output}")
        logger.info("=" * 60)
        
        # Cleanup
        ingestor.close()
        
    except FileNotFoundError as e:
        logger.error(f"Required file not found: {e}")
        logger.error("Run setup.sh to download all required data files")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error during execution: {e}")
        sys.exit(1)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
