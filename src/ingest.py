"""
Data ingestion module for Medicaid Fraud Signal Detection Engine.

Loads and joins:
1. HHS Medicaid Provider Spending (parquet)
2. OIG LEIE Exclusion List (CSV)
3. NPPES NPI Registry (CSV from zip)
"""

import duckdb
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DataIngestor:
    """Handles loading and joining of all data sources."""
    
    def __init__(self, data_dir: Path, memory_limit: str = '2GB', temp_dir: str = None):
        self.data_dir = Path(data_dir)
        self.conn = duckdb.connect()
        
        # Determine temp directory
        if temp_dir is None:
            temp_dir = str(Path(data_dir) / 'temp')
        Path(temp_dir).mkdir(parents=True, exist_ok=True)
        
        # Configure DuckDB for aggressive disk spillover
        # Key insight: DuckDB can handle large datasets with limited RAM
        # by spilling to disk, but we need enough disk space
        self.conn.execute(f"SET memory_limit='{memory_limit}'")
        self.conn.execute("SET threads=2")  # 2 threads for parallelism
        self.conn.execute(f"SET temp_directory='{temp_dir}'")
        self.conn.execute("SET max_temp_directory_size='20GB'")  # Allow more temp space
        self.conn.execute("SET preserve_insertion_order=false")
        self.conn.execute("SET checkpoint_threshold='128MB'")  # More frequent checkpoints
        self.conn.execute("SET force_external=true")  # Force external algorithms for large ops
        
    def load_spending_data(self) -> None:
        """Load HHS Medicaid Provider Spending parquet."""
        parquet_path = self.data_dir / "medicaid-provider-spending.parquet"
        
        if not parquet_path.exists():
            raise FileNotFoundError(f"Spending data not found: {parquet_path}")
        
        logger.info(f"Loading spending data from {parquet_path}")
        
        # Create view (doesn't load all into memory)
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW spending AS 
            SELECT 
                BILLING_PROVIDER_NPI_NUM,
                SERVICING_PROVIDER_NPI_NUM,
                HCPCS_CODE,
                CLAIM_FROM_MONTH,
                TOTAL_UNIQUE_BENEFICIARIES,
                TOTAL_CLAIMS,
                TOTAL_PAID
            FROM read_parquet('{parquet_path}')
        """)
        
        # Get row count
        count = self.conn.execute("SELECT COUNT(*) FROM spending").fetchone()[0]
        logger.info(f"Spending data loaded: {count:,} rows")
        
    def load_leie_data(self) -> None:
        """Load OIG LEIE Exclusion List."""
        leie_path = self.data_dir / "UPDATED.csv"
        
        if not leie_path.exists():
            raise FileNotFoundError(f"LEIE data not found: {leie_path}")
        
        logger.info(f"Loading LEIE data from {leie_path}")
        
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE leie AS
            SELECT 
                LASTNAME,
                FIRSTNAME,
                MIDNAME,
                BUSNAME,
                GENERAL,
                SPECIALTY,
                NPI,
                STATE,
                EXCLTYPE,
                -- Parse dates from YYYYMMDD format (cast to VARCHAR, filter invalid)
                CASE 
                    WHEN EXCLDATE IS NOT NULL 
                         AND LENGTH(CAST(EXCLDATE AS VARCHAR)) = 8 
                         AND CAST(EXCLDATE AS VARCHAR) != '00000000'
                    THEN strptime(CAST(EXCLDATE AS VARCHAR), '%Y%m%d')::DATE
                    ELSE NULL
                END AS EXCLDATE,
                CASE 
                    WHEN REINDATE IS NOT NULL 
                         AND LENGTH(CAST(REINDATE AS VARCHAR)) = 8 
                         AND CAST(REINDATE AS VARCHAR) != '00000000'
                    THEN strptime(CAST(REINDATE AS VARCHAR), '%Y%m%d')::DATE
                    ELSE NULL
                END AS REINDATE
            FROM read_csv_auto('{leie_path}', header=true, ignore_errors=true)
        """)
        
        count = self.conn.execute("SELECT COUNT(*) FROM leie").fetchone()[0]
        npi_count = self.conn.execute(
            "SELECT COUNT(*) FROM leie WHERE NPI IS NOT NULL AND NPI != ''"
        ).fetchone()[0]
        logger.info(f"LEIE data loaded: {count:,} rows ({npi_count:,} with NPI)")
        
    def load_nppes_data(self) -> None:
        """Load NPPES NPI Registry (required columns only)."""
        # Check for unzipped CSV
        nppes_csv = None
        for f in self.data_dir.glob("npidata_pfile_*.csv"):
            nppes_csv = f
            break
        
        if nppes_csv is None:
            # Try to find and extract from zip
            nppes_zip = self.data_dir / "nppes.zip"
            if nppes_zip.exists():
                logger.info("Extracting NPPES zip...")
                import zipfile
                with zipfile.ZipFile(nppes_zip, 'r') as z:
                    for name in z.namelist():
                        if name.startswith("npidata_pfile") and name.endswith(".csv"):
                            z.extract(name, self.data_dir)
                            nppes_csv = self.data_dir / name
                            break
        
        if nppes_csv is None or not nppes_csv.exists():
            raise FileNotFoundError("NPPES data not found. Run setup.sh first.")
        
        logger.info(f"Loading NPPES data from {nppes_csv}")
        
        # Load only required columns (10 out of 329)
        # Note: Read full CSV then select needed columns (DuckDB optimizes this)
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE nppes AS
            SELECT 
                "NPI" AS npi,
                "Entity Type Code" AS entity_type_code,
                "Provider Organization Name (Legal Business Name)" AS org_name,
                "Provider Last Name (Legal Name)" AS last_name,
                "Provider First Name" AS first_name,
                "Provider Business Practice Location Address State Name" AS state,
                "Provider Business Practice Location Address Postal Code" AS zip_code,
                "Healthcare Provider Taxonomy Code_1" AS taxonomy_code,
                "Provider Enumeration Date" AS enumeration_date,
                "Authorized Official Last Name" AS auth_official_last,
                "Authorized Official First Name" AS auth_official_first
            FROM read_csv_auto(
                '{nppes_csv}', 
                header=true, 
                ignore_errors=true,
                all_varchar=true
            )
            WHERE "NPI" IS NOT NULL
        """)
        
        count = self.conn.execute("SELECT COUNT(*) FROM nppes").fetchone()[0]
        logger.info(f"NPPES data loaded: {count:,} rows")
        
    def load_all(self) -> None:
        """Load all data sources."""
        self.load_spending_data()
        self.load_leie_data()
        self.load_nppes_data()
        logger.info("All data sources loaded successfully")
        
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return the DuckDB connection for signal processing."""
        return self.conn
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
