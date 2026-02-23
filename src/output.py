"""
JSON output generation for fraud signal reports.

Formats results according to the competition schema.
"""

import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import asdict
import duckdb
import logging

from .signals import FraudSignal

logger = logging.getLogger(__name__)

# Version of this tool
TOOL_VERSION = "1.0.0"

# FCA statute mappings per signal type
STATUTE_MAPPING = {
    "excluded_provider": "31 U.S.C. § 3729(a)(1)(A)",
    "billing_outlier": "31 U.S.C. § 3729(a)(1)(A)",
    "rapid_escalation": "31 U.S.C. § 3729(a)(1)(A)",
    "workforce_impossibility": "31 U.S.C. § 3729(a)(1)(B)",
    "shared_official": "31 U.S.C. § 3729(a)(1)(C)",
    "geographic_implausibility": "31 U.S.C. § 3729(a)(1)(G)",
}

# Claim type descriptions per signal
CLAIM_TYPE_MAPPING = {
    "excluded_provider": "False claims submitted by excluded provider - provider was barred from federal healthcare programs but continued billing",
    "billing_outlier": "Potential overbilling - provider billing volume significantly exceeds peer group norms",
    "rapid_escalation": "Potential bust-out scheme - newly formed entity showed rapid, unsustainable billing escalation",
    "workforce_impossibility": "False records - claimed service volume is physically impossible given workforce constraints",
    "shared_official": "Conspiracy - coordinated billing through multiple entities controlled by same individual",
    "geographic_implausibility": "Reverse false claims - repeated billing on same patients suggests phantom services",
}


def generate_next_steps(signal: FraudSignal, provider_info: Dict) -> List[str]:
    """Generate specific next steps for a fraud signal."""
    steps = []
    npi = signal.npi
    evidence = signal.evidence
    
    if signal.signal_type == "excluded_provider":
        steps.append(f"Verify exclusion status of NPI {npi} in OIG LEIE database")
        steps.append(f"Request detailed claims records for {npi} from {evidence.get('exclusion_date', 'exclusion date')} forward")
        steps.append(f"Calculate total Medicaid payments to {npi} during exclusion period")
        if provider_info.get('state'):
            steps.append(f"Contact {provider_info['state']} Medicaid Fraud Control Unit")
            
    elif signal.signal_type == "billing_outlier":
        taxonomy = evidence.get('taxonomy_code', 'unknown')
        state = evidence.get('state', 'unknown')
        steps.append(f"Audit claims for NPI {npi} against peer providers in {taxonomy}/{state}")
        steps.append(f"Request medical records supporting high-volume claims")
        steps.append(f"Compare service patterns to specialty norms")
        steps.append(f"Interview beneficiaries to verify services were rendered")
        
    elif signal.signal_type == "rapid_escalation":
        enum_date = evidence.get('enumeration_date', 'unknown')
        steps.append(f"Investigate ownership/management of entity NPI {npi} (enumerated {enum_date})")
        steps.append(f"Review business formation documents and license applications")
        steps.append(f"Analyze referral patterns for evidence of kickback arrangements")
        steps.append(f"Compare growth trajectory to legitimate new practices")
        
    elif signal.signal_type == "workforce_impossibility":
        claims_per_hour = evidence.get('implied_claims_per_hour', 0)
        steps.append(f"Request employment records and staffing levels for NPI {npi}")
        steps.append(f"Verify claimed {claims_per_hour:.1f} claims/hour is humanly possible")
        steps.append(f"Audit time-of-service documentation for sample claims")
        steps.append(f"Interview staff and patients regarding actual service delivery")
        
    elif signal.signal_type == "shared_official":
        official = evidence.get('authorized_official_name', 'unknown')
        npi_count = evidence.get('controlled_npi_count', 0)
        steps.append(f"Investigate business relationships among {npi_count} entities controlled by {official}")
        steps.append(f"Review corporate formation documents for common ownership")
        steps.append(f"Analyze billing patterns for coordinated fraud indicators")
        steps.append(f"Examine referral patterns between controlled entities")
        
    elif signal.signal_type == "geographic_implausibility":
        state = evidence.get('state', 'unknown')
        codes = evidence.get('flagged_hcpcs_codes', [])
        steps.append(f"Audit home health claims for NPI {npi} in {state}")
        steps.append(f"Verify beneficiary addresses and ability to receive home services")
        steps.append(f"Request documentation for HCPCS codes: {', '.join(codes[:5])}")
        steps.append(f"Interview beneficiaries regarding services actually received")
    
    return steps[:3]  # Return at least 2, cap at 3


class ReportGenerator:
    """Generates the final JSON report."""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        
    def get_provider_info(self, npi: str) -> Dict[str, Any]:
        """Fetch provider details from NPPES."""
        result = self.conn.execute(f"""
            SELECT 
                npi,
                COALESCE(org_name, last_name || ', ' || first_name) AS provider_name,
                CASE WHEN entity_type_code = '1' THEN 'individual' ELSE 'organization' END AS entity_type,
                taxonomy_code,
                state,
                enumeration_date
            FROM nppes
            WHERE npi = '{npi}'
        """).fetchone()
        
        if result:
            return {
                "npi": result[0],
                "provider_name": result[1],
                "entity_type": result[2],
                "taxonomy_code": result[3],
                "state": result[4],
                "enumeration_date": result[5],
            }
        return {
            "npi": npi,
            "provider_name": "Unknown",
            "entity_type": "unknown",
            "taxonomy_code": None,
            "state": None,
            "enumeration_date": None,
        }
    
    def get_provider_totals(self, npi: str) -> Dict[str, Any]:
        """Get aggregate billing statistics for a provider."""
        result = self.conn.execute(f"""
            SELECT 
                SUM(TOTAL_PAID) AS total_paid,
                SUM(TOTAL_CLAIMS) AS total_claims,
                SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries
            FROM spending
            WHERE BILLING_PROVIDER_NPI_NUM = '{npi}'
        """).fetchone()
        
        if result:
            return {
                "total_paid_all_time": float(result[0]) if result[0] else 0,
                "total_claims_all_time": int(result[1]) if result[1] else 0,
                "total_unique_beneficiaries_all_time": int(result[2]) if result[2] else 0,
            }
        return {
            "total_paid_all_time": 0,
            "total_claims_all_time": 0,
            "total_unique_beneficiaries_all_time": 0,
        }
    
    def generate_report(
        self, 
        signals_by_type: Dict[str, List[FraudSignal]],
        output_path: str
    ) -> Dict[str, Any]:
        """Generate the full fraud signals report."""
        
        # Get total providers scanned
        total_providers = self.conn.execute(
            "SELECT COUNT(DISTINCT BILLING_PROVIDER_NPI_NUM) FROM spending"
        ).fetchone()[0]
        
        # Aggregate signals by provider
        provider_signals: Dict[str, List[FraudSignal]] = {}
        for signal_type, signals in signals_by_type.items():
            for signal in signals:
                if signal.npi not in provider_signals:
                    provider_signals[signal.npi] = []
                provider_signals[signal.npi].append(signal)
        
        logger.info(f"Building report for {len(provider_signals)} flagged providers...")
        
        # Batch fetch provider info and totals for efficiency
        # Get unique NPIs
        unique_npis = list(provider_signals.keys())
        logger.info(f"Batch fetching provider info for {len(unique_npis)} providers...")
        
        # Create temp table with flagged NPIs for efficient joins
        self.conn.execute("CREATE TEMP TABLE IF NOT EXISTS flagged_npis (npi VARCHAR PRIMARY KEY)")
        # Insert in batches
        batch_size = 10000
        for i in range(0, len(unique_npis), batch_size):
            batch = unique_npis[i:i+batch_size]
            values = ", ".join([f"('{npi}')" for npi in batch])
            self.conn.execute(f"INSERT OR IGNORE INTO flagged_npis VALUES {values}")
        
        # Batch fetch provider info
        provider_info_map = {}
        results = self.conn.execute("""
            SELECT 
                n.npi,
                COALESCE(n.org_name, n.last_name || ', ' || n.first_name) AS provider_name,
                CASE WHEN n.entity_type_code = '1' THEN 'individual' ELSE 'organization' END AS entity_type,
                n.taxonomy_code,
                n.state,
                n.enumeration_date
            FROM nppes n
            INNER JOIN flagged_npis f ON n.npi = f.npi
        """).fetchall()
        for row in results:
            provider_info_map[row[0]] = {
                "npi": row[0], "provider_name": row[1], "entity_type": row[2],
                "taxonomy_code": row[3], "state": row[4], "enumeration_date": row[5]
            }
        logger.info(f"Fetched info for {len(provider_info_map)} providers")
        
        # Batch fetch provider totals
        provider_totals_map = {}
        results = self.conn.execute("""
            SELECT 
                s.BILLING_PROVIDER_NPI_NUM,
                SUM(s.TOTAL_PAID),
                SUM(s.TOTAL_CLAIMS),
                SUM(s.TOTAL_UNIQUE_BENEFICIARIES)
            FROM spending s
            INNER JOIN flagged_npis f ON s.BILLING_PROVIDER_NPI_NUM = f.npi
            GROUP BY s.BILLING_PROVIDER_NPI_NUM
        """).fetchall()
        for row in results:
            provider_totals_map[row[0]] = {
                "total_paid_all_time": float(row[1]) if row[1] else 0,
                "total_claims_all_time": int(row[2]) if row[2] else 0,
                "total_unique_beneficiaries_all_time": int(row[3]) if row[3] else 0
            }
        logger.info(f"Fetched totals for {len(provider_totals_map)} providers")
        
        # Cleanup temp table
        self.conn.execute("DROP TABLE IF EXISTS flagged_npis")
        
        # Build flagged providers list
        flagged_providers = []
        for npi, signals in provider_signals.items():
            provider_info = provider_info_map.get(npi, {
                "npi": npi, "provider_name": "Unknown", "entity_type": "unknown",
                "taxonomy_code": None, "state": None, "enumeration_date": None
            })
            provider_totals = provider_totals_map.get(npi, {
                "total_paid_all_time": 0, "total_claims_all_time": 0,
                "total_unique_beneficiaries_all_time": 0
            })
            
            # Calculate total estimated overpayment
            total_overpayment = sum(s.estimated_overpayment for s in signals)
            
            # Build signals list
            signal_entries = []
            for signal in signals:
                next_steps = generate_next_steps(signal, provider_info)
                
                signal_entries.append({
                    "signal_type": signal.signal_type,
                    "severity": signal.severity,
                    "evidence": signal.evidence,
                })
            
            # Determine highest severity
            severities = [s.severity for s in signals]
            if "critical" in severities:
                highest_severity = "critical"
            elif "high" in severities:
                highest_severity = "high"
            else:
                highest_severity = "medium"
            
            # Get primary signal for FCA reference
            primary_signal = signals[0]
            
            flagged_providers.append({
                "npi": npi,
                "provider_name": provider_info.get("provider_name", "Unknown"),
                "entity_type": provider_info.get("entity_type", "unknown"),
                "taxonomy_code": provider_info.get("taxonomy_code"),
                "state": provider_info.get("state"),
                "enumeration_date": provider_info.get("enumeration_date"),
                "total_paid_all_time": provider_totals["total_paid_all_time"],
                "total_claims_all_time": provider_totals["total_claims_all_time"],
                "total_unique_beneficiaries_all_time": provider_totals["total_unique_beneficiaries_all_time"],
                "signals": signal_entries,
                "estimated_overpayment_usd": total_overpayment,
                "fca_relevance": {
                    "claim_type": CLAIM_TYPE_MAPPING.get(
                        primary_signal.signal_type, 
                        "Potential false claims violation"
                    ),
                    "statute_reference": STATUTE_MAPPING.get(
                        primary_signal.signal_type,
                        "31 U.S.C. § 3729(a)(1)(A)"
                    ),
                    "suggested_next_steps": generate_next_steps(primary_signal, provider_info),
                }
            })
        
        # Sort by estimated overpayment descending
        flagged_providers.sort(key=lambda x: x["estimated_overpayment_usd"], reverse=True)
        
        # Build signal counts
        signal_counts = {
            "excluded_provider": len(signals_by_type.get("excluded_provider", [])),
            "billing_outlier": len(signals_by_type.get("billing_outlier", [])),
            "rapid_escalation": len(signals_by_type.get("rapid_escalation", [])),
            "workforce_impossibility": len(signals_by_type.get("workforce_impossibility", [])),
            "shared_official": len(signals_by_type.get("shared_official", [])),
            "geographic_implausibility": len(signals_by_type.get("geographic_implausibility", [])),
        }
        
        # Build final report
        report = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "tool_version": TOOL_VERSION,
            "total_providers_scanned": total_providers,
            "total_providers_flagged": len(flagged_providers),
            "signal_counts": signal_counts,
            "flagged_providers": flagged_providers,
        }
        
        # Write to file
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Report written to {output_path}")
        logger.info(f"Total providers scanned: {total_providers:,}")
        logger.info(f"Total providers flagged: {len(flagged_providers):,}")
        
        return report
    
    def write_report(self, report: Dict[str, Any], output_path: str) -> None:
        """Write or re-write report to file (e.g., to add execution metrics)."""
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"Report updated with execution metrics: {output_path}")
