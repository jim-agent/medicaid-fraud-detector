"""
Fraud signal detection implementations.

Each signal has a precise definition from the competition spec.
"""

import duckdb
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import date
import logging

logger = logging.getLogger(__name__)


@dataclass
class FraudSignal:
    """Represents a detected fraud signal for a provider."""
    npi: str
    signal_type: str
    severity: str  # critical, high, medium
    evidence: Dict[str, Any]
    estimated_overpayment: float


class SignalDetector:
    """Detects all 6 fraud signals from the data."""
    
    # Home health HCPCS codes for Signal 6
    HOME_HEALTH_CODES = [
        # G0151-G0162
        'G0151', 'G0152', 'G0153', 'G0154', 'G0155', 'G0156', 
        'G0157', 'G0158', 'G0159', 'G0160', 'G0161', 'G0162',
        # G0299-G0300
        'G0299', 'G0300',
        # S9122-S9124
        'S9122', 'S9123', 'S9124',
        # T1019-T1022
        'T1019', 'T1020', 'T1021', 'T1022'
    ]
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        
    def detect_signal_1_excluded_provider(self) -> List[FraudSignal]:
        """
        Signal 1: Excluded Provider Still Billing
        
        Definition: A BILLING_PROVIDER_NPI_NUM or SERVICING_PROVIDER_NPI_NUM 
        matches an NPI in LEIE where EXCLDATE < CLAIM_FROM_MONTH and 
        REINDATE is empty or > CLAIM_FROM_MONTH.
        """
        logger.info("Detecting Signal 1: Excluded Provider Still Billing")
        
        results = self.conn.execute("""
            WITH excluded_billing AS (
                -- Check billing NPIs
                SELECT DISTINCT
                    s.BILLING_PROVIDER_NPI_NUM AS npi,
                    l.EXCLDATE,
                    l.EXCLTYPE,
                    l.REINDATE,
                    MIN(s.CLAIM_FROM_MONTH) AS first_post_exclusion_month,
                    SUM(s.TOTAL_PAID) AS total_paid_after_exclusion
                FROM spending s
                JOIN leie l ON s.BILLING_PROVIDER_NPI_NUM = l.NPI
                WHERE l.NPI IS NOT NULL 
                    AND l.NPI != ''
                    AND l.EXCLDATE IS NOT NULL
                    AND CAST(s.CLAIM_FROM_MONTH || '-01' AS DATE) >= l.EXCLDATE
                    AND (l.REINDATE IS NULL OR CAST(s.CLAIM_FROM_MONTH || '-01' AS DATE) < l.REINDATE)
                GROUP BY s.BILLING_PROVIDER_NPI_NUM, l.EXCLDATE, l.EXCLTYPE, l.REINDATE
                
                UNION
                
                -- Check servicing NPIs
                SELECT DISTINCT
                    s.SERVICING_PROVIDER_NPI_NUM AS npi,
                    l.EXCLDATE,
                    l.EXCLTYPE,
                    l.REINDATE,
                    MIN(s.CLAIM_FROM_MONTH) AS first_post_exclusion_month,
                    SUM(s.TOTAL_PAID) AS total_paid_after_exclusion
                FROM spending s
                JOIN leie l ON s.SERVICING_PROVIDER_NPI_NUM = l.NPI
                WHERE l.NPI IS NOT NULL 
                    AND l.NPI != ''
                    AND l.EXCLDATE IS NOT NULL
                    AND CAST(s.CLAIM_FROM_MONTH || '-01' AS DATE) >= l.EXCLDATE
                    AND (l.REINDATE IS NULL OR CAST(s.CLAIM_FROM_MONTH || '-01' AS DATE) < l.REINDATE)
                GROUP BY s.SERVICING_PROVIDER_NPI_NUM, l.EXCLDATE, l.EXCLTYPE, l.REINDATE
            )
            SELECT * FROM excluded_billing
            ORDER BY total_paid_after_exclusion DESC
        """).fetchall()
        
        signals = []
        for row in results:
            npi, excl_date, excl_type, rein_date, first_month, total_paid = row
            
            signals.append(FraudSignal(
                npi=npi,
                signal_type="excluded_provider",
                severity="critical",  # Always critical per spec
                evidence={
                    "exclusion_date": str(excl_date) if excl_date else None,
                    "exclusion_type": excl_type,
                    "reinstatement_date": str(rein_date) if rein_date else None,
                    "first_post_exclusion_billing": str(first_month) if first_month else None,
                    "total_paid_after_exclusion": float(total_paid) if total_paid else 0
                },
                estimated_overpayment=float(total_paid) if total_paid else 0
            ))
        
        logger.info(f"Signal 1: Found {len(signals)} excluded providers still billing")
        return signals
    
    def detect_signal_2_billing_outlier(self) -> List[FraudSignal]:
        """
        Signal 2: Billing Volume Outlier
        
        Definition: Provider's total TOTAL_PAID is above 99th percentile
        of their taxonomy+state peer group.
        """
        logger.info("Detecting Signal 2: Billing Volume Outlier")
        
        results = self.conn.execute("""
            WITH provider_totals AS (
                SELECT 
                    BILLING_PROVIDER_NPI_NUM AS npi,
                    SUM(TOTAL_PAID) AS total_paid
                FROM spending
                GROUP BY BILLING_PROVIDER_NPI_NUM
            ),
            provider_with_taxonomy AS (
                SELECT 
                    pt.npi,
                    pt.total_paid,
                    COALESCE(n.taxonomy_code, 'UNKNOWN') AS taxonomy_code,
                    COALESCE(n.state, 'UNKNOWN') AS state
                FROM provider_totals pt
                LEFT JOIN nppes n ON pt.npi = n.npi
            ),
            peer_stats AS (
                SELECT 
                    taxonomy_code,
                    state,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_paid) AS peer_median,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_paid) AS peer_99th
                FROM provider_with_taxonomy
                GROUP BY taxonomy_code, state
                HAVING COUNT(*) >= 10  -- Only meaningful peer groups
            )
            SELECT 
                p.npi,
                p.total_paid,
                p.taxonomy_code,
                p.state,
                ps.peer_median,
                ps.peer_99th,
                p.total_paid / NULLIF(ps.peer_median, 0) AS ratio_to_median
            FROM provider_with_taxonomy p
            JOIN peer_stats ps ON p.taxonomy_code = ps.taxonomy_code AND p.state = ps.state
            WHERE p.total_paid > ps.peer_99th
            ORDER BY p.total_paid DESC
        """).fetchall()
        
        signals = []
        for row in results:
            npi, total, taxonomy, state, median, p99, ratio = row
            
            # Severity: high if ratio > 5x, else medium
            severity = "high" if ratio and ratio > 5 else "medium"
            
            # Estimated overpayment: (provider_total - peer_99th), floored at 0
            overpayment = max(0, float(total) - float(p99)) if total and p99 else 0
            
            signals.append(FraudSignal(
                npi=npi,
                signal_type="billing_outlier",
                severity=severity,
                evidence={
                    "total_paid": float(total) if total else 0,
                    "taxonomy_code": taxonomy,
                    "state": state,
                    "peer_group_median": float(median) if median else 0,
                    "peer_group_99th_percentile": float(p99) if p99 else 0,
                    "ratio_to_peer_median": float(ratio) if ratio else 0
                },
                estimated_overpayment=overpayment
            ))
        
        logger.info(f"Signal 2: Found {len(signals)} billing outliers")
        return signals
    
    def detect_signal_3_rapid_escalation(self) -> List[FraudSignal]:
        """
        Signal 3: Rapid Billing Escalation
        
        Definition: Providers showing extreme month-over-month billing increases
        (>500% growth from a baseline of at least $1000), indicating potential 
        billing fraud, upcoding, or phantom billing schemes.
        """
        logger.info("Detecting Signal 3: Rapid Billing Escalation")
        
        results = self.conn.execute("""
            WITH provider_monthly AS (
                SELECT 
                    BILLING_PROVIDER_NPI_NUM AS npi,
                    CLAIM_FROM_MONTH,
                    SUM(TOTAL_PAID) AS monthly_paid
                FROM spending
                GROUP BY BILLING_PROVIDER_NPI_NUM, CLAIM_FROM_MONTH
            ),
            with_growth AS (
                SELECT 
                    npi,
                    CLAIM_FROM_MONTH,
                    monthly_paid,
                    LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) AS prev_paid,
                    CASE 
                        WHEN LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) >= 1000
                        THEN (monthly_paid - LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH)) 
                            / LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) * 100
                        ELSE NULL
                    END AS growth_pct
                FROM provider_monthly
            ),
            flagged AS (
                SELECT 
                    npi,
                    CLAIM_FROM_MONTH AS escalation_month,
                    prev_paid AS before_amount,
                    monthly_paid AS after_amount,
                    growth_pct AS peak_growth
                FROM with_growth
                WHERE growth_pct > 500
            ),
            provider_totals AS (
                SELECT 
                    f.npi,
                    f.escalation_month,
                    f.before_amount,
                    f.after_amount,
                    f.peak_growth,
                    SUM(pm.monthly_paid) AS total_billing
                FROM flagged f
                JOIN provider_monthly pm ON f.npi = pm.npi
                GROUP BY f.npi, f.escalation_month, f.before_amount, f.after_amount, f.peak_growth
            )
            SELECT 
                npi,
                escalation_month,
                before_amount,
                after_amount,
                peak_growth,
                total_billing
            FROM provider_totals
            ORDER BY peak_growth DESC

        """).fetchall()
        
        signals = []
        for row in results:
            npi, escalation_month, before_amount, after_amount, peak_growth, total_billing = row
            
            # Severity: high if growth > 1000%, else medium
            severity = "high" if peak_growth and peak_growth > 1000 else "medium"
            
            # Estimated overpayment: the spike amount (after - before)
            overpayment = float(after_amount - before_amount) if after_amount and before_amount else 0
            
            signals.append(FraudSignal(
                npi=npi,
                signal_type="rapid_escalation",
                severity=severity,
                evidence={
                    "escalation_month": str(escalation_month) if escalation_month else None,
                    "before_amount": float(before_amount) if before_amount else 0,
                    "after_amount": float(after_amount) if after_amount else 0,
                    "growth_rate_pct": float(peak_growth) if peak_growth else 0,
                    "total_provider_billing": float(total_billing) if total_billing else 0
                },
                estimated_overpayment=overpayment
            ))
        
        logger.info(f"Signal 3: Found {len(signals)} rapid escalation cases")
        return signals
    
    def detect_signal_4_workforce_impossibility(self) -> List[FraudSignal]:
        """
        Signal 4: Workforce Impossibility
        
        Definition: For organizations (Entity Type = 2), if max monthly claims
        implies > 6 claims per hour (claims / 22 days / 8 hours > 6).
        
        Optimized: Use temp tables and staged aggregation.
        """
        logger.info("Detecting Signal 4: Workforce Impossibility")
        
        # Step 1: Get organization NPIs (entity type 2)
        self.conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS org_npis AS
            SELECT DISTINCT npi FROM nppes WHERE entity_type_code = '2'
        """)
        
        # Step 2: Aggregate monthly claims only for orgs, filtering early
        # Threshold: 6 claims/hr * 8 hrs * 22 days = 1056 claims/month
        self.conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS monthly_claims AS
            SELECT 
                s.BILLING_PROVIDER_NPI_NUM AS npi,
                s.CLAIM_FROM_MONTH,
                SUM(s.TOTAL_CLAIMS) AS month_claims,
                SUM(s.TOTAL_PAID) AS month_paid
            FROM spending s
            INNER JOIN org_npis o ON s.BILLING_PROVIDER_NPI_NUM = o.npi
            GROUP BY s.BILLING_PROVIDER_NPI_NUM, s.CLAIM_FROM_MONTH
            HAVING SUM(s.TOTAL_CLAIMS) > 1056
        """)
        
        # Step 3: Find max month per NPI and get results
        results = self.conn.execute("""
            WITH max_per_npi AS (
                SELECT npi, MAX(month_claims) AS max_claims
                FROM monthly_claims
                GROUP BY npi
            )
            SELECT 
                mc.npi,
                mc.CLAIM_FROM_MONTH AS peak_month,
                mc.month_claims AS peak_claims,
                mc.month_paid AS peak_paid,
                mc.month_claims / 22.0 / 8.0 AS claims_per_hour
            FROM monthly_claims mc
            JOIN max_per_npi m ON mc.npi = m.npi AND mc.month_claims = m.max_claims
            ORDER BY claims_per_hour DESC

        """).fetchall()
        
        # Cleanup
        self.conn.execute("DROP TABLE IF EXISTS org_npis")
        self.conn.execute("DROP TABLE IF EXISTS monthly_claims")
        
        signals = []
        for row in results:
            npi, peak_month, peak_claims, peak_paid, claims_per_hour = row
            
            # Estimated overpayment: (peak_claims - threshold) * avg_claim_value
            # threshold = 6 * 8 * 22 = 1056 claims
            threshold = 6 * 8 * 22
            excess_claims = max(0, int(peak_claims) - threshold)
            avg_claim_value = float(peak_paid) / float(peak_claims) if peak_claims else 0
            overpayment = excess_claims * avg_claim_value
            
            signals.append(FraudSignal(
                npi=npi,
                signal_type="workforce_impossibility",
                severity="high",  # Always high per spec
                evidence={
                    "peak_month": str(peak_month) if peak_month else None,
                    "peak_claims_count": int(peak_claims) if peak_claims else 0,
                    "implied_claims_per_hour": float(claims_per_hour) if claims_per_hour else 0,
                    "total_paid_peak_month": float(peak_paid) if peak_paid else 0
                },
                estimated_overpayment=overpayment
            ))
        
        logger.info(f"Signal 4: Found {len(signals)} workforce impossibility cases")
        return signals
    
    def detect_signal_5_shared_official(self) -> List[FraudSignal]:
        """
        Signal 5: Shared Authorized Official Across Multiple NPIs
        
        Definition: Same authorized official controls 5+ NPIs with 
        combined total > $1,000,000.
        
        Optimized: Avoid UNNEST/ARRAY operations by using simple JOINs.
        """
        logger.info("Detecting Signal 5: Shared Authorized Official")
        
        # Step 1: Pre-aggregate NPI totals to reduce data size
        self.conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS npi_totals AS
            SELECT 
                BILLING_PROVIDER_NPI_NUM AS npi,
                SUM(TOTAL_PAID) AS total_paid
            FROM spending
            GROUP BY BILLING_PROVIDER_NPI_NUM
        """)
        
        # Step 2: Create official key for each NPI
        self.conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS official_npis AS
            SELECT 
                UPPER(TRIM(COALESCE(CAST(auth_official_last AS VARCHAR), ''))) || '|' || 
                UPPER(TRIM(COALESCE(CAST(auth_official_first AS VARCHAR), ''))) AS official_key,
                auth_official_last,
                auth_official_first,
                npi
            FROM nppes
            WHERE auth_official_last IS NOT NULL 
                AND TRIM(CAST(auth_official_last AS VARCHAR)) != ''
                AND auth_official_first IS NOT NULL
                AND TRIM(CAST(auth_official_first AS VARCHAR)) != ''
        """)
        
        # Step 3: Join with spending and aggregate by official
        results = self.conn.execute("""
            SELECT 
                o.official_key,
                o.auth_official_last,
                o.auth_official_first,
                COUNT(DISTINCT o.npi) AS npi_count,
                STRING_AGG(DISTINCT o.npi, ',') AS npi_list_str,
                SUM(COALESCE(nt.total_paid, 0)) AS combined_total
            FROM official_npis o
            LEFT JOIN npi_totals nt ON o.npi = nt.npi
            GROUP BY o.official_key, o.auth_official_last, o.auth_official_first
            HAVING COUNT(DISTINCT o.npi) >= 5
                AND SUM(COALESCE(nt.total_paid, 0)) > 1000000
            ORDER BY combined_total DESC

        """).fetchall()
        
        # Cleanup temp tables
        self.conn.execute("DROP TABLE IF EXISTS npi_totals")
        self.conn.execute("DROP TABLE IF EXISTS official_npis")
        
        signals = []
        for row in results:
            official_key, last_name, first_name, npi_count, npi_list_str, combined = row
            
            # Parse NPI list from comma-separated string
            npi_list = npi_list_str.split(',') if npi_list_str else []
            
            # Severity: high if combined > $5M, else medium
            severity = "high" if combined and combined > 5000000 else "medium"
            
            signals.append(FraudSignal(
                npi=npi_list[0] if npi_list else "",  # Primary NPI for the signal
                signal_type="shared_official",
                severity=severity,
                evidence={
                    "authorized_official_name": f"{first_name} {last_name}",
                    "controlled_npi_count": int(npi_count) if npi_count else 0,
                    "controlled_npis": npi_list[:10],  # Limit to first 10 for output
                    "combined_total_paid": float(combined) if combined else 0
                },
                estimated_overpayment=0  # Not estimated per spec
            ))
        
        logger.info(f"Signal 5: Found {len(signals)} shared official cases")
        return signals
    
    def detect_signal_6_geographic_implausibility(self) -> List[FraudSignal]:
        """
        Signal 6: Geographic Implausibility
        
        Definition: Home health providers with >100 claims/month and
        beneficiaries/claims ratio < 0.1.
        """
        logger.info("Detecting Signal 6: Geographic Implausibility")
        
        # Build the HCPCS code list for SQL
        codes = "','".join(self.HOME_HEALTH_CODES)
        
        results = self.conn.execute(f"""
            WITH home_health_billing AS (
                SELECT 
                    s.BILLING_PROVIDER_NPI_NUM AS npi,
                    n.state,
                    s.HCPCS_CODE,
                    s.CLAIM_FROM_MONTH,
                    SUM(s.TOTAL_CLAIMS) AS monthly_claims,
                    SUM(s.TOTAL_UNIQUE_BENEFICIARIES) AS monthly_beneficiaries,
                    SUM(s.TOTAL_UNIQUE_BENEFICIARIES) * 1.0 / NULLIF(SUM(s.TOTAL_CLAIMS), 0) AS ratio
                FROM spending s
                JOIN nppes n ON s.BILLING_PROVIDER_NPI_NUM = n.npi
                WHERE s.HCPCS_CODE IN ('{codes}')
                GROUP BY s.BILLING_PROVIDER_NPI_NUM, n.state, s.HCPCS_CODE, s.CLAIM_FROM_MONTH
                HAVING SUM(s.TOTAL_CLAIMS) > 100
            )
            SELECT 
                npi,
                state,
                ARRAY_AGG(DISTINCT HCPCS_CODE) AS flagged_codes,
                CLAIM_FROM_MONTH AS flagged_month,
                monthly_claims,
                monthly_beneficiaries,
                ratio
            FROM home_health_billing
            WHERE ratio < 0.1
            GROUP BY npi, state, CLAIM_FROM_MONTH, monthly_claims, monthly_beneficiaries, ratio
            ORDER BY ratio ASC
        """).fetchall()
        
        signals = []
        for row in results:
            npi, state, codes, month, claims, beneficiaries, ratio = row
            
            signals.append(FraudSignal(
                npi=npi,
                signal_type="geographic_implausibility",
                severity="medium",  # Per spec
                evidence={
                    "state": state,
                    "flagged_hcpcs_codes": list(codes) if codes else [],
                    "flagged_month": str(month) if month else None,
                    "claims_count": int(claims) if claims else 0,
                    "unique_beneficiaries": int(beneficiaries) if beneficiaries else 0,
                    "beneficiary_to_claims_ratio": float(ratio) if ratio else 0
                },
                estimated_overpayment=0  # Not estimated per spec
            ))
        
        logger.info(f"Signal 6: Found {len(signals)} geographic implausibility cases")
        return signals
    
    def detect_all_signals(self) -> Dict[str, List[FraudSignal]]:
        """Run all signal detections and return results."""
        return {
            "excluded_provider": self.detect_signal_1_excluded_provider(),
            "billing_outlier": self.detect_signal_2_billing_outlier(),
            "rapid_escalation": self.detect_signal_3_rapid_escalation(),
            "workforce_impossibility": self.detect_signal_4_workforce_impossibility(),
            "shared_official": self.detect_signal_5_shared_official(),
            "geographic_implausibility": self.detect_signal_6_geographic_implausibility(),
        }
