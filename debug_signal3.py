#!/usr/bin/env python3
"""Debug Signal 3 - focus on growth rates."""

import duckdb

conn = duckdb.connect()
conn.execute("SET memory_limit='1GB'")
conn.execute("SET threads=1")
conn.execute("SET temp_directory='/home/deploy/medicaid-fraud-detector/temp'")
conn.execute("SET max_temp_directory_size='4GB'")

print("=== CLAIM_FROM_MONTH range ===")
r = conn.execute("""
    SELECT MIN(CLAIM_FROM_MONTH), MAX(CLAIM_FROM_MONTH)
    FROM read_parquet('data/medicaid-provider-spending.parquet')
""").fetchone()
print(f"  Range: {r[0]} to {r[1]}")

print("\n=== Checking growth rates (sampled 10k providers) ===")
r = conn.execute("""
    WITH sample_providers AS (
        SELECT DISTINCT BILLING_PROVIDER_NPI_NUM as npi
        FROM read_parquet('data/medicaid-provider-spending.parquet')
        USING SAMPLE 10000
    ),
    provider_monthly AS (
        SELECT 
            s.BILLING_PROVIDER_NPI_NUM AS npi,
            s.CLAIM_FROM_MONTH,
            SUM(s.TOTAL_PAID) AS monthly_paid
        FROM read_parquet('data/medicaid-provider-spending.parquet') s
        WHERE s.BILLING_PROVIDER_NPI_NUM IN (SELECT npi FROM sample_providers)
        GROUP BY s.BILLING_PROVIDER_NPI_NUM, s.CLAIM_FROM_MONTH
    ),
    with_growth AS (
        SELECT 
            npi,
            CLAIM_FROM_MONTH,
            monthly_paid,
            LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) AS prev_paid,
            CASE 
                WHEN LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) > 100
                THEN (monthly_paid - LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH)) 
                    / LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) * 100
                ELSE NULL
            END AS growth_pct
        FROM provider_monthly
    )
    SELECT 
        COUNT(*) FILTER (WHERE growth_pct > 200) as over_200pct,
        COUNT(*) FILTER (WHERE growth_pct > 100) as over_100pct,
        COUNT(*) FILTER (WHERE growth_pct > 50) as over_50pct,
        COUNT(*) as total_months_with_growth,
        MAX(growth_pct) as max_growth
    FROM with_growth
    WHERE growth_pct IS NOT NULL
""").fetchone()
print(f"  Total months with calculable growth: {r[3]:,}")
print(f"  >200% growth events: {r[0]:,}")
print(f"  >100% growth events: {r[1]:,}")
print(f"  >50% growth events: {r[2]:,}")
print(f"  Max growth: {r[4]:,.0f}%" if r[4] else "  Max growth: N/A")

print("\n=== Top 5 growth events ===")
r = conn.execute("""
    WITH sample_providers AS (
        SELECT DISTINCT BILLING_PROVIDER_NPI_NUM as npi
        FROM read_parquet('data/medicaid-provider-spending.parquet')
        USING SAMPLE 10000
    ),
    provider_monthly AS (
        SELECT 
            s.BILLING_PROVIDER_NPI_NUM AS npi,
            s.CLAIM_FROM_MONTH,
            SUM(s.TOTAL_PAID) AS monthly_paid
        FROM read_parquet('data/medicaid-provider-spending.parquet') s
        WHERE s.BILLING_PROVIDER_NPI_NUM IN (SELECT npi FROM sample_providers)
        GROUP BY s.BILLING_PROVIDER_NPI_NUM, s.CLAIM_FROM_MONTH
    ),
    with_growth AS (
        SELECT 
            npi,
            CLAIM_FROM_MONTH,
            monthly_paid,
            LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) AS prev_paid,
            CASE 
                WHEN LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) > 100
                THEN (monthly_paid - LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH)) 
                    / LAG(monthly_paid) OVER (PARTITION BY npi ORDER BY CLAIM_FROM_MONTH) * 100
                ELSE NULL
            END AS growth_pct
        FROM provider_monthly
    )
    SELECT npi, CLAIM_FROM_MONTH, prev_paid, monthly_paid, growth_pct
    FROM with_growth
    WHERE growth_pct IS NOT NULL
    ORDER BY growth_pct DESC
    LIMIT 5
""").fetchall()
for row in r:
    print(f"  NPI {row[0]}: {row[1]} - ${row[2]:,.0f} -> ${row[3]:,.0f} = {row[4]:,.0f}% growth")

print("\n=== Done ===")
