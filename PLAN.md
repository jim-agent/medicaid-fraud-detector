# Medicaid Fraud Signal Detection Engine - GSD Plan

## Competition Details
- **Job ID:** 24a94492-f7eb-4adc-ae01-632021f42165
- **Prize:** 1000 NEAR
- **Deadline:** Feb 27, 2026
- **Repo:** https://github.com/jim-agent/medicaid-fraud-detector

## Progress Tracker

### Phase 1: Data Understanding & Architecture ⏳
- [ ] Download all 3 datasets
- [ ] Verify schemas
- [ ] Choose tech stack (DuckDB + Polars)
- [ ] Document architecture

### Phase 2: Core Implementation
- [ ] src/ingest.py - Data loading & joining
- [ ] src/signals.py - All 6 signal implementations
- [ ] src/output.py - JSON report generation

### Phase 3: Testing & Fixtures
- [ ] tests/fixtures/ - Synthetic test data
- [ ] tests/test_signals.py - 6+ unit tests

### Phase 4: Cross-Platform & Scripts
- [ ] setup.sh
- [ ] run.sh
- [ ] requirements.txt

### Phase 5: Full Data Run
- [ ] Run on 2.9GB dataset
- [ ] Generate fraud_signals.json

### Phase 6: Documentation
- [ ] README.md

### Phase 7: QA
- [ ] All 100pt checklist items

### Phase 8: Submission
- [ ] Aaron reviews code
- [ ] Submit to competition

## Signal Definitions (Reference)

### Signal 1: Excluded Provider Still Billing
- Match NPI against LEIE
- EXCLDATE < CLAIM_FROM_MONTH AND (REINDATE empty OR > CLAIM_FROM_MONTH)
- Output: NPI, exclusion_date, exclusion_type, post_exclusion_paid

### Signal 2: Billing Volume Outlier
- Aggregate TOTAL_PAID by NPI
- Group by taxonomy+state
- Flag: > 99th percentile
- Output: NPI, total, peer_median, peer_99th, ratio

### Signal 3: Rapid Billing Escalation
- New entities (enumerated within 24 months)
- 3-month rolling avg growth > 200%
- Output: NPI, enum_date, monthly_paid[], peak_growth

### Signal 4: Workforce Impossibility
- Organizations only (Entity Type = 2)
- claims_per_hour = max_monthly_claims / 22 / 8
- Flag: > 6 claims/hour
- Output: NPI, peak_month, peak_claims, implied_rate

### Signal 5: Shared Authorized Official
- Group NPIs by (Last Name, First Name)
- Flag: 5+ NPIs AND combined > $1M
- Output: official_name, npi_list[], combined_total

### Signal 6: Geographic Implausibility
- Home health HCPCS: G0151-G0162, G0299-G0300, S9122-S9124, T1019-T1022
- Claims > 100/month
- Flag: beneficiaries/claims ratio < 0.1
- Output: NPI, hcpcs_codes[], ratio

## Data Sources
1. HHS Medicaid Spending: https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet
2. OIG LEIE: https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv
3. NPPES: https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip
