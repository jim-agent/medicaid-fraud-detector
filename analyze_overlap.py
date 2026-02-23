#!/usr/bin/env python3
"""Analyze provider overlap across signals"""
import json
from collections import Counter

with open("fraud_signals.json", "r") as f:
    data = json.load(f)

print("=== Output Structure ===")
print(f"Total scanned: {data.get('total_providers_scanned', 'N/A')}")
print(f"Total flagged: {data.get('total_providers_flagged', 'N/A')}")
print()
print("Signal counts:")
for sig, count in data.get("signal_counts", {}).items():
    print(f"  {sig}: {count:,}")

# Analyze overlap
provider_signals = {}
for provider in data.get("flagged_providers", []):
    npi = provider["npi"]
    signals = provider["signals"]
    provider_signals[npi] = signals

# Count by number of signals
signal_counts = Counter(len(signals) for signals in provider_signals.values())

print()
print("=== Provider Overlap Analysis ===")
print(f"Total unique providers: {len(provider_signals):,}")
print()
print("Providers by # of signals:")
for num_signals in sorted(signal_counts.keys(), reverse=True):
    count = signal_counts[num_signals]
    pct = count / len(provider_signals) * 100
    print(f"  {num_signals} signal(s): {count:,} providers ({pct:.1f}%)")

# High risk
print()
print("=== Highest Risk (3+ signals) ===")
high_risk = [(npi, sigs) for npi, sigs in provider_signals.items() if len(sigs) >= 3]
print(f"Count: {len(high_risk)}")
for npi, sigs in sorted(high_risk, key=lambda x: -len(x[1]))[:10]:
    sig_ids = [s["signal_id"] for s in sigs]
    print(f"  NPI {npi}: {sig_ids}")
