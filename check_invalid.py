#!/usr/bin/env python3
import json

with open("fraud_signals.json") as f:
    data = json.load(f)

invalid = 0
invalid_examples = []
for p in data["flagged_providers"]:
    npi = p.get("npi", "")
    if not npi or len(npi) != 10 or npi == "0000000000" or not npi.isdigit():
        invalid += 1
        if len(invalid_examples) < 5:
            invalid_examples.append(npi)
        
print(f"Invalid NPIs: {invalid} / {len(data['flagged_providers'])}")
print(f"Examples: {invalid_examples}")
