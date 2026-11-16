# Offline parity on Spark-semantics edge cases (nulls, wrong types, cross-window dedup, -0.0)

- Golden windows (legacy Spark): 7
- Candidate records: 7
- **Matching windows: 7/7**
- Missing: 0 · Extra: 0 · Field mismatches: 0
- Duplicate windows: 0 · Per-VIN ordering violations: 0
- Float tolerance: relative 1e-9 (sign must match, so -0.0 != 0.0; absent == null)

**Result: PASS**
