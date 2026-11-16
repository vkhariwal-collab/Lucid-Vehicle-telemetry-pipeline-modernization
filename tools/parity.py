#!/usr/bin/env python3
"""Compare candidate window aggregates against the legacy Spark golden.

Checks: same set of (vin, window) keys, exact match on ids/counts/flags, floats within
relative tolerance 1e-9, no duplicate windows, and per-VIN ordering in candidate file order.
Usage: parity.py GOLDEN CANDIDATE [--report out.md] [--title T]
"""
import argparse, json, sys
from collections import Counter

EXACT = ["vin", "window_start_ms", "window_end_ms", "events", "thermal_alert"]
FLOAT = ["avg_cell_temp_c", "max_cell_temp_c", "min_soc_pct", "max_power_kw", "energy_kwh", "last_lat", "last_lon"]

def close(a, b):
    if a is None or b is None:
        return a is b  # Spark omits aggregates over only-null inputs
    return abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1.0) and str(a).startswith("-") == str(b).startswith("-")

def load(p):
    with open(p) as f:
        return [json.loads(l) for l in f if l.strip()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("golden"); ap.add_argument("candidate")
    ap.add_argument("--report"); ap.add_argument("--title", default="Parity report")
    a = ap.parse_args()
    g, c = load(a.golden), load(a.candidate)
    key = lambda r: (r["vin"], r["window_start_ms"])
    counts = Counter(key(r) for r in c)
    dups = sum(n - 1 for n in counts.values() if n > 1)
    gm, cm = {key(r): r for r in g}, {key(r): r for r in c}
    missing, extra = sorted(set(gm) - set(cm)), sorted(set(cm) - set(gm))
    diffs = []
    for k in sorted(set(gm) & set(cm)):
        bad = [f for f in EXACT if gm[k].get(f) != cm[k].get(f)] + [f for f in FLOAT if not close(gm[k].get(f), cm[k].get(f))]
        if bad:
            diffs.append((k, bad))
    last, order_violations = {}, 0
    for r in c:
        if r["window_start_ms"] <= last.get(r["vin"], -1):
            order_violations += 1
        last[r["vin"]] = r["window_start_ms"]
    matched = len(gm) - len(missing) - len(diffs)
    ok = not (missing or extra or diffs or dups or order_violations)
    lines = [f"# {a.title}", "", f"- Golden windows (legacy Spark): {len(g)}", f"- Candidate records: {len(c)}",
             f"- **Matching windows: {matched}/{len(g)}**", f"- Missing: {len(missing)} · Extra: {len(extra)} · Field mismatches: {len(diffs)}",
             f"- Duplicate windows: {dups} · Per-VIN ordering violations: {order_violations}",
             f"- Float tolerance: relative 1e-9 (sign must match, so -0.0 != 0.0; absent == null)", "", f"**Result: {'PASS' if ok else 'FAIL'}**"]
    for k, bad in diffs[:5]:
        lines.append(f"- mismatch {k[0][-6:]}@{k[1]}: {', '.join(bad)}")
    out = "\n".join(lines) + "\n"
    if a.report:
        open(a.report, "w").write(out)
    print(out)
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
