#!/usr/bin/env python3
"""Generate a deterministic, synthetic battery-telemetry replay (Kafka topic contents in ingest order).

Injects the real-world mess the legacy job has to cope with: resends (duplicates), late arrivals,
future-dated events (bad vehicle clock), sensor-fault sentinels, energy-counter resets, malformed lines.
All VINs and coordinates are fake.
"""
import json, random, sys

def vin(i):
    return "5L1SYN0TEL%07d" % i

def main(out_path, seed=7, vins=25, minutes=120):
    rng = random.Random(seed)
    t0 = 1_793_000_000_000  # 2026-10-26T... (ms); arbitrary fixed epoch
    recs = []
    eid = 0
    for v in range(vins):
        lat, lon = 37.30 + rng.random() * 0.6, -122.20 + rng.random() * 0.6
        temp, soc, energy = 24 + rng.random() * 8, 20 + rng.random() * 60, 0.0
        charging = rng.random() < 0.6
        t = t0 + rng.randint(0, 59_000)
        end = t0 + minutes * 60_000
        while t < end:
            eid += 1
            if charging:
                energy += rng.uniform(0.15, 0.45)
                soc = min(100.0, soc + rng.uniform(0.1, 0.4))
                power = rng.uniform(40, 190)
                temp += rng.uniform(-0.2, 0.5)
            else:
                soc = max(3.0, soc - rng.uniform(0.05, 0.3))
                power = rng.uniform(0, 12)
                temp += rng.uniform(-0.4, 0.2)
                lat += rng.uniform(-0.002, 0.002); lon += rng.uniform(-0.002, 0.002)
            if rng.random() < 0.01:          # new charging session: cumulative counter resets
                charging, energy = not charging, 0.0
            if v % 9 == 0 and rng.random() < 0.05:
                temp += 4.0                  # a few hot packs -> thermal alerts
            cell = round(temp, 1)
            if rng.random() < 0.004:
                cell = -128.0                # sensor fault sentinel
            ev_ts = t
            if rng.random() < 0.005:
                ev_ts = t + 40 * 60_000      # vehicle clock 40 min fast
            delay = rng.randint(200, 4_000)
            r = rng.random()
            if r < 0.015:
                delay = rng.randint(660_000, 1_800_000)   # offline, uploaded late (>10 min)
            elif r < 0.02:
                delay = 600_000                           # exactly at the lateness limit (kept)
            rec = {"event_id": "e%07d" % eid, "vin": vin(v), "event_ts_ms": ev_ts, "ingest_ts_ms": t + delay,
                   "cell_temp_c": cell, "soc_pct": round(soc, 2), "power_kw": round(power, 1),
                   "energy_kwh": round(energy, 3), "lat": round(lat, 6), "lon": round(lon, 6)}
            recs.append(rec)
            if rng.random() < 0.02:          # resend after reconnect: same event, later ingest
                dup = dict(rec); dup["ingest_ts_ms"] = rec["ingest_ts_ms"] + rng.randint(5_000, 60_000)
                recs.append(dup)
            t += rng.randint(25_000, 35_000)
    recs.sort(key=lambda r: (r["ingest_ts_ms"], r["event_id"]))
    lines = [json.dumps(r, separators=(",", ":")) for r in recs]
    # malformed payloads: truncated JSON and a record missing event_ts_ms
    lines.insert(len(lines) // 3, lines[len(lines) // 3][:40])
    bad = dict(recs[len(recs) // 2]); bad.pop("event_ts_ms"); bad["event_id"] = "e9999999"
    lines.insert(len(lines) // 2, json.dumps(bad, separators=(",", ":")))
    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {len(lines)} lines to {out_path}", file=sys.stderr)

if __name__ == "__main__":
    main(sys.argv[1])
