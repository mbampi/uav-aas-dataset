#!/usr/bin/env python3
"""End-to-end latency from injected sequence numbers.

replay.py injects a monotonically increasing counter into a carrier telemetry
field and logs it in dispatch_log.csv as `Seq`. The receiver
(opcua_server.py / mqtt_receiver.py) logs the value it observes as `Value_Raw`.
Because the counter rides through the connector unchanged, each arrival is
matched 1:1 to the dispatch that produced it, giving true per-message latency
without guessing from timestamps.

    latency = Arrival_Timestamp_Ms(seq) - Dispatch_Timestamp_Ms(seq)

Both timestamps must be produced on the SAME machine (run replay.py and the
receiver here on your computer) so there is no cross-host clock skew.

Usage:
    python3 latency-calculate.py dispatch_log.csv opcua_ros_log4841.csv
    python3 latency-calculate.py dispatch_log.csv mqtt_received_log.csv \
        --seq-node-suffix LocalPositionNED/X

If you change replay.py's --seq-msg/--seq-field, pass the matching carrier
property path via --seq-node-suffix (default matches LOCAL_POSITION_NED.x).
"""

import argparse
import csv
import math
import sys


def load_dispatch(path):
    """seq -> earliest dispatch timestamp (ms)."""
    out = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "Seq" not in reader.fieldnames:
            sys.exit(f"{path}: no 'Seq' column. Re-run replay.py without --no-seq "
                     f"(sequence injection is required for latency matching).")
        for row in reader:
            raw = (row.get("Seq") or "").strip()
            if raw == "":
                continue
            try:
                seq = int(float(raw))
                ts = float(row["Dispatch_Timestamp_Ms"])
            except (ValueError, KeyError, TypeError):
                continue
            if seq not in out or ts < out[seq]:
                out[seq] = ts
    return out


def load_arrivals(path, suffix):
    """seq -> earliest arrival timestamp (ms), for rows on the carrier node."""
    out = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            node = row.get("Node_ID") or ""
            if suffix and not node.endswith(suffix):
                continue
            raw = (row.get("Value_Raw") or "").strip()
            try:
                seq = int(round(float(raw)))
                ts = float(row["Arrival_Timestamp_Ms"])
            except (ValueError, KeyError, TypeError):
                continue
            if seq not in out or ts < out[seq]:
                out[seq] = ts
    return out


def percentile(sorted_vals, q):
    """Linear-interpolated percentile (numpy default method)."""
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = q / 100.0 * (len(sorted_vals) - 1)
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return sorted_vals[lo]
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (idx - lo)


def analyze_latency(dispatch_csv, arrival_csv, suffix):
    dispatch = load_dispatch(dispatch_csv)
    arrivals = load_arrivals(arrival_csv, suffix)

    latencies = []
    negatives = 0
    for seq, a_ts in arrivals.items():
        d_ts = dispatch.get(seq)
        if d_ts is None:
            continue
        lat = a_ts - d_ts
        if lat < 0:
            negatives += 1
            continue
        latencies.append(lat)
    latencies.sort()

    print(f"--- LATENCY METRICS ({arrival_csv}) ---")
    print(f"Carrier node suffix:     {suffix}")
    print(f"Dispatched (unique seq): {len(dispatch)}")
    print(f"Arrived    (unique seq): {len(arrivals)}")
    print(f"Matched samples:         {len(latencies)}")
    if dispatch:
        print(f"Delivery ratio:          {len(latencies) / len(dispatch) * 100:.1f}% "
              f"(matched / dispatched; < 100% is expected — the connector coalesces "
              f"per-property updates under load)")
    if negatives:
        print(f"Discarded (negative):    {negatives}  <-- both logs must be stamped on the "
              f"same machine (no cross-host clock skew)")
    if not latencies:
        print("No matched samples. Check: data actually flowed, the carrier field "
              "(--seq-node-suffix) matches replay's --seq-msg/--seq-field, and "
              "replay ran without --no-seq.")
        return

    mean = sum(latencies) / len(latencies)
    print(f"Average Latency:        {mean:.3f} ms")
    print(f"Median Latency (p50):   {percentile(latencies, 50):.3f} ms")
    print(f"90th Percentile (p90):  {percentile(latencies, 90):.3f} ms")
    print(f"95th Percentile (p95):  {percentile(latencies, 95):.3f} ms")
    print(f"99th Percentile (p99):  {percentile(latencies, 99):.3f} ms")
    print(f"Min Latency:            {latencies[0]:.3f} ms")
    print(f"Max Latency:            {latencies[-1]:.3f} ms")


def main():
    ap = argparse.ArgumentParser(description="End-to-end latency via injected sequence numbers")
    ap.add_argument("dispatch_csv", help="dispatch_log.csv from replay.py")
    ap.add_argument("arrival_csv", help="arrival log from opcua_server.py or mqtt_receiver.py")
    ap.add_argument("--seq-node-suffix", default="LocalPositionNED/X",
                    help="Only arrivals whose Node_ID ends with this carrier property "
                         "are matched (default: LocalPositionNED/X, matching "
                         "replay.py's default LOCAL_POSITION_NED.x)")
    args = ap.parse_args()
    analyze_latency(args.dispatch_csv, args.arrival_csv, args.seq_node_suffix)


if __name__ == "__main__":
    main()
