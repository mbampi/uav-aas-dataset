#!/usr/bin/env python3
"""Unprocessed/Dropped Traffic for the BaSyx baseline (see basyx-baseline/README.md Phase 5).

Compares replay.py's per-type dispatch count against the count of messages
that reached the bridge (one row per attempted PATCH in
basyx_baseline_bridge_metrics.csv), restricted to MAVLink message types the
bridge actually implements (config/mappings/mavlink_ops.yaml's sourceMessage
set). This is the fallback for runs without SOURCE_TLOG_PATH set (no
<bridge CSV base>_delivery_summary.json to read the self-reported ratio
from) - it measures the same thing (UDP-to-bridge loss) from the two CSVs
every run already produces.
"""
import argparse
import csv

MAPPED_TYPES = {
    "ALTITUDE", "ATTITUDE", "ATTITUDE_QUATERNION", "ATTITUDE_TARGET", "ESC_STATUS",
    "GLOBAL_POSITION_INT", "GPS_RAW_INT", "HEARTBEAT", "LOCAL_POSITION_NED",
    "OPEN_DRONE_ID_SYSTEM", "SERVO_OUTPUT_RAW", "SYS_STATUS", "VFR_HUD",
}


def count_by_type(path, column):
    counts = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            t = row[column]
            counts[t] = counts.get(t, 0) + 1
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dispatch_csv", help="dispatch_basyx_mqtt_*.csv from replay.py")
    ap.add_argument("bridge_csv", help="basyx_baseline_bridge_metrics.csv")
    args = ap.parse_args()

    dispatched = count_by_type(args.dispatch_csv, "Msg_Type")
    processed = count_by_type(args.bridge_csv, "Message_Type")

    print(f"{'Message_Type':<24}{'Dispatched':>12}{'Reached bridge':>16}{'Reached %':>12}")
    total_d = total_p = 0
    for t in sorted(MAPPED_TYPES):
        d = dispatched.get(t, 0)
        p = processed.get(t, 0)
        total_d += d
        total_p += p
        pct = f"{p / d * 100:>10.1f}%" if d else f"{'N/A':>11}"
        print(f"{t:<24}{d:>12}{p:>16}{pct}")
    print("-" * 64)
    pct = total_p / total_d * 100 if total_d else float("nan")
    print(f"{'TOTAL (mapped types)':<24}{total_d:>12}{total_p:>16}{pct:>11.1f}%")
    print(f"\nUnprocessed before bridge attempt (never reached the bridge, "
          f"regardless of PATCH outcome): {total_d - total_p} "
          f"({100 - pct:.1f}% of mapped dispatched)")


if __name__ == "__main__":
    main()
