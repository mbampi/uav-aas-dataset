"""Analyzes naive_baseline_metrics.csv / naive_baseline_mqtt_metrics.csv, the
per-message latency logs produced by baseline/python-baseline-conn's main.py
(OPC UA) and main_mqtt.py (MQTT), and reports the same descriptive statistics
(mean/p50/p90/p95/p99/max), in the same table format as latency_analyzer.py
and message_latency_analyzer.py, so the numbers can be placed directly next
to the connector's own.

Both baseline scripts already report one latency sample per MAVLink message
(Processing_Latency_ms: translation + every derived property's write/publish,
sequential and blocking) plus Queue_Wait_ms (time since the previous tracked
message finished, capturing backlog from that same blocking design). This is
the number to compare against
message_latency_analyzer.py's "fully-completed messages only" column for the
connector - NOT latency_analyzer.py's per-property numbers, which measure a
different, finer-grained thing (see message_latency_analyzer.py's docstring).

Both scripts also log a per-message Write_Success flag: a single write/publish
failure - a transient OPC UA write error, or (for MQTT) a QoS 0 publish that
never got confirmed within its wait timeout - is caught per message rather
than crashing the whole run, and abandons whatever fields of that message
were not yet written (see main.py's/main_mqtt.py's comments on this). Rows
with Write_Success=False are latency numbers for an INCOMPLETE operation
(a partial or zero-field write cut short by the failure) and are excluded
from the latency statistics below, exactly as the connector's own
"fully-completed messages only" column excludes traces that were coalesced
before every property was sent - mixing them in would silently corrupt the
distribution with times that don't represent a completed message. The
success/failure counts themselves are reported separately, and are this
baseline's own self-observed completeness signal - complementary to, not a
replacement for, the external cross-check against the OPC UA server's/MQTT
receiver's own log described below.

If a `<CSV base>_delivery_summary.json` file exists next to the input CSV
(written by either baseline script when run with SOURCE_TLOG_PATH set - see
baseline/python-baseline-conn/README.md), its overall delivery ratio is
printed too, clearly labeled as self-reported (not externally cross-checked
against the OPC UA server's/MQTT receiver's own log, unlike the connector's
own validated numbers - see EXPERIMENTS_SUMMARY.md Section 5).
"""
import argparse
import csv
import json
import math
import os
import sys
from collections import Counter
from statistics import mean


def get_percentile(data_sorted, p):
    if not data_sorted:
        return 0.0
    k = (len(data_sorted) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data_sorted[int(k)]
    d0 = data_sorted[int(f)]
    d1 = data_sorted[int(c)]
    return d0 + (d1 - d0) * (k - f)


REQUIRED_COLUMNS = [
    "System_Time", "Message_Type", "MAVLink_Seq", "Dropped_Since_Last",
    "Queue_Wait_ms", "Processing_Latency_ms", "CPU_Percent", "Memory_MB",
]
# Write_Success/Error were added alongside the per-message failure handling
# in main.py/main_mqtt.py. Older-format CSVs without them are still readable
# (see load_rows) - every row is just treated as successful, matching what
# the old, crash-on-any-failure behavior implied for any row that made it
# into the file at all: a run that failed a write never got to log that row.
OPTIONAL_COLUMN_WRITE_SUCCESS = "Write_Success"


def load_rows(filepath):
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or not all(c in reader.fieldnames for c in REQUIRED_COLUMNS):
            print(f"Error: CSV must contain columns: {REQUIRED_COLUMNS}")
            print(f"Found: {reader.fieldnames}")
            sys.exit(1)
        has_write_success = OPTIONAL_COLUMN_WRITE_SUCCESS in reader.fieldnames
        if not has_write_success:
            print("Note: no Write_Success column found (older-format CSV) - "
                  "treating every row as a successful write.")
        for r in reader:
            try:
                write_success = (r["Write_Success"] == "True") if has_write_success else True
                rows.append({
                    "system_time": float(r["System_Time"]),
                    "msg_type": r["Message_Type"],
                    "dropped_since_last": int(r["Dropped_Since_Last"]),
                    "queue_wait_ms": float(r["Queue_Wait_ms"]),
                    "processing_latency_ms": float(r["Processing_Latency_ms"]),
                    "write_success": write_success,
                    "error": r.get("Error", ""),
                })
            except (ValueError, KeyError):
                continue
    return rows


def stats_block(name, data):
    if not data:
        print(f"  {name:<28} n=0")
        return
    d = sorted(data)
    print(f"  {name:<28} n={len(d):<8} avg={mean(d):.3f}ms  "
          f"p50={get_percentile(d, 50):.3f}  p90={get_percentile(d, 90):.3f}  "
          f"p95={get_percentile(d, 95):.3f}  p99={get_percentile(d, 99):.3f}  "
          f"max={d[-1]:.3f}")


def analyze_file(filepath):
    print(f"Reading file: {filepath}...")
    if not os.path.exists(filepath):
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)

    rows = load_rows(filepath)
    if not rows:
        print("No parseable rows found.")
        return

    rows.sort(key=lambda r: r["system_time"])
    n = len(rows)
    duration_sec = rows[-1]["system_time"] - rows[0]["system_time"]
    throughput = n / duration_sec if duration_sec > 0 else 0.0

    type_counts = Counter(r["msg_type"] for r in rows)
    total_dropped = sum(r["dropped_since_last"] for r in rows)

    succeeded = [r for r in rows if r["write_success"]]
    failed = [r for r in rows if not r["write_success"]]
    n_success = len(succeeded)
    n_failed = len(failed)
    success_pct = 100 * n_success / n if n else 0.0

    # Latency stats computed ONLY over successful writes/publishes - a failed
    # row's Processing_Latency_ms is the time until it gave up, not the time
    # to complete a message, and would corrupt the distribution if mixed in
    # (see module docstring).
    queue = [r["queue_wait_ms"] for r in succeeded]
    proc = [r["processing_latency_ms"] for r in succeeded]
    total = [r["queue_wait_ms"] + r["processing_latency_ms"] for r in succeeded]

    print("\n" + "=" * 78)
    print(f"BASELINE MESSAGE LATENCY: {filepath}")
    print("=" * 78)
    print(f"Total Messages Processed:  {n}")
    print(f"  Write-confirmed (success): {n_success} ({success_pct:.2f}%)")
    print(f"  Write failed:              {n_failed} ({100 - success_pct:.2f}%)")
    print(f"Total Duration:            {duration_sec:.4f} s")
    print(f"Overall Throughput:        {throughput:.2f} msgs/sec")
    print(f"Distinct Message Types:    {len(type_counts)}")
    print(f"Total 'Dropped_Since_Last' (wire-seq, UNRELIABLE with the current "
          f"replay/synth tooling - see README): {total_dropped}")
    if failed:
        failed_types = Counter(r["msg_type"] for r in failed)
        print("Write failures by message type:")
        for t, c in sorted(failed_types.items(), key=lambda kv: -kv[1]):
            print(f"  {t:<24} {c}")
    print("-" * 78)
    print("Per-message latency (ms), write-confirmed messages only - directly")
    print("comparable to the connector's message-level reconciliation")
    print("(message_latency_analyzer.py's 'fully-completed messages only'):")
    stats_block("Queue_Wait_ms", queue)
    stats_block("Processing_Latency_ms", proc)
    stats_block("Total (Queue_Wait + Processing)", total)
    print("=" * 78)

    summary_path = os.path.splitext(filepath)[0] + "_delivery_summary.json"
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            summary = json.load(f)
        overall = summary.get("_overall")
        if overall:
            print(f"\nSelf-reported delivery ratio (from {os.path.basename(summary_path)}, "
                  f"NOT externally cross-checked - see docstring):")
            print(f"  {overall['arrived']} / {overall['expected']} "
                  f"({overall['delivery_pct']:.2f}%)")

            # arrived_counts in main.py/main_mqtt.py is only ever incremented
            # alongside total_write_success, in the same branch (see either
            # script) - so overall['arrived'] and n_success here are two
            # independent tallies of the same quantity: one accumulated live
            # during the run, the other recomputed after the fact straight
            # from the CSV rows. They should always match exactly; a mismatch
            # means the CSV is missing rows relative to what the run actually
            # produced (e.g. a crash mid-write, a truncated file, a run
            # interrupted before its last flush) rather than a genuine
            # discrepancy in delivery, and the numbers above should not be
            # trusted until that's explained.
            if overall['arrived'] != n_success:
                print(f"  WARNING: self-reported arrived ({overall['arrived']}) != "
                      f"write-confirmed rows actually found in this CSV ({n_success}). "
                      f"The CSV may be truncated or the run may have been interrupted "
                      f"before its last write - investigate before trusting these numbers.")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze a naive baseline gateway's per-message latency CSV")
    parser.add_argument("file", help="Path to naive_baseline_metrics.csv or naive_baseline_mqtt_metrics.csv")
    args = parser.parse_args()

    analyze_file(args.file)
