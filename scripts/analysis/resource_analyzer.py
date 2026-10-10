"""Analyzes CPU/Memory usage CSVs produced by resource_metrics/resource_metrics.py,
the Docker-stats-based sidecar used to monitor both the Go connector and the
Python baseline gateways (see baseline/python-baseline-conn/README.md's
"Resource Monitoring" section - same script, same formulas, same schema,
for both).

Filtering (ON by default; see --no-zero-filter):
Rows with Memory_Bytes == 0 are excluded before computing statistics. A
memory reading of exactly 0 bytes is not physically meaningful for a live
process. Investigating this for the connector's own dissertation_exps data
found these rows are produced by the sidecar continuing to poll the target
container for a period after it has already been stopped for teardown
between successive runs - see EXPERIMENTS_SUMMARY.md Section 6 for the full
root-cause writeup. Confirmed there: 0% of samples recorded strictly within
the actual dispatch/test window are ever affected, in every run checked; the
zero-reading rows are 100% confined to that post-test tail.

Optional window alignment (--align-to):
The zero-reading filter above does not catch the few genuinely-valid,
near-idle samples recorded in the gap between dispatch ending and the
container actually being stopped (real CPU~0%, real nonzero memory - the
process was legitimately idle, just not during the test). For a fully
rigorous exclusion of that whole post-test tail, not just its later
zero-reading portion, pass the companion log/CSV from the SAME run that
defines the actual test window:
  - a connector latency_*.log (header: ProcessTimestamp_ns,Stage,PropertyID,TraceID_ns)
  - or a baseline naive_baseline_*.csv (header: System_Time,Message_Type,...)
Format is auto-detected from the header line. Only resource samples whose
Timestamp falls within [window_start, window_end] of that companion file are
kept.
"""
import argparse
import csv
import os

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

TIMESTAMP_COL = 'Timestamp'
CPU_COL = 'CPU_Percent'
MEM_BYTES_COL = 'Memory_Bytes'
MEM_PCT_COL = 'Memory_Percent'
REQUIRED_COLS = [TIMESTAMP_COL, CPU_COL, MEM_BYTES_COL, MEM_PCT_COL]


def detect_window(align_path):
    """Returns (start_epoch_sec, end_epoch_sec) spanned by align_path, a
    connector latency_*.log or a baseline naive_baseline_*.csv - format
    auto-detected from the header line."""
    if not os.path.exists(align_path):
        raise ValueError(f"--align-to file not found: {align_path}")

    with open(align_path, 'r') as f:
        header = f.readline().strip()

    if header.startswith('ProcessTimestamp_ns'):
        min_ns, max_ns = None, None
        with open(align_path, 'r') as f:
            next(f, None)
            for line in f:
                ts_str = line.split(',', 1)[0]
                try:
                    ts = int(ts_str)
                except ValueError:
                    continue
                if min_ns is None or ts < min_ns:
                    min_ns = ts
                if max_ns is None or ts > max_ns:
                    max_ns = ts
        if min_ns is None:
            raise ValueError(f"No parseable rows in {align_path}")
        return min_ns / 1e9, max_ns / 1e9

    elif header.startswith('System_Time'):
        with open(align_path, 'r') as f:
            reader = csv.DictReader(f)
            times = []
            for r in reader:
                try:
                    times.append(float(r['System_Time']))
                except (ValueError, KeyError):
                    continue
        if not times:
            raise ValueError(f"No parseable rows in {align_path}")
        return min(times), max(times)

    else:
        raise ValueError(
            f"Could not detect the format of {align_path} from its header "
            f"({header[:60]!r}...) - expected a connector latency_*.log "
            f"(header starting 'ProcessTimestamp_ns') or a baseline "
            f"naive_baseline_*.csv (header starting 'System_Time')."
        )


def analyze_resources(filepath, align_to=None, zero_filter=True):
    if not os.path.exists(filepath):
        print(f"Error: File '{filepath}' not found.")
        return

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    if not all(col in df.columns for col in REQUIRED_COLS):
        print(f"Error: CSV must contain columns: {REQUIRED_COLS}")
        print(f"Found: {list(df.columns)}")
        return

    df = df.sort_values(TIMESTAMP_COL).reset_index(drop=True)
    n_total = len(df)
    df['Memory_MB'] = df[MEM_BYTES_COL] / (1024 * 1024)
    df['Excluded'] = False

    print(f"RESOURCE USAGE ANALYSIS: {filepath}")
    print("=" * 75)
    print(f"Total samples in file: {n_total}")

    if zero_filter:
        is_zero = df[MEM_BYTES_COL] == 0
        df.loc[is_zero, 'Excluded'] = True
        n_zero = int(is_zero.sum())
        pct = 100 * n_zero / n_total if n_total else 0.0
        print(f"Excluded (Memory_Bytes == 0, not physically meaningful): {n_zero} ({pct:.1f}%)")
    else:
        print("Zero-reading filter: DISABLED (--no-zero-filter)")

    window = None
    if align_to:
        try:
            window = detect_window(align_to)
        except ValueError as e:
            print(f"Error: {e}")
            return
        w_start, w_end = window
        print(f"Aligning to window from {align_to}: "
              f"[{w_start:.3f}, {w_end:.3f}] ({w_end - w_start:.1f}s)")
        out_of_window = (~df['Excluded']) & ((df[TIMESTAMP_COL] < w_start) | (df[TIMESTAMP_COL] > w_end))
        n_oow = int(out_of_window.sum())
        df.loc[out_of_window, 'Excluded'] = True
        pct = 100 * n_oow / n_total if n_total else 0.0
        print(f"Additionally excluded (valid reading, outside test window): {n_oow} ({pct:.1f}%)")

    valid = df[~df['Excluded']].copy()
    n_valid = len(valid)
    pct = 100 * n_valid / n_total if n_total else 0.0
    print(f"Valid samples used for statistics: {n_valid} / {n_total} ({pct:.1f}%)")
    print("-" * 75)

    if n_valid == 0:
        print("No valid samples remain after filtering - nothing to report.")
        return

    stats_cols = {
        CPU_COL: "CPU (%)",
        'Memory_MB': "Memory (MB)",
        MEM_PCT_COL: "Memory (%)",
    }
    print(f"{'Metric':<15} {'Mean':<10} {'Std':<10} {'Min':<10} {'Max':<10} {'P95':<10} {'P99':<10}")
    print("-" * 75)
    for col, label in stats_cols.items():
        s = valid[col]
        print(f"{label:<15} {s.mean():<10.2f} {s.std():<10.2f} {s.min():<10.2f} "
              f"{s.max():<10.2f} {s.quantile(0.95):<10.2f} {s.quantile(0.99):<10.2f}")
    print("=" * 75)

    # Plotting: full raw timeline, including excluded points, so an artifact
    # like this remains visually obvious in future runs; excluded points are
    # marked distinctly rather than silently dropped from the chart.
    df['Relative_Time'] = df[TIMESTAMP_COL] - df[TIMESTAMP_COL].min()
    kept = df[~df['Excluded']]
    excluded = df[df['Excluded']]

    fig, ax1 = plt.subplots(figsize=(12, 6))

    l1, = ax1.plot(kept['Relative_Time'], kept[CPU_COL], label='CPU Usage (%) - kept',
                    color='tab:blue', linewidth=1.5)
    if len(excluded):
        ax1.scatter(excluded['Relative_Time'], excluded[CPU_COL], color='red', marker='x',
                     s=15, zorder=5)
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('CPU Usage (%)', color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax1.grid(True, linestyle='--', alpha=0.5)

    ax2 = ax1.twinx()
    l2, = ax2.plot(kept['Relative_Time'], kept['Memory_MB'], label='Memory Usage (MB) - kept',
                    color='tab:orange', linewidth=1.5, linestyle='--')
    if len(excluded):
        ax2.scatter(excluded['Relative_Time'], excluded['Memory_MB'], color='darkred', marker='x',
                     s=15, zorder=5)
    ax2.set_ylabel('Memory Usage (MB)', color='tab:orange')
    ax2.tick_params(axis='y', labelcolor='tab:orange')

    if kept['Memory_MB'].max() == kept['Memory_MB'].min():
        ax2.set_ylim(0, max(1, kept['Memory_MB'].max() * 1.1))

    lines = [l1, l2]
    labels = [l.get_label() for l in lines]
    if len(excluded):
        lines.append(Line2D([0], [0], marker='x', color='red', linestyle='None'))
        labels.append('Excluded samples')
    ax1.legend(lines, labels, loc='upper left', fontsize=8)

    if window:
        w_start_rel = window[0] - df[TIMESTAMP_COL].min()
        w_end_rel = window[1] - df[TIMESTAMP_COL].min()
        ax1.axvspan(w_start_rel, w_end_rel, color='green', alpha=0.05, zorder=0)

    plt.title(f"Resource Usage over Time: {os.path.basename(filepath)}")
    plt.tight_layout()

    output_image = "resource_usage_chart.png"
    plt.savefig(output_image)
    print(f"Chart saved to {output_image}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze CPU/Memory usage from the Docker-stats resource-monitor sidecar")
    parser.add_argument("file", help="Path to the resource-usage CSV")
    parser.add_argument("--align-to", metavar="FILE",
                         help="Companion latency_*.log or naive_baseline_*.csv from the same run, "
                              "used to exclude samples outside the actual test window")
    parser.add_argument("--no-zero-filter", action="store_true",
                         help="Disable the default Memory_Bytes==0 exclusion (for debugging)")
    args = parser.parse_args()

    analyze_resources(args.file, align_to=args.align_to, zero_filter=not args.no_zero_filter)
