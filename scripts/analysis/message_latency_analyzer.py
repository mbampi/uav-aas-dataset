"""Reconciles the connector's per-property latency log into a per-MESSAGE
latency distribution, for a fair comparison against the naive baseline
(baseline/python-baseline-conn), whose Processing_Latency_ms is a single
number per MAVLink message covering that message's entire sequential,
blocking write of every derived property.

latency_analyzer.py reports one latency sample per (TraceID_ns, PropertyID)
pair - i.e. per AAS property, not per MAVLink message. A single ATTITUDE
message fans out into 6 properties (Roll, Pitch, Yaw, RollSpeed, PitchSpeed,
YawSpeed), each logged and reported independently there. Comparing that
per-property distribution directly against the baseline's per-message number
would flatter the connector: the baseline's one latency figure already
includes the cost of writing every one of those 6 fields, sequentially,
before moving on; the connector's per-property figures do not.

The fair basis: group every log line by TraceID_ns (the timestamp assigned
once, at ingestion, to the raw MAVLink message that produced it - shared by
every property that message fans out into), and take
    message_latency = max(SENT timestamp across that trace's properties) - TraceID_ns
i.e. "time until every property this message produced was fully sent" - the
same quantity the baseline's Processing_Latency_ms represents.

A trace can fan out into properties that never reach {PROTOCOL}_SENT for that
specific trace, without that being data loss (Section III-D's coalescing: a
newer trace's update to the same property can supersede this one's before it
drains). Such traces are reported separately from fully-completed ones rather
than silently mixed in, since "time until every property was sent" is not
quite the same measurement for a message that produced a partial trace vs a
full one - see the two_props printed per run below.
"""
import argparse
import math
import sys
from collections import defaultdict
from statistics import mean


def get_percentile(data, percentile):
    if not data:
        return 0.0
    k = (len(data) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data[int(k)]
    d0 = data[int(f)]
    d1 = data[int(c)]
    return d0 + (d1 - d0) * (k - f)


def parse_line(line):
    parts = line.strip().split(',')
    if len(parts) < 4:
        return None
    try:
        timestamp = int(parts[0])
        stage = parts[1].strip()
        variable = parts[2].strip()
        correlation_id = parts[3].strip()
        return timestamp, stage, variable, correlation_id
    except ValueError:
        return None


def analyze_file(filepath, protocol="MQTT"):
    proto = protocol.upper()
    sending_stage, sent_stage = f"{proto}_SENDING", f"{proto}_SENT"
    tracked_stages = {"ADAPTER", "CORE", sending_stage, sent_stage}

    # traces[trace_id][property_id] = {stage: timestamp_ns}
    traces = defaultdict(lambda: defaultdict(dict))

    print(f"Reading file: {filepath}...")
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                data = parse_line(line)
                if not data:
                    continue
                ts, stage, prop, trace_id = data
                if stage in tracked_stages:
                    traces[trace_id][prop][stage] = ts
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)

    full_latencies_ms = []      # every property this message produced was SENT
    partial_latencies_ms = []   # at least one property was SENT, but not all
    lost_message_count = 0      # message produced properties, but NONE were SENT
    fanout_counts = []

    for trace_id_str, props in traces.items():
        try:
            trace_id_ns = int(trace_id_str)
        except ValueError:
            continue

        n_props = len(props)
        fanout_counts.append(n_props)

        sent_timestamps = [
            stages[sent_stage] for stages in props.values() if sent_stage in stages
        ]
        n_sent = len(sent_timestamps)

        if n_sent == 0:
            lost_message_count += 1
            continue

        msg_latency_ms = (max(sent_timestamps) - trace_id_ns) / 1e6
        if n_sent == n_props:
            full_latencies_ms.append(msg_latency_ms)
        else:
            partial_latencies_ms.append(msg_latency_ms)

    total_messages = len(traces)

    def stats_block(name, data):
        if not data:
            print(f"  {name:<32} n=0")
            return
        d = sorted(data)
        print(f"  {name:<32} n={len(d):<8} avg={mean(d):.3f}ms  "
              f"p50={get_percentile(d, 50):.3f}  p90={get_percentile(d, 90):.3f}  "
              f"p95={get_percentile(d, 95):.3f}  p99={get_percentile(d, 99):.3f}  "
              f"max={d[-1]:.3f}")

    print("\n" + "=" * 78)
    print(f"MESSAGE-LEVEL LATENCY (reconciled for baseline comparison): {filepath}")
    print(f"Protocol: {proto}")
    print("=" * 78)
    print(f"Total distinct MAVLink-message traces:  {total_messages}")
    print(f"  Fully completed (every property sent): {len(full_latencies_ms)} "
          f"({100 * len(full_latencies_ms) / total_messages:.2f}%)" if total_messages else "")
    print(f"  Partially completed (some, not all, sent): {len(partial_latencies_ms)} "
          f"({100 * len(partial_latencies_ms) / total_messages:.2f}%)" if total_messages else "")
    print(f"  Fully lost (none of its properties sent): {lost_message_count} "
          f"({100 * lost_message_count / total_messages:.2f}%)" if total_messages else "")
    if fanout_counts:
        print(f"  Mean properties per message (fan-out): {mean(fanout_counts):.2f}")
    print("-" * 78)
    print("Message-level 'time until every produced property was sent' (ms):")
    print("  (this is the number directly comparable to the baseline's Processing_Latency_ms)")
    stats_block("Fully-completed messages only", full_latencies_ms)
    stats_block("Fully-completed + partial (best-effort)", full_latencies_ms + partial_latencies_ms)
    print("=" * 78 + "\n")

    return {
        "total_messages": total_messages,
        "full": full_latencies_ms,
        "partial": partial_latencies_ms,
        "lost": lost_message_count,
        "fanout_counts": fanout_counts,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reconcile per-property latency log into per-message latency, for baseline comparison")
    parser.add_argument("file", help="Path to the log file")
    parser.add_argument("--protocol", choices=["mqtt", "opcua"], default="mqtt",
                         help="Communication protocol used in the log (default: mqtt)")
    args = parser.parse_args()

    analyze_file(args.file, protocol=args.protocol.upper())
