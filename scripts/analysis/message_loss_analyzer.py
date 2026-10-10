"""Splits the connector's "missing trace" gap (see EXPERIMENTS_SUMMARY.md
Section 5) into intentional coalescing vs candidate real message loss,
without needing a capture of the runtime instrumentation.DropCounter /
CoalescedCounter values (which these experiments did not collect).

Background: a MAVLink message enters at pkg/adapters/mavlink (stage
ADAPTER), is broadcast by pkg/core (stage CORE), and is coalesced
per-property - latest value wins - by the exporter (pkg/exporters/mqtt's
coalesce.Coalescer or pkg/exporters/opcua_client's inline pendingUpdates
map) before being sent (stage {PROTOCOL}_SENDING/_SENT). Three independent
things can make a property update never reach SENT:

  Tier 0 (pre-adapter): the dispatched MAVLink message never reached
    handleFrame at all (UDP loss, gomavlib parse failure). Invisible to the
    connector's own log; only detectable by comparing the *dispatch* log
    (ground truth of what was actually sent for this run) against how many
    distinct MAVLink messages produced an ADAPTER line.

  Tier 1 (adapter -> core): pkg/adapters/mavlink/mavlink.go always calls
    instrumentation.Record("ADAPTER", ...) *before* attempting the
    (non-blocking) send to core's inbound channel - the send only fails,
    incrementing adapter.mavlink's DropCounter, if core isn't draining fast
    enough. So any (TraceID, PropertyID) with an ADAPTER row but no CORE row
    is an exact, unambiguous real drop - no inference needed.

  Tier 2 (core -> sent): once CORE is logged, the exporter's coalescer keeps
    only the latest pending value per property (SubmodelID.PropertyID) and
    silently discards an older pending one the instant a newer update for
    that same property arrives, before the old one is sent. This means: for
    a given property, every update strictly before its LAST update in the
    run is either sent, or was superseded by that later update (or a chain
    of them) - i.e. is provably coalesced, not lost. Only a property's LAST
    update in the run, if it also lacks {PROTOCOL}_SENT, cannot be explained
    by coalescing (nothing came after it to supersede it) - this "residual"
    bucket is the candidate-real-loss signal. It is further split by
    comparing each residual entry's CORE timestamp against how close it is
    to the end of the log: an entry within this run's own observed
    worst-case CORE->SENT latency of the log's last timestamp may simply
    still have been in flight when the process/log stopped, which is not a
    defect; anything further back is unexplained by that and is the best
    available estimate of genuine loss at this tier (core.broadcast.<name>
    channel-full, an OPC UA batch abandoned after a failed write+retry, or a
    value still pending in the coalescer when Stop()/WaitDrain's ctx was
    canceled - see coalesce.go's WaitDrain, which drops any still-pending
    batch on shutdown without incrementing any counter at all).

Known blind spot (stated rather than hidden): core.broadcast.<name> can also
drop a *non-final* update for a property (its bounded, per-subscriber 1000
channel is full). That looks identical, from timestamps alone, to
coalescing, unless it happens to land on a property's last update of the
run. Put()/pendingUpdates never block on this channel, so it filling is
expected to be rare to nonexistent - but it cannot be proven at zero from
this log alone. Only the live DropCounter("core.broadcast.<name>") would
close that last sliver.

Inputs, all already produced by these experiments (no re-run needed):
  - the connector's own latency_*.log (ADAPTER/CORE/{PROTOCOL}_SENDING/SENT)
  - that run's dispatch_connector_*.csv (ground truth of what was actually
    replayed for that specific run - used instead of re-parsing the source
    .tlog, since it's already synchronized to this run's actual timestamps
    and message set)
  - the mapping YAML that was active for the run (config/mappings/*.yaml),
    to know which dispatched message types the adapter could ever emit an
    ADAPTER line for (unmapped types, e.g. PARAM_VALUE, never produce one)
"""
import argparse
import csv
import re
import sys
from collections import defaultdict

STAGE_ADAPTER = "ADAPTER"
STAGE_CORE = "CORE"


def parse_latency_log(filepath, sending_stage, sent_stage):
    """Returns traces[trace_id][property_id] = {stage: timestamp_ns}."""
    traces = defaultdict(lambda: defaultdict(dict))
    tracked = {STAGE_ADAPTER, STAGE_CORE, sending_stage, sent_stage}
    max_ts = 0

    with open(filepath, "r") as f:
        for line in f:
            if not line.strip():
                continue
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue
            try:
                ts = int(parts[0])
                stage = parts[1].strip()
                prop = parts[2].strip()
                trace_id = parts[3].strip()
            except ValueError:
                continue
            if ts > max_ts:
                max_ts = ts
            if stage in tracked:
                traces[trace_id][prop][stage] = ts

    return traces, max_ts


def load_mapped_message_types(mapping_path):
    """Extracts the set of sourceMessage values from a mapping YAML without
    a YAML dependency - the schema is a flat list of
    '- sourceMessage: "TYPE"' entries, so a line-level regex is sufficient
    and avoids requiring PyYAML (not installed in this project's Python
    environment)."""
    pattern = re.compile(r'sourceMessage\s*:\s*"([^"]+)"', re.IGNORECASE)
    types = set()
    with open(mapping_path, "r") as f:
        for line in f:
            m = pattern.search(line)
            if m:
                types.add(m.group(1).strip().upper())
    return types


def load_dispatch_counts(dispatch_csv_path):
    """Returns {Msg_Type: count} from a dispatch_connector_*.csv."""
    counts = defaultdict(int)
    with open(dispatch_csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg_type = (row.get("Msg_Type") or "").strip().upper()
            if msg_type:
                counts[msg_type] += 1
    return counts


def analyze(traces, log_end_ts, sending_stage, sent_stage):
    adapter_count = 0
    core_count = 0
    sent_count = 0
    adapter_no_core = 0
    adapter_trace_ids = set()

    # property -> list of (core_ts, trace_id, has_sent), built across ALL
    # traces so we can find each property's true last update in the run.
    property_core_entries = defaultdict(list)

    core_to_sent_latencies = []

    for trace_id, propmap in traces.items():
        has_adapter_in_trace = False
        for prop, stages in propmap.items():
            has_adapter = STAGE_ADAPTER in stages
            has_core = STAGE_CORE in stages
            has_sent = sent_stage in stages

            if has_adapter:
                adapter_count += 1
                has_adapter_in_trace = True
                if not has_core:
                    adapter_no_core += 1
            if has_core:
                core_count += 1
                property_core_entries[prop].append((stages[STAGE_CORE], trace_id, has_sent))
                if has_sent:
                    core_to_sent_latencies.append(stages[sent_stage] - stages[STAGE_CORE])
            if has_sent:
                sent_count += 1
        if has_adapter_in_trace:
            adapter_trace_ids.add(trace_id)

    # Tier 2: classify every CORE event that never reached SENT.
    coalesced = 0
    residual = []  # (property, trace_id, core_ts) - each property's last update, still missing SENT
    for prop, entries in property_core_entries.items():
        entries.sort(key=lambda e: e[0])
        last_idx = len(entries) - 1
        for idx, (ts, trace_id, has_sent) in enumerate(entries):
            if has_sent:
                continue
            if idx == last_idx:
                residual.append((prop, trace_id, ts))
            else:
                coalesced += 1

    max_core_to_sent_latency = max(core_to_sent_latencies) if core_to_sent_latencies else 0
    boundary_ts = log_end_ts - max_core_to_sent_latency

    residual_boundary = [r for r in residual if r[2] > boundary_ts]
    residual_unexplained = [r for r in residual if r[2] <= boundary_ts]

    return {
        "adapter_count": adapter_count,
        "core_count": core_count,
        "sent_count": sent_count,
        "adapter_no_core": adapter_no_core,
        "adapter_trace_ids": adapter_trace_ids,
        "coalesced": coalesced,
        "residual_boundary": residual_boundary,
        "residual_unexplained": residual_unexplained,
        "max_core_to_sent_latency_ms": max_core_to_sent_latency / 1e6,
    }


def print_report(filepath, protocol, result, dispatch_csv_path, mapping_path,
                  dispatch_counts, mapped_types):
    sending_stage, sent_stage = f"{protocol}_SENDING", f"{protocol}_SENT"
    adapter_count = result["adapter_count"]
    core_count = result["core_count"]
    sent_count = result["sent_count"]
    missing = core_count - sent_count
    coalesced = result["coalesced"]
    residual_boundary = result["residual_boundary"]
    residual_unexplained = result["residual_unexplained"]

    print("\n" + "=" * 78)
    print(f"MESSAGE DROP / COALESCE ANALYSIS: {filepath}")
    print(f"Protocol: {protocol}")
    print("=" * 78)

    print("\nPipeline stage counts (one row per (TraceID, PropertyID)):")
    print(f"  {STAGE_ADAPTER:<14} {adapter_count}")
    print(f"  {STAGE_CORE:<14} {core_count}")
    print(f"  {sending_stage:<14} (not required for this analysis)")
    print(f"  {sent_stage:<14} {sent_count}")

    pre_adapter_loss = None
    mapped_dispatched_count = None
    if dispatch_counts is not None and mapped_types is not None:
        mapped_dispatched_count = sum(c for t, c in dispatch_counts.items() if t in mapped_types)
        distinct_adapter_traces = len(result["adapter_trace_ids"])
        pre_adapter_loss = mapped_dispatched_count - distinct_adapter_traces
        print(f"\nTier 0 - Pre-adapter (dispatch -> adapter), from {dispatch_csv_path}")
        print(f"  and {mapping_path}:")
        print(f"  Dispatched messages of a mapped type:         {mapped_dispatched_count}")
        print(f"  Distinct MAVLink messages reaching ADAPTER:   {distinct_adapter_traces}")
        pct = (100 * pre_adapter_loss / mapped_dispatched_count) if mapped_dispatched_count else 0.0
        print(f"  Pre-adapter loss (dispatched, mapped, never parsed/reached adapter):")
        print(f"    {pre_adapter_loss}  ({pct:.4f}% of mapped dispatched messages)")
        print("  (unmapped message types, e.g. PARAM_VALUE, MISSION_CURRENT, are excluded")
        print("   from both sides of this comparison - the adapter never logs anything for")
        print("   them, mapped or not, so they carry no information about loss)")
    else:
        print("\nTier 0 - Pre-adapter: skipped (no --dispatch-csv/--mapping given)")

    print("\nTier 1 - Adapter -> Core (adapter.mavlink DropCounter, reconstructed exactly):")
    pct1 = (100 * result["adapter_no_core"] / adapter_count) if adapter_count else 0.0
    print(f"  ADAPTER rows with no matching CORE row: {result['adapter_no_core']} ({pct1:.4f}% of ADAPTER)")
    print("  (ADAPTER is always logged before the send to core is attempted, so this")
    print("   count is exact, not inferred - any gap here is a real drop)")

    print(f"\nTier 2 - Core -> {sent_stage} (coalescing vs candidate real loss), per property:")
    pct2 = (100 * missing / core_count) if core_count else 0.0
    print(f"  CORE rows with no matching {sent_stage} row: {missing} ({pct2:.4f}% of CORE)")
    pct_coalesced = (100 * coalesced / missing) if missing else 0.0
    print(f"    Provably coalesced (a later update to the same property was sent,")
    print(f"    or was itself superseded further down the chain): {coalesced} ({pct_coalesced:.2f}% of missing)")
    n_residual = len(residual_boundary) + len(residual_unexplained)
    pct_residual = (100 * n_residual / missing) if missing else 0.0
    print(f"    Residual - this property's LAST update in the run, still never sent:")
    print(f"      {n_residual} ({pct_residual:.2f}% of missing)")
    print(f"        - within this run's own worst-case Core->Sent latency")
    print(f"          ({result['max_core_to_sent_latency_ms']:.3f} ms) of the log's last")
    print(f"          timestamp (plausibly still in flight when logging stopped,")
    print(f"          not necessarily a bug): {len(residual_boundary)}")
    print(f"        - occurred with more headroom than that (unexplained by 'still")
    print(f"          in flight' - best estimate of genuine loss at this tier):")
    print(f"          {len(residual_unexplained)}")

    real_loss = len(residual_unexplained) + result["adapter_no_core"]
    denom_label = "mapped dispatched messages"
    denom = mapped_dispatched_count
    if pre_adapter_loss is not None:
        real_loss += pre_adapter_loss
    else:
        denom = adapter_count
        denom_label = "ADAPTER-stage property events"

    pct_loss = (100 * real_loss / denom) if denom else 0.0
    print("\nEstimated real message loss (Tier 0 + Tier 1 + unexplained Tier 2 residual):")
    parts = []
    if pre_adapter_loss is not None:
        parts.append(str(pre_adapter_loss))
    parts.append(str(result["adapter_no_core"]))
    parts.append(str(len(residual_unexplained)))
    print(f"  {' + '.join(parts)} = {real_loss}  ({pct_loss:.4f}% of {denom} {denom_label})")

    print("\nCaveats:")
    print("  - core.broadcast.<name> (the core->exporter channel, buffered 1000/exporter)")
    print("    can itself drop a NON-final update for a property before it reaches this")
    print("    exporter's coalescer. That looks identical, from timestamps alone, to")
    print("    coalescing, unless it happens to be that property's last update of the")
    print("    run (in which case it lands in the residual bucket above). Put()/")
    print("    pendingUpdates never block on this channel, so it filling is expected to")
    print("    be rare to nonexistent, but it cannot be proven at zero from this log")
    print("    alone - only the live DropCounter(\"core.broadcast." + protocol.lower() + "\") would close this gap.")
    print("  - The 'still in flight' / 'unexplained' split of the residual bucket is a")
    print("    heuristic bound (this run's own max observed Core->Sent latency), not a")
    print("    certainty - it exists so residual entries aren't all treated as equally")
    print("    suspicious, not to claim an exact cutoff.")
    print("  - A value still sitting in the coalescer's pending map when the process")
    print("    shut down is dropped by WaitDrain without incrementing any counter at")
    print("    all (see coalesce.go); such entries, if any, fall inside the 'still in")
    print("    flight' bucket above, not the unexplained one, since they're")
    print("    indistinguishable from in-flight values from a log-timestamp view.")
    print("=" * 78)

    print(f"\nSUMMARY: coalesced={coalesced} residual_boundary={len(residual_boundary)} "
          f"residual_unexplained={len(residual_unexplained)} adapter_core_drop={result['adapter_no_core']} "
          f"pre_adapter_loss={pre_adapter_loss if pre_adapter_loss is not None else 'n/a'} "
          f"estimated_real_loss={real_loss}")


def main():
    parser = argparse.ArgumentParser(
        description="Split the connector's missing-trace gap into intentional coalescing "
                     "vs candidate real message loss, from existing experiment artifacts.")
    parser.add_argument("latency_log", help="Path to the connector's latency_*.log")
    parser.add_argument("--protocol", choices=["mqtt", "opcua"], required=True,
                         help="Exporter protocol whose SENDING/SENT stages to analyze")
    parser.add_argument("--dispatch-csv", default=None,
                         help="Path to that run's dispatch_connector_*.csv (enables Tier 0)")
    parser.add_argument("--mapping", default=None,
                         help="Path to the mapping YAML active for that run, e.g. "
                              "config/mappings/mavlink_ops.yaml (enables Tier 0)")
    args = parser.parse_args()

    protocol = args.protocol.upper()
    sending_stage, sent_stage = f"{protocol}_SENDING", f"{protocol}_SENT"

    print(f"Reading file: {args.latency_log}...")
    try:
        traces, log_end_ts = parse_latency_log(args.latency_log, sending_stage, sent_stage)
    except FileNotFoundError:
        print(f"Error: File '{args.latency_log}' not found.")
        sys.exit(1)

    dispatch_counts = None
    mapped_types = None
    if args.dispatch_csv and args.mapping:
        try:
            dispatch_counts = load_dispatch_counts(args.dispatch_csv)
            mapped_types = load_mapped_message_types(args.mapping)
        except FileNotFoundError as e:
            print(f"Warning: {e}. Skipping Tier 0 analysis.")
            dispatch_counts = None
            mapped_types = None
    elif args.dispatch_csv or args.mapping:
        print("Warning: --dispatch-csv and --mapping must both be given to run Tier 0. Skipping it.")

    result = analyze(traces, log_end_ts, sending_stage, sent_stage)
    print_report(args.latency_log, protocol, result, args.dispatch_csv, args.mapping,
                 dispatch_counts, mapped_types)


if __name__ == "__main__":
    main()
