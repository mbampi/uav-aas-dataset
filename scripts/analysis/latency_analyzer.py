import argparse
import sys
import math
from collections import defaultdict
from statistics import mean
import matplotlib.pyplot as plt

def get_stages(protocol):
    """Returns the expected order of events for the given protocol."""
    sending_stage, sent_stage = f"{protocol}_SENDING", f"{protocol}_SENT"
    return ["ADAPTER", "CORE", sending_stage, sent_stage], sending_stage, sent_stage

def get_percentile(data, percentile):
    """Calculates the p-th percentile of a list of numbers."""
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
    """Parses a single line of the log file."""
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
    stages, sending_stage, sent_stage = get_stages(protocol)

    # Data structure: traces[correlation_id][variable_name] = { stage: timestamp }
    traces = defaultdict(lambda: defaultdict(dict))
    
    min_ts = float('inf')
    max_ts = float('-inf')
    
    print(f"Reading file: {filepath}...")
    
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if not line.strip(): continue
                
                data = parse_line(line)
                if not data: continue
                
                ts, stage, var, corr_id = data
                
                # Track global time boundaries for frequency calc
                if ts < min_ts: min_ts = ts
                if ts > max_ts: max_ts = ts
                
                # Store the timestamp for this specific stage
                if stage in stages:
                    traces[corr_id][var][stage] = ts
                    
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)

    # Lists to store latencies for calculations (in milliseconds)
    latencies = {
        "step1": [], # ADAPTER -> CORE
        "step2": [], # CORE -> SENDING
        "step3": [], # SENDING -> SENT
        "adapter_to_sending": [], # ADAPTER -> SENDING
        "total": []  # ADAPTER -> SENT
    }
    
    message_count = 0
    plot_data = [] # Stores (timestamp, latency) tuples for chart
    
    # Analyze the grouped traces
    for corr_id, vars_map in traces.items():
        for var, stages_map in vars_map.items():
            message_count += 1
            
            # Helper to safely get delta in ms
            def get_delta(start_stage, end_stage):
                if start_stage in stages_map and end_stage in stages_map:
                    # Convert nanoseconds to milliseconds
                    return (stages_map[end_stage] - stages_map[start_stage]) / 1e6
                return None

            s1 = get_delta("ADAPTER", "CORE")
            s2 = get_delta("CORE", sending_stage)
            s3 = get_delta(sending_stage, sent_stage)
            total = get_delta("ADAPTER", sent_stage)
            adapter_to_sending = get_delta("ADAPTER", sending_stage)
            
            if s1 is not None: latencies["step1"].append(s1)
            if s2 is not None: latencies["step2"].append(s2)
            if s3 is not None: latencies["step3"].append(s3)
            if total is not None: latencies["total"].append(total)
            if adapter_to_sending is not None: latencies["adapter_to_sending"].append(adapter_to_sending)
            
            # Collect data for plotting (using ADAPTER timestamp as x-axis)
            if total is not None and "ADAPTER" in stages_map:
                plot_data.append((stages_map["ADAPTER"], total))

    # Global Stats
    total_time_ns = max_ts - min_ts
    total_time_sec = total_time_ns / 1e9
    frequency = message_count / total_time_sec if total_time_sec > 0 else 0
    
    print("\n" + "="*50)
    print(f"EXPERIMENT RESULTS: {filepath}")
    print("="*50)
    print(f"Total Unique Messages Processed: {message_count}")
    print(f"Total Experiment Duration:       {total_time_sec:.4f} s")
    print(f"Overall Frequency (Throughput):  {frequency:.2f} msgs/sec")
    print("-" * 50)
    
    # Print Stats Table
    headers = ["Metric", "Count", "Avg (ms)", "p90 (ms)", "p95 (ms)", "p99 (ms)"]
    row_fmt = "{:<25} {:<10} {:<10} {:<10} {:<10} {:<10}"
    print(row_fmt.format(*headers))
    print("-" * 75)
    
    step_names = [
        ("Adapter -> Core", latencies["step1"]),
        (f"Core -> {sending_stage}", latencies["step2"]),
        (f"Adapter -> {sending_stage}", latencies["adapter_to_sending"]),
        (f"{sending_stage} -> {sent_stage}", latencies["step3"]),
        ("Total (End-to-End)", latencies["total"]),
    ]
    
    for name, data in step_names:
        if not data:
            print(row_fmt.format(name, 0, "N/A", "N/A", "N/A", "N/A"))
            continue
            
        data.sort()
        count = len(data)
        avg = mean(data)
        p90 = get_percentile(data, 90)
        p95 = get_percentile(data, 95)
        p99 = get_percentile(data, 99)
        
        print(row_fmt.format(
            name, 
            count, 
            f"{avg:.3f}", 
            f"{p90:.3f}", 
            f"{p95:.3f}", 
            f"{p99:.3f}"
        ))
    print("="*50 + "\n")

    # Generate Chart
    if plot_data:
        print("Generating chart...")
        plot_data.sort(key=lambda x: x[0])
        
        # Normalize time to start at 0 seconds
        start_time = plot_data[0][0]
        x_vals = [(t - start_time) / 1e9 for t, _ in plot_data]
        y_vals = [l for _, l in plot_data]
        
        plt.figure(figsize=(12, 6))
        plt.plot(x_vals, y_vals, label="Total Latency (Adapter -> Sending)", color='blue', alpha=0.5, linewidth=0.8)
        
        # Add a moving average trendline (n=50)
        window = 50
        if len(y_vals) > window:
             moving_avg = []
             for i in range(len(y_vals)):
                 start_idx = max(0, i - window // 2)
                 end_idx = min(len(y_vals), i + window // 2)
                 moving_avg.append(mean(y_vals[start_idx:end_idx]))
             plt.plot(x_vals, moving_avg, label=f"Moving Avg (n={window})", color='red', linewidth=1.5)

        plt.xlabel("Time (seconds)")
        plt.ylabel("Latency (ms)")
        plt.title("End-to-End Latency over Time")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        
        chart_filename = "latency_chart.png"
        plt.savefig(chart_filename)
        print(f"Chart saved to {chart_filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze UAV/AAS Experiment Logs")
    parser.add_argument("file", help="Path to the log file")
    parser.add_argument("--protocol", choices=["mqtt", "opcua"], default="mqtt",
                         help="Communication protocol used in the log (default: mqtt)")
    args = parser.parse_args()

    analyze_file(args.file, protocol=args.protocol.upper())