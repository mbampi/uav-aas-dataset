import docker
import csv
import time
import os
import sys
from datetime import datetime

# --- Configuration ---
# Container to monitor
CONTAINER_NAME = os.getenv("TARGET_CONTAINER", "uav-aas-connector")
INTERVAL_SEC = float(os.getenv("INTERVAL_SEC", 0.5))

# Output setup
OUTPUT_DIR = "/output"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print(f"--- Native Metrics Collector ---")
print(f"Target: {CONTAINER_NAME}")
print(f"Connecting to Docker Socket...")

# Connect to Docker Socket (requires /var/run/docker.sock mounted)
try:
    client = docker.from_env()
    container = client.containers.get(CONTAINER_NAME)
    print(f"Success: Found container '{container.name}' ({container.id[:12]})")
except docker.errors.NotFound:
    print(f"FATAL: Container '{CONTAINER_NAME}' not found. Is it running?")
    sys.exit(1)
except Exception as e:
    print(f"FATAL: Could not connect to Docker Daemon. {e}")
    sys.exit(1)

strTime = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = os.path.join(OUTPUT_DIR, f'{CONTAINER_NAME}_{strTime}.csv')

def calculate_mem_usage_no_cache(mem_stats):
    """Returns memory usage excluding reclaimable page cache, matching what
    `docker stats` itself displays. The raw cgroup 'usage' figure counts page
    cache for every file the container has written (e.g. log files growing
    for the process lifetime), which makes long-running writers look like
    they are leaking memory when they are not.
    """
    usage = mem_stats.get("usage", 0)
    stats = mem_stats.get("stats", {})

    # cgroup v2 reports "inactive_file"; cgroup v1 reports "total_inactive_file"
    # (falling back to "cache" if the "total_*" aggregate isn't present).
    if "inactive_file" in stats:
        cache = stats["inactive_file"]
    elif "total_inactive_file" in stats:
        cache = stats["total_inactive_file"]
    else:
        cache = stats.get("cache", 0)

    return max(usage - cache, 0)


def calculate_cpu_percent(d):
    """Calculates CPU % exactly like 'docker stats'"""
    try:
        cpu_stats = d.get("cpu_stats", {})
        precpu_stats = d.get("precpu_stats", {})
        
        # CPU Usage
        cpu_usage = cpu_stats.get("cpu_usage", {})
        precpu_usage = precpu_stats.get("cpu_usage", {})
        
        cpu_count = len(cpu_usage.get("percpu_usage", []) or [])
        
        # Fallback for cpu count if percpu_usage is missing (common in cgroups v2)
        if cpu_count == 0:
             cpu_count = cpu_stats.get("online_cpus", 1)

        cpu_delta = cpu_usage.get("total_usage", 0) - precpu_usage.get("total_usage", 0)
        system_delta = cpu_stats.get("system_cpu_usage", 0) - precpu_stats.get("system_cpu_usage", 0)
        
        if system_delta > 0.0 and cpu_delta > 0.0:
            cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
            return cpu_percent
    except Exception:
        pass
    return 0.0

# Open CSV
with open(filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Timestamp", "CPU_Percent", "Memory_Bytes", "Memory_Percent"])
    
    next_call = time.time()

    print(f"Starting collection... Writing to {filename}")

    try:
        while True:
            try:
                # Fetch real-time stats
                stats = container.stats(stream=False)

                # 1. CPU
                cpu_pct = calculate_cpu_percent(stats)

                # 2. Memory
                mem_stats = stats.get("memory_stats", {})

                # Check if 'usage' key exists, handle missing key gracefully
                if "usage" in mem_stats:
                    mem_current = calculate_mem_usage_no_cache(mem_stats)
                    mem_limit = mem_stats.get("limit", 1) # avoid divide by zero
                else:
                    # Fallback or zero if cgroups disabled
                    mem_current = 0
                    mem_limit = 1

                mem_pct = (mem_current / mem_limit) * 100.0

                # 3. Timestamp
                now = time.time()

                writer.writerow([now, f"{cpu_pct:.2f}", mem_current, f"{mem_pct:.2f}"])
                csvfile.flush()

                print(f"T: {datetime.fromtimestamp(now).strftime('%H:%M:%S')} | CPU: {cpu_pct:.5f}% | MEM: {mem_pct:.2f}%")

            except Exception as e:
                print(f"Error collecting metrics: {e}")
                try:
                    container.reload()
                    if container.status != 'running':
                        print("Target container stopped. Exiting.")
                        break
                except:
                    break

            next_call = next_call + INTERVAL_SEC
            time.sleep(max(0, next_call - time.time()))
    except KeyboardInterrupt:
        print("Stopped by user.")

print("Collection Complete.")