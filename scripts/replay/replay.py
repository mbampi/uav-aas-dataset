import time
import sys
import math
import csv
import statistics
import argparse
from pymavlink import mavutil

def parse_arguments():
    parser = argparse.ArgumentParser(description="MAVLink TLOG Replay Tool with Dispatch Logging")
    parser.add_argument("file", help="Path to the .tlog file to replay")
    parser.add_argument("--ip", default="192.168.1.50", help="Target IP address (default: 192.168.1.50)")
    parser.add_argument("--port", type=int, default=14550, help="Target UDP port (default: 14550)")
    parser.add_argument("--log", default="dispatch_log.csv", help="CSV log output for dispatch timestamps")
    parser.add_argument("--seq-msg", default="LOCAL_POSITION_NED",
                        help="MAVLink message type carrying the latency sequence counter (default: LOCAL_POSITION_NED)")
    parser.add_argument("--seq-field", default="x",
                        help="Field on --seq-msg overwritten with the counter, as a pymavlink attribute name (default: x)")
    parser.add_argument("--no-seq", action="store_true",
                        help="Disable sequence injection (raw replay; end-to-end latency matching won't be possible)")
    return parser.parse_args()

def scan_log(filename):
    """Scans the log to calculate total metrics and Hz statistics."""
    print(f"Scanning {filename} for metrics...")
    try:
        log = mavutil.mavlink_connection(filename)
    except Exception as e:
        print(f"Error opening file: {e}")
        sys.exit(1)

    total_msgs = 0
    first_time = None
    last_time = None
    
    hz_buckets = {}

    while True:
        msg = log.recv_match()
        if not msg:
            break
        
        total_msgs += 1
        timestamp = getattr(msg, '_timestamp', None)
        
        if timestamp is not None:
            if first_time is None:
                first_time = timestamp
            last_time = timestamp
            
            sec_bucket = math.floor(timestamp)
            hz_buckets[sec_bucket] = hz_buckets.get(sec_bucket, 0) + 1

    total_time = (last_time - first_time) if (first_time and last_time) else 0
    overall_avg_hz = (total_msgs / total_time) if total_time > 0 else 0
    
    keys = sorted(hz_buckets.keys())
    if len(keys) > 2:
        valid_rates = [hz_buckets[k] for k in keys[1:-1]]
    else:
        valid_rates = list(hz_buckets.values())

    min_hz = min(valid_rates) if valid_rates else 0
    max_hz = max(valid_rates) if valid_rates else 0
    median_hz = statistics.median(valid_rates) if valid_rates else 0
    
    return total_msgs, total_time, overall_avg_hz, min_hz, max_hz, median_hz

def main():
    args = parse_arguments()

    # --- 1. Pre-Scan Phase ---
    total_messages, total_time_sec, avg_hz, min_hz, max_hz, med_hz = scan_log(args.file)

    print("-" * 45)
    print(f"Log Summary: {args.file}")
    print(f"Total Messages: {total_messages}")
    print(f"Total Log Time: {total_time_sec:.2f} seconds")
    print("-" * 45)
    print(f"Average Rate:   {avg_hz:.1f} Hz")
    print(f"Median Rate:    {med_hz:.1f} Hz")
    print(f"Min / Max Rate: {min_hz} Hz / {max_hz} Hz")
    print("-" * 45)
    print(f"Streaming data to {args.ip}:{args.port}")
    print(f"Logging dispatch timestamps to: {args.log}")
    seq_msg = args.seq_msg.upper()
    if args.no_seq:
        print("Sequence injection: DISABLED (raw replay)")
    else:
        print(f"Sequence injection: {seq_msg}.{args.seq_field} <- monotonic counter (logged as 'Seq')")
    print("-" * 45)

    # --- 2. Playback Phase ---
    log = mavutil.mavlink_connection(args.file)
    target = mavutil.mavlink_connection(f'udpout:{args.ip}:{args.port}')

    # Encoder used to re-pack carrier messages after injecting the counter.
    # A fresh MAVLink object gives us its own sequence/CRC handling (like synth.py).
    mav_encoder = mavutil.mavlink.MAVLink(None, srcSystem=1, srcComponent=1)
    inject = not args.no_seq
    seq = 0
    warned_pack = False
    warned_timestamp = False
    MAX_DELAY = 5.0  # sane cap; some tlogs contain corrupted/garbage timestamps

    last_timestamp = None
    current_msg_count = 0
    last_print_time = time.time()
    messages_since_last_print = 0

    # Absolute-deadline pacing: each message's wall-clock target is derived
    # from cumulative source-time elapsed since playback start, not from a
    # sleep-per-message delta. This is what keeps high-rate replays (e.g. the
    # 500/1000 Hz synthetic stress files) from drifting off real-time — if a
    # sleep-per-delta model is used, ANY per-message overhead (CSV write,
    # dashboard print, syscall jitter) adds on top of the source interval and
    # compounds over the file, so a 1000 Hz/15 min file can end up taking much
    # longer than 15 minutes to play out. With a fixed deadline, overhead just
    # eats into slack instead of pushing every subsequent message later.
    source_elapsed = 0.0
    playback_start_wall = time.time()

    with open(args.log, "w", newline='') as f_dispatch:
        dispatch_writer = csv.writer(f_dispatch)
        dispatch_writer.writerow(["Dispatch_Timestamp_Ms", "Msg_Type", "Mav_Timestamp", "Seq"])
        f_dispatch.flush()

        try:
            while True:
                msg = log.recv_match()
                if not msg:
                    break

                # Handle Timing
                timestamp = getattr(msg, '_timestamp', None)
                if timestamp is not None:
                    if last_timestamp is not None:
                        delay = timestamp - last_timestamp
                        if delay > MAX_DELAY:
                            if not warned_timestamp:
                                print(f"\n[WARN] corrupted/out-of-range timestamp detected "
                                      f"(delay={delay:.1f}s); clamping sleep to {MAX_DELAY}s.")
                                warned_timestamp = True
                            delay = MAX_DELAY
                        elif delay < 0:
                            delay = 0  # out-of-order timestamp; don't rewind the schedule
                        source_elapsed += delay
                    last_timestamp = timestamp

                    sleep_for = (playback_start_wall + source_elapsed) - time.time()
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                    # else: already behind schedule — send immediately rather
                    # than piling more delay on top (self-correcting).

                msg_type = msg.get_type()

                # Inject a monotonic sequence counter into the carrier message so
                # each dispatched sample can be matched 1:1 to its arrival by value
                # (latency-calculate joins Seq == received Value_Raw). Non-carrier
                # messages pass through byte-for-byte via their original buffer.
                seq_val = ""
                out_buf = None
                if inject and msg_type == seq_msg and hasattr(msg, args.seq_field):
                    seq += 1
                    setattr(msg, args.seq_field, seq)
                    try:
                        out_buf = msg.pack(mav_encoder)
                        seq_val = seq
                    except Exception as e:
                        out_buf = msg.get_msgbuf()
                        seq_val = ""
                        if not warned_pack:
                            print(f"\n[WARN] could not re-pack {seq_msg}.{args.seq_field} ({e}); "
                                  f"sending raw. Latency matching will have no samples — "
                                  f"pick a different --seq-msg/--seq-field.")
                            warned_pack = True
                if out_buf is None:
                    out_buf = msg.get_msgbuf()

                # Capture send timestamp immediately before transmission
                dispatch_ms = time.time() * 1000

                # Send Data
                target.write(out_buf)

                # Log Dispatch
                mav_ts = timestamp if timestamp is not None else 0.0
                dispatch_writer.writerow([f"{dispatch_ms:.3f}", msg_type, f"{mav_ts:.6f}", seq_val])

                # Update Dashboard
                current_msg_count += 1
                messages_since_last_print += 1
                
                now = time.time()
                if now - last_print_time >= 0.5:
                    current_hz = messages_since_last_print / (now - last_print_time)
                    progress_pct = (current_msg_count / total_messages) * 100 if total_messages > 0 else 0
                    
                    status_line = f"\rPlayback: [{progress_pct:>5.1f}%] | Sent: {current_msg_count}/{total_messages} | Current: {current_hz:>6.1f} Hz "
                    sys.stdout.write(status_line)
                    sys.stdout.flush()
                    f_dispatch.flush()  # periodic, not per-message — bounds crash data-loss without capping throughput

                    last_print_time = now
                    messages_since_last_print = 0

            print("\n\nPlayback complete!")

        except KeyboardInterrupt:
            print("\n\nPlayback manually stopped by user.")
            sys.exit(0)

if __name__ == "__main__":
    main()