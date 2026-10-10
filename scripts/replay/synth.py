"""
Synthetic MAVLink .tlog generator for reproducible latency/throughput experiments.

Produces tlog files (8-byte big-endian microsecond timestamp + MAVLink v2 packet,
same on-disk format pymavlink/mavutil expects and replay.py replays) that:
  - reproduce the real message-type mix measured in data/flight_1.tlog and
    data/flight_2.tlog (see their .md reports), scaled to any target aggregate Hz
  - carry field values driven by a simulated flight profile (arm -> takeoff ->
    loiter/cruise -> land) instead of frozen dummy values, so the payloads look
    like a real flight rather than a stress-test placeholder
  - are fully reproducible: same --seed/--hz/--duration/--start-time always
    produce a byte-identical file

Usage:
    python3 synth.py --hz 100 --duration 900 --out data/synth_100hz.tlog
    python3 synth.py --hz 10  --duration 900 --out data/synth_10hz.tlog --seed 7
"""

import argparse
import heapq
import math
import random
import struct

from pymavlink.dialects.v20 import common as mavlink2

# ---------------------------------------------------------------------------
# Message-type mix, averaged from data/flight_1.md and data/flight_2.md.
#
# UNKNOWN_* entries in those reports are PX4-proprietary message IDs with no
# definition in pymavlink's `common` dialect, so we can't (re)construct valid
# payloads for them; they're a negligible ~1.3 Hz combined out of ~78 Hz and
# are dropped here. All other real types are included and reweighted.
# ---------------------------------------------------------------------------
ORIGINAL_COUNTS = {
    'SERVO_OUTPUT_RAW': 19558,
    'ATTITUDE_TARGET': 19554,
    'LOCAL_POSITION_NED': 19554,
    'POSITION_TARGET_LOCAL_NED': 19554,
    'ATTITUDE': 19457,
    'ATTITUDE_QUATERNION': 19457,
    'GLOBAL_POSITION_INT': 19450,
    'HEARTBEAT': 2296,
    'GPS_RAW_INT': 1956,
    'VFR_HUD': 1565,
    'PARAM_VALUE': 987,
    'ALTITUDE': 391,
    'EXTENDED_SYS_STATE': 391,
    'OPEN_DRONE_ID_LOCATION': 391,
    'OPEN_DRONE_ID_SYSTEM': 391,
    'SCALED_PRESSURE': 391,
    'SYS_STATUS': 391,
    'MISSION_CURRENT': 389,
    'HOME_POSITION': 196,
    'BATTERY_STATUS': 195,
    'ESTIMATOR_STATUS': 195,
    'REQUEST_DATA_STREAM': 126,
    'PING': 39,
    'VIBRATION': 39,
    'POSITION_TARGET_GLOBAL_INT': 18,
    'FILE_TRANSFER_PROTOCOL': 13,
    'COMMAND_LONG': 12,
    'PARAM_REQUEST_LIST': 11,
    'STATUSTEXT': 9,
    'COMMAND_ACK': 4,
    'GPS_GLOBAL_ORIGIN': 1,
}

# Reference PX4 home position (Sao Paulo area) used to project local NED
# offsets into lat/lon for GPS/GLOBAL_POSITION_INT messages.
HOME_LAT = -23.5505
HOME_LON = -46.6333
HOME_ALT_M = 760.0
EARTH_RADIUS_M = 6371000.0

CRUISE_ALT_M = 50.0
LOITER_RADIUS_M = 30.0
LOITER_ANGULAR_RATE = 0.15  # rad/s


class FlightProfile:
    """Deterministic arm -> takeoff -> loiter -> land flight, sampled at any t."""

    def __init__(self, duration_s, rng):
        self.duration = duration_s
        self.rng = rng
        self.t_takeoff_end = min(30.0, duration_s * 0.1)
        self.t_land_start = max(self.t_takeoff_end, duration_s - min(30.0, duration_s * 0.1))
        # Small per-run phase offsets so repeated files at different seeds
        # aren't identical in shape, while staying deterministic per seed.
        self.phase = rng.uniform(0, 2 * math.pi)
        self.noise_seed_roll = rng.uniform(0, 2 * math.pi)
        self.noise_seed_pitch = rng.uniform(0, 2 * math.pi)

    def altitude(self, t):
        if t < self.t_takeoff_end:
            frac = t / self.t_takeoff_end if self.t_takeoff_end > 0 else 1.0
            return CRUISE_ALT_M * frac
        if t > self.t_land_start:
            span = self.duration - self.t_land_start
            frac = (t - self.t_land_start) / span if span > 0 else 1.0
            return CRUISE_ALT_M * max(0.0, 1.0 - frac)
        return CRUISE_ALT_M + 1.5 * math.sin(0.05 * t)

    def horizontal_position(self, t):
        """Local NED x,y (m) — spirals out into a loiter circle during climb
        (continuous at t_takeoff_end, so velocity stays finite) and holds the
        circle afterwards."""
        ang = self.phase + LOITER_ANGULAR_RATE * min(t, self.t_land_start)
        x = LOITER_RADIUS_M * math.sin(ang)
        y = LOITER_RADIUS_M * (1 - math.cos(ang))
        if t < self.t_takeoff_end and self.t_takeoff_end > 0:
            climb_frac = t / self.t_takeoff_end
            x *= climb_frac
            y *= climb_frac
        return x, y

    def state(self, t):
        """Return a dict of physical quantities at time t (seconds since arm)."""
        dt = 0.05
        alt = self.altitude(t)
        alt_next = self.altitude(t + dt)
        x, y = self.horizontal_position(t)
        x_next, y_next = self.horizontal_position(t + dt)
        vx = (x_next - x) / dt
        vy = (y_next - y) / dt
        vz = -(alt_next - alt) / dt  # NED: climb is negative z

        airborne = self.t_takeoff_end <= t <= self.t_land_start
        yaw = math.atan2(vy, vx) if airborne and (abs(vx) + abs(vy)) > 1e-6 else 0.0
        roll = math.radians(15.0) * math.sin(LOITER_ANGULAR_RATE * t) if airborne else 0.0
        pitch = math.radians(5.0) * math.cos(LOITER_ANGULAR_RATE * t * 0.7) if airborne else 0.0
        roll += math.radians(0.5) * math.sin(3.1 * t + self.noise_seed_roll)
        pitch += math.radians(0.5) * math.sin(2.3 * t + self.noise_seed_pitch)

        battery_frac = max(0.2, 1.0 - 0.65 * (t / self.duration if self.duration > 0 else 0))

        return {
            't': t,
            'alt_rel': alt,
            'alt_msl': alt + HOME_ALT_M,
            'x': x, 'y': y, 'z': -alt,
            'vx': vx, 'vy': vy, 'vz': vz,
            'roll': roll, 'pitch': pitch, 'yaw': yaw,
            'groundspeed': math.hypot(vx, vy),
            'airspeed': math.hypot(vx, vy) + 0.3,
            'airborne': airborne,
            'battery_frac': battery_frac,
        }

    def latlon(self, x, y):
        lat = HOME_LAT + (x / EARTH_RADIUS_M) * (180.0 / math.pi)
        lon = HOME_LON + (y / (EARTH_RADIUS_M * math.cos(math.radians(HOME_LAT)))) * (180.0 / math.pi)
        return lat, lon


def yaw_to_quaternion(yaw, roll=0.0, pitch=0.0):
    cy, sy = math.cos(yaw * 0.5), math.sin(yaw * 0.5)
    cp, sp = math.cos(pitch * 0.5), math.sin(pitch * 0.5)
    cr, sr = math.cos(roll * 0.5), math.sin(roll * 0.5)
    return [
        cr * cp * cy + sr * sp * sy,
        sr * cp * cy - cr * sp * sy,
        cr * sp * cy + sr * cp * sy,
        cr * cp * sy - sr * sp * cy,
    ]


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


class MessageFactory:
    """Builds one MAVLink message instance per type, from flight state + rng."""

    def __init__(self, mav, profile, rng):
        self.mav = mav
        self.profile = profile
        self.rng = rng
        self._param_index = 0
        self._mission_seq = 0

    def build(self, msg_type, t):
        s = self.profile.state(t)
        time_boot_ms = int(t * 1000) & 0xFFFFFFFF
        time_usec = int(t * 1e6) & 0xFFFFFFFFFFFFFFFF
        handler = getattr(self, '_build_' + msg_type.lower(), None)
        if handler is None:
            raise ValueError(f"No builder for message type {msg_type}")
        return handler(s, time_boot_ms, time_usec)

    def _build_heartbeat(self, s, tb, tu):
        base_mode = (mavlink2.MAV_MODE_FLAG_SAFETY_ARMED | mavlink2.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED
                     if s['airborne'] or s['t'] > 0 else 0)
        return self.mav.heartbeat_encode(
            mavlink2.MAV_TYPE_QUADROTOR, mavlink2.MAV_AUTOPILOT_PX4, base_mode, 0,
            mavlink2.MAV_STATE_ACTIVE if s['airborne'] else mavlink2.MAV_STATE_STANDBY)

    def _build_attitude(self, s, tb, tu):
        return self.mav.attitude_encode(
            tb, s['roll'], s['pitch'], s['yaw'],
            self.rng.uniform(-0.02, 0.02), self.rng.uniform(-0.02, 0.02), self.rng.uniform(-0.02, 0.02))

    def _build_attitude_quaternion(self, s, tb, tu):
        q = yaw_to_quaternion(s['yaw'], s['roll'], s['pitch'])
        return self.mav.attitude_quaternion_encode(
            tb, q[0], q[1], q[2], q[3],
            self.rng.uniform(-0.02, 0.02), self.rng.uniform(-0.02, 0.02), self.rng.uniform(-0.02, 0.02))

    def _build_global_position_int(self, s, tb, tu):
        lat, lon = self.profile.latlon(s['x'], s['y'])
        hdg = int((math.degrees(s['yaw']) % 360) * 100)
        return self.mav.global_position_int_encode(
            tb, int(lat * 1e7), int(lon * 1e7), int(s['alt_msl'] * 1000), int(s['alt_rel'] * 1000),
            int(clamp(s['vx'] * 100, -32768, 32767)), int(clamp(s['vy'] * 100, -32768, 32767)),
            int(clamp(s['vz'] * 100, -32768, 32767)), hdg)

    def _build_local_position_ned(self, s, tb, tu):
        return self.mav.local_position_ned_encode(tb, s['x'], s['y'], s['z'], s['vx'], s['vy'], s['vz'])

    def _build_position_target_local_ned(self, s, tb, tu):
        return self.mav.position_target_local_ned_encode(
            tb, mavlink2.MAV_FRAME_LOCAL_NED, 0, s['x'], s['y'], s['z'],
            s['vx'], s['vy'], s['vz'], 0.0, 0.0, 0.0, s['yaw'], 0.0)

    def _build_position_target_global_int(self, s, tb, tu):
        lat, lon = self.profile.latlon(s['x'], s['y'])
        return self.mav.position_target_global_int_encode(
            tb, mavlink2.MAV_FRAME_GLOBAL_RELATIVE_ALT, 0, int(lat * 1e7), int(lon * 1e7), s['alt_rel'],
            s['vx'], s['vy'], s['vz'], 0.0, 0.0, 0.0, s['yaw'], 0.0)

    def _build_servo_output_raw(self, s, tb, tu):
        base = 1500
        roll_mix = int(math.degrees(s['roll']) * 10)
        pitch_mix = int(math.degrees(s['pitch']) * 10)
        vals = [
            clamp(base + roll_mix + self.rng.randint(-3, 3), 1000, 2000),
            clamp(base + pitch_mix + self.rng.randint(-3, 3), 1000, 2000),
            clamp(base - roll_mix + self.rng.randint(-3, 3), 1000, 2000),
            clamp(base - pitch_mix + self.rng.randint(-3, 3), 1000, 2000),
        ]
        return self.mav.servo_output_raw_encode(tu, 0, *vals, 0, 0, 0, 0)

    def _build_attitude_target(self, s, tb, tu):
        q = yaw_to_quaternion(s['yaw'], s['roll'], s['pitch'])
        thrust = 0.5 if s['airborne'] else 0.0
        return self.mav.attitude_target_encode(tb, 0, q, 0.0, 0.0, 0.0, thrust)

    def _build_gps_raw_int(self, s, tb, tu):
        lat, lon = self.profile.latlon(s['x'], s['y'])
        return self.mav.gps_raw_int_encode(
            tu, 3, int(lat * 1e7), int(lon * 1e7), int(s['alt_msl'] * 1000),
            65535, 65535, int(s['groundspeed'] * 100), int((math.degrees(s['yaw']) % 360) * 100),
            self.rng.randint(10, 14))

    def _build_vfr_hud(self, s, tb, tu):
        throttle = int(clamp(30 + s['groundspeed'] * 5, 0, 100)) if s['airborne'] else 0
        return self.mav.vfr_hud_encode(
            s['airspeed'], s['groundspeed'], int(math.degrees(s['yaw']) % 360), throttle,
            s['alt_msl'], s['vz'] * -1)

    def _build_sys_status(self, s, tb, tu):
        voltage_mv = int((14.0 + 2.8 * s['battery_frac']) * 1000)
        current_ca = int(self.rng.uniform(1500, 2500))
        return self.mav.sys_status_encode(
            0, 0, 0, self.rng.randint(200, 500), voltage_mv, current_ca,
            int(s['battery_frac'] * 100), 0, 0, 0, 0, 0, 0)

    def _build_battery_status(self, s, tb, tu):
        cell_mv = int((3.5 + 0.7 * s['battery_frac']) * 1000)
        return self.mav.battery_status_encode(
            0, 0, 0, int(25 + self.rng.uniform(-2, 2)) * 100,
            [cell_mv, cell_mv, cell_mv, cell_mv, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF],
            int(self.rng.uniform(1500, 2500)), int((1 - s['battery_frac']) * 5000),
            int((1 - s['battery_frac']) * 60000), int(s['battery_frac'] * 100))

    def _build_extended_sys_state(self, s, tb, tu):
        landed = mavlink2.MAV_LANDED_STATE_IN_AIR if s['airborne'] else mavlink2.MAV_LANDED_STATE_ON_GROUND
        return self.mav.extended_sys_state_encode(mavlink2.MAV_VTOL_STATE_UNDEFINED, landed)

    def _build_home_position(self, s, tb, tu):
        q = yaw_to_quaternion(0.0)
        return self.mav.home_position_encode(
            int(HOME_LAT * 1e7), int(HOME_LON * 1e7), int(HOME_ALT_M * 1000),
            0.0, 0.0, 0.0, q, 0.0, 0.0, 0.0, tu)

    def _build_mission_current(self, s, tb, tu):
        return self.mav.mission_current_encode(self._mission_seq, 1, 0, 0)

    def _build_estimator_status(self, s, tb, tu):
        return self.mav.estimator_status_encode(
            tu, 0x1FF, self.rng.uniform(0.1, 0.3), self.rng.uniform(0.1, 0.3), self.rng.uniform(0.1, 0.3),
            self.rng.uniform(0.1, 0.3), self.rng.uniform(0.1, 0.3), self.rng.uniform(0.1, 0.3),
            self.rng.uniform(0.1, 0.5), self.rng.uniform(0.1, 0.5))

    def _build_scaled_pressure(self, s, tb, tu):
        press_abs = 1013.25 - 0.12 * s['alt_rel']
        return self.mav.scaled_pressure_encode(tb, press_abs, self.rng.uniform(-0.5, 0.5), int(2500 + self.rng.uniform(-100, 100)))

    def _build_altitude(self, s, tb, tu):
        return self.mav.altitude_encode(tu, s['alt_rel'], s['alt_msl'], s['alt_rel'], s['alt_rel'], s['alt_rel'], 0.0)

    def _build_vibration(self, s, tb, tu):
        return self.mav.vibration_encode(
            tu, abs(self.rng.gauss(0.15, 0.05)), abs(self.rng.gauss(0.15, 0.05)), abs(self.rng.gauss(0.15, 0.05)), 0, 0, 0)

    def _build_ping(self, s, tb, tu):
        return self.mav.ping_encode(tu, self._mission_seq, 0, 0)

    def _build_statustext(self, s, tb, tu):
        text = b"Synthetic flight status" if s['airborne'] else b"Synthetic flight standby"
        return self.mav.statustext_encode(mavlink2.MAV_SEVERITY_INFO, text)

    def _build_command_ack(self, s, tb, tu):
        return self.mav.command_ack_encode(mavlink2.MAV_CMD_COMPONENT_ARM_DISARM, mavlink2.MAV_RESULT_ACCEPTED)

    def _build_command_long(self, s, tb, tu):
        return self.mav.command_long_encode(1, 1, mavlink2.MAV_CMD_COMPONENT_ARM_DISARM, 0, 1, 0, 0, 0, 0, 0, 0)

    def _build_param_value(self, s, tb, tu):
        self._param_index += 1
        return self.mav.param_value_encode(
            b"SYNTH_PARAM", self.rng.uniform(0, 1), mavlink2.MAV_PARAM_TYPE_REAL32, 1, 0)

    def _build_param_request_list(self, s, tb, tu):
        return self.mav.param_request_list_encode(1, 1)

    def _build_request_data_stream(self, s, tb, tu):
        return self.mav.request_data_stream_encode(1, 1, 0, 10, 1)

    def _build_gps_global_origin(self, s, tb, tu):
        return self.mav.gps_global_origin_encode(int(HOME_LAT * 1e7), int(HOME_LON * 1e7), int(HOME_ALT_M * 1000), tu)

    def _build_file_transfer_protocol(self, s, tb, tu):
        return self.mav.file_transfer_protocol_encode(0, 1, 1, [0] * 251)

    def _build_open_drone_id_location(self, s, tb, tu):
        lat, lon = self.profile.latlon(s['x'], s['y'])
        return self.mav.open_drone_id_location_encode(
            1, 1, [0] * 20, 1, int((math.degrees(s['yaw']) % 360) * 100),
            int(s['groundspeed'] * 100), int(-s['vz'] * 100), int(lat * 1e7), int(lon * 1e7),
            s['alt_msl'], s['alt_msl'], 0, s['alt_rel'], 0, 0, 0, 0, s['t'], 0)

    def _build_open_drone_id_system(self, s, tb, tu):
        return self.mav.open_drone_id_system_encode(
            1, 1, [0] * 20, 0, 0, int(HOME_LAT * 1e7), int(HOME_LON * 1e7), 0, 0, 0.0, 0.0, 0, 0, 0.0, int(tu / 1e6))


def generate_synthetic_tlog(filename, target_hz, duration_seconds, seed=42, start_time=0.0):
    print(f"Generating {filename}: {target_hz} Hz aggregate, {duration_seconds:.0f}s duration, seed={seed}")

    total_original = sum(ORIGINAL_COUNTS.values())
    msg_intervals = {}
    for msg_type, count in ORIGINAL_COUNTS.items():
        proportion = count / total_original
        msg_target_hz = target_hz * proportion
        msg_intervals[msg_type] = 1.0 / msg_target_hz if msg_target_hz > 0 else float('inf')

    rng = random.Random(seed)
    mav = mavlink2.MAVLink(None, srcSystem=1, srcComponent=1)
    profile = FlightProfile(duration_seconds, rng)
    factory = MessageFactory(mav, profile, rng)

    # Min-heap of (next_send_time_offset, msg_type) for O(log n) scheduling.
    heap = [(0.0, msg_type) for msg_type, interval in msg_intervals.items() if interval != float('inf')]
    heapq.heapify(heap)

    total_written = 0
    with open(filename, 'wb') as f:
        while heap:
            t, msg_type = heap[0]
            if t >= duration_seconds:
                break
            heapq.heapreplace(heap, (t + msg_intervals[msg_type], msg_type))

            msg = factory.build(msg_type, t)
            msg.pack(mav)
            packet_bytes = msg.get_msgbuf()

            timestamp_us = int((start_time + t) * 1e6)
            f.write(struct.pack('>Q', timestamp_us))
            f.write(packet_bytes)
            total_written += 1

    actual_hz = total_written / duration_seconds if duration_seconds > 0 else 0
    print(f"Done! Wrote {total_written:,} messages. Actual Avg Hz: {actual_hz:.2f}\n")
    return total_written


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a synthetic, reproducible MAVLink .tlog file for latency/throughput experiments.")
    parser.add_argument("--hz", type=float, required=True, help="Target aggregate message rate (Hz), e.g. 100")
    parser.add_argument("--duration", type=float, required=True, help="Flight duration in seconds, e.g. 900 for 15 min")
    parser.add_argument("--out", required=True, help="Output .tlog path")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility (default: 42)")
    parser.add_argument("--start-time", type=float, default=1735689600.0,
                         help="Unix epoch (s) for the first message timestamp; fixed by default so output is byte-identical across runs (default: 2025-01-01T00:00:00Z)")
    return parser.parse_args()


def main():
    args = parse_args()
    generate_synthetic_tlog(args.out, args.hz, args.duration, seed=args.seed, start_time=args.start_time)


if __name__ == '__main__':
    main()
