#!/usr/bin/env python3
"""MQTT arrival logger for the UAV-AAS end-to-end latency experiment.

Subscribes to the AAS update topics published by the uav-aas-connector's MQTT
exporter and records an arrival timestamp for every message, using the SAME CSV
schema as opcua_server.py so latency-calculate.py can process either identically.

Connector topic layout (see uav-aas-connector/pkg/exporters/mqtt/mqtt.go):
    <prefix>/submodels/<base64url(submodelId)>/elements/<IdShortPath>
payload (JSON):
    {"value": <v>, "timestamp": "<RFC3339Nano>"}

The "Node_ID" column is set to the property path after "/elements/"
(e.g. "Navigation/LocalPositionNED/X") so it matches the suffix of the OPC UA
Node_ID and latency-calculate's --seq-node-suffix works for both transports.

Run the broker (e.g. mosquitto) and this receiver on THIS machine so the arrival
timestamp shares a clock with replay.py's dispatch timestamp.

    pip install paho-mqtt
    python3 mqtt_receiver.py --host localhost --port 1883 --prefix aas/uav-1
"""

import argparse
import csv
import json
import time

import paho.mqtt.client as mqtt


def parse_args():
    p = argparse.ArgumentParser(description="MQTT receiver / arrival logger")
    p.add_argument("--host", default="localhost", help="MQTT broker host (default: localhost)")
    p.add_argument("--port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    p.add_argument("--prefix", default="aas/uav-1",
                   help="topic_prefix configured on the connector's mqtt exporter (default: aas/uav-1)")
    p.add_argument("--log", default="mqtt_received_log.csv", help="CSV output for arrival timestamps")
    p.add_argument("--qos", type=int, default=0, help="Subscription QoS (default: 0, matches the exporter)")
    p.add_argument("--username", default=None)
    p.add_argument("--password", default=None)
    return p.parse_args()


def node_id_from_topic(topic):
    """Return the property path after '/elements/', matching the OPC UA Node_ID suffix."""
    marker = "/elements/"
    idx = topic.find(marker)
    if idx >= 0:
        return topic[idx + len(marker):]
    return topic


def main():
    args = parse_args()

    f = open(args.log, "w", newline="")
    writer = csv.writer(f)
    writer.writerow(["Arrival_Timestamp_Ms", "Node_ID", "Value_Raw"])
    f.flush()

    sub_topic = args.prefix.rstrip("/") + "/#"

    def on_connect(client, userdata, flags, reason_code, properties=None):
        rc = getattr(reason_code, "value", reason_code)
        if rc != 0:
            print(f"❌ Connect failed (rc={reason_code})")
            return
        client.subscribe(sub_topic, qos=args.qos)
        print(f"🔗 Connected to {args.host}:{args.port}")
        print(f"📡 Subscribed to: {sub_topic}")
        print(f"📄 Logging to: {args.log}")

    def on_message(client, userdata, msg):
        # Stamp arrival first, before any parsing work.
        arrival_ms = time.time() * 1000
        node_id = node_id_from_topic(msg.topic)

        # Default to the raw payload; extract the scalar "value" when it parses.
        value = msg.payload.decode("utf-8", "replace")
        try:
            obj = json.loads(msg.payload)
            if isinstance(obj, dict) and "value" in obj:
                value = obj["value"]
        except (ValueError, TypeError):
            pass

        writer.writerow([f"{arrival_ms:.3f}", node_id, value])
        f.flush()

    # paho-mqtt 2.x requires an explicit callback API version; fall back for 1.x.
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        client = mqtt.Client()

    if args.username:
        client.username_pw_set(args.username, args.password)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(args.host, args.port, keepalive=60)
    print("🚀 MQTT receiver running. Ctrl+C to stop.")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n🛑 Receiver stopped")
    finally:
        f.close()


if __name__ == "__main__":
    main()
