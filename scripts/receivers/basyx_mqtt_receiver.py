#!/usr/bin/env python3
"""MQTT arrival logger for the BaSyx baseline's end-to-end latency experiment.

Same purpose as uav-aas-tools/mqtt_receiver/mqtt_receiver.py (which logs
arrivals from the Go connector's own MQTT exporter), but for BaSyx's Submodel
Repository MQTT eventing feature, which uses a different, BaSyx-defined
topic layout and payload shape - it is NOT a drop-in target for the
connector's own receiver:

Two BaSyx bridges write to this baseline, and they trigger two DIFFERENT
MQTT topics/payload shapes - both handled here (added 2026-08-14, after a
live 200 Hz batched-bridge run came back with 0 rows in this receiver's
log despite ~107k successful PATCHes in the bridge's own flush CSV; see
uav-aas-connector/baseline/README.md's BaSyx section for the full writeup):

1. bridge.py (naive, one PATCH per property, .../submodel-elements/{path}/$value)
   -> SubmodelRepository.setSubmodelElementValue() -> MqttSubmodelRepository
   publishes on:
       sm-repository/<repoId>/submodels/<base64url(submodelId)>/submodelElements/<idShortPath>/value/updated
   payload (JSON): the single updated SubmodelElement, e.g.
       {"idShort": "X", "modelType": "Property", "valueType": "xs:double", "value": "1.23"}

2. bridge_batched.py (whole-submodel PATCH .../$value, full element-list
   replace) -> SubmodelRepository.patchSubmodelElements() -> the SAME
   decorator instead publishes on a DIFFERENT topic:
       sm-repository/<repoId>/submodels/<base64url(submodelId)>/submodelElements/patched
   payload (JSON): an ARRAY of every SubmodelElement in that submodel (not
   one element) - Property leaves as in (1), SubmodelElementCollection
   entries nested one level via their own "value" array. Confirmed against
   MqttSubmodelRepository.java (submodelElementsPatched) and
   MqttSubmodelRepositoryTopicFactory.java (createPatchSubmodelElementsTopic)
   in eclipse-basyx/basyx-java-server-sdk directly, not assumed.

Both cases:
  - <repoId> defaults to "sm-repo" (basyx.smrepo.name), NOT configured by
    this baseline's application.properties, so left at its default.
  - idShortPath segments are '.'-separated (e.g. "LocalPositionNED.X" or,
    recursively reconstructed from case 2's nested arrays, "GlobalPosition.
    Latitude"), NOT '/'-separated like the connector's own MQTT exporter
    topics or the OPC UA baseline's Node_IDs.
  - Property.value is serialized as a JSON string per the AAS V3 schema,
    regardless of valueType - same convention as config/aas/quad_op.json.

A submodelElements/patched message expands into one CSV row per leaf
Property, all sharing that message's single arrival timestamp (they arrived
together, in one MQTT publish) - see _iter_leaf_properties below.

Output uses the SAME CSV schema as uav-aas-tools/mqtt_receiver.py
(Arrival_Timestamp_Ms, Node_ID, Value_Raw) specifically so
uav-aas-tools/latency-calculate/latency-calculate.py works UNMODIFIED against
this baseline's log - just pass a dot-separated --seq-node-suffix instead of
the connector's slash-separated default, e.g.:

    python3 latency-calculate.py dispatch_log.csv basyx_mqtt_received_log.csv \\
        --seq-node-suffix LocalPositionNED.X

Run the broker (e.g. mosquitto) and this receiver on THIS machine (the
MacBook) so the arrival timestamp shares a clock with replay.py's dispatch
timestamp - no cross-host clock skew. See ../README.md Phase 3.

    pip install paho-mqtt
    python3 basyx_mqtt_receiver.py --host localhost --port 1883
"""

import argparse
import csv
import json
import time

import paho.mqtt.client as mqtt


def parse_args():
    p = argparse.ArgumentParser(description="BaSyx MQTT receiver / arrival logger")
    p.add_argument("--host", default="localhost", help="MQTT broker host (default: localhost)")
    p.add_argument("--port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    p.add_argument("--repo-id", default="sm-repo",
                   help="BaSyx submodel repository name / basyx.smrepo.name (default: sm-repo, "
                        "BaSyx's own default - not overridden by this baseline's application.properties)")
    p.add_argument("--log", default="basyx_mqtt_received_log.csv", help="CSV output for arrival timestamps")
    p.add_argument("--qos", type=int, default=0, help="Subscription QoS (default: 0)")
    p.add_argument("--username", default=None)
    p.add_argument("--password", default=None)
    return p.parse_args()


VALUE_UPDATED_MARKER = "/submodelElements/"
VALUE_UPDATED_SUFFIX = "/value/updated"
PATCHED_SUFFIX = "/submodelElements/patched"


def node_id_from_topic(topic):
    """Return the '.'-separated idShortPath, matching latency-calculate's Node_ID join key."""
    idx = topic.find(VALUE_UPDATED_MARKER)
    if idx < 0:
        return topic
    rest = topic[idx + len(VALUE_UPDATED_MARKER):]
    if rest.endswith(VALUE_UPDATED_SUFFIX):
        rest = rest[: -len(VALUE_UPDATED_SUFFIX)]
    return rest


def _iter_leaf_properties(elements, prefix=""):
    """Yields ('.'-separated idShortPath, value) for every Property in a
    submodelElements/patched payload, recursively descending into
    SubmodelElementCollection.value - mirrors bridge_batched.py's own
    _walk_elements/_serialize_elements, so the paths produced here match
    the naive bridge's node_id_from_topic() convention exactly (only
    Property and SubmodelElementCollection appear in config/aas/quad_op.json,
    same assumption bridge_batched.py makes)."""
    for el in elements:
        id_short = el.get("idShort", "")
        path = f"{prefix}.{id_short}" if prefix else id_short
        if el.get("modelType") == "SubmodelElementCollection":
            yield from _iter_leaf_properties(el.get("value", []), path)
        else:
            yield path, el.get("value")


def main():
    args = parse_args()

    f = open(args.log, "w", newline="")
    writer = csv.writer(f)
    writer.writerow(["Arrival_Timestamp_Ms", "Node_ID", "Value_Raw"])
    f.flush()

    # Subscribe broadly under the repo and filter to value/updated in
    # on_message - simpler than building every possible wildcard, and this
    # is a benchmarking tool, not a production subscriber.
    sub_topic = f"sm-repository/{args.repo_id}/submodels/#"

    def on_connect(client, userdata, flags, reason_code, properties=None):
        rc = getattr(reason_code, "value", reason_code)
        if rc != 0:
            print(f"Connect failed (rc={reason_code})")
            return
        client.subscribe(sub_topic, qos=args.qos)
        print(f"Connected to {args.host}:{args.port}")
        print(f"Subscribed to: {sub_topic}")
        print(f"Logging to: {args.log}")

    def on_message(client, userdata, msg):
        # Stamp arrival first, before any parsing work.
        arrival_ms = time.time() * 1000

        if msg.topic.endswith(PATCHED_SUFFIX):
            # Batched bridge (bridge_batched.py): one message per
            # whole-submodel PATCH .../$value, payload is a JSON ARRAY of
            # every element in that submodel, not a single element - see
            # module docstring. Expand into one row per leaf property, all
            # sharing this message's arrival timestamp.
            try:
                elements = json.loads(msg.payload)
            except (ValueError, TypeError):
                return
            if not isinstance(elements, list):
                return
            for node_id, value in _iter_leaf_properties(elements):
                writer.writerow([f"{arrival_ms:.3f}", node_id, value])
            f.flush()
            return

        if not msg.topic.endswith(VALUE_UPDATED_SUFFIX):
            # Submodel-level created/updated/deleted events, or element
            # created/updated/deleted (full-element, not value-only) events
            # also land under submodels/# - only value/updated (naive
            # bridge, handled below) and submodelElements/patched (batched
            # bridge, handled above) correspond to an actual bridge write.
            return

        node_id = node_id_from_topic(msg.topic)

        # Default to the raw payload; extract the scalar "value" field of
        # the serialized SubmodelElement when it parses (see module docstring
        # for the payload shape).
        value = msg.payload.decode("utf-8", "replace")
        try:
            obj = json.loads(msg.payload)
            if isinstance(obj, dict) and "value" in obj:
                value = obj["value"]
        except (ValueError, TypeError):
            pass

        writer.writerow([f"{arrival_ms:.3f}", node_id, value])
        f.flush()

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        client = mqtt.Client()

    if args.username:
        client.username_pw_set(args.username, args.password)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(args.host, args.port, keepalive=60)
    print("BaSyx MQTT receiver running. Ctrl+C to stop.")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nReceiver stopped")
    finally:
        f.close()


if __name__ == "__main__":
    main()
