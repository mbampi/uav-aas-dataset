import asyncio
import gc
import logging
import csv
import socket
import time
from datetime import datetime
import argparse
import os
import sys
from asyncua import Server, ua
from asyncua.server.binary_server_asyncio import OPCUAProtocol
from asyncua.server.users import User, UserRole

# Configuration
LOG_FILE = "opcua_server_log"
SERVER_ENDPOINT = "opc.tcp://0.0.0.0:"
TARGET_NAMESPACE_URI = "http://example.org/UAV-AAS/"

class AdminUserManager:
    def get_user(self, iserver, username=None, password=None, certificate=None):
        return User(role=UserRole.Admin)

def _patch_disable_nagle():
    """Forces TCP_NODELAY on every accepted OPC UA connection.

    asyncua's create_server() never sets TCP_NODELAY, so Nagle stays enabled
    on the server's outgoing WriteResponses. Combined with the client OS's
    delayed-ACK timer, a WriteResponse sent while the connection has
    unacked data waits for that ACK instead of going out immediately —
    tens of ms per write, confirmed against this experiment's latency data
    (OPCUA_SENDING -> OPCUA_SENT clustered at ~2x/~4x a base unit, the
    signature of delayed-ACK stalls). The Go client already defaults to
    NoDelay=true, so only the server side needs this.
    """
    original_connection_made = OPCUAProtocol.connection_made

    def connection_made(self, transport):
        original_connection_made(self, transport)
        sock = transport.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass

    OPCUAProtocol.connection_made = connection_made

def make_raw_datachange_callback(queue: "asyncio.Queue[list[str]]", node_id: str):
    """Builds a callback registered directly via AddressSpace.add_datachange_callback,
    bypassing the Subscription/MonitoredItem machinery entirely.

    subscribe_data_change() would route every write through
    MonitoredItemService.datachange_callback, which does a copy.deepcopy() of
    the DataValue plus deadband-filter bookkeeping meant for real subscribing
    clients — all of it synchronous, inside the WriteRequest whose response
    the Go connector is waiting on. Nothing ever sends a PublishRequest for
    this logger (it's not a real subscriber, just an in-process CSV sink), so
    none of that is needed: just queue the raw value. This also removes the
    old subscription's publish-interval quantization on the arrival
    timestamp — it's now stamped synchronously in the write path itself.

    Enqueues rows instead of writing them inline: any blocking work done here
    — CSV encoding, and especially file.flush() — would stall the whole
    server, including in-flight OPC UA WriteRequests from other properties.
    Queuing keeps this callback down to in-memory work only; csv_writer_task
    does the actual I/O off the request path.
    """

    async def callback(handle: int, value) -> None:
        arrival_time_ms = time.time() * 1000
        val = value.Value.Value if value.Value is not None else None
        row = [f"{arrival_time_ms:.3f}", node_id, val]
        try:
            queue.put_nowait(row)
        except asyncio.QueueFull:
            # Log throughput can't keep up; drop the row rather than block
            # the event loop (and thus every other OPC UA request) waiting
            # for room in the queue.
            pass

    return callback

async def csv_writer_task(queue: "asyncio.Queue[list[str]]", writer, file_handle, flush_interval: float = 1.0):
    """Drains queued rows to disk, flushing on a timer instead of per-row.

    file.flush() is a blocking syscall; running it via run_in_executor keeps
    it off the event loop entirely instead of just reducing how often it
    stalls the loop.
    """
    loop = asyncio.get_running_loop()
    last_flush = time.monotonic()
    while True:
        try:
            row = await asyncio.wait_for(queue.get(), timeout=flush_interval)
            writer.writerow(row)
        except asyncio.TimeoutError:
            pass

        now = time.monotonic()
        if now - last_flush >= flush_interval:
            await loop.run_in_executor(None, file_handle.flush)
            last_flush = now

async def configure_nodes(server, ns_idx):
    nodes_to_monitor = []
    objects_node = server.nodes.objects
    print(f"--- Unlocking Nodes in Namespace Index {ns_idx} ---")

    async def scan_recursive(node):
        try:
            children = await node.get_children()
        except Exception:
            return

        for child in children:
            if child.nodeid.NamespaceIndex != ns_idx and child != objects_node:
                continue

            try:
                node_class = await child.read_node_class()
                if node_class == ua.NodeClass.Object:
                    await scan_recursive(child)
                elif node_class == ua.NodeClass.Variable:
                    await child.set_writable()
                    nodes_to_monitor.append(child)
            except Exception:
                continue

    await scan_recursive(objects_node)
    return nodes_to_monitor

async def main():
    parser = argparse.ArgumentParser()
    # quad_op.nodeset.xml matches the connector's current AAS (config/aas/quad_op.json)
    # and its opcua_client node IDs (ns=2;s=<Submodel>/<path>, e.g.
    # Attitude/EulerAngles/Roll). UAV_AAS.aas.nodeset.xml uses an incompatible
    # structure (UAV/ActuatorSpec/...) — writes to it would target unknown nodes
    # and be silently dropped.
    parser.add_argument("xml_file", nargs='?', default="quad_op.nodeset.xml")
    parser.add_argument("port", nargs='?', default="4840")
    args = parser.parse_args()

    if not os.path.exists(args.xml_file):
        print(f"Error: {args.xml_file} not found.")
        sys.exit(1)

    _patch_disable_nagle()

    server = Server(user_manager=AdminUserManager())
    await server.init()
    server_endpoint = f'{SERVER_ENDPOINT}{args.port}/'
    server.set_endpoint(server_endpoint)
    server.set_server_name("UAV AAS Server")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    # Every Value write otherwise pays datetime.now() + a full dataclasses.replace()
    # (address_space.py's AttributeService.write) to stamp a ServerTimestamp nobody
    # reads: the CSV log stamps its own arrival time independently, and the Go
    # connector only looks at the write StatusCode results.
    server.set_force_server_timestamp(False)

    print(f"📥 Importing: {args.xml_file}")
    await server.import_xml(args.xml_file)
    
    ns_idx = await server.get_namespace_index(TARGET_NAMESPACE_URI)
    print(f"✅ Loaded. Namespace Index: {ns_idx}")

    nodes = await configure_nodes(server, ns_idx)

    # The address space (nodes, attribute dicts, references) is fully built
    # now and stays alive for the rest of the process. CPython's cyclic GC
    # rescans every tracked container on each gen1/gen2 pass regardless of
    # whether it's actually garbage, and this address space is large enough
    # to make that scan cost ~90-100ms — long enough to stall the
    # single-threaded event loop and hold up every in-flight WriteRequest at
    # once (confirmed: latency spikes hit many concurrent writes
    # simultaneously with near-identical durations, the signature of a
    # stop-the-world pause rather than per-request cost). Freezing it here
    # excludes it from all future scans, so collections only look at
    # genuinely new, mostly short-lived objects (per-write DataValue/Variant
    # churn); the higher thresholds make those now-cheap passes fire less
    # often too.
    gc.collect()
    gc.freeze()
    gc.set_threshold(50_000, 100, 100)

    logfile = f'{LOG_FILE}{args.port}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

    with open(logfile, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Arrival_Timestamp_Ms", "Node_ID", "Value_Raw"])
        f.flush()
        
        queue: "asyncio.Queue[list[str]]" = asyncio.Queue(maxsize=10_000)
        writer_task = asyncio.create_task(csv_writer_task(queue, writer, f))

        if nodes:
            print(f"--- Registering {len(nodes)} raw datachange callbacks ---")
            for node in nodes:
                node_id = node.nodeid.to_string()
                callback = make_raw_datachange_callback(queue, node_id)
                server.iserver.aspace.add_datachange_callback(node.nodeid, ua.AttributeIds.Value, callback)

        print(f"🚀 Server Running.")
        print(f"Use namespaceIndex: {ns_idx} in client config.")
        print(f"🔗 Endpoint: {server_endpoint}")
        print(f"📄 Logging to: {logfile}")

        try:
            async with server:
                while True:
                    await asyncio.sleep(1)
        finally:
            # Drain whatever's still queued and flush once more before the
            # `with open(...)` block closes the file out from under the task.
            writer_task.cancel()
            try:
                await writer_task
            except asyncio.CancelledError:
                pass
            while not queue.empty():
                writer.writerow(queue.get_nowait())
            f.flush()

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Server Stopped")