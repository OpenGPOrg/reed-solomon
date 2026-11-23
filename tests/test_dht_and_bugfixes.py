"""Unit & integration tests covering DHT correctness, bug fixes, and passive RAID offload."""

from pathlib import Path
import hashlib
import shutil
import socket
import sys
import tempfile
import threading
import time
import grpc

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rs_p2p.config import NS_STORAGE_INDEX
from rs_p2p.dht.kademlia import KademliaNode
from rs_p2p.dht.node import Contact, RoutingTable, hash_key, int_to_hex, xor_distance
from rs_p2p.network.client import ActiveNodeClient
from rs_p2p.network.peer_node import PassivePeerServer
from rs_p2p.proto import dht_pb2, dht_pb2_grpc
from rs_p2p.storage.disk import DiskStorage


def test_atomic_disk_storage():
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_disk_test_"))
    try:
        disk = DiskStorage(temp_dir, node_id="disk_test")
        written = disk.write_chunk("chk_1_chunk_0", b"hello-world")
        assert written == 11
        assert disk.read_chunk("chk_1_chunk_0") == b"hello-world"
        assert disk.has_chunk("chk_1_chunk_0") == (True, 11)
        assert list(temp_dir.glob("*.tmp")) == []

        # Overwrite atomically
        disk.write_chunk("chk_1_chunk_0", b"updated-bytes")
        assert disk.read_chunk("chk_1_chunk_0") == b"updated-bytes"
        assert list(temp_dir.glob("*.tmp")) == []

        for bad_id in ("", ".", "..", "..."):
            try:
                disk.write_chunk(bad_id, b"x")
                raise AssertionError(f"Expected ValueError for bad chunk_id {bad_id!r}")
            except ValueError:
                pass
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    print("✓ test_atomic_disk_storage passed")


def test_active_client_does_not_pollute_routing_table():
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_pollution_test_"))
    peers = []
    client = None
    ch = None
    try:
        seed_ep = "127.0.0.1:9300"
        p0 = PassivePeerServer("p0", host="127.0.0.1", port=9300, storage_dir=temp_dir / "p0")
        p1 = PassivePeerServer("p1", host="127.0.0.1", port=9301, storage_dir=temp_dir / "p1")
        p0.start()
        p1.start(seed_endpoints=[seed_ep])
        peers.extend([p0, p1])

        client = ActiveNodeClient(seed_endpoints=[seed_ep], client_node_id="active_client_1")

        # Send direct bogus/non-passive contacts via gRPC RPCs
        ch = grpc.insecure_channel(seed_ep)
        stub = dht_pb2_grpc.DHTServiceStub(ch)
        bad_senders = [
            dht_pb2.NodeContact(node_id=int_to_hex(hash_key("bad_active")), host="127.0.0.1", port=9999, role="active"),
            dht_pb2.NodeContact(node_id=int_to_hex(hash_key("bad_port0")), host="127.0.0.1", port=0, role="passive"),
            dht_pb2.NodeContact(node_id=int_to_hex(hash_key("bad_0000")), host="0.0.0.0", port=7777, role="passive"),
        ]
        for sender in bad_senders:
            stub.Ping(dht_pb2.PingRequest(sender=sender), timeout=2.0)
            stub.FindNode(dht_pb2.FindNodeRequest(sender=sender, target_id=sender.node_id), timeout=2.0)
            stub.FindValue(dht_pb2.FindValueRequest(sender=sender, namespace=NS_STORAGE_INDEX, key="k"), timeout=2.0)
            stub.StoreValue(
                dht_pb2.StoreValueRequest(sender=sender, namespace=NS_STORAGE_INDEX, key="k", value=b"v", ttl_seconds=60),
                timeout=2.0,
            )

        for p in peers:
            contacts = p.dht.routing_table.all_contacts()
            for c in contacts:
                assert c.role == "passive", f"Unexpected non-passive contact: {c}"
                assert c.port > 0, f"Unexpected port<=0 contact: {c}"
                assert c.host not in ("", "0.0.0.0", "::"), f"Unroutable host: {c}"
            assert all(c.port in (9300, 9301) for c in contacts)
    finally:
        if ch is not None:
            ch.close()
        if client is not None:
            client.close()
        for p in peers:
            p.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
    print("✓ test_active_client_does_not_pollute_routing_table passed")


def test_rpc_ping_uses_responder_and_deduplicates_endpoints():
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_ping_dedup_"))
    p0 = None
    probe = None
    try:
        p0 = PassivePeerServer(
            "actual_unique_seed_name",
            host="127.0.0.1",
            port=9310,
            storage_dir=temp_dir / "p0",
        )
        p0.start()

        probe = KademliaNode(
            node_id=hash_key("probe"),
            host="127.0.0.1",
            port=0,
            role="active",
        )
        # Guessed ID deliberately differs from hash_key("actual_unique_seed_name")
        guessed = Contact(
            node_id=hash_key("127.0.0.1:9310"),
            host="127.0.0.1",
            port=9310,
            role="passive",
        )
        assert guessed.node_id != p0.dht.contact.node_id
        assert probe.rpc_ping(guessed)

        contacts = probe.routing_table.all_contacts()
        assert len(contacts) == 1
        assert contacts[0].node_id == p0.dht.contact.node_id

        # Cross-bucket deduplication for same host:port with different node_ids
        rt = RoutingTable(local_id=hash_key("local"))
        assert rt.add_contact(Contact(node_id=hash_key("id_alpha"), host="10.0.0.5", port=8000, role="passive"))
        assert rt.add_contact(Contact(node_id=hash_key("id_beta"), host="10.0.0.5", port=8000, role="passive"))
        all_c = rt.all_contacts()
        assert len(all_c) == 1
        assert all_c[0].node_id == hash_key("id_beta")
    finally:
        if probe is not None:
            probe.close()
        if p0 is not None:
            p0.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
    print("✓ test_rpc_ping_uses_responder_and_deduplicates_endpoints passed")


def test_find_value_target_and_lookup_node_dead_pruning():
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_lookup_bugs_"))
    peers = []
    client_dht = None
    ch = None
    try:
        # Bug C: Client-mode iterative_store with no reachable peers returns 0
        client_dht = KademliaNode(node_id=hash_key("lonely_client"), host="127.0.0.1", port=0, role="active")
        assert client_dht.iterative_store(NS_STORAGE_INDEX, "k1", b"val") == 0
        assert not client_dht.store_manifest("m1", {"a": 1})

        # Start 4 real passive peers
        base_port = 9320
        seed_ep = f"127.0.0.1:{base_port}"
        for i in range(4):
            p = PassivePeerServer(
                f"node_{i}",
                host="127.0.0.1",
                port=base_port + i,
                storage_dir=temp_dir / f"node_{i}",
            )
            p.start(seed_endpoints=[seed_ep] if i > 0 else [])
            peers.append(p)
        time.sleep(0.3)

        # Bug A1: Direct FindValue orders closer_contacts by hash_key(request.key), not sender.node_id
        target_key = "special_find_value_key"
        key_hash = hash_key(target_key)
        sender_id = key_hash ^ ((1 << 160) - 1)  # Maximally opposite to key_hash!
        ch = grpc.insecure_channel(seed_ep)
        stub = dht_pb2_grpc.DHTServiceStub(ch)
        resp = stub.FindValue(
            dht_pb2.FindValueRequest(
                sender=dht_pb2.NodeContact(node_id=int_to_hex(sender_id), host="127.0.0.1", port=0, role="active"),
                namespace=NS_STORAGE_INDEX,
                key=target_key,
            ),
            timeout=3.0,
        )
        assert not resp.found
        returned_ids = [int(c.node_id, 16) for c in resp.closer_contacts]
        assert len(returned_ids) >= 2
        dists = [xor_distance(nid, key_hash) for nid in returned_ids]
        assert dists == sorted(dists), "FindValue closer_contacts were not sorted by distance to hash(key)"

        # Bug A2: iterative_find_value follows closer_contacts & preserves empty bytes b""
        # Store empty bytes ONLY on peer 3, and give client_dht ONLY peer 0 as initial contact
        peers[3].dht.local_store(NS_STORAGE_INDEX, "empty_key", b"", ttl_seconds=600)
        client_dht.rpc_ping(Contact(node_id=peers[0].dht.contact.node_id, host="127.0.0.1", port=base_port, role="passive"))
        found_val = client_dht.iterative_find_value(NS_STORAGE_INDEX, "empty_key")
        assert found_val == b"", f"Expected empty bytes b'', got {found_val!r}"

        # Bug B: lookup_node does not stop prematurely after ALPHA=3 dead closest candidates
        target_id = hash_key("dead_node_bypass_target")
        querier = KademliaNode(node_id=hash_key("querier_b"), host="127.0.0.1", port=0, role="active", k=4)
        # Insert 3 dead contacts with XOR distances 1, 2, 3 from target_id
        for offset in (1, 2, 3):
            querier.routing_table.add_contact(
                Contact(node_id=target_id ^ offset, host="127.0.0.1", port=9390 + offset, role="passive")
            )
        # Insert 2 live contacts further away
        for idx in (0, 1):
            querier.routing_table.add_contact(peers[idx].dht.contact)

        live_closest = querier.lookup_node(target_id, count=4)
        live_ports = {c.port for c in live_closest}
        assert len(live_closest) >= 2
        assert {base_port, base_port + 1}.issubset(live_ports)
        assert all(c.port not in (9391, 9392, 9393) for c in live_closest)
        querier.close()
    finally:
        if ch is not None:
            ch.close()
        if client_dht is not None:
            client_dht.close()
        for p in peers:
            p.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
    print("✓ test_find_value_target_and_lookup_node_dead_pruning passed")


def test_seed_restart_persistence_and_republish():
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_seed_restart_"))
    peers = []
    clients = []
    try:
        k, m = 4, 2
        total_peers = k + m + 1  # Keep k+m peers online even while seed is stopped
        base_port = 9340
        seed_ep = f"127.0.0.1:{base_port}"

        for i in range(total_peers):
            p = PassivePeerServer(
                f"persist_node_{i}",
                host="127.0.0.1",
                port=base_port + i,
                storage_dir=temp_dir / f"persist_node_{i}",
                refresh_interval=0.35,
                republish_interval=0.45,
            )
            p.start(seed_endpoints=[seed_ep] if i > 0 else [])
            peers.append(p)

        time.sleep(0.4)

        c1 = ActiveNodeClient(seed_endpoints=[seed_ep], client_node_id="c1")
        clients.append(c1)
        payload1 = b"CHECKPOINT_BEFORE_SEED_RESTART_" * 100
        c1.send_checkpoint_activations(
            activation_payload=payload1,
            checkpoint_id="chk_before_restart",
            k=k,
            m=m,
            serializer_name="raw",
        )

        # Stop the seed node (peer 0)
        peers[0].stop()
        seed_dir = temp_dir / "persist_node_0"
        assert (seed_dir / "routing_table.json").is_file()
        assert (seed_dir / "dht_store.json").is_file()

        # Store a second checkpoint while seed is offline via peer 1
        c2 = ActiveNodeClient(seed_endpoints=[f"127.0.0.1:{base_port + 1}"], client_node_id="c2")
        clients.append(c2)
        payload2 = b"CHECKPOINT_WHILE_SEED_DOWN_" * 100
        c2.send_checkpoint_activations(
            activation_payload=payload2,
            checkpoint_id="chk_while_down",
            k=k,
            m=m,
            serializer_name="raw",
        )

        # Restart seed node with empty seed_endpoints — must reload routing table & store
        restarted_seed = PassivePeerServer(
            "persist_node_0",
            host="127.0.0.1",
            port=base_port,
            storage_dir=seed_dir,
            refresh_interval=0.35,
            republish_interval=0.45,
        )
        peers[0] = restarted_seed
        restarted_seed.start(seed_endpoints=[])

        assert len(restarted_seed.dht.routing_table.all_contacts()) >= 1
        assert restarted_seed.dht.local_get(NS_STORAGE_INDEX, "checkpoint:chk_before_restart") is not None

        # Fresh client connecting ONLY to restarted seed must restore BOTH checkpoints
        c3 = ActiveNodeClient(seed_endpoints=[seed_ep], client_node_id="c3")
        clients.append(c3)
        r1, _ = c3.receive_backup("chk_before_restart", serializer_name="raw")
        r2, _ = c3.receive_backup("chk_while_down", serializer_name="raw")
        assert r1 == payload1
        assert r2 == payload2

        # Wait up to ~2.5s for surviving peers to republish chk_while_down to restarted seed
        deadline = time.time() + 2.5
        replicated = False
        while time.time() < deadline:
            if restarted_seed.dht.local_get(NS_STORAGE_INDEX, "checkpoint:chk_while_down") is not None:
                replicated = True
                break
            time.sleep(0.15)
        assert replicated, "Expected surviving nodes to republish chk_while_down to restarted seed"
    finally:
        for c in clients:
            c.close()
        for p in peers:
            p.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
    print("✓ test_seed_restart_persistence_and_republish passed")


def test_passive_raid_offload_local_chunk_and_failover():
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_failover_test_"))
    peers = []
    client = None
    try:
        k, m = 4, 2
        total_peers = 7  # 1 extra healthy peer for dispersal failover
        base_port = 9360
        seed_ep = f"127.0.0.1:{base_port}"

        for i in range(total_peers):
            p = PassivePeerServer(
                f"raid_peer_{i}",
                host="127.0.0.1",
                port=base_port + i,
                storage_dir=temp_dir / f"raid_peer_{i}",
            )
            p.start(seed_endpoints=[seed_ep] if i > 0 else [])
            peers.append(p)
        time.sleep(0.4)

        client = ActiveNodeClient(seed_endpoints=[seed_ep], client_node_id="active_failover")

        # 1. Relay failover: pass dead preferred node -> client falls back to live passive peer
        payload = b"FAILOVER_CHECKPOINT_BYTES_" * 200
        manifest = client.send_checkpoint_activations(
            activation_payload=payload,
            checkpoint_id="chk_relay_failover",
            k=k,
            m=m,
            serializer_name="raw",
            prefer_passive_node="127.0.0.1:9399",  # dead port
        )
        coord_ep = manifest["coordinator_endpoint"]
        assert coord_ep != "127.0.0.1:9399"
        assert manifest["chunks"][0]["endpoint"] == coord_ep
        assert len({c["endpoint"] for c in manifest["chunks"]}) == k + m

        coord_idx = int(coord_ep.rsplit(":", 1)[1]) - base_port
        assert peers[coord_idx].storage.has_chunk("chk_relay_failover_chunk_0")[0]

        # 2. Load balancing across relays on repeated checkpoints
        coordinators = set()
        for j in range(6):
            mf = client.send_checkpoint_activations(
                activation_payload=payload,
                checkpoint_id=f"chk_lb_{j}",
                k=k,
                m=m,
                serializer_name="raw",
            )
            coordinators.add(mf["coordinator_endpoint"])
        assert len(coordinators) >= 2, f"Expected random relay distribution, got only {coordinators}"

        # 3. Dispersal failover inside passive coordinator:
        # Simulate disk failure on the closest remote peer while it remains DHT-reachable.
        chk_failover_id = "chk_dispersal_failover"
        chk_target_hash = hash_key(chk_failover_id)
        failing_peer = min(
            peers[1:],
            key=lambda p: xor_distance(p.dht.contact.node_id, chk_target_hash),
        )
        orig_write_chunk = failing_peer.storage.write_chunk

        def failing_write_chunk(chunk_id: str, data: bytes) -> int:
            raise OSError("simulated disk full")

        failing_peer.storage.write_chunk = failing_write_chunk
        try:
            mf_failover = client.send_checkpoint_activations(
                activation_payload=payload,
                checkpoint_id=chk_failover_id,
                k=k,
                m=m,
                serializer_name="raw",
                prefer_passive_node=seed_ep,
            )
            used_eps = {c["endpoint"] for c in mf_failover["chunks"]}
            assert len(used_eps) == k + m
            assert failing_peer.dht.contact.endpoint not in used_eps
        finally:
            failing_peer.storage.write_chunk = orig_write_chunk

        restored, _ = client.receive_backup(chk_failover_id, serializer_name="raw")
        assert restored == payload
    finally:
        if client is not None:
            client.close()
        for p in peers:
            p.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
    print("✓ test_passive_raid_offload_local_chunk_and_failover passed")


def test_large_payload_exceeding_4mb_and_fast_early_restore():
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_large_fast_"))
    peers = []
    client = None
    stall_sock = None
    stall_stop = threading.Event()
    accepted_conns = []
    try:
        k, m = 4, 2
        base_port = 9380
        seed_ep = f"127.0.0.1:{base_port}"
        for i in range(k + m):
            p = PassivePeerServer(
                f"large_node_{i}",
                host="127.0.0.1",
                port=base_port + i,
                storage_dir=temp_dir / f"large_node_{i}",
            )
            p.start(seed_endpoints=[seed_ep] if i > 0 else [])
            peers.append(p)
        time.sleep(0.4)

        client = ActiveNodeClient(seed_endpoints=[seed_ep], client_node_id="active_large")

        # Bug D: 5 MiB payload exceeds default 4 MiB gRPC message limit
        big_payload = (b"LARGE_TENSOR_SLICE_0123456789ABCDEF" * ((5 * 1024 * 1024) // 35 + 1))[
            : 5 * 1024 * 1024
        ]
        manifest = client.send_checkpoint_activations(
            activation_payload=big_payload,
            checkpoint_id="chk_5mb",
            k=k,
            m=m,
            serializer_name="raw",
        )
        restored_big, _ = client.receive_backup("chk_5mb", serializer_name="raw")
        assert hashlib.sha256(restored_big).digest() == hashlib.sha256(big_payload).digest()

        # Bug E: Early termination must not wait for stalled endpoints (10s timeout)
        stall_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stall_sock.bind(("127.0.0.1", 0))
        stall_sock.listen(8)
        stall_port = stall_sock.getsockname()[1]

        def _accept_and_stall():
            stall_sock.settimeout(0.2)
            while not stall_stop.is_set():
                try:
                    conn, _ = stall_sock.accept()
                    accepted_conns.append(conn)
                except socket.timeout:
                    continue
                except OSError:
                    break

        t = threading.Thread(target=_accept_and_stall, daemon=True)
        t.start()

        # Store a manifest with chunks 0 & 1 pointing to stalled TCP socket, chunks 2..5 live
        small_payload = b"FAST_EARLY_RESTORE_DATA_" * 50
        fast_mf = client.send_checkpoint_activations(
            activation_payload=small_payload,
            checkpoint_id="chk_fast_base",
            k=k,
            m=m,
            serializer_name="raw",
        )
        stalled_mf = dict(fast_mf)
        stalled_mf["checkpoint_id"] = "chk_fast_stalled"
        stalled_chunks = [dict(c) for c in fast_mf["chunks"]]
        stalled_chunks[0]["endpoint"] = f"127.0.0.1:{stall_port}"
        stalled_chunks[1]["endpoint"] = f"127.0.0.1:{stall_port}"
        stalled_mf["chunks"] = stalled_chunks
        peers[0].dht.store_manifest("chk_fast_stalled", stalled_mf)

        t0 = time.monotonic()
        restored_fast, _ = client.receive_backup("chk_fast_stalled", serializer_name="raw")
        elapsed = time.monotonic() - t0
        assert restored_fast == small_payload
        assert elapsed < 2.5, f"receive_backup hung on stalled peers: {elapsed:.2f}s"
    finally:
        stall_stop.set()
        if client is not None:
            client.close()
        for conn in accepted_conns:
            try:
                conn.close()
            except OSError:
                pass
        if stall_sock is not None:
            stall_sock.close()
        for p in peers:
            p.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
    print("✓ test_large_payload_exceeding_4mb_and_fast_early_restore passed")


if __name__ == "__main__":
    test_atomic_disk_storage()
    test_active_client_does_not_pollute_routing_table()
    test_rpc_ping_uses_responder_and_deduplicates_endpoints()
    test_find_value_target_and_lookup_node_dead_pruning()
    test_seed_restart_persistence_and_republish()
    test_passive_raid_offload_local_chunk_and_failover()
    test_large_payload_exceeding_4mb_and_fast_early_restore()
    print("\nAll DHT, bugfix, and passive-RAID tests passed!")
