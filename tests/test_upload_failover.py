"""Integration test for upload failover with standby nodes (k + m + n)."""

import sys
import shutil
import tempfile
import time
from pathlib import Path
import pytest

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rs_p2p.network.peer_node import PassivePeerServer
from rs_p2p.network.client import ActiveNodeClient


def test_upload_failover_with_standby_nodes():
    """Validates that if k+m+n nodes exist and one or more nodes fail during upload,

    the client dynamically fails over to the standby nodes, successfully saves
    all k+m chunks on surviving nodes, and can restore the data bit-for-bit.
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_failover_test_"))
    peers = []
    client = None
    try:
        k = 2
        m = 1
        n = 2  # 2 standby nodes -> total 5 nodes
        total_nodes = k + m + n
        base_port = 9400
        seed_endpoint = f"127.0.0.1:{base_port}"

        # 1. Start total_nodes passive peers
        for i in range(total_nodes):
            port = base_port + i
            node_id = f"failover_peer_{i}"
            peer_dir = temp_dir / node_id
            peer = PassivePeerServer(
                node_id_str=node_id,
                host="127.0.0.1",
                port=port,
                storage_dir=peer_dir,
            )
            seeds = [seed_endpoint] if i > 0 else []
            peer.start(seed_endpoints=seeds)
            peers.append(peer)

        time.sleep(1.0)

        # 2. Connect client
        client = ActiveNodeClient(
            seed_endpoints=[seed_endpoint],
            client_node_id="client_failover_tester",
            local_port=9450,
        )

        # Intentionally stop peer 1 (base_port + 1) to simulate a dead node before/during upload
        print(f"\nIntentionally stopping peer 1 ({base_port + 1}) to trigger failover...")
        peers[1].stop()
        time.sleep(0.3)

        # 3. Store checkpoint with k=2, m=1 (requires 3 distinct healthy nodes).
        # We supply all 5 endpoints as candidate prefer_passive_nodes.
        # Candidate 1 is dead, so the client must fail over to standby nodes (peer 3 or 4).
        all_endpoints = [f"127.0.0.1:{base_port + i}" for i in range(total_nodes)]
        test_payload = b"FAILOVER_TEST_ACTIVATION_TENSOR_BYTES_" * 64
        meta = {
            "source_node": "A_failover",
            "shape": [2, 128],
            "dtype": "float32",
            "sequence_id": "seq_test_failover",
        }

        manifest = client.send_checkpoint_activations(
            activation_payload=test_payload,
            checkpoint_id="chk_failover_test",
            k=k,
            m=m,
            serializer_name="tensor",
            metadata=meta,
            prefer_passive_nodes=all_endpoints,
        )

        assert manifest is not None
        assert len(manifest["chunks"]) == k + m

        # Check endpoints where chunks were stored
        stored_endpoints = [c["endpoint"] for c in manifest["chunks"]]
        dead_endpoint = f"127.0.0.1:{base_port + 1}"
        assert dead_endpoint not in stored_endpoints, f"Chunk was mistakenly assigned to dead node {dead_endpoint}"
        assert len(set(stored_endpoints)) == k + m, f"Stored endpoints must all be unique: {stored_endpoints}"

        print(f"Chunks successfully dispersed to healthy nodes: {stored_endpoints}")

        # 4. Now test restore from the manifest to guarantee 100% bit-for-bit recovery
        restored_payload, restored_meta = client.receive_backup(
            checkpoint_id="chk_failover_test",
            serializer_name="tensor",
        )

        assert restored_payload == test_payload
        assert restored_meta["shape"] == [2, 128]
        assert restored_meta["dtype"] == "float32"
        assert restored_meta["sequence_id"] == "seq_test_failover"
        print("Restoration after upload failover: 100% bit-for-bit match!")

    finally:
        if client:
            client.close()
        for p in peers:
            try:
                p.stop()
            except Exception:
                pass
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_upload_multiple_cascading_failures():
    """Validates that the upload mechanism stands MULTIPLE simultaneous and

    cascading node failures as long as there are enough spare nodes.
    Setup: 6 nodes total, k=2, m=1 (requires 3 nodes), spare n=3.
    We kill 3 nodes (2 primary candidates + 1 spare), leaving exactly 3 survivors.
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_multi_fail_test_"))
    peers = []
    client = None
    try:
        k = 2
        m = 1
        n = 3  # 3 spare nodes -> total 6 nodes
        total_nodes = k + m + n
        base_port = 9600
        seed_endpoint = f"127.0.0.1:{base_port}"

        for i in range(total_nodes):
            port = base_port + i
            node_id = f"multi_fail_peer_{i}"
            peer = PassivePeerServer(
                node_id_str=node_id,
                host="127.0.0.1",
                port=port,
                storage_dir=temp_dir / node_id,
            )
            seeds = [seed_endpoint] if i > 0 else []
            peer.start(seed_endpoints=seeds)
            peers.append(peer)

        time.sleep(1.0)

        client = ActiveNodeClient(
            seed_endpoints=[seed_endpoint],
            client_node_id="client_multi_fail_tester",
            local_port=9650,
        )

        # Intentionally stop 3 nodes: peer 1, peer 2, and spare peer 3!
        print(f"\nKilling 3 nodes: peer 1 (port {base_port+1}), peer 2 (port {base_port+2}), peer 3 (port {base_port+3})...")
        peers[1].stop()
        peers[2].stop()
        peers[3].stop()
        time.sleep(0.3)

        all_endpoints = [f"127.0.0.1:{base_port + i}" for i in range(total_nodes)]
        test_payload = b"MULTIPLE_FAILURES_SURVIVAL_CHECKPOINT_" * 32

        manifest = client.send_checkpoint_activations(
            activation_payload=test_payload,
            checkpoint_id="chk_multi_fail_test",
            k=k,
            m=m,
            serializer_name="raw",
            prefer_passive_nodes=all_endpoints,
        )

        assert manifest is not None
        assert len(manifest["chunks"]) == k + m

        stored_endpoints = [c["endpoint"] for c in manifest["chunks"]]
        dead_endpoints = {f"127.0.0.1:{base_port + 1}", f"127.0.0.1:{base_port + 2}", f"127.0.0.1:{base_port + 3}"}
        survivor_endpoints = {f"127.0.0.1:{base_port}", f"127.0.0.1:{base_port + 4}", f"127.0.0.1:{base_port + 5}"}

        # None of the dead nodes should be in the stored chunks
        assert not dead_endpoints.intersection(set(stored_endpoints)), f"Dead nodes used: {dead_endpoints.intersection(set(stored_endpoints))}"
        # All stored chunks must be on the 3 survivors
        assert set(stored_endpoints) == survivor_endpoints

        print(f"Successfully survived 3 node failures! Chunks placed on surviving nodes: {stored_endpoints}")

        # Restore from surviving nodes
        restored_payload, _ = client.receive_backup(
            checkpoint_id="chk_multi_fail_test",
            serializer_name="raw",
        )
        assert restored_payload == test_payload
        print("Restoration after multiple failures: 100% bit-for-bit match!")

    finally:
        if client:
            client.close()
        for p in peers:
            try:
                p.stop()
            except Exception:
                pass
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_upload_exhaustion_raises_runtime_error():
    """Validates that when not enough healthy nodes exist to satisfy k+m,

    the client raises a clear RuntimeError rather than hanging or corrupting the manifest.
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="rs_exhaust_test_"))
    peers = []
    client = None
    try:
        base_port = 9500
        seed_endpoint = f"127.0.0.1:{base_port}"

        # Start only 2 peers
        for i in range(2):
            port = base_port + i
            peer = PassivePeerServer(
                node_id_str=f"exhaust_peer_{i}",
                host="127.0.0.1",
                port=port,
                storage_dir=temp_dir / f"peer_{i}",
            )
            seeds = [seed_endpoint] if i > 0 else []
            peer.start(seed_endpoints=seeds)
            peers.append(peer)

        time.sleep(0.5)

        client = ActiveNodeClient(
            seed_endpoints=[seed_endpoint],
            client_node_id="client_exhaust_tester",
            local_port=9550,
        )

        # Attempt to store with k=2, m=1 (requires 3 nodes, but only 2 exist)
        with pytest.raises(RuntimeError, match="Insufficient passive nodes available"):
            client.send_checkpoint_activations(
                activation_payload=b"test_data",
                checkpoint_id="chk_exhaust",
                k=2,
                m=1,
                serializer_name="raw",
                prefer_passive_nodes=[f"127.0.0.1:{base_port}", f"127.0.0.1:{base_port + 1}"],
            )

    finally:
        if client:
            client.close()
        for p in peers:
            try:
                p.stop()
            except Exception:
                pass
        shutil.rmtree(temp_dir, ignore_errors=True)

