"""End-to-end integration test for decoupled Reed-Solomon P2P Storage & DHT."""

import shutil
import sys
import tempfile
import time
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rs_p2p.network.client import ActiveNodeClient
from rs_p2p.network.peer_node import PassivePeerServer


def run_integration_test():
    print("=" * 70)
    print("RUNNING REED-SOLOMON P2P gRPC INTEGRATION TEST")
    print("=" * 70)

    temp_dir = Path(tempfile.mkdtemp(prefix="rs_p2p_test_"))
    peers = []
    client = None
    restorer = None
    try:
        k = 4
        m = 2
        total_nodes = k + m
        base_port = 9100
        seed_endpoint = f"127.0.0.1:{base_port}"

        # 1. Start 6 Passive Volunteer Peer Servers
        print(
            f"\n[1/5] Launching {total_nodes} Passive Peer Servers on ports "
            f"{base_port}..{base_port + total_nodes - 1}..."
        )
        for i in range(total_nodes):
            port = base_port + i
            node_id = f"volunteer_node_{i}"
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

        time.sleep(0.5)
        print("  ✓ All passive peers online and DHT bootstrapped.")

        # 2. Connect Active Node Client (Client Mode)
        print("\n[2/5] Initializing Active Node Client (Kademlia Client Mode)...")
        client = ActiveNodeClient(
            seed_endpoints=[seed_endpoint],
            client_node_id="active_compute_A2",
        )
        print("  ✓ Active Node Client connected to passive DHT overlay.")

        # 3. Offload Checkpoint Activation to a Random Passive Relay
        print("\n[3/5] Offloading Checkpoint Activation to Passive Storage Layer...")
        activation_data = b"ACTV_TENSOR_WEIGHTS_DATA_BLOCK_" * 128
        tensor_meta = {
            "source_node": "A2",
            "layers": "L6-L9",
            "shape": [1, 64, 4096],
            "dtype": "bfloat16",
            "sequence_id": "prompt_eval_7781",
        }

        manifest = client.send_checkpoint_activations(
            activation_payload=activation_data,
            checkpoint_id="chk_stage2_act",
            k=k,
            m=m,
            serializer_name="tensor",
            metadata=tensor_meta,
        )

        endpoints_used = {chk["endpoint"] for chk in manifest["chunks"]}
        assert len(manifest["chunks"]) == total_nodes
        assert len(endpoints_used) == total_nodes, (
            f"Expected {total_nodes} distinct endpoints, got {endpoints_used}"
        )
        assert manifest["chunks"][0]["endpoint"] == manifest["coordinator_endpoint"], (
            "Expected coordinator passive node to store chunk #0 locally"
        )
        for p in peers:
            for c in p.dht.routing_table.all_contacts():
                assert c.role == "passive" and c.port > 0, (
                    f"Routing table polluted with non-server contact: {c}"
                )

        print(
            f"  ✓ Checkpoint offloaded via relay {manifest['coordinator_endpoint']}; "
            f"dispersed across {len(endpoints_used)} distinct peers:"
        )
        for chk in manifest["chunks"]:
            print(
                f"    - Chunk #{chk['chunk_index']} ({chk['chunk_type']}) -> {chk['endpoint']}"
            )

        # 4. Simulate Failure: Stop m=2 passive peers INCLUDING original seed!
        print(
            f"\n[4/5] Simulating Node Failures: Stopping m={m} peers (including seed node 0 and node 1)..."
        )
        peers[0].stop()
        peers[1].stop()
        time.sleep(0.3)
        print("  ✓ Seed node 0 and node 1 killed.")

        # 5. Restore from a fresh ActiveNodeClient bootstrapped ONLY to a surviving peer
        print(
            "\n[5/5] Restoring directly into fresh ActiveNodeClient B3 from surviving peers..."
        )
        restorer = ActiveNodeClient(
            seed_endpoints=[f"127.0.0.1:{base_port + 2}"],
            client_node_id="active_compute_B3",
        )
        restored_payload, restored_meta = restorer.receive_backup(
            checkpoint_id="chk_stage2_act",
            serializer_name="tensor",
        )

        assert restored_payload == activation_data, "Restored activation payload mismatch!"
        assert restored_meta["shape"] == [1, 64, 4096]
        assert restored_meta["dtype"] == "bfloat16"
        assert restored_meta["sequence_id"] == "prompt_eval_7781"
        print("  ✓ 100% BIT-FOR-BIT RESTORATION SUCCESSFUL!")
        print(f"  ✓ Verified Metadata: {restored_meta}")
        print("=" * 70)
        print("INTEGRATION TEST PASSED!")
        print("=" * 70)

    finally:
        if client is not None:
            client.close()
        if restorer is not None:
            restorer.close()
        for p in peers:
            try:
                p.stop()
            except Exception:
                pass
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    run_integration_test()
