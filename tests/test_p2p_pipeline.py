"""End-to-end integration test for Reed-Solomon P2P gRPC Storage & DHT."""

import sys
import shutil
import tempfile
import time
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rs_p2p.network.peer_node import PassivePeerServer
from rs_p2p.network.client import ActiveNodeClient


def run_integration_test():
    print("=" * 70)
    print("RUNNING REED-SOLOMON P2P gRPC INTEGRATION TEST")
    print("=" * 70)

    temp_dir = Path(tempfile.mkdtemp(prefix="rs_p2p_test_"))
    try:
        k = 4
        m = 2
        total_nodes = k + m
        base_port = 9100
        peers = []

        seed_endpoint = f"127.0.0.1:{base_port}"

        # 1. Start 6 Passive Volunteer Peer Servers
        print(f"\n[1/5] Launching {total_nodes} Passive Peer Servers on ports {base_port}..{base_port + total_nodes - 1}...")
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
            # Node 0 is seed; others bootstrap against seed
            seeds = [seed_endpoint] if i > 0 else []
            peer.start(seed_endpoints=seeds)
            peers.append(peer)

        time.sleep(1.0)
        print("  ✓ All passive peers online and DHT bootstrapped.")

        # 2. Connect Active Node Client
        print("\n[2/5] Initializing Active Node Client...")
        client = ActiveNodeClient(
            seed_endpoints=[seed_endpoint],
            client_node_id="active_compute_A2",
            local_port=9200,
        )
        print("  ✓ Active Node Client connected to DHT overlay.")

        # Test NS1: Layer placement
        print("\n[3/5] Testing NS1 (Compute Network Layer Placement)...")
        placement_ok = client.register_layer_placement(
            stage_id="stage_2",
            endpoint="10.0.0.2:50051",
            extra={"layers": "L6-L9", "gpu": "RTX-4090"},
        )
        assert placement_ok, "Failed to register layer placement"
        retrieved_placement = client.get_layer_placement("stage_2")
        assert retrieved_placement is not None, "Failed to get layer placement"
        assert retrieved_placement["stage_id"] == "stage_2"
        assert retrieved_placement["endpoint"] == "10.0.0.2:50051"
        assert retrieved_placement["extra"]["layers"] == "L6-L9"
        print(f"  ✓ NS1 Layer Placement Verified: {retrieved_placement}")

        # 3. Store Checkpoint Activation (NS2 & Passive Sharding)
        print("\n[4/5] Storing Checkpoint Activation from A2 into Passive Network...")
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
            prefer_passive_nodes=[f"127.0.0.1:{base_port + i}" for i in range(total_nodes)],
        )

        print(f"  ✓ Checkpoint stored! Total chunks: {len(manifest['chunks'])}")
        for chk in manifest["chunks"]:
            print(f"    - Chunk #{chk['chunk_index']} ({chk['chunk_type']}) -> Stored on {chk['endpoint']}")

        # 4. Simulate Failure: Shut down m=2 volunteer peers!
        print(f"\n[5/5] Simulating Node Failures: Stopping m={m} passive peers (nodes 0 and 1)...")
        peers[0].stop()
        peers[1].stop()
        time.sleep(0.5)
        print("  ✓ Nodes 0 and 1 killed.")

        # 5. Restore Checkpoint Activation from surviving nodes
        print("\nRestoring activation into Node B3 from surviving peers...")
        restored_payload, restored_meta = client.receive_backup(
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
        # Cleanup
        client.close()
        for p in peers:
            try:
                p.stop()
            except Exception:
                pass
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_p2p_pipeline():
    run_integration_test()


if __name__ == "__main__":
    run_integration_test()
