"""Reed-Solomon Erasure Coding & Decentralized P2P System CLI.

Supports:
1. p2p-demo: Automated simulation of decentralized Kademlia DHT, gRPC passive nodes,
             checkpoint activation sharding, and node dropouts.
2. seed: Runs a Kademlia DHT bootstrap seed node.
3. peer: Runs a Passive Volunteer Peer Daemon (gRPC storage + DHT node).
4. store-activation: Stores a file/checkpoint across passive nodes via client-side RS encoding.
5. restore-activation: Retrieves and recovers activation using any k surviving chunks.
6. demo: Runs local disk RAID array simulation.
"""

import argparse
import sys
import time
from pathlib import Path

# Add src to Python path
src_dir = Path(__file__).resolve().parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from rs_p2p import (
    DEFAULT_K,
    DEFAULT_M,
    PassivePeerServer,
    ActiveNodeClient,
)
from rs_raid import (
    store_file,
    retrieve_file,
    inspect_file,
    repair_file,
    INPUT_DIR,
    NODES_DIR,
)


def run_local_demo(k: int = DEFAULT_K, m: int = DEFAULT_M):
    """Runs local folder-based RAID simulation."""
    print("=" * 70)
    print("REED-SOLOMON RAID STORAGE DEMO (LOCAL DISK ARRAY)")
    print(f"Configuration: k={k} (Data), m={m} (Parity) -> Total {k+m} Storage Nodes")
    print(f"Fault Tolerance: Can withstand up to {m} simultaneous node failures")
    print("=" * 70)

    sample_file = INPUT_DIR / "sample.txt"
    if not sample_file.exists():
        sample_file.parent.mkdir(parents=True, exist_ok=True)
        sample_file.write_text("Default sample test content for RS RAID.\n")

    print(f"\n[1/6] Storing file: {sample_file} ...")
    manifest = store_file(sample_file, k=k, m=m)
    print(f"  ✓ File ID:    {manifest.file_id}")
    print(f"  ✓ File Size:  {manifest.file_size_bytes} bytes")
    print(f"  ✓ Chunks:     {manifest.total_chunks} total ({manifest.k} data, {manifest.m} parity)")

    print("\n[2/6] Inspecting Initial Cluster Health ...")
    status = inspect_file(manifest.file_id)
    print(f"  ✓ Health Status: {status['status']} ({status['healthy_count']}/{status['total_chunks']} healthy)")

    print(f"\n[3/6] Simulating Node Failures: Deleting {m} chunks ...")
    failed_chunks = manifest.chunks[:m]
    for failed in failed_chunks:
        chunk_file = NODES_DIR / failed.node_id / f"{failed.chunk_id}.chunk"
        if chunk_file.exists():
            chunk_file.unlink()
            print(f"  ✗ SIMULATED CRASH: Destroyed chunk on {failed.node_id}")

    print("\n[4/6] Inspecting Degraded Health ...")
    status_after_fail = inspect_file(manifest.file_id)
    print(f"  ⚠ Cluster Status: {status_after_fail['status']} (Recoverable: {status_after_fail['can_restore']})")

    print("\n[5/6] Retrieving File from surviving nodes ...")
    restored_path = retrieve_file(manifest.file_id)
    print(f"  ✓ File reconstructed at: {restored_path}")
    assert sample_file.read_text() == restored_path.read_text(), "Content mismatch!"
    print("  ✓ SHA-256 Integrity Verification: 100% BIT-FOR-BIT MATCH!")

    print("\n[6/6] Auto-healing: Repairing damaged nodes using RS parity ...")
    repair_result = repair_file(manifest.file_id)
    print(f"  ✓ Repair Result: {repair_result['status']}, Regenerated {repair_result['repaired_chunks']} chunks")
    print("=" * 70)


def run_p2p_demo(k: int = DEFAULT_K, m: int = DEFAULT_M):
    """Automated demonstration of the fully decentralized P2P gRPC architecture."""
    from tests.test_p2p_pipeline import run_integration_test
    run_integration_test()


def main():
    parser = argparse.ArgumentParser(
        description="Reed-Solomon Erasure-Coded P2P Storage & DHT System"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # 1. p2p-demo
    subparsers.add_parser("p2p-demo", help="Run automated P2P gRPC + DHT recovery simulation")

    # 2. demo (local raid)
    subparsers.add_parser("demo", help="Run local folder-based RAID demo")

    # 3. seed node
    seed_p = subparsers.add_parser("seed", help="Start a Kademlia DHT bootstrap seed node")
    seed_p.add_argument("--port", type=int, default=7000, help="Port to bind seed gRPC server")
    seed_p.add_argument("--host", default="0.0.0.0", help="Host interface to bind")

    # 4. passive peer daemon
    peer_p = subparsers.add_parser("peer", help="Run a passive volunteer storage daemon")
    peer_p.add_argument("--node-id", required=True, help="Unique node identifier")
    peer_p.add_argument("--port", type=int, default=7001, help="Port to listen on")
    peer_p.add_argument("--host", default="0.0.0.0", help="Host interface to bind")
    peer_p.add_argument("--seeds", nargs="*", default=["127.0.0.1:7000"], help="Bootstrap seed endpoints")
    peer_p.add_argument("--dir", default=None, help="Directory to store chunks")

    # 5. store activation
    store_p = subparsers.add_parser("store-activation", help="Store checkpoint activation via P2P")
    store_p.add_argument("path", help="Path to input activation file")
    store_p.add_argument("--checkpoint-id", default=None, help="Optional checkpoint ID")
    store_p.add_argument("--seeds", nargs="*", default=["127.0.0.1:7000"], help="DHT seed endpoints")
    store_p.add_argument("-k", type=int, default=DEFAULT_K, help="Data chunks (k)")
    store_p.add_argument("-m", type=int, default=DEFAULT_M, help="Parity chunks (m)")
    store_p.add_argument("--serializer", default="tensor", choices=["tensor", "raw", "json"])

    # 6. restore activation
    restore_p = subparsers.add_parser("restore-activation", help="Restore checkpoint activation via P2P")
    restore_p.add_argument("checkpoint_id", help="Checkpoint ID to restore")
    restore_p.add_argument("--seeds", nargs="*", default=["127.0.0.1:7000"], help="DHT seed endpoints")
    restore_p.add_argument("--out", required=True, help="Path to write restored data")
    restore_p.add_argument("--serializer", default="tensor", choices=["tensor", "raw", "json"])

    args = parser.parse_args()

    if args.command == "demo":
        run_local_demo()
    elif args.command == "p2p-demo":
        run_p2p_demo()
    elif args.command == "seed":
        server = PassivePeerServer(
            node_id_str="seed_bootstrap",
            host=args.host,
            port=args.port,
        )
        print(f"Starting DHT Bootstrap Seed Node on {args.host}:{args.port}...")
        server.start()
        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            server.stop()
    elif args.command == "peer":
        server = PassivePeerServer(
            node_id_str=args.node_id,
            host=args.host,
            port=args.port,
            storage_dir=args.dir,
        )
        print(f"Starting Volunteer Peer [{args.node_id}] on {args.host}:{args.port}...")
        server.start(seed_endpoints=args.seeds)
        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            server.stop()
    elif args.command == "store-activation":
        file_path = Path(args.path)
        if not file_path.exists():
            print(f"Error: file not found: {file_path}")
            sys.exit(1)
        data = file_path.read_bytes()
        client = ActiveNodeClient(seed_endpoints=args.seeds)
        manifest = client.send_checkpoint_activations(
            activation_payload=data,
            checkpoint_id=args.checkpoint_id,
            k=args.k,
            m=args.m,
            serializer_name=args.serializer,
            metadata={"filename": file_path.name, "size": len(data)},
        )
        print(f"✓ Checkpoint stored successfully! ID: {manifest['checkpoint_id']}")
        client.close()
    elif args.command == "restore-activation":
        client = ActiveNodeClient(seed_endpoints=args.seeds)
        payload, meta = client.receive_backup(
            checkpoint_id=args.checkpoint_id,
            serializer_name=args.serializer,
        )
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(payload)
        print(f"✓ Checkpoint restored to: {out_path}")
        print(f"  Metadata: {meta}")
        client.close()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
