"""Reed-Solomon RAID Command Line Interface & P2P Demo Runner."""

import argparse
import sys
import time
from pathlib import Path

# Add src to Python path so rs_raid can be imported directly
src_dir = Path(__file__).resolve().parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from rs_raid import (
    store_file,
    retrieve_file,
    inspect_file,
    repair_file,
    DEFAULT_K,
    DEFAULT_M,
    INPUT_DIR,
    OUTPUT_DIR,
    NODES_DIR,
    RESTORED_DIR,
)
from rs_raid.network.coordinator import CoordinatorHandler, CoordinatorState, run_coordinator
from rs_raid.network.peer_daemon import VolunteerPeerServer, run_peer_daemon
from rs_raid.network.client import P2PClient


def run_demo(k: int = DEFAULT_K, m: int = DEFAULT_M):
    """Runs local folder-based RAID simulation."""
    print("=" * 70)
    print("REED-SOLOMON RAID STORAGE DEMO (LOCAL DISK ARRAY)")
    print(f"Configuration: k={k} (Data), m={m} (Parity) -> Total {k+m} Storage Nodes")
    print(f"Fault Tolerance: Can withstand up to {m} simultaneous node failures")
    print("=" * 70)

    sample_file = INPUT_DIR / "sample.txt"
    if not sample_file.exists():
        sample_file.write_text("Default sample test content for RS RAID.\n")

    print(f"\n[1/6] Storing file: {sample_file} ...")
    manifest = store_file(sample_file, k=k, m=m)
    print(f"  ✓ File ID:    {manifest.file_id}")
    print(f"  ✓ File Size:  {manifest.file_size_bytes} bytes")
    print(f"  ✓ SHA-256:    {manifest.file_sha256[:16]}...")
    print(f"  ✓ Chunks:     {manifest.total_chunks} total ({manifest.k} data, {manifest.m} parity)")
    for chunk in manifest.chunks:
        print(f"    - [{chunk.chunk_type.upper():6}] Chunk #{chunk.chunk_index} -> Stored on {chunk.node_id}")

    print("\n[2/6] Inspecting Initial Cluster Health ...")
    status = inspect_file(manifest.file_id)
    print(f"  ✓ Health Status: {status['status']} ({status['healthy_count']}/{status['total_chunks']} healthy)")

    print(f"\n[3/6] Simulating DISK/NODE FAILURES: Intentionally deleting {m} nodes ...")
    failed_chunks = manifest.chunks[:m]
    for failed in failed_chunks:
        chunk_file = NODES_DIR / failed.node_id / f"{failed.chunk_id}.chunk"
        if chunk_file.exists():
            chunk_file.unlink()
            print(f"  ✗ SIMULATED CRASH: Destroyed chunk on {failed.node_id}")

    print("\n[4/6] Inspecting Degraded Cluster Health ...")
    status_after_fail = inspect_file(manifest.file_id)
    print(f"  ⚠ Cluster Status: {status_after_fail['status']} (Recoverable: {status_after_fail['can_restore']})")

    print("\n[5/6] Retrieving & Reconstructing File from remaining surviving nodes ...")
    restored_path = retrieve_file(manifest.file_id)
    print(f"  ✓ File reconstructed at: {restored_path}")

    assert sample_file.read_text() == restored_path.read_text(), "Content mismatch!"
    print("  ✓ SHA-256 Integrity Verification: 100% BIT-FOR-BIT MATCH!")

    print("\n[6/6] Auto-healing: Repairing damaged nodes using Reed-Solomon parity ...")
    repair_result = repair_file(manifest.file_id)
    print(f"  ✓ Repair Result: {repair_result['status']}, Regenerated {repair_result['repaired_chunks']} chunks")
    print("=" * 70)


def run_p2p_demo(k: int = DEFAULT_K, m: int = DEFAULT_M):
    """Demonstrates the asymmetric P2P architecture:
    1. Spins up Coordinator (port 8000).
    2. Spins up k+m Volunteer Peer servers on ports 8001..800N.
    3. Active node sends file to Coordinator in a fast single stream.
    4. Coordinator RS-encodes and fans out chunks to volunteer peers.
    5. Shuts down m volunteer peers (simulating laptop/Wi-Fi dropout).
    6. Active node downloads chunks directly in parallel from surviving peers.
    7. Reconstructs file with 100% SHA-256 integrity match.
    """
    total_nodes = k + m
    coord_port = 8000
    base_peer_port = 8001
    coordinator_url = f"http://127.0.0.1:{coord_port}"

    print("=" * 75)
    print("ASYMMETRIC P2P REED-SOLOMON VOLUNTEER NETWORK DEMO")
    print(f"Ingress:  Active Node -> Fast single stream to Coordinator")
    print(f"Storage:  Dispersed across {total_nodes} Volunteer Peers (k={k} data, m={m} parity)")
    print(f"Egress:   Active Node -> DIRECT P2P parallel download from Volunteer Peers")
    print("=" * 75)

    import threading
    from http.server import ThreadingHTTPServer

    # Step 1: Start Coordinator in background thread
    print("\n[1/5] Starting Coordinating Node on port 8000...")
    coord_meta_dir = OUTPUT_DIR / "p2p_metadata"
    coord_state = CoordinatorState(metadata_dir=coord_meta_dir)
    handler_class = CoordinatorHandler
    handler_class.state = coord_state
    coord_httpd = ThreadingHTTPServer(("127.0.0.1", coord_port), handler_class)
    coord_thread = threading.Thread(target=coord_httpd.serve_forever, daemon=True)
    coord_thread.start()
    time.sleep(0.5)
    print(f"  ✓ Coordinator online at {coordinator_url}")

    # Step 2: Start k+m Volunteer Peer servers
    print(f"\n[2/5] Launching {total_nodes} Volunteer Peer Daemons on ports {base_peer_port}..{base_peer_port + total_nodes - 1}...")
    peer_servers = []
    peer_threads = []

    for i in range(total_nodes):
        port = base_peer_port + i
        node_id = f"volunteer_pc_{i}"
        storage_dir = OUTPUT_DIR / "p2p_nodes" / node_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        server = VolunteerPeerServer(
            node_id=node_id,
            host="127.0.0.1",
            port=port,
            storage_dir=storage_dir,
            coordinator_url=coordinator_url,
        )
        peer_servers.append(server)
        t = threading.Thread(target=lambda s=server: s.start(block=True), daemon=True)
        t.start()
        peer_threads.append(t)

    # Wait for peers to register with coordinator
    time.sleep(1.5)
    healthy_peers = coord_state.get_healthy_peers()
    print(f"  ✓ {len(healthy_peers)} Volunteer Peers registered and reporting heartbeats to Coordinator!")

    # Step 3: Active Node stores file via Coordinator
    sample_file = INPUT_DIR / "sample.txt"
    if not sample_file.exists():
        sample_file.write_text("Hello from P2P Reed-Solomon Volunteer Network!\n")

    client = P2PClient(coordinator_url=coordinator_url)
    print(f"\n[3/5] Active Node: Streaming '{sample_file.name}' to Coordinator...")
    manifest = client.store(sample_file, k=k, m=m)
    print(f"  ✓ Dispersed to peers! File ID: {manifest['file_id']}")
    for chunk in manifest["chunks"]:
        print(f"    - Part #{chunk['chunk_index']} ({chunk['chunk_type']}) -> Stored at {chunk['node_id']} ({chunk['peer_url']})")

    # Step 4: Simulate Volunteer Churn (Drop m volunteer peers!)
    print(f"\n[4/5] Simulating VOLUNTEER CHURN: Disconnecting {m} volunteer computers...")
    dropped_servers = peer_servers[:m]
    for dropped in dropped_servers:
        print(f"  ✗ Volunteer [{dropped.node_id}] shut down / disconnected from Wi-Fi!")
        dropped.stop()
    time.sleep(1.0)

    # Step 5: Active Node directly retrieves from surviving peers (bypassing coordinator)
    print(f"\n[5/5] Active Node: Executing DIRECT P2P RETRIEVAL from surviving peers...")
    restored_dir = OUTPUT_DIR / "p2p_restored"
    restored_file = client.retrieve(manifest["file_id"], output_dir=restored_dir, manifest=manifest)

    # Verification
    original_data = sample_file.read_bytes()
    restored_data = restored_file.read_bytes()
    assert original_data == restored_data, "Data mismatch in P2P retrieval!"
    print("\n" + "=" * 75)
    print("✓ SUCCESS: File reconstructed with 100% BIT-FOR-BIT SHA-256 match!")
    print(f"✓ Reconstructed path: {restored_file}")
    print(f"✓ Direct P2P swarm retrieval succeeded despite losing {m} volunteer computers!")
    print("=" * 75)

    # Clean shutdown
    for s in peer_servers[m:]:
        s.stop()
    coord_httpd.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Reed-Solomon Erasure-Coded RAID & P2P Storage System",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Local demo
    demo_parser = subparsers.add_parser("demo", help="Run local folder-based RAID simulation")
    demo_parser.add_argument("--k", type=int, default=DEFAULT_K, help="Data chunks")
    demo_parser.add_argument("--m", type=int, default=DEFAULT_M, help="Parity chunks")

    # P2P demo
    p2p_demo_parser = subparsers.add_parser("p2p-demo", help="Run full P2P volunteer network simulation")
    p2p_demo_parser.add_argument("--k", type=int, default=DEFAULT_K, help="Data chunks")
    p2p_demo_parser.add_argument("--m", type=int, default=DEFAULT_M, help="Parity chunks")

    # Coordinator command
    coord_parser = subparsers.add_parser("coordinator", help="Run the Coordinating Node")
    coord_parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    coord_parser.add_argument("--host", type=str, default="0.0.0.0", help="Host interface")
    coord_parser.add_argument("--metadata-dir", type=str, default=None, help="Manifest directory")

    # Peer command
    peer_parser = subparsers.add_parser("peer", help="Run a Volunteer Storage Peer Daemon")
    peer_parser.add_argument("--node-id", type=str, required=True, help="Unique ID for this peer")
    peer_parser.add_argument("--port", type=int, default=8001, help="Port to listen on")
    peer_parser.add_argument("--host", type=str, default="0.0.0.0", help="Host interface")
    peer_parser.add_argument("--advertise-host", type=str, default="127.0.0.1", help="Advertised IP")
    peer_parser.add_argument("--storage-dir", type=str, default=None, help="Chunk storage directory")
    peer_parser.add_argument("--coordinator", type=str, default="http://127.0.0.1:8000", help="Coordinator URL")

    # P2P Store
    store_parser = subparsers.add_parser("p2p-store", help="Upload a file to Coordinator")
    store_parser.add_argument("file_path", type=str, help="Path to input file")
    store_parser.add_argument("--coordinator", type=str, default="http://127.0.0.1:8000", help="Coordinator URL")
    store_parser.add_argument("--k", type=int, default=DEFAULT_K, help="Data chunks")
    store_parser.add_argument("--m", type=int, default=DEFAULT_M, help="Parity chunks")

    # P2P Retrieve
    retrieve_parser = subparsers.add_parser("p2p-retrieve", help="Retrieve file directly from volunteer peers")
    retrieve_parser.add_argument("file_id", type=str, help="File ID to retrieve")
    retrieve_parser.add_argument("--coordinator", type=str, default="http://127.0.0.1:8000", help="Coordinator URL")
    retrieve_parser.add_argument("--output-dir", type=str, default=str(OUTPUT_DIR / "p2p_restored"), help="Output dir")

    args = parser.parse_args()

    if not args.command or args.command == "demo":
        run_demo(k=getattr(args, "k", DEFAULT_K), m=getattr(args, "m", DEFAULT_M))
    elif args.command == "p2p-demo":
        run_p2p_demo(k=args.k, m=args.m)
    elif args.command == "coordinator":
        run_coordinator(port=args.port, host=args.host, metadata_dir=args.metadata_dir, block=True)
    elif args.command == "peer":
        run_peer_daemon(
            node_id=args.node_id,
            port=args.port,
            host=args.host,
            advertise_host=args.advertise_host,
            storage_dir=args.storage_dir,
            coordinator_url=args.coordinator,
        )
    elif args.command == "p2p-store":
        client = P2PClient(coordinator_url=args.coordinator)
        manifest = client.store(args.file_path, k=args.k, m=args.m)
        print(f"Stored file ID: {manifest['file_id']}")
    elif args.command == "p2p-retrieve":
        client = P2PClient(coordinator_url=args.coordinator)
        dest = client.retrieve(args.file_id, output_dir=args.output_dir)
        print(f"Retrieved file saved to: {dest}")


if __name__ == "__main__":
    main()
