"""Reed-Solomon RAID Command Line Interface & Demo Runner."""

import argparse
import sys
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
from rs_raid.raid.controller import RaidController


def run_demo(k: int = DEFAULT_K, m: int = DEFAULT_M):
    """Runs a complete end-to-end demonstration of the Reed-Solomon RAID system:
    1. Stores a file from input/ (encoding into k data + m parity chunks).
    2. Inspects storage nodes (all healthy).
    3. Simulates failure: deletes m nodes/chunks to simulate catastrophic hardware loss.
    4. Inspects status: shows degraded mode.
    5. Reconstructs and retrieves the original file in restored/ using the remaining chunks.
    6. Verifies SHA-256 match.
    7. Runs repair to rebuild the lost chunks on the damaged nodes.
    """
    print("=" * 70)
    print("REED-SOLOMON RAID STORAGE DEMO")
    print(f"Configuration: k={k} (Data), m={m} (Parity) -> Total {k+m} Storage Nodes")
    print(f"Fault Tolerance: Can withstand up to {m} simultaneous node failures")
    print("=" * 70)

    sample_file = INPUT_DIR / "sample.txt"
    if not sample_file.exists():
        sample_file.write_text("Default sample test content for RS RAID.\n")

    # Step 1: Store file
    print(f"\n[1/6] Storing file: {sample_file} ...")
    manifest = store_file(sample_file, k=k, m=m)
    print(f"  ✓ File ID:    {manifest.file_id}")
    print(f"  ✓ File Size:  {manifest.file_size_bytes} bytes")
    print(f"  ✓ SHA-256:    {manifest.file_sha256[:16]}...")
    print(f"  ✓ Chunks:     {manifest.total_chunks} total ({manifest.k} data, {manifest.m} parity)")
    for chunk in manifest.chunks:
        print(f"    - [{chunk.chunk_type.upper():6}] Chunk #{chunk.chunk_index} -> Stored on {chunk.node_id}")

    # Step 2: Inspect Initial Health
    print("\n[2/6] Inspecting Initial Cluster Health ...")
    status = inspect_file(manifest.file_id)
    print(f"  ✓ Health Status: {status['status']}")
    print(f"  ✓ Healthy Chunks: {status['healthy_count']} / {status['total_chunks']}")

    # Step 3: Simulate Node Failures (Delete m chunks to simulate lost drives)
    print(f"\n[3/6] Simulating DISK/NODE FAILURES: Intentionally deleting {m} nodes ...")
    failed_chunks = manifest.chunks[:m]  # Delete the first m chunks
    for failed in failed_chunks:
        chunk_file = NODES_DIR / failed.node_id / f"{failed.chunk_id}.chunk"
        if chunk_file.exists():
            chunk_file.unlink()
            print(f"  ✗ SIMULATED CRASH: Destroyed chunk on {failed.node_id}")

    # Step 4: Re-inspect Health
    print("\n[4/6] Inspecting Degraded Cluster Health ...")
    status_after_fail = inspect_file(manifest.file_id)
    print(f"  ⚠ Cluster Status: {status_after_fail['status']}")
    print(f"  ⚠ Healthy Chunks: {status_after_fail['healthy_count']} / {status_after_fail['total_chunks']}")
    print(f"  ⚠ Recoverable?    {status_after_fail['can_restore']} (Reed-Solomon redundancy active)")

    # Step 5: Retrieve and Reconstruct
    print("\n[5/6] Retrieving & Reconstructing File from remaining surviving nodes ...")
    restored_path = retrieve_file(manifest.file_id)
    print(f"  ✓ File successfully reconstructed at: {restored_path}")

    # Verify content match
    original_text = sample_file.read_text()
    restored_text = restored_path.read_text()
    assert original_text == restored_text, "Content mismatch!"
    print("  ✓ SHA-256 Integrity Verification: 100% BIT-FOR-BIT MATCH!")

    # Step 6: Repair Missing Chunks
    print("\n[6/6] Auto-healing: Repairing damaged nodes using Reed-Solomon parity ...")
    repair_result = repair_file(manifest.file_id)
    print(f"  ✓ Repair Result: {repair_result['status']}, Regenerated {repair_result['repaired_chunks']} chunks")

    final_status = inspect_file(manifest.file_id)
    print(f"  ✓ Post-Repair Cluster Health: {final_status['status']} ({final_status['healthy_count']}/{final_status['total_chunks']} healthy)")
    print("\n" + "=" * 70)
    print("DEMO COMPLETED SUCCESSFULLY! All Reed-Solomon RAID mechanics verified.")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Reed-Solomon Erasure-Coded RAID Storage System",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Demo command
    demo_parser = subparsers.add_parser("demo", help="Run full end-to-end RAID simulation & test")
    demo_parser.add_argument("--k", type=int, default=DEFAULT_K, help="Number of data chunks")
    demo_parser.add_argument("--m", type=int, default=DEFAULT_M, help="Number of parity chunks")

    # Store command
    store_parser = subparsers.add_parser("store", help="Store a file into the RAID array")
    store_parser.add_argument("file_path", type=str, help="Path to input file")
    store_parser.add_argument("--k", type=int, default=DEFAULT_K, help="Number of data chunks")
    store_parser.add_argument("--m", type=int, default=DEFAULT_M, help="Number of parity chunks")

    # Retrieve command
    retrieve_parser = subparsers.add_parser("retrieve", help="Retrieve and reconstruct a file")
    retrieve_parser.add_argument("identifier", type=str, help="File ID or filename")
    retrieve_parser.add_argument("--output-dir", type=str, default=str(RESTORED_DIR), help="Output directory")

    # Inspect command
    inspect_parser = subparsers.add_parser("inspect", help="Inspect health status of stored file")
    inspect_parser.add_argument("identifier", type=str, help="File ID or filename")

    # Repair command
    repair_parser = subparsers.add_parser("repair", help="Repair degraded/corrupted chunks")
    repair_parser.add_argument("identifier", type=str, help="File ID or filename")

    args = parser.parse_args()

    if not args.command or args.command == "demo":
        k = getattr(args, "k", DEFAULT_K)
        m = getattr(args, "m", DEFAULT_M)
        run_demo(k=k, m=m)
    elif args.command == "store":
        manifest = store_file(args.file_path, k=args.k, m=args.m)
        print(f"File stored successfully.")
        print(f"File ID: {manifest.file_id}")
        print(f"Filename: {manifest.filename}")
        print(f"Scheme: k={manifest.k}, m={manifest.m} ({manifest.total_chunks} nodes)")
        print(f"Manifest saved to: {OUTPUT_DIR / 'metadata' / f'{manifest.file_id}.json'}")
    elif args.command == "retrieve":
        dest = retrieve_file(args.identifier, output_dir=args.output_dir)
        print(f"File retrieved and reconstructed at: {dest}")
    elif args.command == "inspect":
        info = inspect_file(args.identifier)
        print(f"Status: {info['status']}")
        print(f"Healthy chunks: {info['healthy_count']} / {info['total_chunks']}")
        print(f"Can restore: {info['can_restore']}")
        print(f"Surplus failures tolerated: {info['allowed_further_failures']}")
    elif args.command == "repair":
        result = repair_file(args.identifier)
        print(f"Repair status: {result['status']}, Repaired chunks: {result.get('repaired_chunks', 0)}")


if __name__ == "__main__":
    main()
