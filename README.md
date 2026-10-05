# Reed-Solomon Erasure-Coded RAID System (Python)

A fault-tolerant distributed storage and RAID simulation built in Python using **Reed-Solomon Erasure Coding** (`zfec`).

---

## 📌 Features

- **$(k, m)$ Reed-Solomon Erasure Coding**: Configurable data chunks ($k$) and parity chunks ($m$).
- **Configurable Fault Tolerance**: Defaults to the industrial standard **$k=4, m=2$** (6 nodes, tolerates any 2 simultaneous node crashes with only $1.5\times$ storage overhead). Both $k$ and $m$ can be changed on the fly.
- **Node-Decoupled Architecture**: Clean `StorageNode` abstraction. Currently simulates multi-disk nodes as local directories (`output/nodes/node_0`, `node_1`, etc.), and is architected to swap in remote network nodes (HTTP/gRPC) in the future.
- **Bit-Rot & Corruption Detection**: Every chunk is signed with a SHA-256 checksum in a metadata manifest to prevent silent data corruption.
- **Degraded Read & Self-Healing (Repair)**: Seamlessly reconstructs files from any $k$ surviving chunks and can rebuild missing chunks back onto damaged nodes.

---

## 📁 Directory Structure

```text
reed-solomon/
├── input/                      # Place your input files here
│   └── sample.txt
├── output/                    # RAID storage testbed
│   ├── metadata/              # JSON manifests (chunk checksums, file metadata)
│   ├── nodes/                 # Simulated storage drives (node_0, node_1, ...)
│   │   ├── node_0/
│   │   ├── node_1/
│   │   └── ...
│   └── restored/              # Reconstructed files from retrieval
├── src/
│   └── rs_raid/
│       ├── config.py          # Default k, m, and directory settings
│       ├── core/
│       │   └── codec.py       # Reed-Solomon encoder/decoder (zfec wrapper)
│       ├── storage/
│       │   ├── base.py        # Abstract StorageNode interface
│       │   ├── local.py       # LocalStorageNode (simulates drive directories)
│       │   └── remote.py      # RemoteStorageNode skeleton for future network PCs
│       ├── metadata/
│       │   └── manifest.py    # FileManifest and ChunkMetadata tracking
│       └── raid/
│           ├── cluster.py     # Node cluster & health management
│           └── controller.py  # store_file, retrieve_file, inspect_file, repair_file
└── main.py                    # CLI & comprehensive demo runner
```

---

## 🚀 Quick Start & CLI Usage

### 1. Run the Full End-to-End Simulation & Verification
Runs an automated test that encodes `input/sample.txt`, intentionally deletes $m$ storage nodes, reconstructs the file, verifies the SHA-256 hash bit-for-bit, and auto-repairs the lost chunks:
```bash
python main.py demo
```

To test with custom $(k, m)$ configurations (e.g., $k=6, m=3$ like HDFS):
```bash
python main.py demo --k 6 --m 3
```

### 2. Store a File
Splits the file into $k$ data and $m$ parity chunks, distributed across nodes:
```bash
python main.py store input/sample.txt
# Or with custom k, m:
python main.py store input/sample.txt --k 4 --m 2
```

### 3. Inspect Cluster Health for a File
Checks which nodes have the chunks, detects missing/corrupted chunks, and reports if the file is `HEALTHY`, `DEGRADED`, or `UNRECOVERABLE`:
```bash
python main.py inspect sample.txt
```

### 4. Retrieve & Reconstruct a File
Reconstructs the file (tolerating up to $m$ failed or offline nodes) and saves it to `output/restored/`:
```bash
python main.py retrieve sample.txt
```

### 5. Auto-Repair Degraded Files
Rebuilds missing or corrupted chunks and writes them back to the nodes:
```bash
python main.py repair sample.txt
```

---

## 🐍 Using the Python Helper Functions in Code

You can import and use the helper functions directly in your own scripts:

```python
from pathlib import Path
from rs_raid import store_file, retrieve_file, inspect_file, repair_file

# 1. Store a file (defaults to k=4, m=2; or pass custom values)
manifest = store_file("input/sample.txt", k=4, m=2)
print("Stored with file ID:", manifest.file_id)

# 2. Inspect cluster status
status = inspect_file("sample.txt")
print(f"Status: {status['status']} ({status['healthy_count']}/{status['total_chunks']} chunks healthy)")

# 3. Retrieve / Reconstruct
restored_path = retrieve_file("sample.txt")
print("Reconstructed at:", restored_path)

# 4. Repair degraded file
repair_report = repair_file("sample.txt")
print("Repaired chunks:", repair_report["repaired_chunks"])
```

---

## ⚙️ Changing Industrial Defaults $(k, m)$

You have 3 ways to change $k$ and $m$:

1. **In Code**: Pass `k` and `m` directly to `store_file(path, k=6, m=3)`.
2. **Via Environment Variables**:
   ```bash
   export RS_K=6
   export RS_M=3
   python main.py demo
   ```
3. **In Configuration**: Edit `src/rs_raid/config.py`:
   ```python
   DEFAULT_K = 4  # Change default data chunks
   DEFAULT_M = 2  # Change default parity chunks
   ```

---

## 🌐 Future Roadmap: Distributed Multi-Node Storage

The system is designed with an abstract `StorageNode` interface:
```python
class StorageNode(ABC):
    def write_chunk(self, chunk_id: str, data: bytes) -> bool: ...
    def read_chunk(self, chunk_id: str) -> bytes | None: ...
    def is_healthy(self) -> bool: ...
```

To move from local folders to independent networked PCs/nodes:
1. Run a lightweight worker server (FastAPI, Flask, or gRPC) on each target machine.
2. Implement `RemoteStorageNode` in `src/rs_raid/storage/remote.py` to forward chunk reads/writes over HTTP/gRPC.
3. The `RaidController`, `FileManifest`, and Reed-Solomon erasure logic remain identical!
