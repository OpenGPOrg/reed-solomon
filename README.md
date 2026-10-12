# Reed-Solomon Erasure-Coded RAID & P2P Storage System (Python)

A fault-tolerant distributed storage and peer-to-peer volunteer network simulation built in Python using **Reed-Solomon Erasure Coding** (`zfec`).

---

## 📌 Architecture Highlights

### 1. Asymmetric Hybrid-P2P Architecture
- **Fast Ingress (Store)**: Active node sends the file to the **Coordinating Node** in a single fast stream. The Coordinator offloads the Reed-Solomon erasure encoding $(k, m)$ and fans out chunks to volunteer peer computers in parallel.
- **Direct P2P Egress (Retrieve)**: Active node queries the Coordinator for a tiny manifest, then downloads chunks **in parallel directly from the Volunteer Storage Peers**. Bypasses the Coordinator entirely for high throughput, swarm bandwidth aggregation, and zero bottleneck.
- **Volunteer Churn Resilience**: Reconstructs files from **any $k$ surviving peers**. If up to $m$ volunteer laptops disconnect or go to sleep, retrieval still succeeds seamlessly.
- **Bit-Rot & Byzantine Protection**: Every chunk's SHA-256 hash is checked on arrival. Bad or tampered chunks from untrusted volunteers are dropped immediately.

---

## 📁 Project Structure

```text
reed-solomon/
├── input/                      # Place source files here
│   └── sample.txt
├── output/
│   ├── metadata/               # Local RAID manifests
│   ├── nodes/                  # Local simulated disks
│   ├── p2p_metadata/           # P2P manifests catalog
│   ├── p2p_nodes/              # Chunks on volunteer PCs
│   └── p2p_restored/           # Reconstructed files from P2P retrieval
├── src/
│   └── rs_raid/
│       ├── config.py           # Industrial defaults (k=4, m=2)
│       ├── core/
│       │   └── codec.py        # zfec Reed-Solomon wrapper
│       ├── metadata/
│       │   └── manifest.py     # FileManifest and ChunkMetadata models
│       ├── storage/
│       │   ├── base.py         # StorageNode abstract interface
│       │   ├── local.py        # Local folder disk simulation
│       │   └── remote.py       # HTTP client node implementation
│       └── network/
│           ├── peer_daemon.py  # Daemon running on Volunteer PCs (HTTP server)
│           ├── coordinator.py  # Ingress & Erasure Hub + Peer Tracker
│           └── client.py       # Active Node P2P Client
└── main.py                     # CLI & automated demo runners
```

---

## 🚀 Quick Start: Automated Demos

### 1. Run the P2P Volunteer Network Demo
Spins up 1 Coordinator, 6 Volunteer Peer Servers on local ports, sends a file to the Coordinator, **shuts down $m=2$ volunteer peers to simulate laptop dropouts**, and retrieves the file **directly from the surviving peers** in parallel:
```bash
python main.py p2p-demo
```

### 2. Run Local Drive RAID Simulation
Tests the folder-based RAID array (encoding, deleting $m$ drives, reconstructing, and auto-repairing):
```bash
python main.py demo
```

---

## 🌐 Running Across Real Volunteer PCs on LAN / Wi-Fi

You can run this across multiple separate laptops/PCs on the same local network or Wi-Fi:

### Machine 1: The Coordinator
Run on a designated machine (e.g., IP `192.168.1.10`):
```bash
python main.py coordinator --port 8000 --host 0.0.0.0
```

### Machine 2, 3, 4...: Volunteer PCs
Run on any volunteer computer (e.g., PC 1 at `192.168.1.51`, PC 2 at `192.168.1.52`):
```bash
# On Volunteer PC 1:
python main.py peer --node-id volunteer_laptop_1 --port 8001 --advertise-host 192.168.1.51 --coordinator http://192.168.1.10:8000

# On Volunteer PC 2:
python main.py peer --node-id volunteer_desktop_2 --port 8001 --advertise-host 192.168.1.52 --coordinator http://192.168.1.10:8000
```
*The volunteer peers will automatically register with the Coordinator and start sending periodic heartbeats.*

### Active Node (Client)
On any computer that wants to store or retrieve files:

1. **Fast Store (Sends to Coordinator, which fans out to volunteers)**:
   ```bash
   python main.py p2p-store input/sample.txt --coordinator http://192.168.1.10:8000
   ```

2. **Direct P2P Retrieve (Fetches chunks directly from volunteer PCs)**:
   ```bash
   python main.py p2p-retrieve <FILE_ID> --coordinator http://192.168.1.10:8000
   ```

---

## 🐍 Using the P2P Client in Python Code

```python
from rs_raid.network.client import P2PClient

client = P2PClient(coordinator_url="http://192.168.1.10:8000")

# 1. Store: single fast stream to Coordinator
manifest = client.store("input/sample.txt", k=4, m=2)
print("Stored with File ID:", manifest["file_id"])

# 2. Retrieve: parallel direct pull from volunteer peer swarm
restored_path = client.retrieve(manifest["file_id"])
print("Restored to:", restored_path)
```
