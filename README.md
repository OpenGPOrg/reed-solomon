# Decentralized Reed-Solomon P2P Storage & DHT System

A fault-tolerant, peer-to-peer distributed storage and checkpoint network built in Python using **Reed-Solomon Erasure Coding** (`zfec`), **Kademlia DHT overlay**, and **gRPC** for low-latency node-to-node communication.

Designed for distributed LLM pipeline parallel inference, passive volunteer storage nodes, and terminal activation checkpointing.

---

## 📌 Architecture Highlights

```
                                 ┌────────────────────────────────────────────────────────┐
                                 │                   Kademlia DHT Overlay                 │
                                 │  NS1: Active Layer Placement & Volunteer Node Liveness │
                                 │  NS2: Shared Chunk Index & Manifest Context Store      │
                                 └───────────────┬────────────────────────┬───────────────┘
                                                 ▲                        ▲
                                DHT Registration │                        │ DHT Store / Lookup
                                & Heartbeats     │                        │ (Manifest & Locations)
                                                 │                        │
[Active Compute Node A2] ───────────────[ActiveNodeClient SDK]────────────┘
(Computes L6-L9)                        │ 1. Encode Activation via RSCodec (k+m shards)
                                        │ 2. Find closest/healthy passive peers via DHT
                                        ▼ 3. Parallel gRPC Chunk Streams
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
             (gRPC StoreChunk)   (gRPC StoreChunk)   (gRPC StoreChunk)
                    ▼                   ▼                   ▼
              [Passive Peer 0]    [Passive Peer 1]    [Passive Peer N]  (Disk/SSD Storage)
                    │                   │                   │
                    └───────────────────┼───────────────────┘
                                        │ 1. Restoring Node B3 queries DHT NS2 for manifest
                                        │ 2. Downloads k fastest chunks in parallel via gRPC
                                        ▼ 3. RS Decodes & recovers activation tensor
                               [Active Compute Node B3] (Resumes L10-L15)
```

### 1. Zero Single Point of Failure (SPOF-free)
- **Decentralized Coordination**: Central coordinator eliminated. Replaced by a native **Kademlia DHT** overlay with 160-bit XOR distance routing and K-buckets.
- **Dual Namespace Isolation**:
  - `NS1`: Active compute node layer placement (`L6-L9 -> endpoint`) and node liveness heartbeats.
  - `NS2`: Shared chunk location index and activation checkpoint manifests.

### 2. High-Performance gRPC Communication
- Node-to-node communication runs entirely over **gRPC & Protocol Buffers (`.proto`)**:
  - `StorageService`: `StoreChunk`, `GetChunk`, `HasChunk`, `DeleteChunk`, `GetHealth`.
  - `DHTService`: `Ping`, `FindNode`, `FindValue`, `StoreValue`.
- Binary wire protocol eliminates JSON / HTTP REST serialization overhead.

### 3. Configurable Checkpoint Activation Format
Extensible `ActivationSerializer` abstract base class with pluggable serializers:
- `TensorActivationSerializer`: LLM activation serialization with tensor metadata (`shape`, `dtype`, `layer_index`, `sequence_id`).
- `RawBytesSerializer`: Header-prefixed raw bytes buffer.
- `JSONPayloadSerializer`: Generic JSON-serializable dictionaries.
- Custom serializers can be registered via `register_serializer("custom", MySerializer())`.

### 4. Resilient Passive Volunteer Network
- Any volunteer laptop or server with CPU/Disk/SSD can participate (zero GPU requirement).
- Tolerates simultaneous failure of up to $m$ passive nodes. Files/activations are reconstructed from **any $k$ surviving chunks**.

---

## 📁 Repository Structure

```text
reed-solomon/
├── proto/                          # Protocol Buffer definitions
│   ├── storage.proto               # gRPC Chunk Storage Service
│   └── dht.proto                   # gRPC Kademlia DHT Service
│
├── src/
│   ├── rs_p2p/                     # Decentralized P2P & gRPC Engine
│   │   ├── config.py               # Cluster defaults (k=4, m=2, ports, namespaces)
│   │   ├── core/
│   │   │   ├── codec.py            # zfec Reed-Solomon wrapper
│   │   │   └── serializer.py       # Configurable Activation Serializer format
│   │   ├── storage/
│   │   │   └── disk.py             # Volunteer node disk chunk storage
│   │   ├── dht/
│   │   │   ├── node.py             # XOR distance, Contacts, K-Buckets, RoutingTable
│   │   │   └── kademlia.py         # KademliaNode: iterative lookup, NS1 & NS2 store
│   │   ├── proto/                  # Compiled gRPC Python stubs (storage_pb2, dht_pb2)
│   │   └── network/
│   │       ├── services.py         # gRPC Servicers (StorageServicer, DHTServicer)
│   │       ├── peer_node.py        # Passive Volunteer Peer Server daemon
│   │       └── client.py           # ActiveNodeClient SDK (send_checkpoint, receive_backup)
│   │
│   └── rs_raid/                    # Local folder-based disk array simulation
│
├── tests/
│   ├── test_codec_and_serializer.py# Unit tests for RS and serializers
│   └── test_p2p_pipeline.py        # End-to-end integration test (sharding + dropout + recovery)
│
├── main.py                         # CLI tool for demos, peer daemons, seed, store/restore
└── pyproject.toml
```

---

## 🚀 Quick Start

### 1. Run Automated P2P gRPC + DHT Demo
Spins up 6 passive peer servers on local ports, bootstraps DHT, stores an activation checkpoint, **intentionally kills $m=2$ volunteer peers**, and recovers the activation **100% bit-for-bit** directly from surviving peers:

```bash
uv run python main.py p2p-demo
```

### 2. Run Local RAID Array Simulation
Tests folder-based Reed-Solomon RAID array encoding, drive failure, and auto-healing:

```bash
uv run python main.py demo
```

---

## 🌐 Running & Testing Across Two Laptops (LAN / Wi-Fi)

With two laptops on the same Wi-Fi/LAN, you can test the entire Kademlia DHT and Reed-Solomon P2P system in two ways:
1. **Run multiple nodes per laptop on different ports** (e.g., 3 nodes on Laptop A + 3 nodes on Laptop B = 6-node cluster across 2 physical machines).
2. **Use `k=1, m=1` (2 total nodes)** so each laptop runs just a single node.

### ⚠️ Important: Pass Your Laptop's Actual LAN IP to `--host` (Not `0.0.0.0`)
In `src/rs_p2p/dht/kademlia.py`, when a node pings or registers itself in another node's Kademlia `RoutingTable`, it sends `self.contact.host`. If you leave `--host 0.0.0.0` (the default in `main.py`), your laptop will advertise its address as `0.0.0.0:7001` to the other laptop!
Always pass each laptop's actual Wi-Fi/LAN IP (e.g., `192.168.1.10` and `192.168.1.20`) to `--host`.

---

### Step-by-Step Test Across Two Laptops

#### Step 1: Find the LAN IP of Both Laptops
Run this on both laptops to get their local Wi-Fi IP addresses:
```bash
hostname -I | awk '{print $1}'
```
Suppose:
* **Laptop A** IP = `192.168.1.10`
* **Laptop B** IP = `192.168.1.20`

---

#### Step 2: Start Nodes on Both Laptops
To test full $k=4, m=2$ (6 nodes total), run **3 terminals on Laptop A** and **3 terminals on Laptop B**:

**On Laptop A (`192.168.1.10`):**
```bash
# Terminal 1 (Bootstrap Seed + Peer 1 on port 7000)
uv run python main.py peer --node-id laptopA_1 --host 192.168.1.10 --port 7000 --seeds

# Terminal 2 (Peer 2 on port 7001, bootstrapping from 7000)
uv run python main.py peer --node-id laptopA_2 --host 192.168.1.10 --port 7001 --seeds 192.168.1.10:7000

# Terminal 3 (Peer 3 on port 7002, bootstrapping from 7000)
uv run python main.py peer --node-id laptopA_3 --host 192.168.1.10 --port 7002 --seeds 192.168.1.10:7000
```

**On Laptop B (`192.168.1.20`):**
Point `--seeds` to Laptop A (`192.168.1.10:7000`). Notice we don't even tell Laptop B about `7001` or `7002`—**Kademlia's `lookup_node` (`FindNode` RPC) will automatically discover them!**
```bash
# Terminal 1 (Peer 4 on port 7001)
uv run python main.py peer --node-id laptopB_1 --host 192.168.1.20 --port 7001 --seeds 192.168.1.10:7000

# Terminal 2 (Peer 5 on port 7002)
uv run python main.py peer --node-id laptopB_2 --host 192.168.1.20 --port 7002 --seeds 192.168.1.10:7000

# Terminal 3 (Peer 6 on port 7003)
uv run python main.py peer --node-id laptopB_3 --host 192.168.1.20 --port 7003 --seeds 192.168.1.10:7000
```

---

#### Step 3: Verify Kademlia Discovery, `rpc_ping`, and `iterative_store`
From **Laptop B**, store a file while only giving it Laptop A's seed (`192.168.1.10:7000`):
```bash
echo "Hello across two laptops via Kademlia DHT and Reed-Solomon!" > test_input.txt

uv run python main.py store-activation test_input.txt \
  --checkpoint-id test_chk_1 \
  --seeds 192.168.1.10:7000 \
  --serializer raw
```
**What this proves if it succeeds:**
1. **`rpc_ping` & `FindNode` work**: The client on Laptop B only knew `192.168.1.10:7000`, yet via Kademlia `lookup_node` it discovered all 6 nodes across both laptops (`192.168.1.10:7000..7002` and `192.168.1.20:7001..7003`).
2. **Chunk Dispersal works**: It split the payload into 6 Reed-Solomon chunks (4 data, 2 parity) and uploaded 3 chunks to Laptop A and 3 chunks to Laptop B.
3. **DHT `iterative_store` (`StoreValue`) works**: It stored the checkpoint manifest in Kademlia Namespace 2 (`NS_STORAGE_INDEX`) across the closest nodes by XOR distance.

---

#### Step 4: Verify Fault Tolerance & Kademlia `iterative_find_value`
Now test killing nodes (press `Ctrl+C` on **up to 2 terminals**, e.g. kill `laptopA_1`—even the original seed!—and `laptopB_1`), then restore from **Laptop A** pointing to any surviving node (e.g. `192.168.1.20:7002`):
```bash
uv run python main.py restore-activation test_chk_1 \
  --seeds 192.168.1.20:7002 \
  --out restored.txt \
  --serializer raw

cat restored.txt
```
**What this proves:**
1. Even with the original seed node dead, Kademlia locates the manifest from the surviving nodes via `iterative_find_value`.
2. Reed-Solomon reconstructs the exact file from the 4 surviving chunks over the network.

---

#### Bonus: Quick One-Liner to Inspect a Node's Kademlia Routing Table
If you want to directly inspect the K-buckets and test `rpc_ping` from either laptop, run this against any running node:

```bash
uv run python -c '
import sys; sys.path.insert(0, "src")
from rs_p2p.dht.kademlia import KademliaNode
from rs_p2p.dht.node import Contact, hash_key

dht = KademliaNode(node_id=hash_key("inspector"), host="127.0.0.1", port=0)
seed = Contact(node_id=hash_key("192.168.1.10:7000"), host="192.168.1.10", port=7000)

print("1. Ping seed:", dht.rpc_ping(seed))
contacts = dht.lookup_node(dht.contact.node_id)
print(f"2. Discovered {len(contacts)} peers in Kademlia routing table:")
for c in contacts:
    print(f"   - {c.endpoint} (role={c.role}, id={c.hex_id[:10]}...)")
'
```

---

### Python SDK Usage (`ActiveNodeClient`)
In your Python pipeline code:

```python
from rs_p2p import ActiveNodeClient

# Connect to DHT overlay via seed
client = ActiveNodeClient(seed_endpoints=["192.168.1.10:7000"])

# 1. Register Active Node layer placement in DHT NS1
client.register_layer_placement("stage_2", "192.168.1.20:50051", extra={"layers": "L6-L9"})

# 2. Backup checkpoint activation from Stage A2
activation_tensor_bytes = ... # tensor byte buffer
manifest = client.send_checkpoint_activations(
    activation_payload=activation_tensor_bytes,
    checkpoint_id="chk_stage2_step42",
    k=4,
    m=2,
    serializer_name="tensor",
    metadata={"shape": [1, 64, 4096], "dtype": "bfloat16", "sequence_id": "prompt_102"}
)

# 3. Restoring Node B3 recovers backup after A3 crashes
restored_tensor, meta = client.receive_backup("chk_stage2_step42", serializer_name="tensor")
print(f"Recovered tensor for {meta['sequence_id']}, shape={meta['shape']}")
```
