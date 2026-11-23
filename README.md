# Decentralized Reed-Solomon P2P Storage & DHT System

A fault-tolerant, peer-to-peer distributed storage system built in Python using **Reed-Solomon Erasure Coding** (`zfec`), a **Kademlia DHT overlay**, and **gRPC**.

Designed as a **decoupled passive storage layer** for distributed LLM pipeline activation checkpointing: active compute nodes offload serialization + RAID encoding/dispersal to a random passive peer, while recovering directly from `k` surviving peers in parallel.

---

## 📌 Architecture Highlights

```text
                                 ┌────────────────────────────────────────────────────────┐
                                 │            Passive Kademlia DHT Overlay                │
                                 │  Server Mode: Passive peers only (role="passive")      │
                                 │  Namespace:   storage_index (Checkpoint Manifests)     │
                                 └───────────────┬────────────────────────┬───────────────┘
                                                 ▲                        ▲
                         Random Relay Discovery  │                        │ Manifest Store /
                         (Kademlia Client Mode)  │                        │ Lookup
                                                 │                        │
[Active Compute Node A] ──────────[ActiveNodeClient SDK]                  │
                                  │ 1. Serialize locally                  │
                                  │ 2. Upload payload (StoreCheckpoint)   │
                                  ▼    to random passive relay + failover │
                        [Passive Relay Peer 0]────────────────────────────┘
                        │ • Stores Chunk #0 locally (DiskStorage)
                        │ • RS-encodes into k+m shards
                        │ • Selects k+m-1 distinct XOR-closest peers (+ failover)
                        │ • Parallel gRPC StoreChunk + publishes manifest to DHT
                        ├──────────────────────┬──────────────────────┐
                        ▼                      ▼                      ▼
                [Passive Peer 1]       [Passive Peer 2]       [Passive Peer N]
                        │                      │                      │
                        └──────────────────────┼──────────────────────┘
                                               │ 1. ActiveNodeClient B fetches manifest
                                               │ 2. Parallel gRPC GetChunk (fast-path k)
                                               ▼ 3. RS Decode & deserialize locally
                                      [Active Compute Node B]
```

### 1. Decoupled Active & Passive Layers (Client Mode vs. Server Mode)
- **Passive Storage Overlay (`PassivePeerServer`)**: Runs in Kademlia Server Mode (`role="passive"`, `port > 0`). Only routable passive nodes enter K-buckets, keeping DHT routing tables clean.
- **Active Compute Interface (`ActiveNodeClient`)**: Runs in Kademlia Client Mode (`role="active"`, `port=0`) so active compute meshes remain completely decoupled from storage DHT routing and churn.
- **Load-Balanced Random Relay Offload**: `ActiveNodeClient.send_checkpoint_activations()` picks a random passive relay via 160-bit random lookup (with automatic failover) and offloads Reed-Solomon sharding + dispersal via `StorageService.StoreCheckpoint`.

### 2. Zero Single Point of Failure & Restart Resilience
- **Decentralized Metadata**: Manifests are replicated across the `k` closest passive nodes in DHT namespace `storage_index`.
- **Disk-Backed DHT State**: Passive peers persist `routing_table.json` and `dht_store.json` atomically inside their `storage_dir`, re-ping saved peers on restart (even without `--seeds`), and run periodic background bucket refresh and manifest republishing.

### 3. High-Performance gRPC Communication
- Runs over **gRPC & Protocol Buffers (`.proto`)** with a `64 MiB` message limit:
  - `StorageService`: `StoreCheckpoint`, `StoreChunk`, `GetChunk`, `HasChunk`, `DeleteChunk`, `GetHealth`.
  - `DHTService`: `Ping`, `FindNode`, `FindValue`, `StoreValue`.
- Reads use fast-path parallel `GetChunk` retrieval and decode immediately once any `k` valid shards arrive.

### 4. Configurable Checkpoint Activation Format
Extensible `ActivationSerializer` abstract base class with pluggable serializers:
- `TensorActivationSerializer`: LLM activation serialization with tensor metadata (`shape`, `dtype`, `layer_index`, `sequence_id`).
- `RawBytesSerializer`: Header-prefixed raw bytes buffer.
- `JSONPayloadSerializer`: Generic JSON-serializable dictionaries.
- Custom serializers can be registered via `register_serializer("custom", MySerializer())`.

---

## 📁 Repository Structure

```text
reed-solomon/
├── proto/                          # Protocol Buffer definitions
│   ├── storage.proto               # gRPC Chunk & Checkpoint Storage Service
│   └── dht.proto                   # gRPC Kademlia DHT Service
│
├── src/
│   └── rs_p2p/                     # Decentralized P2P & gRPC Engine
│       ├── config.py               # Cluster defaults (k=4, m=2, gRPC options, intervals)
│       ├── core/
│       │   ├── codec.py            # zfec Reed-Solomon wrapper
│       │   └── serializer.py       # Configurable Activation Serializer format
│       ├── storage/
│       │   └── disk.py             # Atomic volunteer node disk chunk storage
│       ├── dht/
│       │   ├── node.py             # XOR distance, Contacts, thread-safe RoutingTable
│       │   └── kademlia.py         # KademliaNode: Server/Client Mode, persistence, lookups
│       ├── proto/                  # Compiled gRPC Python stubs (storage_pb2, dht_pb2)
│       └── network/
│           ├── services.py         # gRPC Servicers (StorageServicer, DHTServicer)
│           ├── peer_node.py        # PassivePeerServer daemon & maintenance loop
│           └── client.py           # ActiveNodeClient SDK (Client Mode offload & restore)
│
├── tests/
│   ├── test_codec_and_serializer.py# Unit tests for RS codec and serializers
│   ├── test_dht_and_bugfixes.py    # DHT correctness, persistence, failover & bugfix suite
│   └── test_p2p_pipeline.py        # End-to-end integration test (offload + dropout + recovery)
│
├── DECISIONS.md                    # Architecture & engineering decision records (ADRs)
├── TODO.md                         # Resolved bug tracker & future enhancements
├── main.py                         # CLI tool for demos, peer daemons, seed, store/restore
└── pyproject.toml
```

---

## 🚀 Quick Start & Testing

### Run Automated P2P gRPC + DHT Demo
Spins up 6 passive peer servers on local ports, offloads an activation checkpoint through a random passive relay, **intentionally kills $m=2$ volunteer peers (including the seed)**, and recovers the activation **100% bit-for-bit** from a fresh client:

```bash
uv run python main.py p2p-demo
```

### Run All Test Suites
```bash
uv run python tests/test_codec_and_serializer.py
uv run python tests/test_dht_and_bugfixes.py
uv run python tests/test_p2p_pipeline.py
```

---

## 🌐 Running & Testing Across Two Laptops (LAN / Wi-Fi)

With two laptops on the same Wi-Fi/LAN, you can test the Kademlia DHT and passive-layer Reed-Solomon storage in two ways:
1. **Run multiple nodes per laptop on different ports** (e.g., 3 nodes on Laptop A + 3 nodes on Laptop B = 6-node cluster across 2 physical machines).
2. **Use `k=1, m=1` (2 total nodes)** so each laptop runs just a single node.

### ⚠️ Important: Advertise Your Laptop's Actual LAN IP (Not `0.0.0.0`)
In `src/rs_p2p/dht/kademlia.py`, passive nodes advertise `self.contact.host` to peers in the DHT. Either pass your LAN IP directly to `--host`, or bind `0.0.0.0` and pass your LAN IP to `--advertise-host` (e.g., `--host 0.0.0.0 --advertise-host 192.168.1.10`).

---

### Step-by-Step Test Across Two Laptops

#### Step 1: Find the LAN IP of Both Laptops
```bash
hostname -I | awk '{print $1}'
```
Suppose **Laptop A** = `192.168.1.10` and **Laptop B** = `192.168.1.20`.

#### Step 2: Start Nodes on Both Laptops
To test full $k=4, m=2$ (6 nodes total), run **3 terminals on Laptop A** and **3 terminals on Laptop B**:

**On Laptop A (`192.168.1.10`):**
```bash
# Terminal 1 (Bootstrap Seed + Peer 1 on port 7000)
uv run python main.py seed --node-id laptopA_1 --host 192.168.1.10 --port 7000

# Terminal 2 (Peer 2 on port 7001, bootstrapping from 7000)
uv run python main.py peer --node-id laptopA_2 --host 192.168.1.10 --port 7001 --seeds 192.168.1.10:7000

# Terminal 3 (Peer 3 on port 7002, bootstrapping from 7000)
uv run python main.py peer --node-id laptopA_3 --host 192.168.1.10 --port 7002 --seeds 192.168.1.10:7000
```

**On Laptop B (`192.168.1.20`):**
Point `--seeds` to Laptop A (`192.168.1.10:7000`). **Kademlia `lookup_node` (`FindNode`) automatically discovers the rest of the cluster.**
```bash
# Terminal 1 (Peer 4 on port 7001)
uv run python main.py peer --node-id laptopB_1 --host 192.168.1.20 --port 7001 --seeds 192.168.1.10:7000

# Terminal 2 (Peer 5 on port 7002)
uv run python main.py peer --node-id laptopB_2 --host 192.168.1.20 --port 7002 --seeds 192.168.1.10:7000

# Terminal 3 (Peer 6 on port 7003)
uv run python main.py peer --node-id laptopB_3 --host 192.168.1.20 --port 7003 --seeds 192.168.1.10:7000
```

#### Step 3: Verify Random Relay Offload & Dispersal
From **Laptop B**, store a file while only giving it Laptop A's seed (`192.168.1.10:7000`):
```bash
echo "Hello across two laptops via Kademlia DHT and Reed-Solomon!" > test_input.txt

uv run python main.py store-activation test_input.txt \
  --checkpoint-id test_chk_1 \
  --seeds 192.168.1.10:7000 \
  --serializer raw
```
**What this proves:**
1. **`ActiveNodeClient` (Client Mode)** discovers the cluster from one seed without polluting passive K-buckets.
2. **Passive-layer RAID storing**: The client uploads the payload to a random passive relay, which stores chunk `#0` locally, disperses chunks `#1..#5` across 5 distinct remote passive peers, and publishes the manifest into DHT `storage_index`.

#### Step 4: Verify Fault Tolerance & Seed Restart Persistence
Kill up to $m=2$ terminals (e.g. `laptopA_1` seed and `laptopB_1`), then restore from any surviving peer:
```bash
uv run python main.py restore-activation test_chk_1 \
  --seeds 192.168.1.20:7002 \
  --out restored.txt \
  --serializer raw

cat restored.txt
```
If you restart `laptopA_1` (`seed`) using the same `--dir` / `--node-id`, it automatically reloads `routing_table.json` and `dht_store.json` from disk, re-pings known peers, and rejoins the overlay without requiring `--seeds`.

#### Bonus: Inspect a Node's Kademlia Routing Table (Zero Pollution)
```bash
uv run python -c '
import sys; sys.path.insert(0, "src")
from rs_p2p.dht.kademlia import KademliaNode
from rs_p2p.dht.node import Contact, hash_key

dht = KademliaNode(node_id=hash_key("inspector"), host="127.0.0.1", port=0, role="active")
seed = Contact(node_id=hash_key("192.168.1.10:7000"), host="192.168.1.10", port=7000)

print("1. Ping seed:", dht.rpc_ping(seed))
contacts = dht.lookup_node(dht.contact.node_id)
print(f"2. Discovered {len(contacts)} passive peers in Kademlia routing table:")
for c in contacts:
    print(f"   - {c.endpoint} (role={c.role}, id={c.hex_id[:10]}...)")
dht.close()
'
```

---

### Python SDK Usage (`ActiveNodeClient`)
```python
from rs_p2p import ActiveNodeClient

# Connect to passive storage overlay in Client Mode
client = ActiveNodeClient(seed_endpoints=["192.168.1.10:7000"])

# 1. Offload checkpoint activation to a random passive relay (passive layer performs RS RAID)
activation_tensor_bytes = ...  # tensor byte buffer
manifest = client.send_checkpoint_activations(
    activation_payload=activation_tensor_bytes,
    checkpoint_id="chk_stage2_step42",
    k=4,
    m=2,
    serializer_name="tensor",
    metadata={"shape": [1, 64, 4096], "dtype": "bfloat16", "sequence_id": "prompt_102"},
)

# 2. Restoring active node pulls k surviving chunks directly in parallel
restored_tensor, meta = client.receive_backup("chk_stage2_step42", serializer_name="tensor")
print(f"Recovered tensor for {meta['sequence_id']}, shape={meta['shape']}")

client.close()
```
