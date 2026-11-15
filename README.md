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

## 🌐 Running Across Real Volunteer Computers

### 1. Bootstrap DHT Seed Node
On the initial seed machine (e.g. `192.168.1.10`):
```bash
uv run python main.py seed --host 0.0.0.0 --port 7000
```

### 2. Volunteer Storage Peers (Passive Nodes)
On any volunteer PC or laptop (e.g. PC 1 at `192.168.1.51`, PC 2 at `192.168.1.52`):
```bash
# On Volunteer PC 1:
uv run python main.py peer --node-id laptop_1 --port 7001 --seeds 192.168.1.10:7000

# On Volunteer PC 2:
uv run python main.py peer --node-id desktop_2 --port 7001 --seeds 192.168.1.10:7000
```

### 3. Active Compute Node (Pipeline Inference & Checkpoint Restore)
In your Python pipeline code or via CLI:

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
