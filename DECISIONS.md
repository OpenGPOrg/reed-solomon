# Architecture & Engineering Decision Log

This document records the architectural and engineering decisions made for the decoupled Reed-Solomon P2P Storage & Kademlia DHT system (`src/rs_p2p`).

---

## ADR-1: Offload Store Path Only; Keep Direct Parallel Restore on Active Nodes
- **Decision:** Move Reed-Solomon encoding, chunk dispersal, and DHT manifest publishing onto a receiving passive peer (`StorageService.StoreCheckpoint`), while keeping checkpoint recovery (`ActiveNodeClient.receive_backup`) directly on the active node fetching `k` chunks in parallel from surviving passive peers.
- **Rationale:**
  - **Write path:** Active compute nodes (GPUs running inference) offload erasure coding CPU work and `k + m - 1` outbound network fan-out to the passive storage layer via a single upload RPC.
  - **Read path:** Fetching directly from `k` distinct passive peers in parallel avoids funneling `k` chunk streams back through an intermediate relay hop, preserving up to $k\times$ aggregate download bandwidth and allowing immediate decode once the fastest `k` chunks arrive.
- **Trade-offs:** `ActiveNodeClient` retains `RSCodec.decode` and `ActivationSerializer.deserialize`, which is lightweight compared to encoding + dispersing `k + m` shards.

---

## ADR-2: Coordinator Passive Node Stores One Chunk Locally (`chunk #0`)
- **Decision:** When a passive peer receives `StoreCheckpoint`, it writes chunk `#0` directly to its own `DiskStorage` and uploads the remaining `k + m - 1` chunks to distinct remote passive peers.
- **Rationale:**
  - Eliminates 1 network hop and serialization copy on every store operation.
  - Enables minimal clusters of exactly `k + m` passive nodes (e.g., 6 nodes for `k=4, m=2`) rather than requiring `k + m + 1` nodes.
- **Trade-offs:** The coordinator node is always one of the `k + m` shard holders for that checkpoint (balanced across the cluster by random relay selection in ADR-5).

---

## ADR-3: Synchronous `StoreCheckpoint` RPC
- **Decision:** `StorageServicer.StoreCheckpoint` executes RS encoding, parallel `StoreChunk` fan-out, and DHT manifest storage synchronously before returning `StoreCheckpointResponse`.
- **Rationale:**
  - Simplest, stateless implementation — no staging queue, background retry worker, or orphaned un-indexed payloads on coordinator crash.
  - Gives callers strong durability confirmation (`manifest_json` returned only after all `k + m` chunks and DHT index entry succeed).
  - Enables immediate client-side failover to an alternate passive relay if the chosen coordinator fails or lacks enough healthy peers.
- **Future Option:** An asynchronous mode (`202 Accepted` + staging buffer) can be added later if active compute pipelines require non-blocking fire-and-forget checkpoints.

---

## ADR-4: Client-Side Activation Serialization
- **Decision:** `ActiveNodeClient.send_checkpoint_activations` runs `ActivationSerializer.serialize()` locally and transmits `serialized_payload` bytes, `serializer_name`, and `metadata_json` to the passive node.
- **Rationale:**
  - Single serialization pass; avoids forcing passive volunteer nodes to understand Python dictionaries (`JSONPayloadSerializer`) or ML framework tensor formats / custom serializers registered via `register_serializer()`.
  - Symmetric with client-side `deserialize()` in `receive_backup()`.
- **Trade-offs:** Adds a tiny fixed header (`TENS` / JSON metadata) before RS encoding on the active client (negligible CPU/size overhead).

---

## ADR-5: Decoupled Active & Passive P2P Overlays (Kademlia Server Mode vs. Client Mode)
- **Decision:** Remove active-compute layer placement and heartbeat coordination (`NS1` / `NS_COMPUTE_LIVENESS`) from `rs_p2p` so the active inference layer can manage its own P2P mesh independently. Operate `PassivePeerServer` in **Kademlia Server Mode** (`role="passive"`, `port > 0`, routable host) and `ActiveNodeClient` in **Kademlia Client Mode** (`role="active"`, `port=0`), selecting passive relays via random 160-bit ID lookup (`lookup_node(secrets.randbits(160))`) with automatic failover.
- **Rationale:**
  - Prevents ephemeral/compute nodes (`port=0` or separate active protocols) from polluting passive K-buckets (`TODO #1` / libp2p-style Client Mode).
  - Isolates failure domains and churn between GPU inference meshes and volunteer CPU/SSD storage nodes.
  - Uniform random 160-bit lookups spread `StoreCheckpoint` coordinator load across the passive overlay without needing a central load balancer or fixed coordinator list.

---

## ADR-6: Reliability & Bug-Fix Engineering Decisions
- **True Identity Resolution & Cross-Bucket Endpoint Deduplication (`TODO #3`):** `KademliaNode.rpc_ping` constructs contacts from `PingResponse.responder` (falling back to `target.host` if unspecified) rather than guessed `hash_key(host:port)` IDs, and `RoutingTable.add_contact` deduplicates by `(host, port)` across all 160 buckets so a physical node never receives multiple chunks for the same checkpoint.
- **XOR-Ordered Dispersal & Failover (`TODO #2`):** `StorageServicer._select_remote_candidates` sorts distinct healthy peers by XOR distance to `hash_key(checkpoint_id)` and uses a lock-protected `backup_queue` to transparently fail over if a `StoreChunk` RPC fails, rolling back partial uploads if fewer than `k + m` chunks succeed.
- **Restart Resilience (`TODO #4`):** Passive peers atomically persist `routing_table.json` and `dht_store.json` (with expiration timestamps) in their `storage_dir`, re-ping persisted contacts on startup even when `--seeds` is empty, and run a periodic daemon maintenance loop (`refresh_routing_table` + `republish_store`).
- **Kademlia Lookup & RPC Correctness:**
  - `DHTServicer.FindValue` searches closest to `hash_key(request.key)` (not `sender.node_id`).
  - `KademliaNode.lookup_node` and `iterative_find_value` maintain a dynamic shortlist, query `ALPHA` peers concurrently, prune dead peers, traverse `closer_contacts`, and handle `b""` values properly (`value is not None`).
  - `KademliaNode.iterative_store` only counts local storage when `is_server` is `True`, preventing client-mode false positives.
- **gRPC Transport & Resource Safety:**
  - Raised max message size to `64 MiB` (`GRPC_SERVER_OPTIONS` / `GRPC_CHANNEL_OPTIONS`) so un-sharded checkpoint payloads above gRPC's default `4 MiB` succeed.
  - Enabled `grpc.use_local_subchannel_pool=1` and fast reconnect backoff (`200ms`–`2000ms`) so cached `TRANSIENT_FAILURE` state on one channel does not poison fresh clients after a seed restarts.
  - Awaited `server.stop(grace).wait(...)` and checked `add_insecure_port` return value on `PassivePeerServer` to eliminate port-rebind races during fast restarts.
  - Fixed `receive_backup` early termination by calling `executor.shutdown(wait=False, cancel_futures=True)` instead of blocking inside a `with ThreadPoolExecutor` context manager.
  - Added `RLock` concurrency guards in `RoutingTable` / `KademliaNode` / gRPC stub pools and atomic `NamedTemporaryFile` + `os.replace` writes in `DiskStorage`.
