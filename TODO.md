# Known Issues & Architecture TODOs

## 1. Prevent Client-Only `ActiveNodeClient` (`port=0`) from Polluting DHT `RoutingTable`
- **Locations**:
  - `src/rs_p2p/network/services.py` (`DHTServicer._record_sender`)
  - `src/rs_p2p/network/client.py` (`ActiveNodeClient.__init__`)
- **Problem**: `ActiveNodeClient` (and ad-hoc inspector scripts) uses `port=0` and does not run a listening `DHTService` gRPC server. However, when it sends `Ping`, `FindNode`, `FindValue`, or `StoreValue` to a passive node, `_record_sender()` inserts `Contact(host="127.0.0.1", port=0)` into the passive node's `RoutingTable`. Subsequent `lookup_node()` / `iterative_store()` calls return this unroutable `127.0.0.1:0` contact to other nodes.
- **Proposed Fix**:
  - In `_record_sender()`, ignore contacts with `port <= 0` (or `role != "passive"` unless active nodes also host a DHT server).

## 2. [COMPLETED] Passive Node Selection Load-Balancing & Upload Failover
- **Location**: `src/rs_p2p/network/client.py` (`ActiveNodeClient.send_checkpoint_activations`)
- **Status**: Implemented with XOR distance sort against `hash_key(checkpoint_id)` for natural load balancing, concurrent upload workers, and dynamic failover to standby candidate nodes if any node drops during upload. Evicts dead nodes from DHT routing table. Validated in `tests/test_upload_failover.py`.

## 3. Use `resp.responder` in `rpc_ping` to Prevent Duplicate Endpoints with Guessed Node IDs
- **Locations**:
  - `src/rs_p2p/dht/kademlia.py` (`KademliaNode.rpc_ping`)
  - `src/rs_p2p/network/peer_node.py` (`PassivePeerServer.start`)
  - `src/rs_p2p/network/client.py` (`ActiveNodeClient._bootstrap`)
- **Problem**:
  - During bootstrap, callers do not know the target seed's actual `--node-id`, so they construct a temporary `Contact` with a guessed ID (`hash_key(f"{shost}:{sport}")`).
  - When `rpc_ping(target)` succeeds, `DHTServicer.Ping` returns the server's true `node_id` in `resp.responder`, but `rpc_ping` ignores `resp.responder` and adds `target` (with the guessed `node_id`) into `self.routing_table`.
  - When `lookup_node` subsequently learns the peer's real `node_id` from other peers, the same physical `host:port` ends up in the `RoutingTable` twice under two different `node_id`s (because `KBucket` deduplicates by `node_id`).
- **Proposed Fix**:
  - In `KademliaNode.rpc_ping`, construct/update the `Contact` from `resp.responder` (`node_id=hex_to_int(resp.responder.node_id)`, `host`, `port`, `role`) before calling `self.routing_table.add_contact(...)`.
  - Optionally also deduplicate or evict stale entries with the same `host:port` but different `node_id` in `KBucket.add`.

## 4. Seed Node Restart Isolation (In-Memory DHT State & Empty Bootstrap List)
- **Locations**:
  - `src/rs_p2p/dht/kademlia.py` (`KademliaNode._store`, `RoutingTable`)
  - `src/rs_p2p/network/peer_node.py` (`PassivePeerServer.start`)
- **Problem**:
  - While Reed-Solomon `.chunk` files are persisted to disk via `DiskStorage`, Kademlia's `RoutingTable` and `_store` (which holds `NS_STORAGE_INDEX` checkpoint manifests) are stored purely in memory.
  - If a normal peer restarts, it re-bootstraps against `--seeds` and rejoins the routing overlay, while its disk chunks remain intact.
  - However, if the bootstrap seed node itself restarts with no `--seeds`, it comes up with an empty `RoutingTable` and empty `_store`. Any client that subsequently uses only that restarted seed in `--seeds` cannot find existing manifests or discover the rest of the surviving cluster.
- **Proposed Fix**:
  - Persist DHT `_store` (or at least `NS_STORAGE_INDEX` manifests) and known peer contacts (`routing_table.json`) to `storage_dir` on disk so a restarted seed/peer automatically reloads its routing table and re-pings known peers on startup.
  - Add periodic Kademlia bucket refresh / key republishing so surviving nodes replicate manifests back to rejoined nodes.

## Resolved Issues

### 1. [Resolved] Prevent Client-Only `ActiveNodeClient` (`port=0`) from Polluting DHT `RoutingTable`
- **Resolution:** Enforced Kademlia Server Mode vs. Client Mode (`is_routable_server_contact()` in `src/rs_p2p/dht/node.py`). `DHTServicer._record_sender()`, `rpc_find_node`, and `rpc_find_value` ignore contacts with `role != "passive"`, `port <= 0`, or unspecified hosts (`""`, `"0.0.0.0"`, `"::"`). Active-compute `NS1` coordination was also decoupled and removed from `rs_p2p`.
- **Verified by:** `tests/test_dht_and_bugfixes.py::test_active_client_does_not_pollute_routing_table`.

### 2. [Resolved] Passive Node Selection, Load-Balancing & Failover
- **Resolution:** Moved RS encoding and chunk dispersal onto the passive layer (`StorageServicer.StoreCheckpoint`). `ActiveNodeClient.send_checkpoint_activations()` selects a random passive relay via `discover_random_passive_peers()` with caller-side relay failover. The receiving passive coordinator stores chunk `#0` locally, orders remote peers by XOR distance to `hash_key(checkpoint_id)`, and uses a `backup_queue` for chunk upload failover (rolling back partial chunks if fewer than `k + m` succeed).
- **Verified by:** `tests/test_dht_and_bugfixes.py::test_passive_raid_offload_local_chunk_and_failover` and `tests/test_p2p_pipeline.py`.

### 3. [Resolved] Use `resp.responder` in `rpc_ping` & Deduplicate Endpoints Across Buckets
- **Resolution:** `KademliaNode.rpc_ping` constructs the `Contact` from `PingResponse.responder` (falling back to `target.host` when responder advertises `0.0.0.0`). `RoutingTable.add_contact` deduplicates by `(host, port)` across all 160 K-buckets so stale guessed-ID contacts cannot cause multiple chunks to land on the same physical node.
- **Verified by:** `tests/test_dht_and_bugfixes.py::test_rpc_ping_uses_responder_and_deduplicates_endpoints`.

### 4. [Resolved] Seed & Peer Restart Resilience (Disk-Backed DHT State + Maintenance Loop)
- **Resolution:** `KademliaNode` atomically persists `routing_table.json` and `dht_store.json` (with expiration timestamps) in `storage_dir`. On startup, `PassivePeerServer` reloads persisted state, re-pings known contacts even when `--seeds` is empty, and runs periodic background `refresh_routing_table()` and `republish_store()` maintenance.
- **Verified by:** `tests/test_dht_and_bugfixes.py::test_seed_restart_persistence_and_republish`.

---

## Additional Bugs Fixed During Audit
- **`DHTServicer.FindValue` & `iterative_find_value`:** Fixed `FindValue` to sort closer contacts by `hash_key(request.key)` (instead of `sender.node_id`); fixed `iterative_find_value` to traverse `closer_contacts`, prune dead peers, and accept valid empty `b""` values (`value is not None`).
- **`KademliaNode.lookup_node`:** Fixed premature loop termination after `ALPHA=3` unqueried failures and ensured unreachable peers are excluded from the returned shortlist.
- **`KademliaNode.iterative_store`:** Stopped Client-Mode nodes (`is_server=False`) from storing locally and returning false-positive `success_count = 1`.
- **gRPC Limits & Reconnect Pooling:** Raised max gRPC send/receive message size to `64 MiB`, isolated channel pools with `grpc.use_local_subchannel_pool=1` and fast reconnect backoff (`200ms..2000ms`), and awaited `server.stop(grace).wait()` + port bind verification on `PassivePeerServer`.
- **`receive_backup` Fast Early Termination:** Replaced `with ThreadPoolExecutor` with explicit `shutdown(wait=False, cancel_futures=True)` and duplicate `chunk_index` protection so recovery does not stall on unresponsive peers once `k` shards arrive.
- **Concurrency & Atomic Disk Writes:** Added `RLock` protection across `RoutingTable`, `_store`, and channel pools; made `DiskStorage.write_chunk` and DHT JSON persistence atomic via `NamedTemporaryFile` + `os.replace` with `chunk_id` validation.

---

## Future Enhancements
- **Optional Async `StoreCheckpoint` Mode:** Return immediately after staging raw activation payload on the passive relay while dispersing chunks asynchronously (for ultra-low-latency GPU pipelines that can trade synchronous durability confirmation).
- **Automated Chunk Auto-Healing:** Background scrubbing loop on passive peers to detect missing/corrupt peer chunks and reconstruct/re-replicate degraded checkpoints.
- **Transport Security:** Optional TLS/mTLS gRPC credentials and node identity verification for untrusted WAN deployments.
