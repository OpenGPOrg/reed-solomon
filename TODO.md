# Issues Tracker & Future Enhancements

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
