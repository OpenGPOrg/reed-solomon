# Known Issues & Architecture TODOs

## 1. Prevent Client-Only `ActiveNodeClient` (`port=0`) from Polluting DHT `RoutingTable`
- **Locations**:
  - `src/rs_p2p/network/services.py` (`DHTServicer._record_sender`)
  - `src/rs_p2p/network/client.py` (`ActiveNodeClient.__init__`)
- **Problem**: `ActiveNodeClient` (and ad-hoc inspector scripts) uses `port=0` and does not run a listening `DHTService` gRPC server. However, when it sends `Ping`, `FindNode`, `FindValue`, or `StoreValue` to a passive node, `_record_sender()` inserts `Contact(host="127.0.0.1", port=0)` into the passive node's `RoutingTable`. Subsequent `lookup_node()` / `iterative_store()` calls return this unroutable `127.0.0.1:0` contact to other nodes.
- **Proposed Fix**:
  - In `_record_sender()`, ignore contacts with `port <= 0` (or `role != "passive"` unless active nodes also host a DHT server).

## 2. Passive Node Selection Load-Balancing & Upload Failover
- **Location**: `src/rs_p2p/network/client.py` (`ActiveNodeClient.send_checkpoint_activations`)
- **Problem**:
  - Currently selects `passive[:total_chunks]` (always the first $k+m$ nodes in bucket order) rather than selecting nodes closest to `hash(checkpoint_id)` or load-balancing across healthy peers.
  - If 1 chosen node is unreachable during `StoreChunk`, upload immediately raises `RuntimeError` instead of falling back to another healthy peer in the routing table.

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

