# Known Issues & Architecture TODOs

## 1. Prevent Client-Only `ActiveNodeClient` (`port=0`) from Polluting DHT `RoutingTable`
- **Locations**:
  - `src/rs_p2p/network/services.py` (`DHTServicer._record_sender`)
  - `src/rs_p2p/network/client.py` (`ActiveNodeClient.__init__`)
- **Problem**: `ActiveNodeClient` is a client SDK (`role="active"`, default `local_port=0`) and does not run a listening `DHTService` gRPC server. However, when it sends `Ping`, `FindNode`, `FindValue`, or `StoreValue` to a passive node, `_record_sender()` inserts `Contact(host="127.0.0.1", port=0, role="active")` into the passive node's `RoutingTable`. Subsequent `lookup_node()` / `iterative_store()` calls return this unroutable `127.0.0.1:0` contact to other nodes.
- **Proposed Fix**:
  - In `_record_sender()`, ignore contacts with `port <= 0` (or `role != "passive"` unless active nodes also host a DHT server).

## 2. Passive Node Selection Load-Balancing & Upload Failover
- **Location**: `src/rs_p2p/network/client.py` (`ActiveNodeClient.send_checkpoint_activations`)
- **Problem**:
  - Currently selects `passive[:total_chunks]` (always the first $k+m$ nodes in bucket order) rather than selecting nodes closest to `hash(checkpoint_id)` or load-balancing across healthy peers.
  - If 1 chosen node is unreachable during `StoreChunk`, upload immediately raises `RuntimeError` instead of falling back to another healthy peer in the routing table.
