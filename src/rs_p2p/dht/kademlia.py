"""Kademlia Distributed Hash Table Node implementation over gRPC.

Supports:
- Server Mode (`role="passive"`, `port > 0`): Participates in DHT routing,
  stores NS_STORAGE_INDEX records, persists state, and maintains K-buckets.
- Client Mode (`role="active"`, `port == 0`): Queries passive DHT servers
  without entering passive routing tables.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import base64
import json
import logging
import math
import os
import random
import secrets
import tempfile
import threading
import time
import grpc

from ..config import (
    GRPC_CHANNEL_OPTIONS,
    KADEMLIA_ALPHA,
    KADEMLIA_K,
    NS_STORAGE_INDEX,
)
from ..proto import dht_pb2, dht_pb2_grpc
from .node import (
    Contact,
    RoutingTable,
    hash_key,
    hex_to_int,
    int_to_hex,
    is_routable_server_contact,
    xor_distance,
)

logger = logging.getLogger("KademliaDHT")

_UNSPECIFIED_HOSTS = ("", "0.0.0.0", "::")


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Writes JSON payload atomically via temporary file and os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.stem}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            json.dump(payload, tmp, indent=2)
            tmp.flush()
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise


class KademliaNode:
    """A Kademlia DHT node operating in the P2P overlay."""

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        role: str = "passive",
        k: int = KADEMLIA_K,
        state_dir: Optional[Path | str] = None,
        advertise_host: Optional[str] = None,
    ):
        effective_host = advertise_host or host
        self.contact = Contact(
            node_id=node_id,
            host=effective_host,
            port=port,
            role=role,
        )
        self.is_server: bool = role == "passive" and port > 0
        self.routing_table = RoutingTable(local_id=node_id, k=k)
        self.k = k
        self.state_dir = Path(state_dir) if state_dir is not None else None

        self._store_lock = threading.RLock()
        self._channels_lock = threading.RLock()
        self._persist_lock = threading.Lock()

        # Local store: store[namespace][key] = (value_bytes, expire_at)
        self._store: Dict[str, Dict[str, Tuple[bytes, float]]] = {
            NS_STORAGE_INDEX: {},
        }
        self._channels: Dict[str, grpc.Channel] = {}

    def _self_pb(self) -> dht_pb2.NodeContact:
        return dht_pb2.NodeContact(
            node_id=self.contact.hex_id,
            host=self.contact.host,
            port=self.contact.port,
            role=self.contact.role,
        )

    def _get_stub(self, contact: Contact) -> dht_pb2_grpc.DHTServiceStub:
        endpoint = contact.endpoint
        with self._channels_lock:
            if endpoint not in self._channels:
                channel = grpc.insecure_channel(endpoint, options=GRPC_CHANNEL_OPTIONS)
                self._channels[endpoint] = channel
            return dht_pb2_grpc.DHTServiceStub(self._channels[endpoint])

    def close(self):
        with self._channels_lock:
            for ch in self._channels.values():
                try:
                    ch.close()
                except Exception:
                    pass
            self._channels.clear()

    # --- Persistence (Routing Table & DHT Store) ---

    def save_routing_table(self) -> None:
        if not self.is_server or self.state_dir is None:
            return
        contacts_data = self.routing_table.to_list()
        with self._persist_lock:
            try:
                _atomic_write_json(self.state_dir / "routing_table.json", contacts_data)
            except Exception as e:
                logger.warning(f"Failed saving routing_table.json: {e}")

    def save_store(self) -> None:
        if not self.is_server or self.state_dir is None:
            return
        now = time.time()
        snapshot: Dict[str, Dict[str, Dict[str, Any]]] = {}
        with self._store_lock:
            for ns, entries in self._store.items():
                ns_dict: Dict[str, Dict[str, Any]] = {}
                expired_keys = []
                for key, (val, expire_at) in entries.items():
                    if now > expire_at:
                        expired_keys.append(key)
                        continue
                    ns_dict[key] = {
                        "value_b64": base64.b64encode(val).decode("ascii"),
                        "expire_at": None if math.isinf(expire_at) else expire_at,
                    }
                for key in expired_keys:
                    del entries[key]
                snapshot[ns] = ns_dict

        with self._persist_lock:
            try:
                _atomic_write_json(self.state_dir / "dht_store.json", snapshot)
            except Exception as e:
                logger.warning(f"Failed saving dht_store.json: {e}")

    def load_state(self) -> List[Contact]:
        """Loads persisted DHT store and returns saved peer contacts for re-pinging."""
        if not self.is_server or self.state_dir is None:
            return []

        store_path = self.state_dir / "dht_store.json"
        if store_path.is_file():
            try:
                raw_store = json.loads(store_path.read_text(encoding="utf-8"))
                now = time.time()
                with self._store_lock:
                    for ns, entries in raw_store.items():
                        if not isinstance(entries, dict):
                            continue
                        ns_bucket = self._store.setdefault(ns, {})
                        for key, entry in entries.items():
                            try:
                                expire_raw = entry.get("expire_at")
                                expire_at = (
                                    float("inf") if expire_raw is None else float(expire_raw)
                                )
                                if now > expire_at:
                                    continue
                                val = base64.b64decode(entry["value_b64"].encode("ascii"))
                                ns_bucket[key] = (val, expire_at)
                            except (KeyError, TypeError, ValueError):
                                continue
            except Exception as e:
                logger.warning(f"Failed loading dht_store.json: {e}")

        rt_path = self.state_dir / "routing_table.json"
        if rt_path.is_file():
            try:
                raw_rt = json.loads(rt_path.read_text(encoding="utf-8"))
                if isinstance(raw_rt, list):
                    return self.routing_table.load_from_list(raw_rt)
            except Exception as e:
                logger.warning(f"Failed loading routing_table.json: {e}")

        return []

    # --- Local Storage Operations ---

    def local_store(
        self, namespace: str, key: str, value: bytes, ttl_seconds: int = 3600
    ) -> None:
        expire_at = time.time() + ttl_seconds if ttl_seconds > 0 else float("inf")
        with self._store_lock:
            if namespace not in self._store:
                self._store[namespace] = {}
            self._store[namespace][key] = (value, expire_at)
        self.save_store()
        logger.debug(f"Local DHT store [{namespace}] {key}")

    def local_get(self, namespace: str, key: str) -> Optional[bytes]:
        expired = False
        with self._store_lock:
            ns = self._store.get(namespace, {})
            entry = ns.get(key)
            if not entry:
                return None
            value, expire_at = entry
            if time.time() > expire_at:
                del ns[key]
                expired = True
            else:
                return value
        if expired:
            self.save_store()
        return None

    # --- Direct RPC calls to remote peers ---

    def _evict_dead_target(self, target: Contact) -> None:
        removed = self.routing_table.remove_by_endpoint(target.host, target.port)
        if self.routing_table.remove_contact(target.node_id):
            removed = True
        if removed:
            self.save_routing_table()

    def rpc_ping(self, target: Contact, timeout: float = 2.0) -> bool:
        """Pings a target node and records its true responder Contact."""
        try:
            stub = self._get_stub(target)
            req = dht_pb2.PingRequest(sender=self._self_pb())
            resp = stub.Ping(req, timeout=timeout)
            if resp.ok:
                r = resp.responder
                actual_id = hex_to_int(r.node_id) if r and r.node_id else target.node_id
                resp_host = (r.host or "").strip() if r else ""
                actual_host = (
                    target.host
                    if (not resp_host or resp_host in _UNSPECIFIED_HOSTS)
                    else resp_host
                )
                actual_port = r.port if (r and r.port > 0) else target.port
                actual_role = r.role if (r and r.role) else target.role
                actual_contact = Contact(
                    node_id=actual_id,
                    host=actual_host,
                    port=actual_port,
                    role=actual_role,
                )
                if actual_contact.node_id != self.contact.node_id:
                    if self.routing_table.add_contact(actual_contact):
                        self.save_routing_table()
                return True
        except Exception:
            self._evict_dead_target(target)
        return False

    def _rpc_find_node_checked(
        self, target: Contact, target_id_hex: str, timeout: float = 3.0
    ) -> Tuple[bool, List[Contact]]:
        """Queries target node for contacts closest to target_id, returning (rpc_ok, contacts)."""
        try:
            stub = self._get_stub(target)
            req = dht_pb2.FindNodeRequest(
                sender=self._self_pb(),
                target_id=target_id_hex,
            )
            resp = stub.FindNode(req, timeout=timeout)
            contacts: List[Contact] = []
            changed = self.routing_table.add_contact(target)
            for c in resp.contacts:
                if not c.node_id:
                    continue
                contact = Contact(
                    node_id=hex_to_int(c.node_id),
                    host=c.host,
                    port=c.port,
                    role=c.role,
                )
                if (
                    contact.node_id == self.contact.node_id
                    or not is_routable_server_contact(contact)
                ):
                    continue
                contacts.append(contact)
                if self.routing_table.add_contact(contact):
                    changed = True
            if changed:
                self.save_routing_table()
            return True, contacts
        except Exception:
            self._evict_dead_target(target)
            return False, []

    def rpc_find_node(
        self, target: Contact, target_id_hex: str, timeout: float = 3.0
    ) -> List[Contact]:
        """Queries target node for contacts closest to target_id."""
        _, contacts = self._rpc_find_node_checked(target, target_id_hex, timeout=timeout)
        return contacts

    def _rpc_find_value_checked(
        self, target: Contact, namespace: str, key: str, timeout: float = 3.0
    ) -> Tuple[bool, bool, Optional[bytes], List[Contact]]:
        """Queries target node for key, returning (rpc_ok, found, value, closer_contacts)."""
        try:
            stub = self._get_stub(target)
            req = dht_pb2.FindValueRequest(
                sender=self._self_pb(),
                namespace=namespace,
                key=key,
            )
            resp = stub.FindValue(req, timeout=timeout)
            changed = self.routing_table.add_contact(target)
            if resp.found:
                if changed:
                    self.save_routing_table()
                return True, True, resp.value, []

            closer: List[Contact] = []
            for c in resp.closer_contacts:
                if not c.node_id:
                    continue
                contact = Contact(
                    node_id=hex_to_int(c.node_id),
                    host=c.host,
                    port=c.port,
                    role=c.role,
                )
                if (
                    contact.node_id == self.contact.node_id
                    or not is_routable_server_contact(contact)
                ):
                    continue
                closer.append(contact)
                if self.routing_table.add_contact(contact):
                    changed = True
            if changed:
                self.save_routing_table()
            return True, False, None, closer
        except Exception:
            self._evict_dead_target(target)
            return False, False, None, []

    def rpc_find_value(
        self, target: Contact, namespace: str, key: str, timeout: float = 3.0
    ) -> Tuple[bool, Optional[bytes], List[Contact]]:
        """Queries target node for a key in namespace."""
        _, found, value, closer = self._rpc_find_value_checked(
            target, namespace, key, timeout=timeout
        )
        return found, value, closer

    def rpc_store_value(
        self,
        target: Contact,
        namespace: str,
        key: str,
        value: bytes,
        ttl_seconds: int = 3600,
        timeout: float = 3.0,
    ) -> bool:
        """Stores a value on a remote node."""
        try:
            stub = self._get_stub(target)
            req = dht_pb2.StoreValueRequest(
                sender=self._self_pb(),
                namespace=namespace,
                key=key,
                value=value,
                ttl_seconds=ttl_seconds,
            )
            resp = stub.StoreValue(req, timeout=timeout)
            return resp.success
        except Exception:
            self._evict_dead_target(target)
            return False

    # --- Iterative Kademlia Lookup / Store Algorithms ---

    def _is_self_endpoint(self, contact: Contact) -> bool:
        if contact.port != self.contact.port:
            return False
        if contact.host == self.contact.host:
            return True
        if self.contact.host in _UNSPECIFIED_HOSTS and contact.host in (
            "127.0.0.1",
            "localhost",
            "0.0.0.0",
            "::",
        ):
            return True
        return False

    def bootstrap(self, seed_contacts: List[Contact]) -> int:
        """Bootstraps by pinging seeds/persisted contacts concurrently and discovering peers."""
        targets: List[Contact] = []
        seen_eps: Set[str] = set()
        for seed in seed_contacts:
            if self._is_self_endpoint(seed) or seed.endpoint in seen_eps:
                continue
            seen_eps.add(seed.endpoint)
            targets.append(seed)

        if not targets:
            return 0

        reachable = 0
        with ThreadPoolExecutor(max_workers=min(len(targets), 8)) as executor:
            futures = [executor.submit(self.rpc_ping, s) for s in targets]
            for fut in as_completed(futures):
                if fut.result():
                    reachable += 1

        if reachable > 0:
            self.lookup_node(self.contact.node_id)
            self.save_routing_table()

        logger.info(
            f"[{self.contact.hex_id[:8]}] Bootstrapped via {reachable}/{len(targets)} seeds. "
            f"Total contacts: {len(self.routing_table.all_contacts())}"
        )
        return reachable

    def lookup_node(self, target_id: int, count: Optional[int] = None) -> List[Contact]:
        """Iteratively finds up to count live passive contacts closest to target_id."""
        limit = count or self.k
        candidates_map: Dict[int, Contact] = {
            c.node_id: c for c in self.routing_table.all_contacts()
        }
        queried: Set[int] = {self.contact.node_id}
        alive: Dict[int, Contact] = {}
        dead: Set[int] = set()
        target_hex = int_to_hex(target_id)

        while True:
            shortlist = sorted(
                candidates_map.values(),
                key=lambda c: xor_distance(c.node_id, target_id),
            )[:limit]
            unqueried = [c for c in shortlist if c.node_id not in queried]
            if not unqueried:
                break

            batch = unqueried[:KADEMLIA_ALPHA]
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                future_to_contact = {
                    executor.submit(
                        self._rpc_find_node_checked, c, target_hex
                    ): c
                    for c in batch
                }
                for fut in as_completed(future_to_contact):
                    c = future_to_contact[fut]
                    queried.add(c.node_id)
                    ok, new_contacts = fut.result()
                    if not ok:
                        dead.add(c.node_id)
                        candidates_map.pop(c.node_id, None)
                        continue
                    alive[c.node_id] = c
                    for nc in new_contacts:
                        if nc.node_id in dead or nc.node_id == self.contact.node_id:
                            continue
                        candidates_map[nc.node_id] = nc

        return sorted(
            alive.values(),
            key=lambda c: xor_distance(c.node_id, target_id),
        )[:limit]

    def iterative_store(
        self, namespace: str, key: str, value: bytes, ttl_seconds: int = 3600
    ) -> int:
        """Stores key-value across the k passive nodes closest to hash(key)."""
        key_id = hash_key(key)
        success_count = 0
        if self.is_server:
            self.local_store(namespace, key, value, ttl_seconds)
            success_count += 1

        closest_nodes = [
            c for c in self.lookup_node(key_id) if c.node_id != self.contact.node_id
        ]
        if closest_nodes:
            with ThreadPoolExecutor(max_workers=min(len(closest_nodes), self.k)) as executor:
                futures = [
                    executor.submit(
                        self.rpc_store_value,
                        contact,
                        namespace,
                        key,
                        value,
                        ttl_seconds,
                    )
                    for contact in closest_nodes
                ]
                for fut in as_completed(futures):
                    if fut.result():
                        success_count += 1
        return success_count

    def iterative_find_value(self, namespace: str, key: str) -> Optional[bytes]:
        """Finds value across the DHT overlay for the given namespace and key."""
        if self.is_server:
            val = self.local_get(namespace, key)
            if val is not None:
                return val

        key_id = hash_key(key)
        candidates_map: Dict[int, Contact] = {
            c.node_id: c for c in self.routing_table.all_contacts()
        }
        queried: Set[int] = {self.contact.node_id}
        dead: Set[int] = set()

        while True:
            shortlist = sorted(
                candidates_map.values(),
                key=lambda c: xor_distance(c.node_id, key_id),
            )[: self.k]
            unqueried = [c for c in shortlist if c.node_id not in queried]
            if not unqueried:
                break

            batch = unqueried[:KADEMLIA_ALPHA]
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                future_to_contact = {
                    executor.submit(
                        self._rpc_find_value_checked, c, namespace, key
                    ): c
                    for c in batch
                }
                for fut in as_completed(future_to_contact):
                    c = future_to_contact[fut]
                    queried.add(c.node_id)
                    ok, found, value, closer = fut.result()
                    if not ok:
                        dead.add(c.node_id)
                        candidates_map.pop(c.node_id, None)
                        continue
                    if found and value is not None:
                        if self.is_server:
                            self.local_store(namespace, key, value, ttl_seconds=3600)
                        return value
                    for nc in closer:
                        if nc.node_id in dead or nc.node_id == self.contact.node_id:
                            continue
                        candidates_map[nc.node_id] = nc

        return None

    # --- Peer Discovery & Periodic Maintenance ---

    def discover_random_passive_peers(
        self, count: Optional[int] = None
    ) -> List[Contact]:
        """Samples passive peers via a random 160-bit lookup and shuffles them."""
        discovered = self.lookup_node(secrets.randbits(160), count=count or self.k)
        if not discovered:
            discovered = self.routing_table.all_contacts()

        dedup: Dict[str, Contact] = {}
        for c in discovered:
            if c.node_id == self.contact.node_id or not is_routable_server_contact(c):
                continue
            dedup[c.endpoint] = c

        peers = list(dedup.values())
        random.SystemRandom().shuffle(peers)
        return peers

    def refresh_routing_table(self) -> int:
        """Refreshes routing buckets via random and self lookups and prunes dead peers."""
        self.lookup_node(self.contact.node_id)
        self.lookup_node(secrets.randbits(160))
        self.save_routing_table()
        return len(self.routing_table.all_contacts())

    def republish_store(self) -> int:
        """Republishes live local DHT records to closest peers in the overlay."""
        if not self.is_server:
            return 0
        now = time.time()
        snapshot: List[Tuple[str, str, bytes, int]] = []
        expired_any = False

        with self._store_lock:
            for ns, entries in self._store.items():
                expired_keys = []
                for key, (val, expire_at) in entries.items():
                    if now > expire_at:
                        expired_keys.append(key)
                        continue
                    ttl_arg = (
                        86400
                        if math.isinf(expire_at)
                        else max(1, int(expire_at - now))
                    )
                    snapshot.append((ns, key, val, ttl_arg))
                for key in expired_keys:
                    del entries[key]
                    expired_any = True

        if expired_any:
            self.save_store()

        republished = 0
        for ns, key, val, ttl_arg in snapshot:
            key_id = hash_key(key)
            closest_nodes = [
                c for c in self.lookup_node(key_id) if c.node_id != self.contact.node_id
            ]
            if not closest_nodes:
                continue
            for contact in closest_nodes:
                self.rpc_store_value(contact, ns, key, val, ttl_seconds=ttl_arg)
            republished += 1
        return republished

    # --- Storage Location Index & Manifests (NS_STORAGE_INDEX) ---

    def store_manifest(
        self, checkpoint_id: str, manifest_dict: Dict[str, Any], ttl_seconds: int = 86400
    ) -> bool:
        """Stores chunk location manifest into DHT NS_STORAGE_INDEX."""
        payload = json.dumps(manifest_dict).encode("utf-8")
        count = self.iterative_store(
            NS_STORAGE_INDEX, f"checkpoint:{checkpoint_id}", payload, ttl_seconds
        )
        return count > 0

    def get_manifest(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """Fetches chunk location manifest from DHT NS_STORAGE_INDEX."""
        raw = self.iterative_find_value(
            NS_STORAGE_INDEX, f"checkpoint:{checkpoint_id}"
        )
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))
