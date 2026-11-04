"""Kademlia Distributed Hash Table Node implementation over gRPC.

Supports:
- NS1: Layer Placement & Active Compute Node Liveness
- NS2: Shared Location Index & Checkpoint Manifests
"""

from typing import Dict, List, Optional, Tuple, Any
import json
import logging
import time
import grpc

from ..config import (
    KADEMLIA_K,
    KADEMLIA_ALPHA,
    NS_COMPUTE_LIVENESS,
    NS_STORAGE_INDEX,
)
from ..proto import dht_pb2, dht_pb2_grpc
from .node import (
    Contact,
    RoutingTable,
    hash_key,
    int_to_hex,
    hex_to_int,
    xor_distance,
)

logger = logging.getLogger("KademliaDHT")


class KademliaNode:
    """A Kademlia DHT node operating in the P2P overlay."""

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        role: str = "passive",
        k: int = KADEMLIA_K,
    ):
        self.contact = Contact(node_id=node_id, host=host, port=port, role=role)
        self.routing_table = RoutingTable(local_id=node_id, k=k)
        self.k = k

        # Namespace separated local store: store[namespace][key] = (value_bytes, expire_at)
        self._store: Dict[str, Dict[str, Tuple[bytes, float]]] = {
            NS_COMPUTE_LIVENESS: {},
            NS_STORAGE_INDEX: {},
        }
        self._channels: Dict[str, grpc.Channel] = {}

    def _get_stub(self, contact: Contact) -> dht_pb2_grpc.DHTServiceStub:
        endpoint = contact.endpoint
        if endpoint not in self._channels:
            channel = grpc.insecure_channel(endpoint)
            self._channels[endpoint] = channel
        return dht_pb2_grpc.DHTServiceStub(self._channels[endpoint])

    def close(self):
        for ch in self._channels.values():
            try:
                ch.close()
            except Exception:
                pass
        self._channels.clear()

    # --- Local Storage Operations ---

    def local_store(
        self, namespace: str, key: str, value: bytes, ttl_seconds: int = 3600
    ) -> None:
        if namespace not in self._store:
            self._store[namespace] = {}
        expire_at = time.time() + ttl_seconds if ttl_seconds > 0 else float("inf")
        self._store[namespace][key] = (value, expire_at)
        logger.debug(f"Local DHT store [{namespace}] {key}")

    def local_get(self, namespace: str, key: str) -> Optional[bytes]:
        ns = self._store.get(namespace, {})
        entry = ns.get(key)
        if not entry:
            return None
        value, expire_at = entry
        if time.time() > expire_at:
            del ns[key]
            return None
        return value

    # --- Direct RPC calls to remote peers ---

    def rpc_ping(self, target: Contact, timeout: float = 2.0) -> bool:
        """Pings a target node. Returns True if alive."""
        try:
            stub = self._get_stub(target)
            req = dht_pb2.PingRequest(
                sender=dht_pb2.NodeContact(
                    node_id=self.contact.hex_id,
                    host=self.contact.host,
                    port=self.contact.port,
                    role=self.contact.role,
                )
            )
            resp = stub.Ping(req, timeout=timeout)
            if resp.ok:
                self.routing_table.add_contact(target)
                return True
        except Exception:
            self.routing_table.remove_contact(target.node_id)
        return False

    def rpc_find_node(
        self, target: Contact, target_id_hex: str, timeout: float = 3.0
    ) -> List[Contact]:
        """Queries target node for contacts closest to target_id."""
        try:
            stub = self._get_stub(target)
            req = dht_pb2.FindNodeRequest(
                sender=dht_pb2.NodeContact(
                    node_id=self.contact.hex_id,
                    host=self.contact.host,
                    port=self.contact.port,
                    role=self.contact.role,
                ),
                target_id=target_id_hex,
            )
            resp = stub.FindNode(req, timeout=timeout)
            contacts = []
            for c in resp.contacts:
                contact = Contact(
                    node_id=hex_to_int(c.node_id),
                    host=c.host,
                    port=c.port,
                    role=c.role,
                )
                contacts.append(contact)
                self.routing_table.add_contact(contact)
            return contacts
        except Exception:
            self.routing_table.remove_contact(target.node_id)
            return []

    def rpc_find_value(
        self, target: Contact, namespace: str, key: str, timeout: float = 3.0
    ) -> Tuple[bool, Optional[bytes], List[Contact]]:
        """Queries target node for a key in namespace."""
        try:
            stub = self._get_stub(target)
            req = dht_pb2.FindValueRequest(
                sender=dht_pb2.NodeContact(
                    node_id=self.contact.hex_id,
                    host=self.contact.host,
                    port=self.contact.port,
                    role=self.contact.role,
                ),
                namespace=namespace,
                key=key,
            )
            resp = stub.FindValue(req, timeout=timeout)
            if resp.found:
                return True, resp.value, []

            closer = []
            for c in resp.closer_contacts:
                contact = Contact(
                    node_id=hex_to_int(c.node_id),
                    host=c.host,
                    port=c.port,
                    role=c.role,
                )
                closer.append(contact)
                self.routing_table.add_contact(contact)
            return False, None, closer
        except Exception:
            self.routing_table.remove_contact(target.node_id)
            return False, None, []

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
                sender=dht_pb2.NodeContact(
                    node_id=self.contact.hex_id,
                    host=self.contact.host,
                    port=self.contact.port,
                    role=self.contact.role,
                ),
                namespace=namespace,
                key=key,
                value=value,
                ttl_seconds=ttl_seconds,
            )
            resp = stub.StoreValue(req, timeout=timeout)
            return resp.success
        except Exception:
            self.routing_table.remove_contact(target.node_id)
            return False

    # --- Iterative Kademlia Lookup / Store Algorithms ---

    def bootstrap(self, seed_contacts: List[Contact]) -> int:
        """Bootstraps the node by pinging seed nodes and finding own ID."""
        reachable = 0
        for seed in seed_contacts:
            if seed.node_id == self.contact.node_id:
                continue
            if self.rpc_ping(seed):
                reachable += 1
                self.lookup_node(self.contact.node_id)
        logger.info(
            f"[{self.contact.hex_id[:8]}] Bootstrapped via {reachable}/{len(seed_contacts)} seeds. Total contacts: {len(self.routing_table.all_contacts())}"
        )
        return reachable

    def lookup_node(self, target_id: int) -> List[Contact]:
        """Iteratively finds the closest nodes to target_id."""
        closest = self.routing_table.find_closest(target_id, count=self.k)
        visited = {self.contact.node_id}

        while True:
            unvisited = [c for c in closest if c.node_id not in visited]
            if not unvisited:
                break

            # Query up to ALPHA nodes in parallel or sequentially
            candidates = unvisited[:KADEMLIA_ALPHA]
            found_any_new = False
            for c in candidates:
                visited.add(c.node_id)
                new_contacts = self.rpc_find_node(c, int_to_hex(target_id))
                for nc in new_contacts:
                    if nc.node_id not in [x.node_id for x in closest]:
                        closest.append(nc)
                        found_any_new = True

            closest.sort(key=lambda x: xor_distance(x.node_id, target_id))
            closest = closest[: self.k]
            if not found_any_new:
                break

        return closest

    def iterative_store(
        self, namespace: str, key: str, value: bytes, ttl_seconds: int = 3600
    ) -> int:
        """Stores key-value across the k nodes closest to hash(key), and locally."""
        key_id = hash_key(key)
        # Store locally first
        self.local_store(namespace, key, value, ttl_seconds)
        success_count = 1

        closest_nodes = self.lookup_node(key_id)
        for contact in closest_nodes:
            if contact.node_id == self.contact.node_id:
                continue
            if self.rpc_store_value(contact, namespace, key, value, ttl_seconds):
                success_count += 1
        return success_count

    def iterative_find_value(self, namespace: str, key: str) -> Optional[bytes]:
        """Finds value across DHT overlay for the given namespace and key."""
        # 1. Check local store
        val = self.local_get(namespace, key)
        if val is not None:
            return val

        # 2. Iteratively search closest nodes
        key_id = hash_key(key)
        closest = self.lookup_node(key_id)
        for contact in closest:
            if contact.node_id == self.contact.node_id:
                continue
            found, value, _ = self.rpc_find_value(contact, namespace, key)
            if found and value:
                # Cache locally for faster subsequent lookups
                self.local_store(namespace, key, value, ttl_seconds=3600)
                return value
        return None

    # --- Namespace 1 (NS1): Compute Network Liveness & Layer Placement ---

    def register_layer_placement(
        self, layer_key: str, placement_info: Dict[str, Any], ttl_seconds: int = 1800
    ) -> bool:
        """Publishes pipeline stage layer placement to NS1."""
        payload = json.dumps(placement_info).encode("utf-8")
        count = self.iterative_store(NS_COMPUTE_LIVENESS, f"layer:{layer_key}", payload, ttl_seconds)
        return count > 0

    def get_layer_placement(self, layer_key: str) -> Optional[Dict[str, Any]]:
        """Retrieves pipeline stage layer placement from NS1."""
        raw = self.iterative_find_value(NS_COMPUTE_LIVENESS, f"layer:{layer_key}")
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))

    def send_heartbeat(
        self, node_id: str, liveness_info: Dict[str, Any], ttl_seconds: int = 60
    ) -> bool:
        """Registers/updates node liveness in NS1."""
        payload = json.dumps(liveness_info).encode("utf-8")
        count = self.iterative_store(NS_COMPUTE_LIVENESS, f"heartbeat:{node_id}", payload, ttl_seconds)
        return count > 0

    def get_node_details(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves liveness/details for an active compute or passive node."""
        raw = self.iterative_find_value(NS_COMPUTE_LIVENESS, f"heartbeat:{node_id}")
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))

    # --- Namespace 2 (NS2): Storage Location Index & Manifests ---

    def store_manifest(self, checkpoint_id: str, manifest_dict: Dict[str, Any], ttl_seconds: int = 86400) -> bool:
        """Stores chunk location manifest into DHT NS2."""
        payload = json.dumps(manifest_dict).encode("utf-8")
        count = self.iterative_store(NS_STORAGE_INDEX, f"checkpoint:{checkpoint_id}", payload, ttl_seconds)
        return count > 0

    def get_manifest(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """Fetches chunk location manifest from DHT NS2."""
        raw = self.iterative_find_value(NS_STORAGE_INDEX, f"checkpoint:{checkpoint_id}")
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))
