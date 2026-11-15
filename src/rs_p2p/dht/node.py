"""Kademlia Node, Contact, XOR distance metric, and K-Bucket routing table."""

from dataclasses import dataclass
from typing import List, Optional, Dict
import hashlib
import time

from ..config import KADEMLIA_K, DHT_ID_BITS


def hash_key(key: str | bytes) -> int:
    """Hashes string or bytes to a 160-bit integer using SHA-1."""
    if isinstance(key, str):
        key = key.encode("utf-8")
    digest = hashlib.sha1(key).digest()
    return int.from_bytes(digest, byteorder="big")


def int_to_hex(val: int) -> str:
    """Converts a 160-bit int to a 40-character hex string."""
    return f"{val:040x}"


def hex_to_int(hex_str: str) -> int:
    """Converts a hex string to an integer."""
    return int(hex_str, 16)


def xor_distance(id1: int, id2: int) -> int:
    """Computes XOR distance metric between two 160-bit IDs."""
    return id1 ^ id2


@dataclass
class Contact:
    """A contact representing a peer in the Kademlia DHT overlay."""

    node_id: int
    host: str
    port: int
    role: str = "passive"  # "passive" (storage) or "active" (compute)
    last_seen: float = 0.0

    @property
    def hex_id(self) -> str:
        return int_to_hex(self.node_id)

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"

    def touch(self):
        self.last_seen = time.time()

    def is_alive(self, timeout_sec: float = 30.0) -> bool:
        return (time.time() - self.last_seen) < timeout_sec


class KBucket:
    """A single Kademlia bucket storing up to k contacts."""

    def __init__(self, k: int = KADEMLIA_K):
        self.k = k
        self.contacts: List[Contact] = []

    def add(self, contact: Contact) -> bool:
        """Adds contact or moves to tail (most recently seen). Returns True if added/updated."""
        for i, c in enumerate(self.contacts):
            if c.node_id == contact.node_id:
                # Update contact and move to end (MRU)
                c.host = contact.host
                c.port = contact.port
                c.role = contact.role
                c.touch()
                self.contacts.append(self.contacts.pop(i))
                return True

        if len(self.contacts) < self.k:
            contact.touch()
            self.contacts.append(contact)
            return True

        # Bucket is full; in basic implementation, drop if head is still active
        head = self.contacts[0]
        if not head.is_alive():
            self.contacts.pop(0)
            contact.touch()
            self.contacts.append(contact)
            return True
        return False

    def remove(self, node_id: int) -> bool:
        for i, c in enumerate(self.contacts):
            if c.node_id == node_id:
                self.contacts.pop(i)
                return True
        return False

    def get_contacts(self) -> List[Contact]:
        return list(self.contacts)


class RoutingTable:
    """Routing table maintaining 160 K-buckets based on XOR distance from local node."""

    def __init__(self, local_id: int, k: int = KADEMLIA_K):
        self.local_id = local_id
        self.k = k
        self.buckets = [KBucket(k=k) for _ in range(DHT_ID_BITS)]

    def _get_bucket_index(self, node_id: int) -> int:
        dist = xor_distance(self.local_id, node_id)
        if dist == 0:
            return 0
        return dist.bit_length() - 1

    def add_contact(self, contact: Contact) -> bool:
        """Inserts or updates a contact in the appropriate bucket."""
        if contact.node_id == self.local_id:
            return False
        idx = self._get_bucket_index(contact.node_id)
        return self.buckets[idx].add(contact)

    def remove_contact(self, node_id: int) -> bool:
        idx = self._get_bucket_index(node_id)
        return self.buckets[idx].remove(node_id)

    def find_closest(
        self, target_id: int, count: int = KADEMLIA_K, exclude_id: Optional[int] = None
    ) -> List[Contact]:
        """Finds up to count contacts closest to target_id ordered by XOR distance."""
        all_contacts = self.all_contacts()
        if exclude_id is not None:
            all_contacts = [c for c in all_contacts if c.node_id != exclude_id]

        all_contacts.sort(key=lambda c: xor_distance(c.node_id, target_id))
        return all_contacts[:count]

    def all_contacts(self) -> List[Contact]:
        result = []
        for b in self.buckets:
            result.extend(b.get_contacts())
        return result

    def get_passive_storage_nodes(self, count: int) -> List[Contact]:
        """Finds available passive nodes for chunk storage."""
        passive = [c for c in self.all_contacts() if c.role == "passive"]
        return passive[:count]
