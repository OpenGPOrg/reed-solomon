"""Passive Volunteer Peer Server.

Combines gRPC Chunk Storage (including passive-layer RAID coordination) and
Kademlia DHT server mode into a single lightweight daemon.
"""

from concurrent import futures
from pathlib import Path
from typing import Dict, List, Optional
import logging
import threading
import time
import grpc

from ..config import (
    DHT_REFRESH_INTERVAL_SEC,
    DHT_REPUBLISH_INTERVAL_SEC,
    GRPC_SERVER_OPTIONS,
    VOLUNTEER_STORAGE_DIR,
)
from ..dht.kademlia import KademliaNode
from ..dht.node import Contact, hash_key, int_to_hex
from ..proto import dht_pb2_grpc, storage_pb2_grpc
from ..storage.disk import DiskStorage
from .services import DHTServicer, StorageServicer

logger = logging.getLogger("PassivePeerServer")


class PassivePeerServer:
    """Unified Passive Volunteer Node serving gRPC storage and participating in DHT."""

    def __init__(
        self,
        node_id_str: str,
        host: str = "127.0.0.1",
        port: int = 7001,
        storage_dir: Optional[Path | str] = None,
        max_workers: int = 16,
        advertise_host: Optional[str] = None,
        refresh_interval: float = DHT_REFRESH_INTERVAL_SEC,
        republish_interval: float = DHT_REPUBLISH_INTERVAL_SEC,
    ):
        self.node_id_str = node_id_str
        self.node_id_int = hash_key(node_id_str)
        self.host = host
        self.advertise_host = advertise_host or host
        self.port = port
        self.refresh_interval = refresh_interval
        self.republish_interval = republish_interval

        if self.advertise_host in ("0.0.0.0", "::"):
            logger.warning(
                f"Peer [{node_id_str}] advertising unspecified address {self.advertise_host}:{port}. "
                "Pass a reachable LAN IP via --host or --advertise-host for multi-machine clusters."
            )

        # Dedicated disk storage directory for this peer
        self.storage_dir = Path(storage_dir or (VOLUNTEER_STORAGE_DIR / node_id_str))
        self.storage = DiskStorage(self.storage_dir, node_id=node_id_str)

        # Kademlia DHT Node (Server Mode + disk-backed state)
        self.dht = KademliaNode(
            node_id=self.node_id_int,
            host=self.host,
            port=self.port,
            role="passive",
            state_dir=self.storage_dir,
            advertise_host=self.advertise_host,
        )

        # gRPC Server & Servicers
        self.storage_servicer = StorageServicer(self.storage, dht_node=self.dht)
        self.dht_servicer = DHTServicer(self.dht)
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=max_workers),
            options=GRPC_SERVER_OPTIONS,
        )
        storage_pb2_grpc.add_StorageServiceServicer_to_server(
            self.storage_servicer, self.server
        )
        dht_pb2_grpc.add_DHTServiceServicer_to_server(
            self.dht_servicer, self.server
        )
        bound_port = self.server.add_insecure_port(f"{self.host}:{self.port}")
        if bound_port == 0:
            raise RuntimeError(f"Failed to bind gRPC server on {self.host}:{self.port}")
        self.port = bound_port
        self.dht.contact.port = bound_port

        self._is_running = False
        self._stop_event = threading.Event()
        self._maint_thread: Optional[threading.Thread] = None

    def _parse_seed_endpoints(self, seed_endpoints: Optional[List[str]]) -> List[Contact]:
        contacts: List[Contact] = []
        if not seed_endpoints:
            return contacts
        for raw_ep in seed_endpoints:
            ep = (raw_ep or "").strip()
            if not ep or ":" not in ep:
                continue
            shost, sport_str = ep.rsplit(":", 1)
            shost = shost.strip()
            try:
                sport = int(sport_str)
            except ValueError:
                continue
            if sport <= 0 or not shost:
                continue
            candidate = Contact(
                node_id=hash_key(f"{shost}:{sport}"),
                host=shost,
                port=sport,
                role="passive",
            )
            if self.dht._is_self_endpoint(candidate):
                continue
            contacts.append(candidate)
        return contacts

    def _maintenance_loop(self) -> None:
        now = time.monotonic()
        next_refresh = (
            now + self.refresh_interval if self.refresh_interval > 0 else float("inf")
        )
        next_republish = (
            now + self.republish_interval if self.republish_interval > 0 else float("inf")
        )

        while not self._stop_event.is_set():
            next_wake = min(next_refresh, next_republish)
            if next_wake == float("inf"):
                self._stop_event.wait(timeout=5.0)
                continue
            wait_sec = max(0.05, next_wake - time.monotonic())
            if self._stop_event.wait(timeout=wait_sec):
                break

            cur = time.monotonic()
            if cur >= next_refresh:
                try:
                    self.dht.refresh_routing_table()
                except Exception as e:
                    logger.debug(f"[{self.node_id_str}] Refresh error: {e}")
                next_refresh = time.monotonic() + self.refresh_interval

            if cur >= next_republish:
                try:
                    self.dht.republish_store()
                except Exception as e:
                    logger.debug(f"[{self.node_id_str}] Republish error: {e}")
                next_republish = time.monotonic() + self.republish_interval

    def start(self, seed_endpoints: Optional[List[str]] = None) -> None:
        """Starts the gRPC server, restores persisted DHT state, and joins the overlay."""
        self.server.start()
        self._is_running = True
        self._stop_event.clear()
        logger.info(
            f"Passive Volunteer Peer [{self.node_id_str}] started at {self.host}:{self.port} "
            f"(advertise={self.advertise_host}:{self.port}, DHT ID: {int_to_hex(self.node_id_int)[:8]}...)"
        )

        persisted_contacts = self.dht.load_state()
        explicit_seeds = self._parse_seed_endpoints(seed_endpoints)

        # Combine explicit seeds and persisted contacts deduplicated by endpoint
        combined_by_ep: Dict[str, Contact] = {}
        for c in explicit_seeds + persisted_contacts:
            if not self.dht._is_self_endpoint(c):
                combined_by_ep.setdefault(c.endpoint, c)

        if combined_by_ep:
            self.dht.bootstrap(list(combined_by_ep.values()))

        if self.refresh_interval > 0 or self.republish_interval > 0:
            self._maint_thread = threading.Thread(
                target=self._maintenance_loop,
                name=f"dht-maint-{self.node_id_str}",
                daemon=True,
            )
            self._maint_thread.start()

    def stop(self, grace: float = 2.0) -> None:
        """Stops maintenance, persists DHT state, and shuts down gRPC server."""
        if self._is_running:
            self._is_running = False
            self._stop_event.set()
            if self._maint_thread is not None and self._maint_thread.is_alive():
                self._maint_thread.join(timeout=2.0)
            self.dht.save_routing_table()
            self.dht.save_store()
            self.storage_servicer.close()
            self.dht.close()
            stop_event = self.server.stop(grace)
            stop_event.wait(timeout=max(grace, 0.0) + 1.0)
            logger.info(f"Peer [{self.node_id_str}] stopped.")

    def wait_for_termination(self) -> None:
        self.server.wait_for_termination()
