"""Passive Volunteer Peer Server.

Combines gRPC Chunk Storage and Kademlia DHT node into a single lightweight daemon.
Can run on any CPU/RAM/SSD volunteer machine with zero GPU requirements.
"""

from concurrent import futures
from pathlib import Path
from typing import List, Optional
import logging
import threading
import time
import grpc

from ..config import VOLUNTEER_STORAGE_DIR
from ..proto import storage_pb2_grpc, dht_pb2_grpc
from ..storage.disk import DiskStorage
from ..dht.kademlia import KademliaNode
from ..dht.node import Contact, hash_key, int_to_hex
from .services import StorageServicer, DHTServicer

logger = logging.getLogger("PassivePeerServer")


class PassivePeerServer:
    """Unified Passive Volunteer Node serving gRPC storage and participating in DHT."""

    def __init__(
        self,
        node_id_str: str,
        host: str = "127.0.0.1",
        port: int = 7001,
        storage_dir: Optional[Path | str] = None,
        max_workers: int = 10,
    ):
        self.node_id_str = node_id_str
        self.node_id_int = hash_key(node_id_str)
        self.host = host
        self.port = port

        # Dedicated disk storage directory for this peer
        self.storage_dir = Path(storage_dir or (VOLUNTEER_STORAGE_DIR / node_id_str))
        self.storage = DiskStorage(self.storage_dir, node_id=node_id_str)

        # Kademlia DHT Node
        self.dht = KademliaNode(
            node_id=self.node_id_int,
            host=self.host,
            port=self.port,
            role="passive",
        )

        # gRPC Server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
        storage_pb2_grpc.add_StorageServiceServicer_to_server(
            StorageServicer(self.storage), self.server
        )
        dht_pb2_grpc.add_DHTServiceServicer_to_server(
            DHTServicer(self.dht), self.server
        )
        self.server.add_insecure_port(f"{self.host}:{self.port}")
        self._is_running = False

    def start(self, seed_endpoints: Optional[List[str]] = None) -> None:
        """Starts the gRPC server and joins the DHT overlay."""
        self.server.start()
        self._is_running = True
        logger.info(
            f"Passive Volunteer Peer [{self.node_id_str}] started at {self.host}:{self.port} "
            f"(DHT ID: {int_to_hex(self.node_id_int)[:8]}...)"
        )

        # Bootstrap with known seeds if provided
        if seed_endpoints:
            seeds = []
            for ep in seed_endpoints:
                if not ep:
                    continue
                parts = ep.split(":")
                shost = parts[0]
                sport = int(parts[1])
                # Skip self
                if shost == self.host and sport == self.port:
                    continue
                # Seed node ID can be resolved dynamically via ping
                seed_contact = Contact(
                    node_id=hash_key(f"{shost}:{sport}"),
                    host=shost,
                    port=sport,
                    role="passive",
                )
                seeds.append(seed_contact)

            if seeds:
                self.dht.bootstrap(seeds)

    def stop(self, grace: float = 2.0) -> None:
        """Stops the peer server and DHT client connections."""
        if self._is_running:
            self._is_running = False
            self.dht.close()
            self.server.stop(grace)
            logger.info(f"Peer [{self.node_id_str}] stopped.")

    def wait_for_termination(self) -> None:
        self.server.wait_for_termination()
