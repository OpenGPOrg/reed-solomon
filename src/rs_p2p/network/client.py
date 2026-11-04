"""Active Node P2P Client SDK.

Handles:
1. send_checkpoint_activations(): Client-side RS-encoding & parallel gRPC dispersal to passive nodes.
2. receive_backup(): Concurrent gRPC chunk retrieval (fast-path any k chunks) & RS reconstruction.
3. NS1 compute coordination: Layer placement & node liveness heartbeats via DHT.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import json
import logging
import uuid
import grpc

from ..config import (
    DEFAULT_K,
    DEFAULT_M,
    NS_COMPUTE_LIVENESS,
    NS_STORAGE_INDEX,
)
from ..core.codec import RSCodec
from ..core.serializer import get_serializer
from ..dht.kademlia import KademliaNode
from ..dht.node import Contact, hash_key
from ..proto import storage_pb2, storage_pb2_grpc

logger = logging.getLogger("ActiveNodeClient")


class ActiveNodeClient:
    """SDK for Active Compute Network nodes to interact with the P2P storage & DHT."""

    def __init__(
        self,
        seed_endpoints: List[str],
        client_node_id: Optional[str] = None,
        local_port: int = 0,
    ):
        self.client_node_id_str = client_node_id or f"active_{uuid.uuid4().hex[:8]}"
        self.node_id_int = hash_key(self.client_node_id_str)
        self.seed_endpoints = seed_endpoints

        # Ephemeral DHT client node
        self.dht = KademliaNode(
            node_id=self.node_id_int,
            host="127.0.0.1",
            port=local_port,
            role="active",
        )

        self._storage_channels: Dict[str, grpc.Channel] = {}
        self._bootstrap()

    def _bootstrap(self):
        seeds = []
        for ep in self.seed_endpoints:
            if not ep:
                continue
            parts = ep.split(":")
            shost = parts[0]
            sport = int(parts[1])
            seed_contact = Contact(
                node_id=hash_key(f"{shost}:{sport}"),
                host=shost,
                port=sport,
                role="passive",
            )
            seeds.append(seed_contact)

        if seeds:
            self.dht.bootstrap(seeds)

    def _get_storage_stub(self, endpoint: str) -> storage_pb2_grpc.StorageServiceStub:
        if endpoint not in self._storage_channels:
            channel = grpc.insecure_channel(endpoint)
            self._storage_channels[endpoint] = channel
        return storage_pb2_grpc.StorageServiceStub(self._storage_channels[endpoint])

    def close(self):
        self.dht.close()
        for ch in self._storage_channels.values():
            try:
                ch.close()
            except Exception:
                pass
        self._storage_channels.clear()

    # --- NS1: Active Compute Pipeline Coordination ---

    def register_layer_placement(
        self, stage_id: str, endpoint: str, extra: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Publishes pipeline stage layer placement to DHT NS1."""
        info = {
            "stage_id": stage_id,
            "endpoint": endpoint,
            "extra": extra or {},
        }
        return self.dht.register_layer_placement(stage_id, info)

    def get_layer_placement(self, stage_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves pipeline stage layer placement from DHT NS1."""
        return self.dht.get_layer_placement(stage_id)

    def send_heartbeat(self, liveness_info: Optional[Dict[str, Any]] = None) -> bool:
        """Sends liveness heartbeat to DHT NS1."""
        info = liveness_info or {}
        info["client_id"] = self.client_node_id_str
        return self.dht.send_heartbeat(self.client_node_id_str, info)

    # --- NS2 & Passive Storage: Checkpoint Backup & Restore ---

    def send_checkpoint_activations(
        self,
        activation_payload: Any,
        checkpoint_id: Optional[str] = None,
        k: int = DEFAULT_K,
        m: int = DEFAULT_M,
        serializer_name: str = "tensor",
        metadata: Optional[Dict[str, Any]] = None,
        prefer_passive_nodes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Encodes and disperses activation checkpoint into passive volunteer nodes.

        Steps:
        1. Serializes activation using configurable serializer.
        2. Encodes into k+m chunks using Reed-Solomon.
        3. Identifies k+m available passive nodes via DHT.
        4. Parallel uploads chunks over gRPC.
        5. Publishes manifest into DHT NS2.
        """
        chk_id = checkpoint_id or f"chk_{uuid.uuid4().hex[:12]}"
        serializer = get_serializer(serializer_name)
        serialized_bytes = serializer.serialize(activation_payload, metadata=metadata)

        # Reed-Solomon Erasure Coding
        codec = RSCodec(k=k, m=m)
        chunks, original_length = codec.encode(serialized_bytes)
        total_chunks = k + m

        # Find passive nodes
        target_endpoints: List[str] = []
        if prefer_passive_nodes and len(prefer_passive_nodes) >= total_chunks:
            target_endpoints = prefer_passive_nodes[:total_chunks]
        else:
            # Query DHT for passive storage contacts
            all_contacts = self.dht.routing_table.all_contacts()
            passive = [c for c in all_contacts if c.role == "passive"]
            if len(passive) < total_chunks:
                # If local routing table doesn't have enough, try lookup
                passive = self.dht.lookup_node(self.node_id_int)
                passive = [c for c in passive if c.role == "passive"]

            if len(passive) < total_chunks:
                # Fallback to seed endpoints if discovered list is short
                available_eps = list({c.endpoint for c in passive}.union(set(self.seed_endpoints)))
                if len(available_eps) < total_chunks:
                    raise RuntimeError(
                        f"Insufficient passive nodes available: need {total_chunks}, found {len(available_eps)}"
                    )
                target_endpoints = available_eps[:total_chunks]
            else:
                target_endpoints = [c.endpoint for c in passive[:total_chunks]]

        # Prepare chunk payloads and metadata
        chunk_descriptors = []
        for idx, chunk_bytes in enumerate(chunks):
            chunk_type = "data" if idx < k else "parity"
            chunk_id = f"{chk_id}_chunk_{idx}"
            checksum = hashlib.sha256(chunk_bytes).hexdigest()
            endpoint = target_endpoints[idx]

            chunk_descriptors.append(
                {
                    "chunk_id": chunk_id,
                    "chunk_index": idx,
                    "chunk_type": chunk_type,
                    "checksum_sha256": checksum,
                    "size_bytes": len(chunk_bytes),
                    "endpoint": endpoint,
                    "data": chunk_bytes,
                }
            )

        # Upload chunks in parallel via gRPC
        def _upload_chunk(desc: dict) -> Tuple[bool, dict]:
            stub = self._get_storage_stub(desc["endpoint"])
            req = storage_pb2.StoreChunkRequest(
                chunk_id=desc["chunk_id"],
                chunk_index=desc["chunk_index"],
                data=desc["data"],
                checksum_sha256=desc["checksum_sha256"],
            )
            try:
                resp = stub.StoreChunk(req, timeout=10.0)
                return resp.success, desc
            except Exception as e:
                logger.warning(f"Failed to upload chunk {desc['chunk_id']} to {desc['endpoint']}: {e}")
                return False, desc

        uploaded_manifest_chunks = []
        with ThreadPoolExecutor(max_workers=total_chunks) as executor:
            future_to_chunk = {
                executor.submit(_upload_chunk, desc): desc for desc in chunk_descriptors
            }
            for fut in as_completed(future_to_chunk):
                success, desc = fut.result()
                if not success:
                    raise RuntimeError(f"Failed storing chunk {desc['chunk_id']} on {desc['endpoint']}")
                # Strip binary data from manifest record
                manifest_record = {k: v for k, v in desc.items() if k != "data"}
                uploaded_manifest_chunks.append(manifest_record)

        uploaded_manifest_chunks.sort(key=lambda c: c["chunk_index"])

        manifest = {
            "checkpoint_id": chk_id,
            "k": k,
            "m": m,
            "original_length": original_length,
            "serializer": serializer_name,
            "metadata": metadata or {},
            "chunks": uploaded_manifest_chunks,
        }

        # Publish manifest to DHT NS2
        self.dht.store_manifest(chk_id, manifest)
        logger.info(
            f"Successfully stored checkpoint [{chk_id}] ({original_length} bytes) "
            f"across {total_chunks} passive peers via gRPC."
        )
        return manifest

    def receive_backup(
        self,
        checkpoint_id: str,
        serializer_name: Optional[str] = None,
    ) -> Tuple[Any, Dict[str, Any]]:
        """Recovers checkpoint activation from passive nodes.

        Steps:
        1. Queries DHT NS2 for manifest.
        2. Parallel downloads chunks over gRPC.
        3. As soon as k valid chunks arrive, decodes via RSCodec.
        4. Deserializes payload and returns (payload, metadata).
        """
        manifest = self.dht.get_manifest(checkpoint_id)
        if not manifest:
            raise FileNotFoundError(f"Checkpoint manifest not found in DHT NS2 for id: {checkpoint_id}")

        k = manifest["k"]
        m = manifest["m"]
        original_length = manifest["original_length"]
        serializer_to_use = serializer_name or manifest.get("serializer", "raw")
        chunks_info = manifest["chunks"]

        # Download chunks in parallel
        def _fetch_chunk(chunk_meta: dict) -> Optional[Tuple[int, bytes]]:
            endpoint = chunk_meta["endpoint"]
            chunk_id = chunk_meta["chunk_id"]
            expected_hash = chunk_meta["checksum_sha256"]
            chunk_idx = chunk_meta["chunk_index"]

            try:
                stub = self._get_storage_stub(endpoint)
                req = storage_pb2.GetChunkRequest(chunk_id=chunk_id)
                resp = stub.GetChunk(req, timeout=10.0)
                if not resp.found:
                    return None

                data = resp.data
                actual_hash = hashlib.sha256(data).hexdigest()
                if actual_hash != expected_hash:
                    logger.warning(f"Corrupt chunk {chunk_id} from {endpoint}")
                    return None

                return chunk_idx, data
            except Exception as e:
                logger.warning(f"Failed to fetch chunk {chunk_id} from {endpoint}: {e}")
                return None

        available_blocks: List[bytes] = []
        block_indices: List[int] = []

        with ThreadPoolExecutor(max_workers=len(chunks_info)) as executor:
            futures = [executor.submit(_fetch_chunk, meta) for meta in chunks_info]
            for fut in as_completed(futures):
                res = fut.result()
                if res is not None:
                    idx, data = res
                    available_blocks.append(data)
                    block_indices.append(idx)
                    # Early termination: once k valid chunks are obtained, we can decode!
                    if len(available_blocks) >= k:
                        break

        if len(available_blocks) < k:
            raise RuntimeError(
                f"Failed to restore checkpoint [{checkpoint_id}]: only retrieved "
                f"{len(available_blocks)}/{k} chunks required for RS reconstruction."
            )

        # Reed-Solomon Decode
        codec = RSCodec(k=k, m=m)
        reconstructed_bytes = codec.decode(available_blocks, block_indices, original_length)

        # Deserialize
        serializer = get_serializer(serializer_to_use)
        payload, meta = serializer.deserialize(reconstructed_bytes)
        logger.info(f"Successfully restored checkpoint [{checkpoint_id}] from {len(available_blocks)} surviving chunks.")
        return payload, meta
