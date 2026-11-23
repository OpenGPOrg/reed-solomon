"""Active Node P2P Client SDK (Kademlia Client Mode).

Handles:
1. send_checkpoint_activations(): Client-side activation serialization & upload
   to a random passive peer, which performs Reed-Solomon sharding and RAID storage.
2. receive_backup(): Direct parallel gRPC chunk retrieval (fast-path any k chunks)
   & client-side RS reconstruction for maximum aggregate read bandwidth.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import json
import logging
import threading
import uuid
import grpc

from ..config import DEFAULT_K, DEFAULT_M, GRPC_CHANNEL_OPTIONS
from ..core.codec import RSCodec
from ..core.serializer import get_serializer
from ..dht.kademlia import KademliaNode
from ..dht.node import Contact, hash_key
from ..proto import storage_pb2, storage_pb2_grpc

logger = logging.getLogger("ActiveNodeClient")


class ActiveNodeClient:
    """Client-Mode SDK for Active Compute nodes to offload backups & restore directly."""

    def __init__(
        self,
        seed_endpoints: List[str],
        client_node_id: Optional[str] = None,
    ):
        self.client_node_id_str = client_node_id or f"active_{uuid.uuid4().hex[:8]}"
        self.node_id_int = hash_key(self.client_node_id_str)
        self.seed_endpoints = [ep.strip() for ep in seed_endpoints if ep and ep.strip()]

        # Client-Mode DHT handle (port=0, never enters passive routing tables)
        self.dht = KademliaNode(
            node_id=self.node_id_int,
            host="127.0.0.1",
            port=0,
            role="active",
            state_dir=None,
        )

        self._storage_channels: Dict[str, grpc.Channel] = {}
        self._channels_lock = threading.RLock()
        self._bootstrap()

    def _bootstrap(self) -> None:
        seeds: List[Contact] = []
        for ep in self.seed_endpoints:
            if ":" not in ep:
                continue
            shost, sport_str = ep.rsplit(":", 1)
            shost = shost.strip()
            try:
                sport = int(sport_str)
            except ValueError:
                continue
            if sport <= 0 or not shost:
                continue
            seeds.append(
                Contact(
                    node_id=hash_key(f"{shost}:{sport}"),
                    host=shost,
                    port=sport,
                    role="passive",
                )
            )

        if seeds:
            self.dht.bootstrap(seeds)

    def _get_storage_stub(self, endpoint: str) -> storage_pb2_grpc.StorageServiceStub:
        with self._channels_lock:
            if endpoint not in self._storage_channels:
                channel = grpc.insecure_channel(endpoint, options=GRPC_CHANNEL_OPTIONS)
                self._storage_channels[endpoint] = channel
            return storage_pb2_grpc.StorageServiceStub(self._storage_channels[endpoint])

    def close(self) -> None:
        self.dht.close()
        with self._channels_lock:
            for ch in self._storage_channels.values():
                try:
                    ch.close()
                except Exception:
                    pass
            self._storage_channels.clear()

    def _select_relay_candidates(
        self, prefer_passive_node: Optional[str] = None
    ) -> List[str]:
        """Returns deduplicated passive relay endpoints starting with optional preference."""
        candidates: List[str] = []
        seen = set()

        if prefer_passive_node and prefer_passive_node.strip():
            ep = prefer_passive_node.strip()
            candidates.append(ep)
            seen.add(ep)

        for contact in self.dht.discover_random_passive_peers():
            if contact.endpoint not in seen:
                candidates.append(contact.endpoint)
                seen.add(contact.endpoint)

        for ep in self.seed_endpoints:
            if ep not in seen:
                candidates.append(ep)
                seen.add(ep)

        return candidates

    # --- Storage Index & Passive RAID Offload ---

    def send_checkpoint_activations(
        self,
        activation_payload: Any,
        checkpoint_id: Optional[str] = None,
        k: int = DEFAULT_K,
        m: int = DEFAULT_M,
        serializer_name: str = "tensor",
        metadata: Optional[Dict[str, Any]] = None,
        prefer_passive_node: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Serializes activation locally and offloads RS encoding + RAID dispersal to a passive node."""
        if k < 1 or m < 1 or k + m > 256:
            raise ValueError(f"Invalid RS parameters k={k}, m={m}")

        chk_id = checkpoint_id or f"chk_{uuid.uuid4().hex[:12]}"
        serializer = get_serializer(serializer_name)
        serialized_bytes = serializer.serialize(activation_payload, metadata=metadata)
        metadata_json = json.dumps(metadata or {})

        candidates = self._select_relay_candidates(prefer_passive_node=prefer_passive_node)
        if not candidates:
            raise RuntimeError("No passive relay endpoints available in DHT or seed list")

        req = storage_pb2.StoreCheckpointRequest(
            checkpoint_id=chk_id,
            serialized_payload=serialized_bytes,
            k=k,
            m=m,
            serializer_name=serializer_name,
            metadata_json=metadata_json,
        )

        last_error = "no attempts executed"
        for endpoint in candidates:
            try:
                stub = self._get_storage_stub(endpoint)
                resp = stub.StoreCheckpoint(req, timeout=30.0)
                if resp.success:
                    manifest = json.loads(resp.manifest_json)
                    logger.info(
                        f"Checkpoint [{chk_id}] offloaded to passive coordinator {endpoint} "
                        f"({len(manifest.get('chunks', []))} chunks stored)."
                    )
                    return manifest
                last_error = resp.error_message or "StoreCheckpoint returned success=False"
                logger.warning(
                    f"Passive relay {endpoint} failed storing [{chk_id}]: {last_error}"
                )
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Passive relay {endpoint} unreachable for [{chk_id}]: {e}"
                )

        raise RuntimeError(
            f"Failed storing checkpoint [{chk_id}] via passive relays ({len(candidates)} tried): {last_error}"
        )

    def receive_backup(
        self,
        checkpoint_id: str,
        serializer_name: Optional[str] = None,
    ) -> Tuple[Any, Dict[str, Any]]:
        """Recovers checkpoint activation directly from any k surviving passive peers."""
        manifest = self.dht.get_manifest(checkpoint_id)
        if not manifest:
            raise FileNotFoundError(
                f"Checkpoint manifest not found in DHT storage_index for id: {checkpoint_id}"
            )

        k = manifest["k"]
        m = manifest["m"]
        original_length = manifest["original_length"]
        serializer_to_use = serializer_name or manifest.get("serializer", "raw")
        chunks_info = manifest["chunks"]

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
        seen_indices: set[int] = set()

        executor = ThreadPoolExecutor(max_workers=max(1, len(chunks_info)))
        try:
            futures = [executor.submit(_fetch_chunk, meta) for meta in chunks_info]
            for fut in as_completed(futures):
                res = fut.result()
                if res is not None:
                    idx, data = res
                    if idx in seen_indices:
                        continue
                    seen_indices.add(idx)
                    available_blocks.append(data)
                    block_indices.append(idx)
                    if len(available_blocks) >= k:
                        break
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        if len(available_blocks) < k:
            raise RuntimeError(
                f"Failed to restore checkpoint [{checkpoint_id}]: only retrieved "
                f"{len(available_blocks)}/{k} chunks required for RS reconstruction."
            )

        codec = RSCodec(k=k, m=m)
        reconstructed_bytes = codec.decode(available_blocks, block_indices, original_length)

        serializer = get_serializer(serializer_to_use)
        payload, meta = serializer.deserialize(reconstructed_bytes)
        logger.info(
            f"Successfully restored checkpoint [{checkpoint_id}] from {len(available_blocks)} surviving chunks."
        )
        return payload, meta
