"""gRPC Service Implementations for Chunk Storage and Kademlia DHT."""

from collections import deque
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
from ..dht.kademlia import KademliaNode
from ..dht.node import (
    Contact,
    hash_key,
    hex_to_int,
    is_routable_server_contact,
    xor_distance,
)
from ..proto import dht_pb2, dht_pb2_grpc, storage_pb2, storage_pb2_grpc
from ..storage.disk import DiskStorage

logger = logging.getLogger("GRPCServices")


class StorageServicer(storage_pb2_grpc.StorageServiceServicer):
    """gRPC service exposing chunk operations and passive-layer RAID coordination."""

    def __init__(
        self,
        storage: DiskStorage,
        dht_node: Optional[KademliaNode] = None,
    ):
        self.storage = storage
        self.dht_node = dht_node
        self._peer_channels: Dict[str, grpc.Channel] = {}
        self._channels_lock = threading.RLock()

    def _get_peer_stub(self, endpoint: str) -> storage_pb2_grpc.StorageServiceStub:
        with self._channels_lock:
            if endpoint not in self._peer_channels:
                self._peer_channels[endpoint] = grpc.insecure_channel(
                    endpoint, options=GRPC_CHANNEL_OPTIONS
                )
            return storage_pb2_grpc.StorageServiceStub(self._peer_channels[endpoint])

    def close(self) -> None:
        with self._channels_lock:
            for ch in self._peer_channels.values():
                try:
                    ch.close()
                except Exception:
                    pass
            self._peer_channels.clear()

    def StoreChunk(
        self, request: storage_pb2.StoreChunkRequest, context
    ) -> storage_pb2.StoreChunkResponse:
        chunk_id = request.chunk_id
        data = request.data
        checksum = request.checksum_sha256

        # Verify integrity before writing to disk
        if checksum:
            computed = hashlib.sha256(data).hexdigest()
            if computed != checksum:
                logger.warning(
                    f"Checksum mismatch for chunk {chunk_id}: {computed} != {checksum}"
                )
                return storage_pb2.StoreChunkResponse(
                    success=False,
                    error_message="Checksum verification failed",
                    bytes_written=0,
                )

        try:
            written = self.storage.write_chunk(chunk_id, data)
            return storage_pb2.StoreChunkResponse(
                success=True,
                bytes_written=written,
            )
        except Exception as e:
            logger.error(f"Failed to write chunk {chunk_id}: {e}")
            return storage_pb2.StoreChunkResponse(
                success=False,
                error_message=str(e),
                bytes_written=0,
            )

    def GetChunk(
        self, request: storage_pb2.GetChunkRequest, context
    ) -> storage_pb2.GetChunkResponse:
        chunk_id = request.chunk_id
        data = self.storage.read_chunk(chunk_id)
        if data is None:
            return storage_pb2.GetChunkResponse(found=False)

        checksum = hashlib.sha256(data).hexdigest()
        return storage_pb2.GetChunkResponse(
            found=True,
            data=data,
            checksum_sha256=checksum,
        )

    def HasChunk(
        self, request: storage_pb2.HasChunkRequest, context
    ) -> storage_pb2.HasChunkResponse:
        exists, size_b = self.storage.has_chunk(request.chunk_id)
        return storage_pb2.HasChunkResponse(exists=exists, size_bytes=size_b)

    def DeleteChunk(
        self, request: storage_pb2.DeleteChunkRequest, context
    ) -> storage_pb2.DeleteChunkResponse:
        success = self.storage.delete_chunk(request.chunk_id)
        return storage_pb2.DeleteChunkResponse(success=success)

    def GetHealth(
        self, request: storage_pb2.HealthRequest, context
    ) -> storage_pb2.HealthResponse:
        stats = self.storage.get_stats()
        return storage_pb2.HealthResponse(
            node_id=stats["node_id"],
            status="healthy",
            chunk_count=stats["chunk_count"],
            free_bytes=stats["free_bytes"],
        )

    def _select_remote_candidates(
        self, checkpoint_id: str, total_chunks: int
    ) -> List[Contact]:
        """Selects distinct routable remote passive peers ordered by XOR distance."""
        assert self.dht_node is not None
        target_id = hash_key(checkpoint_id)
        discovered = self.dht_node.lookup_node(
            target_id, count=max(self.dht_node.k, total_chunks * 2)
        )
        known = self.dht_node.routing_table.all_contacts()

        dedup_by_ep: Dict[str, Contact] = {}
        for c in discovered + known:
            if (
                c.node_id == self.dht_node.contact.node_id
                or self.dht_node._is_self_endpoint(c)
                or not is_routable_server_contact(c)
            ):
                continue
            dedup_by_ep[c.endpoint] = c

        candidates = list(dedup_by_ep.values())
        candidates.sort(key=lambda c: xor_distance(c.node_id, target_id))
        return candidates

    def _rollback_chunks(
        self, local_chunk_id: str, remote_uploads: List[Tuple[str, str]]
    ) -> None:
        try:
            self.storage.delete_chunk(local_chunk_id)
        except Exception:
            pass
        for chunk_id, endpoint in remote_uploads:
            try:
                stub = self._get_peer_stub(endpoint)
                stub.DeleteChunk(
                    storage_pb2.DeleteChunkRequest(chunk_id=chunk_id), timeout=3.0
                )
            except Exception:
                pass

    def StoreCheckpoint(
        self, request: storage_pb2.StoreCheckpointRequest, context
    ) -> storage_pb2.StoreCheckpointResponse:
        """Performs passive-layer RS encoding, chunk dispersal with failover, and DHT manifest storage."""
        if self.dht_node is None:
            return storage_pb2.StoreCheckpointResponse(
                success=False,
                error_message="DHT node not attached to StorageServicer",
            )

        k = request.k or DEFAULT_K
        m = request.m or DEFAULT_M
        try:
            codec = RSCodec(k=k, m=m)
        except ValueError as e:
            return storage_pb2.StoreCheckpointResponse(
                success=False, error_message=str(e)
            )

        try:
            metadata: Dict[str, Any] = (
                json.loads(request.metadata_json) if request.metadata_json else {}
            )
        except Exception as e:
            return storage_pb2.StoreCheckpointResponse(
                success=False, error_message=f"Invalid metadata_json: {e}"
            )

        chk_id = request.checkpoint_id or f"chk_{uuid.uuid4().hex[:12]}"
        serializer_name = request.serializer_name or "raw"

        chunks, original_length = codec.encode(request.serialized_payload)
        total_chunks = k + m
        needed_remote = total_chunks - 1

        remote_candidates = self._select_remote_candidates(chk_id, total_chunks)
        if len(remote_candidates) < needed_remote:
            return storage_pb2.StoreCheckpointResponse(
                success=False,
                checkpoint_id=chk_id,
                error_message=(
                    f"Insufficient passive peers available: need {total_chunks} "
                    f"(1 local + {needed_remote} remote), found {1 + len(remote_candidates)}"
                ),
            )

        # ADR-2: Store chunk #0 locally on this receiving passive node
        chunk0 = chunks[0]
        chunk0_id = f"{chk_id}_chunk_0"
        chunk0_checksum = hashlib.sha256(chunk0).hexdigest()
        try:
            self.storage.write_chunk(chunk0_id, chunk0)
        except Exception as e:
            return storage_pb2.StoreCheckpointResponse(
                success=False,
                checkpoint_id=chk_id,
                error_message=f"Failed writing local chunk {chunk0_id}: {e}",
            )

        local_record = {
            "chunk_id": chunk0_id,
            "chunk_index": 0,
            "chunk_type": "data",
            "checksum_sha256": chunk0_checksum,
            "size_bytes": len(chunk0),
            "endpoint": self.dht_node.contact.endpoint,
        }

        primary_targets = remote_candidates[:needed_remote]
        backup_queue = deque(remote_candidates[needed_remote:])
        queue_lock = threading.Lock()

        def _upload_remote_chunk(
            idx: int, chunk_bytes: bytes, initial_target: Contact
        ) -> Tuple[bool, Optional[Dict[str, Any]], str]:
            chunk_type = "data" if idx < k else "parity"
            chunk_id = f"{chk_id}_chunk_{idx}"
            checksum = hashlib.sha256(chunk_bytes).hexdigest()
            candidate: Optional[Contact] = initial_target
            last_error = "no candidate available"

            while candidate is not None:
                stub = self._get_peer_stub(candidate.endpoint)
                req = storage_pb2.StoreChunkRequest(
                    chunk_id=chunk_id,
                    chunk_index=idx,
                    data=chunk_bytes,
                    checksum_sha256=checksum,
                )
                try:
                    resp = stub.StoreChunk(req, timeout=10.0)
                    if resp.success:
                        return (
                            True,
                            {
                                "chunk_id": chunk_id,
                                "chunk_index": idx,
                                "chunk_type": chunk_type,
                                "checksum_sha256": checksum,
                                "size_bytes": len(chunk_bytes),
                                "endpoint": candidate.endpoint,
                            },
                            "",
                        )
                    last_error = resp.error_message or "StoreChunk returned success=False"
                    logger.warning(
                        f"Peer {candidate.endpoint} rejected chunk {chunk_id}: {last_error}"
                    )
                except Exception as e:
                    last_error = str(e)
                    logger.warning(
                        f"Failed uploading chunk {chunk_id} to {candidate.endpoint}: {e}"
                    )
                    self.dht_node._evict_dead_target(candidate)

                with queue_lock:
                    candidate = backup_queue.popleft() if backup_queue else None

            return False, None, last_error

        uploaded_manifest_chunks: List[Dict[str, Any]] = [local_record]
        remote_uploads: List[Tuple[str, str]] = []
        errors: List[str] = []

        if needed_remote > 0:
            with ThreadPoolExecutor(max_workers=needed_remote) as executor:
                futures = [
                    executor.submit(_upload_remote_chunk, idx, chunks[idx], target)
                    for idx, target in zip(range(1, total_chunks), primary_targets)
                ]
                for fut in as_completed(futures):
                    ok, record, err = fut.result()
                    if ok and record is not None:
                        uploaded_manifest_chunks.append(record)
                        remote_uploads.append((record["chunk_id"], record["endpoint"]))
                    else:
                        errors.append(err)

        if errors:
            self._rollback_chunks(chunk0_id, remote_uploads)
            return storage_pb2.StoreCheckpointResponse(
                success=False,
                checkpoint_id=chk_id,
                error_message=f"Chunk dispersal failed: {errors[0]}",
            )

        uploaded_manifest_chunks.sort(key=lambda c: c["chunk_index"])

        manifest = {
            "checkpoint_id": chk_id,
            "k": k,
            "m": m,
            "original_length": original_length,
            "serializer": serializer_name,
            "metadata": metadata,
            "coordinator_endpoint": self.dht_node.contact.endpoint,
            "chunks": uploaded_manifest_chunks,
        }

        if not self.dht_node.store_manifest(chk_id, manifest):
            self._rollback_chunks(chunk0_id, remote_uploads)
            return storage_pb2.StoreCheckpointResponse(
                success=False,
                checkpoint_id=chk_id,
                error_message="Failed to publish checkpoint manifest to DHT",
            )

        logger.info(
            f"[{self.dht_node.contact.endpoint}] Stored checkpoint [{chk_id}] "
            f"({original_length} bytes) across {total_chunks} passive peers (chunk #0 local)."
        )
        return storage_pb2.StoreCheckpointResponse(
            success=True,
            checkpoint_id=chk_id,
            manifest_json=json.dumps(manifest),
        )


class DHTServicer(dht_pb2_grpc.DHTServiceServicer):
    """gRPC service exposing Kademlia DHT RPCs."""

    def __init__(self, dht_node: KademliaNode):
        self.dht_node = dht_node

    def _record_sender(self, sender_pb: dht_pb2.NodeContact) -> Optional[int]:
        """Records sender only if it is a routable passive DHT server."""
        if not sender_pb or not sender_pb.node_id:
            return None
        try:
            sender_id = hex_to_int(sender_pb.node_id)
        except ValueError:
            return None

        contact = Contact(
            node_id=sender_id,
            host=sender_pb.host,
            port=sender_pb.port,
            role=sender_pb.role,
        )
        if not is_routable_server_contact(contact):
            return sender_id

        if self.dht_node.routing_table.add_contact(contact):
            self.dht_node.save_routing_table()
        return sender_id

    def Ping(self, request: dht_pb2.PingRequest, context) -> dht_pb2.PingResponse:
        self._record_sender(request.sender)
        return dht_pb2.PingResponse(
            responder=dht_pb2.NodeContact(
                node_id=self.dht_node.contact.hex_id,
                host=self.dht_node.contact.host,
                port=self.dht_node.contact.port,
                role=self.dht_node.contact.role,
            ),
            ok=True,
        )

    def FindNode(
        self, request: dht_pb2.FindNodeRequest, context
    ) -> dht_pb2.FindNodeResponse:
        sender_id = self._record_sender(request.sender)
        target_id = hex_to_int(request.target_id)
        closest = self.dht_node.routing_table.find_closest(
            target_id, count=self.dht_node.k, exclude_id=sender_id
        )
        contacts_pb = [
            dht_pb2.NodeContact(
                node_id=c.hex_id,
                host=c.host,
                port=c.port,
                role=c.role,
            )
            for c in closest
        ]
        return dht_pb2.FindNodeResponse(contacts=contacts_pb)

    def FindValue(
        self, request: dht_pb2.FindValueRequest, context
    ) -> dht_pb2.FindValueResponse:
        sender_id = self._record_sender(request.sender)
        val = self.dht_node.local_get(request.namespace, request.key)
        if val is not None:
            return dht_pb2.FindValueResponse(found=True, value=val)

        # Value not found locally; return contacts closest to hash(key)
        target_id = hash_key(request.key)
        closest = self.dht_node.routing_table.find_closest(
            target_id, count=self.dht_node.k, exclude_id=sender_id
        )
        contacts_pb = [
            dht_pb2.NodeContact(
                node_id=c.hex_id,
                host=c.host,
                port=c.port,
                role=c.role,
            )
            for c in closest
        ]
        return dht_pb2.FindValueResponse(found=False, closer_contacts=contacts_pb)

    def StoreValue(
        self, request: dht_pb2.StoreValueRequest, context
    ) -> dht_pb2.StoreValueResponse:
        self._record_sender(request.sender)
        try:
            ttl = request.ttl_seconds if request.ttl_seconds > 0 else 3600
            self.dht_node.local_store(
                namespace=request.namespace,
                key=request.key,
                value=request.value,
                ttl_seconds=ttl,
            )
            return dht_pb2.StoreValueResponse(success=True)
        except Exception as e:
            return dht_pb2.StoreValueResponse(success=False, error_message=str(e))
