"""gRPC Service Implementations for Chunk Storage and Kademlia DHT."""

import hashlib
import logging
import grpc

from ..proto import storage_pb2, storage_pb2_grpc, dht_pb2, dht_pb2_grpc
from ..storage.disk import DiskStorage
from ..dht.kademlia import KademliaNode
from ..dht.node import Contact, hex_to_int, int_to_hex

logger = logging.getLogger("GRPCServices")


class StorageServicer(storage_pb2_grpc.StorageServiceServicer):
    """gRPC service exposing chunk operations backed by DiskStorage."""

    def __init__(self, storage: DiskStorage):
        self.storage = storage

    def StoreChunk(self, request: storage_pb2.StoreChunkRequest, context) -> storage_pb2.StoreChunkResponse:
        chunk_id = request.chunk_id
        data = request.data
        checksum = request.checksum_sha256

        # Verify integrity before writing to disk
        if checksum:
            computed = hashlib.sha256(data).hexdigest()
            if computed != checksum:
                logger.warning(f"Checksum mismatch for chunk {chunk_id}: {computed} != {checksum}")
                return storage_pb2.StoreChunkResponse(
                    success=False,
                    error_message=f"Checksum verification failed",
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

    def GetChunk(self, request: storage_pb2.GetChunkRequest, context) -> storage_pb2.GetChunkResponse:
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

    def HasChunk(self, request: storage_pb2.HasChunkRequest, context) -> storage_pb2.HasChunkResponse:
        exists, size_b = self.storage.has_chunk(request.chunk_id)
        return storage_pb2.HasChunkResponse(exists=exists, size_bytes=size_b)

    def DeleteChunk(self, request: storage_pb2.DeleteChunkRequest, context) -> storage_pb2.DeleteChunkResponse:
        success = self.storage.delete_chunk(request.chunk_id)
        return storage_pb2.DeleteChunkResponse(success=success)

    def GetHealth(self, request: storage_pb2.HealthRequest, context) -> storage_pb2.HealthResponse:
        stats = self.storage.get_stats()
        return storage_pb2.HealthResponse(
            node_id=stats["node_id"],
            status="healthy",
            chunk_count=stats["chunk_count"],
            free_bytes=stats["free_bytes"],
        )


class DHTServicer(dht_pb2_grpc.DHTServiceServicer):
    """gRPC service exposing Kademlia DHT RPCs."""

    def __init__(self, dht_node: KademliaNode):
        self.dht_node = dht_node

    def _record_sender(self, sender_pb: dht_pb2.NodeContact):
        # TODO(bugfix): ActiveNodeClient is a client-only SDK (defaults to port=0, role="active")
        # and does not run a listening DHTService gRPC server. Recording it in RoutingTable
        # causes lookup_node() / iterative_store() to return unroutable 127.0.0.1:0 contacts.
        # Fix: Ignore client-only callers (e.g., if sender_pb.port <= 0 or sender_pb.role != "passive": return)
        # unless Active Nodes also run a listening DHTService server.
        if not sender_pb or not sender_pb.node_id:
            return
        contact = Contact(
            node_id=hex_to_int(sender_pb.node_id),
            host=sender_pb.host,
            port=sender_pb.port,
            role=sender_pb.role,
        )
        self.dht_node.routing_table.add_contact(contact)

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

    def FindNode(self, request: dht_pb2.FindNodeRequest, context) -> dht_pb2.FindNodeResponse:
        self._record_sender(request.sender)
        target_id = hex_to_int(request.target_id)
        closest = self.dht_node.routing_table.find_closest(
            target_id, count=self.dht_node.k
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

    def FindValue(self, request: dht_pb2.FindValueRequest, context) -> dht_pb2.FindValueResponse:
        self._record_sender(request.sender)
        val = self.dht_node.local_get(request.namespace, request.key)
        if val is not None:
            return dht_pb2.FindValueResponse(found=True, value=val)

        # Value not found locally; return closer contacts
        target_id = hex_to_int(request.sender.node_id) if request.sender.node_id else self.dht_node.contact.node_id
        closest = self.dht_node.routing_table.find_closest(target_id, count=self.dht_node.k)
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

    def StoreValue(self, request: dht_pb2.StoreValueRequest, context) -> dht_pb2.StoreValueResponse:
        self._record_sender(request.sender)
        try:
            self.dht_node.local_store(
                namespace=request.namespace,
                key=request.key,
                value=request.value,
                ttl_seconds=request.ttl_seconds or 3600,
            )
            return dht_pb2.StoreValueResponse(success=True)
        except Exception as e:
            return dht_pb2.StoreValueResponse(success=False, error_message=str(e))
