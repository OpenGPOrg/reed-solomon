"""Abstract base interface for storage nodes."""

from abc import ABC, abstractmethod
from typing import List, Optional


class StorageNode(ABC):
    """Abstract interface representing an individual storage node/disk.

    Implementations can be a local directory (LocalStorageNode) or a remote
    network node running over HTTP/gRPC/sockets (RemoteStorageNode).
    """

    @property
    @abstractmethod
    def node_id(self) -> str:
        """Returns the unique identifier for this node."""
        pass

    @abstractmethod
    def write_chunk(self, chunk_id: str, data: bytes) -> bool:
        """Stores chunk bytes on the node. Returns True if successful."""
        pass

    @abstractmethod
    def read_chunk(self, chunk_id: str) -> Optional[bytes]:
        """Retrieves chunk bytes. Returns None if chunk is missing or node is down."""
        pass

    @abstractmethod
    def has_chunk(self, chunk_id: str) -> bool:
        """Checks if a chunk is currently stored on this node."""
        pass

    @abstractmethod
    def delete_chunk(self, chunk_id: str) -> bool:
        """Deletes a chunk from the node. Returns True if deleted."""
        pass

    @abstractmethod
    def list_chunks(self) -> List[str]:
        """Lists all chunk identifiers stored on this node."""
        pass

    @abstractmethod
    def is_healthy(self) -> bool:
        """Pings the node to check if it is active and reachable."""
        pass
