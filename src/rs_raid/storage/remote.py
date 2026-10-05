"""Remote storage node client skeleton (Future expansion for distributed network)."""

from typing import List, Optional
from .base import StorageNode


class RemoteStorageNode(StorageNode):
    """Client for a remote storage node reachable over the network (HTTP/gRPC/Socket).

    When you expand to multi-PC / network nodes, implement the HTTP/gRPC requests
    inside these methods. The RAID controller will interact with it identically
    to a LocalStorageNode.
    """

    def __init__(self, node_id: str, endpoint: str, auth_token: Optional[str] = None):
        self._node_id = node_id
        self.endpoint = endpoint.rstrip("/")
        self.auth_token = auth_token

    @property
    def node_id(self) -> str:
        return self._node_id

    def write_chunk(self, chunk_id: str, data: bytes) -> bool:
        # Example: requests.post(f"{self.endpoint}/chunks/{chunk_id}", data=data)
        raise NotImplementedError("Remote node communication to be implemented in network phase")

    def read_chunk(self, chunk_id: str) -> Optional[bytes]:
        # Example: resp = requests.get(f"{self.endpoint}/chunks/{chunk_id}")
        # return resp.content if resp.status_code == 200 else None
        raise NotImplementedError("Remote node communication to be implemented in network phase")

    def has_chunk(self, chunk_id: str) -> bool:
        # Example: resp = requests.head(f"{self.endpoint}/chunks/{chunk_id}")
        # return resp.status_code == 200
        raise NotImplementedError("Remote node communication to be implemented in network phase")

    def delete_chunk(self, chunk_id: str) -> bool:
        # Example: resp = requests.delete(f"{self.endpoint}/chunks/{chunk_id}")
        # return resp.status_code == 200
        raise NotImplementedError("Remote node communication to be implemented in network phase")

    def list_chunks(self) -> List[str]:
        # Example: resp = requests.get(f"{self.endpoint}/chunks")
        # return resp.json()["chunks"]
        raise NotImplementedError("Remote node communication to be implemented in network phase")

    def is_healthy(self) -> bool:
        # Example: resp = requests.get(f"{self.endpoint}/health")
        # return resp.status_code == 200
        return False
