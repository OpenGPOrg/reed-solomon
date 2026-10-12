"""Remote storage node client implementation over HTTP."""

from typing import List, Optional
import urllib.request
import urllib.error
import json
from .base import StorageNode


class RemoteStorageNode(StorageNode):
    """Client for a remote volunteer storage node reachable over HTTP."""

    def __init__(self, node_id: str, endpoint: str):
        self._node_id = node_id
        self.endpoint = endpoint.rstrip("/")

    @property
    def node_id(self) -> str:
        return self._node_id

    def write_chunk(self, chunk_id: str, data: bytes) -> bool:
        url = f"{self.endpoint}/chunks/{chunk_id}"
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/octet-stream"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status in (200, 201)
        except Exception:
            return False

    def read_chunk(self, chunk_id: str) -> Optional[bytes]:
        url = f"{self.endpoint}/chunks/{chunk_id}"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return resp.read()
                return None
        except Exception:
            return None

    def has_chunk(self, chunk_id: str) -> bool:
        url = f"{self.endpoint}/chunks/{chunk_id}"
        req = urllib.request.Request(url, method="HEAD")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception:
            return False

    def delete_chunk(self, chunk_id: str) -> bool:
        url = f"{self.endpoint}/chunks/{chunk_id}"
        req = urllib.request.Request(url, method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status in (200, 204)
        except Exception:
            return False

    def list_chunks(self) -> List[str]:
        # Health endpoint contains chunk count
        return []

    def is_healthy(self) -> bool:
        url = f"{self.endpoint}/health"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception:
            return False

    def __repr__(self) -> str:
        return f"<RemoteStorageNode id={self._node_id} endpoint={self.endpoint}>"
