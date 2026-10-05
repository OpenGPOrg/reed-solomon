"""Local filesystem implementation of a StorageNode."""

from pathlib import Path
from typing import List, Optional
import os
from .base import StorageNode


class LocalStorageNode(StorageNode):
    """Simulates an individual storage node as a dedicated directory on disk.

    Useful for local testing, development, and multi-folder disk array simulation.
    """

    def __init__(self, node_id: str, root_dir: Path):
        self._node_id = node_id
        self._root_dir = Path(root_dir)
        self._is_online = True
        self._root_dir.mkdir(parents=True, exist_ok=True)

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def root_dir(self) -> Path:
        return self._root_dir

    def set_online_status(self, online: bool) -> None:
        """Allows programmatic simulation of node downtime/network failure."""
        self._is_online = online

    def is_healthy(self) -> bool:
        if not self._is_online:
            return False
        return self._root_dir.exists() and os.access(self._root_dir, os.W_OK | os.R_OK)

    def _chunk_path(self, chunk_id: str) -> Path:
        return self._root_dir / f"{chunk_id}.chunk"

    def write_chunk(self, chunk_id: str, data: bytes) -> bool:
        if not self.is_healthy():
            return False
        try:
            chunk_file = self._chunk_path(chunk_id)
            chunk_file.write_bytes(data)
            return True
        except (OSError, IOError):
            return False

    def read_chunk(self, chunk_id: str) -> Optional[bytes]:
        if not self.is_healthy():
            return None
        chunk_file = self._chunk_path(chunk_id)
        if not chunk_file.is_file():
            return None
        try:
            return chunk_file.read_bytes()
        except (OSError, IOError):
            return None

    def has_chunk(self, chunk_id: str) -> bool:
        if not self.is_healthy():
            return False
        return self._chunk_path(chunk_id).is_file()

    def delete_chunk(self, chunk_id: str) -> bool:
        if not self.is_healthy():
            return False
        chunk_file = self._chunk_path(chunk_id)
        try:
            if chunk_file.is_file():
                chunk_file.unlink()
                return True
            return False
        except (OSError, IOError):
            return False

    def list_chunks(self) -> List[str]:
        if not self.is_healthy():
            return []
        try:
            return [
                f.stem for f in self._root_dir.iterdir()
                if f.is_file() and f.suffix == ".chunk"
            ]
        except (OSError, IOError):
            return []

    def __repr__(self) -> str:
        status = "ONLINE" if self._is_online else "OFFLINE"
        return f"<LocalStorageNode id={self._node_id} path={self._root_dir} status={status}>"
