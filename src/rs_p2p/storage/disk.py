"""Disk Storage Engine for Passive Volunteer Nodes."""

from pathlib import Path
from typing import Optional, Tuple
import shutil
import logging

logger = logging.getLogger("DiskStorage")


class DiskStorage:
    """Stores Reed-Solomon shards on local disk/SSD storage."""

    def __init__(self, storage_dir: Path | str, node_id: str):
        self.storage_dir = Path(storage_dir)
        self.node_id = node_id
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def write_chunk(self, chunk_id: str, data: bytes) -> int:
        """Writes chunk bytes to disk. Returns bytes written."""
        safe_id = "".join(c for c in chunk_id if c.isalnum() or c in ("-", "_", "."))
        chunk_file = self.storage_dir / f"{safe_id}.chunk"
        chunk_file.write_bytes(data)
        logger.debug(f"[{self.node_id}] Stored chunk {safe_id} ({len(data)} bytes)")
        return len(data)

    def read_chunk(self, chunk_id: str) -> Optional[bytes]:
        """Reads chunk bytes from disk if exists."""
        safe_id = "".join(c for c in chunk_id if c.isalnum() or c in ("-", "_", "."))
        chunk_file = self.storage_dir / f"{safe_id}.chunk"
        if not chunk_file.is_file():
            return None
        return chunk_file.read_bytes()

    def has_chunk(self, chunk_id: str) -> Tuple[bool, int]:
        """Checks if chunk exists on disk and returns (exists, size_bytes)."""
        safe_id = "".join(c for c in chunk_id if c.isalnum() or c in ("-", "_", "."))
        chunk_file = self.storage_dir / f"{safe_id}.chunk"
        if chunk_file.is_file():
            return True, chunk_file.stat().st_size
        return False, 0

    def delete_chunk(self, chunk_id: str) -> bool:
        """Deletes chunk from disk."""
        safe_id = "".join(c for c in chunk_id if c.isalnum() or c in ("-", "_", "."))
        chunk_file = self.storage_dir / f"{safe_id}.chunk"
        if chunk_file.is_file():
            chunk_file.unlink()
            return True
        return False

    def list_chunks(self) -> list[str]:
        """Lists all chunk IDs present in storage."""
        return [f.stem for f in self.storage_dir.glob("*.chunk")]

    def get_stats(self) -> dict:
        """Returns storage metrics."""
        chunks = list(self.storage_dir.glob("*.chunk"))
        try:
            total_b, used_b, free_b = shutil.disk_usage(self.storage_dir)
        except Exception:
            free_b = 0
        return {
            "node_id": self.node_id,
            "chunk_count": len(chunks),
            "free_bytes": free_b,
        }
