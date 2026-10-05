"""Metadata and manifest management for stored files."""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import json
import uuid


@dataclass
class ChunkMetadata:
    chunk_index: int
    chunk_type: str  # "data" or "parity"
    chunk_id: str
    node_id: str
    checksum_sha256: str
    size_bytes: int


@dataclass
class FileManifest:
    file_id: str
    filename: str
    file_size_bytes: int
    file_sha256: str
    k: int
    m: int
    chunk_size_bytes: int
    created_at: str
    chunks: List[ChunkMetadata]

    @property
    def total_chunks(self) -> int:
        return self.k + self.m

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileManifest":
        chunks = [ChunkMetadata(**c) for c in data["chunks"]]
        data_copy = dict(data)
        data_copy["chunks"] = chunks
        return cls(**data_copy)

    def save(self, destination_path: Path) -> Path:
        dest = Path(destination_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        return dest

    @classmethod
    def load(cls, source_path: Path) -> "FileManifest":
        with open(source_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)

    @classmethod
    def create_new(
        cls,
        filename: str,
        file_size: int,
        file_sha256: str,
        k: int,
        m: int,
        chunk_size: int,
        chunks: List[ChunkMetadata],
        file_id: str = None,
    ) -> "FileManifest":
        return cls(
            file_id=file_id or str(uuid.uuid4()),
            filename=filename,
            file_size_bytes=file_size,
            file_sha256=file_sha256,
            k=k,
            m=m,
            chunk_size_bytes=chunk_size,
            created_at=datetime.now(timezone.utc).isoformat(),
            chunks=chunks,
        )
