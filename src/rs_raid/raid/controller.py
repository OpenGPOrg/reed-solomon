"""High-level RAID Controller providing store, retrieve, inspect, and repair helpers."""

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import hashlib
import uuid

from ..config import (
    DEFAULT_K,
    DEFAULT_M,
    INPUT_DIR,
    METADATA_DIR,
    RESTORED_DIR,
    NODES_DIR,
)
from ..core.codec import RSCodec
from ..metadata.manifest import FileManifest, ChunkMetadata
from .cluster import NodeCluster


class RaidController:
    """Orchestrates Reed-Solomon RAID storage, retrieval, health inspection, and repair."""

    def __init__(self, cluster: Optional[NodeCluster] = None):
        self.cluster = cluster or NodeCluster.create_local_cluster(DEFAULT_K + DEFAULT_M)
        METADATA_DIR.mkdir(parents=True, exist_ok=True)
        RESTORED_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _compute_sha256(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def store_file(
        self,
        input_path: Path,
        k: Optional[int] = None,
        m: Optional[int] = None,
    ) -> FileManifest:
        """Encodes an input file and distributes chunks across storage nodes.

        Args:
            input_path: Path to the source file (e.g., in input/ directory).
            k: Number of data chunks (defaults to DEFAULT_K, easily changed).
            m: Number of parity chunks (defaults to DEFAULT_M, easily changed).

        Returns:
            The generated FileManifest containing chunk metadata and node placement.
        """
        source_path = Path(input_path)
        if not source_path.is_file():
            raise FileNotFoundError(f"Input file not found: {source_path}")

        file_bytes = source_path.read_bytes()
        file_sha256 = self._compute_sha256(file_bytes)
        file_size = len(file_bytes)

        actual_k = k if k is not None else DEFAULT_K
        actual_m = m if m is not None else DEFAULT_M
        total_chunks = actual_k + actual_m

        # Ensure we have enough storage nodes in the cluster
        self.cluster.ensure_capacity(total_chunks, base_dir=NODES_DIR)
        selected_nodes = self.cluster.select_nodes(total_chunks)

        # Encode via Reed-Solomon
        codec = RSCodec(k=actual_k, m=actual_m)
        chunks, _ = codec.encode(file_bytes)
        chunk_size = len(chunks[0]) if chunks else 0

        file_id = str(uuid.uuid4())
        chunk_metas: List[ChunkMetadata] = []

        # Store chunks across nodes
        for index, (chunk_data, node) in enumerate(zip(chunks, selected_nodes)):
            chunk_type = "data" if index < actual_k else "parity"
            chunk_id = f"{file_id}_part_{index}"
            chunk_sha256 = self._compute_sha256(chunk_data)

            success = node.write_chunk(chunk_id=chunk_id, data=chunk_data)
            if not success:
                raise IOError(
                    f"Failed to write chunk {chunk_id} to node {node.node_id}."
                )

            chunk_metas.append(
                ChunkMetadata(
                    chunk_index=index,
                    chunk_type=chunk_type,
                    chunk_id=chunk_id,
                    node_id=node.node_id,
                    checksum_sha256=chunk_sha256,
                    size_bytes=len(chunk_data),
                )
            )

        manifest = FileManifest.create_new(
            file_id=file_id,
            filename=source_path.name,
            file_size=file_size,
            file_sha256=file_sha256,
            k=actual_k,
            m=actual_m,
            chunk_size=chunk_size,
            chunks=chunk_metas,
        )

        manifest_path = METADATA_DIR / f"{file_id}.json"
        manifest.save(manifest_path)
        return manifest

    def find_manifest(self, identifier: str) -> FileManifest:
        """Finds a manifest by file_id, filename, or JSON file path."""
        # 1. Direct path
        path = Path(identifier)
        if path.is_file():
            return FileManifest.load(path)

        # 2. Check by file_id in METADATA_DIR
        by_id = METADATA_DIR / f"{identifier}.json"
        if by_id.is_file():
            return FileManifest.load(by_id)

        # 3. Search metadata directory by filename or partial id
        for f in METADATA_DIR.glob("*.json"):
            try:
                manifest = FileManifest.load(f)
                if manifest.file_id == identifier or manifest.filename == identifier:
                    return manifest
            except Exception:
                continue

        raise FileNotFoundError(f"Manifest not found for identifier: {identifier}")

    def inspect_file(self, identifier: str) -> Dict[str, Any]:
        """Inspects chunk health and calculates cluster fault-tolerance state."""
        manifest = self.find_manifest(identifier)
        healthy_chunks = []
        corrupted_chunks = []
        missing_chunks = []

        for chunk_meta in manifest.chunks:
            node = self.cluster.get_node(chunk_meta.node_id)
            if not node or not node.is_healthy():
                missing_chunks.append((chunk_meta, "node_offline"))
                continue

            chunk_data = node.read_chunk(chunk_meta.chunk_id)
            if chunk_data is None:
                missing_chunks.append((chunk_meta, "chunk_missing"))
                continue

            # Verify checksum against bit rot / tampering
            checksum = self._compute_sha256(chunk_data)
            if checksum != chunk_meta.checksum_sha256:
                corrupted_chunks.append((chunk_meta, "checksum_mismatch"))
            else:
                healthy_chunks.append(chunk_meta)

        total_healthy = len(healthy_chunks)
        can_restore = total_healthy >= manifest.k
        margin = total_healthy - manifest.k

        if total_healthy == manifest.total_chunks:
            status = "HEALTHY"
        elif can_restore:
            status = "DEGRADED"  # Still recoverable thanks to Reed-Solomon!
        else:
            status = "UNRECOVERABLE"

        return {
            "status": status,
            "manifest": manifest,
            "k": manifest.k,
            "m": manifest.m,
            "total_chunks": manifest.total_chunks,
            "healthy_count": total_healthy,
            "missing_count": len(missing_chunks),
            "corrupted_count": len(corrupted_chunks),
            "allowed_further_failures": max(0, margin),
            "can_restore": can_restore,
            "missing_chunks": missing_chunks,
            "corrupted_chunks": corrupted_chunks,
        }

    def retrieve_file(
        self,
        identifier: str,
        output_dir: Optional[Path] = None,
    ) -> Path:
        """Retrieves and reconstructs an encoded file from storage nodes.

        Tolerates up to m missing or corrupted chunks/nodes.

        Args:
            identifier: file_id, filename, or path to manifest JSON.
            output_dir: Directory where the restored file should be written.
                        Defaults to output/restored/.

        Returns:
            The Path to the reconstructed file.
        """
        manifest = self.find_manifest(identifier)
        destination_dir = Path(output_dir) if output_dir else RESTORED_DIR
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination_file = destination_dir / manifest.filename

        collected_blocks: List[bytes] = []
        collected_indices: List[int] = []

        for chunk_meta in manifest.chunks:
            node = self.cluster.get_node(chunk_meta.node_id)
            if not node or not node.is_healthy():
                continue

            chunk_data = node.read_chunk(chunk_meta.chunk_id)
            if chunk_data is None:
                continue

            # Verify checksum integrity
            if self._compute_sha256(chunk_data) == chunk_meta.checksum_sha256:
                collected_blocks.append(chunk_data)
                collected_indices.append(chunk_meta.chunk_index)

            # We only need k healthy blocks to reconstruct!
            if len(collected_blocks) == manifest.k:
                break

        if len(collected_blocks) < manifest.k:
            raise RuntimeError(
                f"Cannot reconstruct file '{manifest.filename}'. "
                f"Retrieved {len(collected_blocks)} valid chunks, but {manifest.k} are required. "
                f"(Total chunks: {manifest.total_chunks}, Fault tolerance limit: {manifest.m})"
            )

        codec = RSCodec(k=manifest.k, m=manifest.m)
        reconstructed_data = codec.decode(
            available_blocks=collected_blocks,
            block_indices=collected_indices,
            original_length=manifest.file_size_bytes,
        )

        # Verify overall reconstructed file integrity
        recovered_sha256 = self._compute_sha256(reconstructed_data)
        if recovered_sha256 != manifest.file_sha256:
            raise ValueError(
                f"Integrity check failed: Reconstructed SHA-256 ({recovered_sha256}) "
                f"does not match original ({manifest.file_sha256})"
            )

        destination_file.write_bytes(reconstructed_data)
        return destination_file

    def repair_file(self, identifier: str) -> Dict[str, Any]:
        """Detects missing or corrupt chunks and restores them to healthy nodes."""
        manifest = self.find_manifest(identifier)
        inspection = self.inspect_file(identifier)

        if not inspection["can_restore"]:
            raise RuntimeError(
                f"Cannot repair '{manifest.filename}': Not enough healthy chunks ({inspection['healthy_count']} < {manifest.k})"
            )

        if inspection["status"] == "HEALTHY":
            return {"status": "ALREADY_HEALTHY", "repaired_chunks": 0}

        # First, reconstruct the full file bytes
        collected_blocks: List[bytes] = []
        collected_indices: List[int] = []

        for chunk_meta in manifest.chunks:
            node = self.cluster.get_node(chunk_meta.node_id)
            if node and node.is_healthy():
                data = node.read_chunk(chunk_meta.chunk_id)
                if data and self._compute_sha256(data) == chunk_meta.checksum_sha256:
                    collected_blocks.append(data)
                    collected_indices.append(chunk_meta.chunk_index)
                    if len(collected_blocks) == manifest.k:
                        break

        codec = RSCodec(k=manifest.k, m=manifest.m)
        full_data = codec.decode(
            available_blocks=collected_blocks,
            block_indices=collected_indices,
            original_length=manifest.file_size_bytes,
        )

        # Re-encode to re-obtain all original n chunks
        all_regenerated_chunks, _ = codec.encode(full_data)

        repaired_count = 0
        for chunk_meta in manifest.chunks:
            node = self.cluster.get_node(chunk_meta.node_id)
            is_bad = False
            if not node or not node.is_healthy():
                is_bad = True
            else:
                existing_data = node.read_chunk(chunk_meta.chunk_id)
                if not existing_data or self._compute_sha256(existing_data) != chunk_meta.checksum_sha256:
                    is_bad = True

            if is_bad:
                target_node = node if (node and node.is_healthy()) else self.cluster.get_node(chunk_meta.node_id)
                if target_node:
                    repaired_data = all_regenerated_chunks[chunk_meta.chunk_index]
                    target_node.write_chunk(chunk_meta.chunk_id, repaired_data)
                    repaired_count += 1

        return {
            "status": "REPAIRED",
            "repaired_chunks": repaired_count,
            "filename": manifest.filename,
        }
