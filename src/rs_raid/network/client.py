"""Active Node P2P Client.

Performs:
1. Fast Store: Sends file to Coordinator in a single request (offloading RS encoding).
2. Direct P2P Retrieve: Downloads chunks in parallel directly from Volunteer Peers,
   bypassing the Coordinator for maximum speed and zero bottleneck.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import hashlib
import json
import logging
import urllib.parse
import urllib.request
import urllib.error

from ..config import DEFAULT_K, DEFAULT_M, RESTORED_DIR
from ..core.codec import RSCodec

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [P2PClient] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("P2PClient")


class P2PClient:
    """Client running on an Active Node to store and retrieve files in the P2P network."""

    def __init__(self, coordinator_url: str):
        self.coordinator_url = coordinator_url.rstrip("/")

    def store(
        self,
        file_path: Path | str,
        k: int = DEFAULT_K,
        m: int = DEFAULT_M,
    ) -> Dict[str, Any]:
        """Fast store: Offloads encoding to coordinator in a single fast stream.

        Args:
            file_path: Path to the local file to store.
            k: Number of data chunks.
            m: Number of parity chunks.

        Returns:
            The FileManifest dict containing chunk locations across volunteer peers.
        """
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"File not found: {path}")

        file_bytes = path.read_bytes()
        logger.info(
            f"Streaming '{path.name}' ({len(file_bytes)} bytes) to Coordinator at {self.coordinator_url}..."
        )

        query = urllib.parse.urlencode({"filename": path.name, "k": k, "m": m})
        url = f"{self.coordinator_url}/upload?{query}"

        req = urllib.request.Request(
            url,
            data=file_bytes,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(file_bytes)),
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Coordinator error: HTTP {resp.status}")
                manifest = json.loads(resp.read().decode("utf-8"))
                logger.info(
                    f"✓ File successfully stored! File ID: {manifest['file_id']} "
                    f"dispersed across {manifest['k'] + manifest['m']} volunteer peers."
                )
                return manifest
        except urllib.error.HTTPError as e:
            err_msg = e.read().decode("utf-8")
            raise RuntimeError(f"Coordinator rejected upload: {err_msg}")

    def fetch_manifest(self, file_id: str) -> Dict[str, Any]:
        """Fetches file manifest metadata from coordinator."""
        url = f"{self.coordinator_url}/manifest/{file_id}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                raise FileNotFoundError(f"Manifest not found for file_id: {file_id}")
            return json.loads(resp.read().decode("utf-8"))

    def retrieve(
        self,
        file_id: str,
        output_dir: Optional[Path | str] = None,
        manifest: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Direct P2P Retrieval: Fetches chunks directly from volunteer peers in parallel.

        Bypasses the Coordinator entirely for data streaming.
        As soon as the fastest k valid chunks arrive, decodes locally via Reed-Solomon.
        """
        dest_dir = Path(output_dir or RESTORED_DIR).resolve()
        dest_dir.mkdir(parents=True, exist_ok=True)

        if not manifest:
            logger.info(f"Querying coordinator for manifest of {file_id}...")
            manifest = self.fetch_manifest(file_id)

        filename = manifest["filename"]
        file_size = manifest["file_size_bytes"]
        file_sha256 = manifest["file_sha256"]
        k = manifest["k"]
        m = manifest["m"]
        chunks_info = manifest["chunks"]

        logger.info(
            f"Beginning DIRECT P2P RETRIEVAL for '{filename}' ({file_size} bytes). "
            f"Need any {k} chunks out of {k + m} volunteer peers."
        )

        collected_blocks: List[bytes] = []
        collected_indices: List[int] = []

        # Launch concurrent direct HTTP requests to all volunteer peers
        with ThreadPoolExecutor(max_workers=len(chunks_info)) as executor:
            future_to_chunk = {
                executor.submit(
                    self._fetch_chunk_direct,
                    peer_url=c["peer_url"],
                    chunk_id=c["chunk_id"],
                    expected_sha256=c["checksum_sha256"],
                ): c
                for c in chunks_info
                if c.get("peer_url")
            }

            for future in as_completed(future_to_chunk):
                chunk_meta = future_to_chunk[future]
                chunk_data, error = future.result()

                if error:
                    logger.warning(
                        f"Failed chunk #{chunk_meta['chunk_index']} from {chunk_meta['node_id']} ({error})"
                    )
                    continue

                logger.info(
                    f"✓ Received valid chunk #{chunk_meta['chunk_index']} ({len(chunk_data)} bytes) "
                    f"directly from {chunk_meta['node_id']} ({chunk_meta['peer_url']})"
                )
                collected_blocks.append(chunk_data)
                collected_indices.append(chunk_meta["chunk_index"])

                # Fast path: Once we have k chunks, we have enough to reconstruct!
                if len(collected_blocks) == k:
                    logger.info(f"⚡ Acquired {k} valid chunks from fastest peers. Proceeding to RS decode!")
                    break

        if len(collected_blocks) < k:
            raise RuntimeError(
                f"P2P retrieval failed: Only retrieved {len(collected_blocks)} valid chunks, "
                f"need at least {k} (k={k}, m={m})."
            )

        # Reconstruct file using Reed-Solomon
        logger.info(f"Decoding {k} chunks with Reed-Solomon engine...")
        codec = RSCodec(k=k, m=m)
        reconstructed_data = codec.decode(
            available_blocks=collected_blocks,
            block_indices=collected_indices,
            original_length=file_size,
        )

        # Integrity verification
        recovered_sha = hashlib.sha256(reconstructed_data).hexdigest()
        if recovered_sha != file_sha256:
            raise ValueError(
                f"Reconstructed data SHA-256 mismatch! Expected {file_sha256}, got {recovered_sha}"
            )

        output_file = dest_dir / filename
        output_file.write_bytes(reconstructed_data)
        logger.info(f"✓ Reconstructed file saved to: {output_file} (100% SHA-256 match)")
        return output_file

    def _fetch_chunk_direct(
        self, peer_url: str, chunk_id: str, expected_sha256: str
    ) -> Tuple[Optional[bytes], Optional[str]]:
        """Downloads a chunk directly from a volunteer peer and verifies SHA-256."""
        url = f"{peer_url}/chunks/{chunk_id}"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    return None, f"HTTP {resp.status}"
                data = resp.read()
                # Verify SHA-256 checksum to protect against tampering/corruption
                actual_sha = hashlib.sha256(data).hexdigest()
                if actual_sha != expected_sha256:
                    return None, "Checksum mismatch (corrupted chunk)"
                return data, None
        except Exception as e:
            return None, str(e)
