"""Coordinating Node for P2P Reed-Solomon Storage.

Acts as the Ingress & Erasure Hub and Peer Tracker:
1. Tracks active volunteer peer computers.
2. Accepts raw file uploads from Active Nodes (single fast transfer).
3. Encodes via Reed-Solomon (k data + m parity).
4. Dispatches chunks to volunteer peers concurrently.
5. Stores & serves file manifests for direct P2P retrieval.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
import argparse
import hashlib
import json
import logging
import time
import urllib.request
import urllib.error
import uuid

from ..config import DEFAULT_K, DEFAULT_M, METADATA_DIR
from ..core.codec import RSCodec
from ..metadata.manifest import FileManifest, ChunkMetadata

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [Coordinator] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Coordinator")


class PeerInfo:
    def __init__(self, node_id: str, host: str, port: int, storage_dir: str = ""):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.storage_dir = storage_dir
        self.last_heartbeat = time.time()
        self.chunk_count = 0

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def is_alive(self, timeout_seconds: float = 15.0) -> bool:
        return (time.time() - self.last_heartbeat) < timeout_seconds

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "base_url": self.base_url,
            "is_alive": self.is_alive(),
            "last_heartbeat": self.last_heartbeat,
            "chunk_count": self.chunk_count,
        }


class CoordinatorState:
    def __init__(self, metadata_dir: Path):
        self.peers: Dict[str, PeerInfo] = {}
        self.metadata_dir = metadata_dir
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    def register_peer(self, node_id: str, host: str, port: int, storage_dir: str = "") -> PeerInfo:
        peer = PeerInfo(node_id, host, port, storage_dir)
        self.peers[node_id] = peer
        logger.info(f"Registered Peer [{node_id}] at {peer.base_url}")
        return peer

    def heartbeat(self, node_id: str, chunk_count: int = 0):
        if node_id in self.peers:
            self.peers[node_id].last_heartbeat = time.time()
            self.peers[node_id].chunk_count = chunk_count

    def get_healthy_peers(self) -> List[PeerInfo]:
        return [p for p in self.peers.values() if p.is_alive()]


class CoordinatorHandler(BaseHTTPRequestHandler):
    state: CoordinatorState

    def log_message(self, format, *args):
        pass

    def _send_json(self, status_code: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # Health & status
        if path == "/health" or path == "/":
            healthy_count = len(self.state.get_healthy_peers())
            self._send_json(
                200,
                {
                    "status": "online",
                    "registered_peers": len(self.state.peers),
                    "healthy_peers": healthy_count,
                },
            )
            return

        # List active peers
        if path == "/peers":
            peers_list = [p.to_dict() for p in self.state.peers.values()]
            self._send_json(200, {"peers": peers_list})
            return

        # Manifest query for direct P2P retrieval
        if path.startswith("/manifest/"):
            file_id = path[len("/manifest/") :].strip()
            manifest_file = self.state.metadata_dir / f"{file_id}.json"
            if not manifest_file.is_file():
                self._send_json(404, {"error": "Manifest not found", "file_id": file_id})
                return
            try:
                manifest_data = json.loads(manifest_file.read_text(encoding="utf-8"))
                self._send_json(200, manifest_data)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return

        self._send_json(404, {"error": "Endpoint not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # Peer Registration
        if path == "/peers/register":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            peer = self.state.register_peer(
                node_id=body["node_id"],
                host=body["host"],
                port=int(body["port"]),
                storage_dir=body.get("storage_dir", ""),
            )
            self._send_json(201, {"status": "registered", "peer": peer.to_dict()})
            return

        # Peer Heartbeat
        if path == "/peers/heartbeat":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            self.state.heartbeat(
                node_id=body["node_id"],
                chunk_count=body.get("chunk_count", 0),
            )
            self._send_json(200, {"status": "heartbeat_ack"})
            return

        # Ingress Upload (Active Node streams file here to offload RS encoding)
        if path == "/upload":
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length <= 0:
                self._send_json(400, {"error": "Empty upload body"})
                return

            filename = query.get("filename", ["unnamed.bin"])[0]
            k_val = int(query.get("k", [str(DEFAULT_K)])[0])
            m_val = int(query.get("m", [str(DEFAULT_M)])[0])
            total_chunks = k_val + m_val

            # Read file bytes from Active Node
            file_data = self.rfile.read(content_length)
            file_sha256 = hashlib.sha256(file_data).hexdigest()
            file_id = str(uuid.uuid4())

            # Check healthy peers
            healthy_peers = self.state.get_healthy_peers()
            if len(healthy_peers) < total_chunks:
                self._send_json(
                    503,
                    {
                        "error": "Insufficient healthy volunteer peers",
                        "needed": total_chunks,
                        "available": len(healthy_peers),
                    },
                )
                return

            # Select n peers
            selected_peers = healthy_peers[:total_chunks]

            logger.info(
                f"Ingress upload: '{filename}' ({len(file_data)} bytes). "
                f"Encoding with RS(k={k_val}, m={m_val}) across {total_chunks} peers..."
            )

            # Perform Reed-Solomon Erasure Coding on Coordinator
            codec = RSCodec(k=k_val, m=m_val)
            chunks, _ = codec.encode(file_data)
            chunk_size = len(chunks[0]) if chunks else 0

            # Dispatch chunks concurrently to volunteer peers
            chunk_metas: List[ChunkMetadata] = []
            dispatch_tasks = []

            with ThreadPoolExecutor(max_workers=min(16, total_chunks)) as executor:
                for idx, (chunk_bytes, peer) in enumerate(zip(chunks, selected_peers)):
                    chunk_id = f"{file_id}_part_{idx}"
                    chunk_sha = hashlib.sha256(chunk_bytes).hexdigest()
                    chunk_type = "data" if idx < k_val else "parity"

                    chunk_meta = ChunkMetadata(
                        chunk_index=idx,
                        chunk_type=chunk_type,
                        chunk_id=chunk_id,
                        node_id=peer.node_id,
                        checksum_sha256=chunk_sha,
                        size_bytes=len(chunk_bytes),
                    )
                    # We inject peer endpoint so clients can retrieve directly!
                    chunk_meta.peer_url = peer.base_url  # type: ignore[attr-defined]
                    chunk_metas.append(chunk_meta)

                    task = executor.submit(
                        self._dispatch_chunk,
                        peer_url=peer.base_url,
                        chunk_id=chunk_id,
                        chunk_data=chunk_bytes,
                    )
                    dispatch_tasks.append((idx, peer.node_id, task))

                # Wait for dispatches to succeed
                for idx, node_id, task in dispatch_tasks:
                    success, err = task.result()
                    if not success:
                        self._send_json(
                            500,
                            {
                                "error": f"Failed to dispatch chunk #{idx} to {node_id}: {err}",
                                "file_id": file_id,
                            },
                        )
                        return

            # Construct Manifest with peer URLs included for direct retrieval
            manifest_dict = {
                "file_id": file_id,
                "filename": filename,
                "file_size_bytes": len(file_data),
                "file_sha256": file_sha256,
                "k": k_val,
                "m": m_val,
                "chunk_size_bytes": chunk_size,
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "chunks": [
                    {
                        "chunk_index": cm.chunk_index,
                        "chunk_type": cm.chunk_type,
                        "chunk_id": cm.chunk_id,
                        "node_id": cm.node_id,
                        "peer_url": getattr(cm, "peer_url", ""),
                        "checksum_sha256": cm.checksum_sha256,
                        "size_bytes": cm.size_bytes,
                    }
                    for cm in chunk_metas
                ],
            }

            # Save manifest locally on coordinator
            manifest_path = self.state.metadata_dir / f"{file_id}.json"
            manifest_path.write_text(json.dumps(manifest_dict, indent=2), encoding="utf-8")
            logger.info(f"File '{filename}' successfully dispersed to {total_chunks} peers. Manifest ID: {file_id}")

            # Return manifest to Active Node
            self._send_json(200, manifest_dict)
            return

        self._send_json(404, {"error": "Endpoint not found"})

    def _dispatch_chunk(self, peer_url: str, chunk_id: str, chunk_data: bytes) -> Tuple[bool, Optional[str]]:
        """Sends a chunk to a volunteer peer via HTTP POST."""
        try:
            req = urllib.request.Request(
                f"{peer_url}/chunks/{chunk_id}",
                data=chunk_data,
                headers={"Content-Type": "application/octet-stream"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status in (200, 201):
                    return True, None
                return False, f"HTTP {resp.status}"
        except Exception as e:
            return False, str(e)


def run_coordinator(
    port: int = 8000,
    host: str = "0.0.0.0",
    metadata_dir: Optional[Path] = None,
    block: bool = True,
) -> ThreadingHTTPServer:
    meta_dir = Path(metadata_dir or METADATA_DIR).resolve()
    state = CoordinatorState(metadata_dir=meta_dir)

    handler_class = CoordinatorHandler
    handler_class.state = state

    httpd = ThreadingHTTPServer((host, port), handler_class)
    logger.info(f"Coordinator running on http://{host}:{port} (Manifests: {meta_dir})")

    if block:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            httpd.shutdown()
            logger.info("Coordinator stopped.")
    return httpd


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coordinating Node for P2P RS Storage")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host interface to bind")
    parser.add_argument("--metadata-dir", type=str, default=None, help="Directory to store manifests")
    args = parser.parse_args()

    run_coordinator(port=args.port, host=args.host, metadata_dir=args.metadata_dir, block=True)
