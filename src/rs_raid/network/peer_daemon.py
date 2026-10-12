"""Volunteer Storage Peer Daemon.

Runs on volunteer computers. Serves chunk read/write requests over HTTP
and periodically reports health/heartbeats to the Coordinator.
"""

from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Optional
import argparse
import json
import logging
import os
import shutil
import socket
import sys
import threading
import time
import urllib.request
import urllib.error

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [Peer %(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("PeerDaemon")


class PeerStorageHandler(BaseHTTPRequestHandler):
    """Handles HTTP chunk read/write requests on the volunteer node."""

    storage_dir: Path
    node_id: str

    def log_message(self, format, *args):
        # Silence default noisy access logs unless debugging
        pass

    def _send_json(self, status_code: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        # Health check
        if self.path == "/health":
            chunks = list(self.storage_dir.glob("*.chunk"))
            total_b, used_b, free_b = shutil.disk_usage(self.storage_dir)
            self._send_json(
                200,
                {
                    "status": "healthy",
                    "node_id": self.node_id,
                    "chunk_count": len(chunks),
                    "free_bytes": free_b,
                },
            )
            return

        # Direct chunk download (called by Active Node during P2P retrieval)
        if self.path.startswith("/chunks/"):
            chunk_id = self.path[len("/chunks/") :].strip()
            chunk_file = self.storage_dir / f"{chunk_id}.chunk"

            if not chunk_file.is_file():
                self._send_json(404, {"error": "Chunk not found", "chunk_id": chunk_id})
                return

            try:
                data = chunk_file.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                logger.info(f"Served chunk {chunk_id} ({len(data)} bytes) directly to client")
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return

        self._send_json(404, {"error": "Endpoint not found"})

    def do_HEAD(self):
        if self.path.startswith("/chunks/"):
            chunk_id = self.path[len("/chunks/") :].strip()
            chunk_file = self.storage_dir / f"{chunk_id}.chunk"
            if chunk_file.is_file():
                self.send_response(200)
                self.send_header("Content-Length", str(chunk_file.stat().st_size))
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        # Store chunk (called by Coordinator during ingress distribution)
        if self.path.startswith("/chunks/"):
            chunk_id = self.path[len("/chunks/") :].strip()
            content_length = int(self.headers.get("Content-Length", 0))

            if content_length <= 0:
                self._send_json(400, {"error": "Empty chunk content"})
                return

            chunk_data = self.rfile.read(content_length)
            chunk_file = self.storage_dir / f"{chunk_id}.chunk"

            try:
                chunk_file.write_bytes(chunk_data)
                logger.info(f"Stored chunk {chunk_id} ({len(chunk_data)} bytes)")
                self._send_json(201, {"status": "stored", "chunk_id": chunk_id, "size": len(chunk_data)})
            except Exception as e:
                logger.error(f"Failed to write chunk {chunk_id}: {e}")
                self._send_json(500, {"error": str(e)})
            return

        self._send_json(404, {"error": "Endpoint not found"})

    def do_DELETE(self):
        if self.path.startswith("/chunks/"):
            chunk_id = self.path[len("/chunks/") :].strip()
            chunk_file = self.storage_dir / f"{chunk_id}.chunk"
            if chunk_file.is_file():
                chunk_file.unlink()
                logger.info(f"Deleted chunk {chunk_id}")
                self._send_json(200, {"status": "deleted", "chunk_id": chunk_id})
            else:
                self._send_json(404, {"error": "Chunk not found"})
            return
        self._send_json(404, {"error": "Endpoint not found"})


class VolunteerPeerServer:
    """Manages the lifecycle of a volunteer storage peer."""

    def __init__(
        self,
        node_id: str,
        host: str = "0.0.0.0",
        port: int = 8001,
        storage_dir: Optional[Path] = None,
        coordinator_url: Optional[str] = None,
        advertise_host: Optional[str] = None,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.storage_dir = Path(storage_dir or f"./volunteer_storage/{node_id}").resolve()
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.coordinator_url = coordinator_url.rstrip("/") if coordinator_url else None
        self.advertise_host = advertise_host or (
            "127.0.0.1" if host in ("0.0.0.0", "127.0.0.1") else host
        )
        self._stop_event = threading.Event()
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

    def start(self, block: bool = True):
        # Configure handler class attributes
        handler_class = PeerStorageHandler
        handler_class.storage_dir = self.storage_dir
        handler_class.node_id = self.node_id

        self._httpd = ThreadingHTTPServer((self.host, self.port), handler_class)
        logger.info(
            f"Volunteer Peer [{self.node_id}] listening on http://{self.host}:{self.port} "
            f"(Storage: {self.storage_dir})"
        )

        # Start heartbeat background thread if coordinator is configured
        if self.coordinator_url:
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()

        if block:
            try:
                self._httpd.serve_forever()
            except KeyboardInterrupt:
                self.stop()

    def stop(self):
        self._stop_event.set()
        if self._httpd:
            self._httpd.shutdown()
        logger.info(f"Volunteer Peer [{self.node_id}] stopped.")

    def _heartbeat_loop(self):
        """Periodically registers and sends heartbeats to Coordinator."""
        reg_payload = json.dumps(
            {
                "node_id": self.node_id,
                "host": self.advertise_host,
                "port": self.port,
                "storage_dir": str(self.storage_dir),
            }
        ).encode("utf-8")

        # Initial registration
        registered = False
        while not self._stop_event.is_set() and not registered:
            try:
                req = urllib.request.Request(
                    f"{self.coordinator_url}/peers/register",
                    data=reg_payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    if resp.status in (200, 201):
                        logger.info(f"Successfully registered with Coordinator at {self.coordinator_url}")
                        registered = True
            except Exception as e:
                logger.warning(f"Registration with Coordinator failed ({e}). Retrying in 3s...")
                time.sleep(3)

        # Heartbeat loop
        while not self._stop_event.is_set():
            time.sleep(5)
            try:
                chunks = list(self.storage_dir.glob("*.chunk"))
                hb_payload = json.dumps(
                    {
                        "node_id": self.node_id,
                        "chunk_count": len(chunks),
                        "timestamp": time.time(),
                    }
                ).encode("utf-8")
                req = urllib.request.Request(
                    f"{self.coordinator_url}/peers/heartbeat",
                    data=hb_payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=3)
            except Exception:
                pass


def run_peer_daemon(
    node_id: str,
    port: int,
    storage_dir: Optional[str] = None,
    coordinator_url: Optional[str] = None,
    host: str = "0.0.0.0",
    advertise_host: Optional[str] = None,
):
    server = VolunteerPeerServer(
        node_id=node_id,
        host=host,
        port=port,
        storage_dir=Path(storage_dir) if storage_dir else None,
        coordinator_url=coordinator_url,
        advertise_host=advertise_host,
    )
    server.start(block=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Volunteer Storage Peer Daemon")
    parser.add_argument("--node-id", type=str, default="volunteer_1", help="Unique ID of this volunteer peer")
    parser.add_argument("--port", type=int, default=8001, help="Port to listen on")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host interface to bind")
    parser.add_argument("--advertise-host", type=str, default="127.0.0.1", help="IP address to advertise to coordinator")
    parser.add_argument("--storage-dir", type=str, default=None, help="Directory to store chunks")
    parser.add_argument("--coordinator", type=str, default=None, help="Coordinator URL, e.g. http://127.0.0.1:8000")

    args = parser.parse_args()
    run_peer_daemon(
        node_id=args.node_id,
        port=args.port,
        storage_dir=args.storage_dir,
        coordinator_url=args.coordinator,
        host=args.host,
        advertise_host=args.advertise_host,
    )
