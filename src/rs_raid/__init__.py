"""Reed-Solomon RAID Package.

Convenience top-level API for storing and retrieving erasure-coded files.
"""

from pathlib import Path
from typing import Optional, Dict, Any

from .config import DEFAULT_K, DEFAULT_M, INPUT_DIR, OUTPUT_DIR, METADATA_DIR, NODES_DIR, RESTORED_DIR
from .metadata.manifest import FileManifest
from .raid.controller import RaidController
from .storage.base import StorageNode
from .storage.local import LocalStorageNode
from .storage.remote import RemoteStorageNode

_default_controller: Optional[RaidController] = None


def get_controller() -> RaidController:
    global _default_controller
    if _default_controller is None:
        _default_controller = RaidController()
    return _default_controller


def store_file(
    input_path: Path | str,
    k: Optional[int] = None,
    m: Optional[int] = None,
) -> FileManifest:
    """Encodes and stores an input file into the RAID array.

    Args:
        input_path: File path (e.g. 'input/sample.txt').
        k: Number of data chunks (default: 4, easily configured).
        m: Number of parity chunks (default: 2, easily configured).

    Returns:
        The generated FileManifest with chunk placement details.
    """
    return get_controller().store_file(Path(input_path), k=k, m=m)


def retrieve_file(
    identifier: str,
    output_dir: Optional[Path | str] = None,
) -> Path:
    """Retrieves and reconstructs an encoded file, tolerating up to m missing/corrupted nodes.

    Args:
        identifier: file_id, filename, or manifest path.
        output_dir: Directory to save the restored file (default: output/restored/).

    Returns:
        Path to the reconstructed file.
    """
    out = Path(output_dir) if output_dir else None
    return get_controller().retrieve_file(identifier, output_dir=out)


def inspect_file(identifier: str) -> Dict[str, Any]:
    """Inspects chunk status and node health for a file (HEALTHY, DEGRADED, UNRECOVERABLE)."""
    return get_controller().inspect_file(identifier)


def repair_file(identifier: str) -> Dict[str, Any]:
    """Reconstructs and repairs missing/corrupted chunks back onto healthy nodes."""
    return get_controller().repair_file(identifier)


__all__ = [
    "store_file",
    "retrieve_file",
    "inspect_file",
    "repair_file",
    "RaidController",
    "StorageNode",
    "LocalStorageNode",
    "RemoteStorageNode",
    "FileManifest",
    "DEFAULT_K",
    "DEFAULT_M",
    "INPUT_DIR",
    "OUTPUT_DIR",
    "METADATA_DIR",
    "NODES_DIR",
    "RESTORED_DIR",
]
