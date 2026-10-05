"""Configuration settings for Reed-Solomon RAID."""

from pathlib import Path
import os

# Default Reed-Solomon parameters (Industrial standard: 4 data chunks, 2 parity chunks -> 6 nodes)
# Tolerates any 2 node failures with 1.5x storage overhead (common in Ceph / cloud storage).
DEFAULT_K: int = int(os.getenv("RS_K", "4"))
DEFAULT_M: int = int(os.getenv("RS_M", "2"))

# Base directories
BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
INPUT_DIR: Path = BASE_DIR / "input"
OUTPUT_DIR: Path = BASE_DIR / "output"
METADATA_DIR: Path = OUTPUT_DIR / "metadata"
NODES_DIR: Path = OUTPUT_DIR / "nodes"
RESTORED_DIR: Path = OUTPUT_DIR / "restored"

# Node naming convention
NODE_NAME_PREFIX: str = "node_"
