"""Configuration constants for Reed-Solomon P2P Storage & DHT."""

from pathlib import Path

# Reed-Solomon Defaults
DEFAULT_K = 4  # Number of data chunks
DEFAULT_M = 2  # Number of parity chunks (fault tolerance up to m failed nodes)

# Kademlia DHT Defaults
KADEMLIA_K = 8  # Bucket size (number of contacts per bucket)
KADEMLIA_ALPHA = 3  # Concurrency parameter for lookups
DHT_ID_BITS = 160  # 160-bit keyspace (SHA-1 equivalent)

# Namespaces
NS_COMPUTE_LIVENESS = "NS1"  # Layer placement & compute node liveness
NS_STORAGE_INDEX = "NS2"     # Chunk location index & checkpoint manifests

# Default Network Ports
DEFAULT_DHT_SEED_PORT = 7000
DEFAULT_PEER_PORT = 7001

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
VOLUNTEER_STORAGE_DIR = BASE_DIR / "volunteer_storage"
