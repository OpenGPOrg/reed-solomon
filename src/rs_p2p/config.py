"""Configuration constants for Reed-Solomon P2P Storage & DHT."""

from pathlib import Path

# Reed-Solomon Defaults
DEFAULT_K = 4  # Number of data chunks
DEFAULT_M = 2  # Number of parity chunks (fault tolerance up to m failed nodes)

# Kademlia DHT Defaults
KADEMLIA_K = 8  # Bucket size (number of contacts per bucket)
KADEMLIA_ALPHA = 3  # Concurrency parameter for lookups
DHT_ID_BITS = 160  # 160-bit keyspace (SHA-1 equivalent)
DHT_REFRESH_INTERVAL_SEC = 60.0
DHT_REPUBLISH_INTERVAL_SEC = 300.0

# Namespaces
NS_STORAGE_INDEX = "storage_index"  # Chunk location index & checkpoint manifests

# gRPC Message Size & Channel Options (64 MiB for large checkpoint payloads)
GRPC_MAX_MESSAGE_LENGTH = 64 * 1024 * 1024
GRPC_SERVER_OPTIONS = [
    ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_LENGTH),
    ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_LENGTH),
]
GRPC_CHANNEL_OPTIONS = [
    ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_LENGTH),
    ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_LENGTH),
    ("grpc.use_local_subchannel_pool", 1),
    ("grpc.initial_reconnect_backoff_ms", 200),
    ("grpc.min_reconnect_backoff_ms", 200),
    ("grpc.max_reconnect_backoff_ms", 2000),
]

# Default Network Ports
DEFAULT_DHT_SEED_PORT = 7000
DEFAULT_PEER_PORT = 7001

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
VOLUNTEER_STORAGE_DIR = BASE_DIR / "volunteer_storage"
