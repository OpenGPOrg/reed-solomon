"""Generated gRPC Protobuf modules with safe path resolution."""

import sys
from pathlib import Path

_proto_dir = str(Path(__file__).resolve().parent)
if _proto_dir not in sys.path:
    sys.path.insert(0, _proto_dir)

from . import storage_pb2, storage_pb2_grpc, dht_pb2, dht_pb2_grpc

__all__ = ["storage_pb2", "storage_pb2_grpc", "dht_pb2", "dht_pb2_grpc"]
