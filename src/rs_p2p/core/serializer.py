"""Configurable Checkpoint Activation Format & Serializers.

Provides an extensible abstract base class and standard serializers to easily
package activations, tensors, and pipeline checkpoint metadata.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
import json
import struct


class ActivationSerializer(ABC):
    """Abstract base class for activation serialization/deserialization."""

    @abstractmethod
    def serialize(self, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bytes:
        """Serializes payload and metadata into binary bytes.

        Args:
            payload: Activation data (raw bytes, array, tensor, or custom struct).
            metadata: Associated metadata (e.g., shape, dtype, layer_index, sequence_id).

        Returns:
            bytes representation ready for Reed-Solomon sharding.
        """
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Tuple[Any, Dict[str, Any]]:
        """Deserializes binary bytes back into payload and metadata.

        Args:
            data: Binary bytes recovered from Reed-Solomon decoding.

        Returns:
            Tuple of (payload, metadata).
        """
        pass


class RawBytesSerializer(ActivationSerializer):
    """Pass-through serializer for raw binary files or pre-serialized buffers.

    Format: [4 bytes header length (N)] [N bytes JSON metadata] [Raw Payload Bytes]
    """

    def serialize(self, payload: bytes | bytearray, metadata: Optional[Dict[str, Any]] = None) -> bytes:
        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError(f"RawBytesSerializer requires bytes/bytearray, got {type(payload)}")

        meta = metadata or {}
        meta_bytes = json.dumps(meta).encode("utf-8")
        header = struct.pack(">I", len(meta_bytes))
        return header + meta_bytes + bytes(payload)

    def deserialize(self, data: bytes) -> Tuple[bytes, Dict[str, Any]]:
        if len(data) < 4:
            raise ValueError("Corrupt data: too short for header")
        header_len = struct.unpack(">I", data[:4])[0]
        meta_bytes = data[4 : 4 + header_len]
        meta = json.loads(meta_bytes.decode("utf-8")) if header_len > 0 else {}
        payload = data[4 + header_len :]
        return payload, meta


class TensorActivationSerializer(ActivationSerializer):
    """Serializer tailored for LLM / NN pipeline activations.

    Supports raw activation buffers with explicit tensor metadata (shape, dtype,
    layer_index, sequence_id, batch_size).

    Format:
    - 4 bytes: Magic bytes (0x54454E53 -> 'TENS')
    - 4 bytes: JSON metadata length (M)
    - M bytes: JSON metadata (e.g. {"shape": [1, 32, 4096], "dtype": "float16", "layer_id": 9})
    - Rest: Raw buffer bytes
    """

    MAGIC = b"TENS"

    def serialize(self, payload: bytes | bytearray, metadata: Optional[Dict[str, Any]] = None) -> bytes:
        meta = metadata.copy() if metadata else {}
        if "layer_index" not in meta:
            meta["layer_index"] = 0
        if "shape" not in meta:
            meta["shape"] = [len(payload)]
        if "dtype" not in meta:
            meta["dtype"] = "uint8"

        meta_bytes = json.dumps(meta).encode("utf-8")
        header = self.MAGIC + struct.pack(">I", len(meta_bytes))
        return header + meta_bytes + bytes(payload)

    def deserialize(self, data: bytes) -> Tuple[bytes, Dict[str, Any]]:
        if len(data) < 8 or not data.startswith(self.MAGIC):
            raise ValueError("Invalid TensorActivation payload format (bad magic)")
        meta_len = struct.unpack(">I", data[4:8])[0]
        meta_bytes = data[8 : 8 + meta_len]
        meta = json.loads(meta_bytes.decode("utf-8"))
        payload = data[8 + meta_len :]
        return payload, meta


class JSONPayloadSerializer(ActivationSerializer):
    """Serializer for JSON-serializable dictionaries/objects."""

    def serialize(self, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bytes:
        container = {
            "metadata": metadata or {},
            "payload": payload,
        }
        return json.dumps(container).encode("utf-8")

    def deserialize(self, data: bytes) -> Tuple[Any, Dict[str, Any]]:
        container = json.loads(data.decode("utf-8"))
        return container.get("payload"), container.get("metadata", {})


# Serializer registry
_SERIALIZERS: Dict[str, ActivationSerializer] = {
    "raw": RawBytesSerializer(),
    "tensor": TensorActivationSerializer(),
    "json": JSONPayloadSerializer(),
}


def register_serializer(name: str, serializer: ActivationSerializer) -> None:
    """Registers a custom activation serializer."""
    _SERIALIZERS[name.lower()] = serializer


def get_serializer(name: str = "raw") -> ActivationSerializer:
    """Retrieves an activation serializer by name."""
    serializer = _SERIALIZERS.get(name.lower())
    if not serializer:
        raise KeyError(
            f"Unknown serializer: {name}. Available: {list(_SERIALIZERS.keys())}"
        )
    return serializer
