"""Unit tests for RSCodec and Activation Serializers."""

import sys
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import unittest
from rs_p2p.core.codec import RSCodec
from rs_p2p.core.serializer import (
    RawBytesSerializer,
    TensorActivationSerializer,
    JSONPayloadSerializer,
    get_serializer,
)


def test_rs_codec_roundtrip():
    k = 4
    m = 2
    codec = RSCodec(k=k, m=m)

    original_data = b"Hello, this is a simulated activation tensor byte buffer!" * 50
    chunks, original_len = codec.encode(original_data)

    assert len(chunks) == k + m

    # Simulate losing m chunks (drop first 2)
    surviving_chunks = chunks[2:]
    surviving_indices = list(range(2, k + m))

    recovered = codec.decode(surviving_chunks, surviving_indices, original_len)
    assert recovered == original_data


def test_raw_bytes_serializer():
    serializer = RawBytesSerializer()
    payload = b"Sample activation raw binary"
    meta = {"source_layer": "L6-L9", "dtype": "float32"}

    serialized = serializer.serialize(payload, meta)
    recovered_payload, recovered_meta = serializer.deserialize(serialized)

    assert recovered_payload == payload
    assert recovered_meta == meta


def test_tensor_activation_serializer():
    serializer = TensorActivationSerializer()
    tensor_bytes = b"\x00\x01\x02\x03" * 256
    meta = {
        "shape": [1, 32, 128],
        "dtype": "float16",
        "layer_index": 9,
        "sequence_id": "req-9901",
    }

    serialized = serializer.serialize(tensor_bytes, meta)
    recovered_tensor, recovered_meta = serializer.deserialize(serialized)

    assert recovered_tensor == tensor_bytes
    assert recovered_meta["shape"] == [1, 32, 128]
    assert recovered_meta["dtype"] == "float16"
    assert recovered_meta["layer_index"] == 9
    assert recovered_meta["sequence_id"] == "req-9901"


def test_json_serializer():
    serializer = JSONPayloadSerializer()
    payload = {"activations": [0.12, 0.45, -0.98], "mask": [1, 1, 0]}
    meta = {"step": 42}

    serialized = serializer.serialize(payload, meta)
    recovered_payload, recovered_meta = serializer.deserialize(serialized)

    assert recovered_payload == payload
    assert recovered_meta == meta


if __name__ == "__main__":
    test_rs_codec_roundtrip()
    test_raw_bytes_serializer()
    test_tensor_activation_serializer()
    test_json_serializer()
    print("All codec and serializer unit tests passed!")
