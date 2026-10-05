"""Reed-Solomon Erasure Coding Engine using zfec."""

from typing import List, Tuple
import math
import zfec


class RSCodec:
    """Wraps zfec Reed-Solomon encoder/decoder for chunk-based storage.

    Parameters:
        k: Number of data chunks needed to reconstruct the original data.
        m: Number of parity chunks added for fault tolerance.
           Total chunks n = k + m.
    """

    def __init__(self, k: int, m: int):
        if k < 1:
            raise ValueError("k (data chunks) must be >= 1")
        if m < 1:
            raise ValueError("m (parity chunks) must be >= 1")
        if k + m > 256:
            raise ValueError("Total chunks (k + m) must be <= 256 in GF(2^8)")

        self.k: int = k
        self.m: int = m
        self.n: int = k + m
        self._encoder = zfec.Encoder(self.k, self.n)
        self._decoder = zfec.Decoder(self.k, self.n)

    def encode(self, data: bytes) -> Tuple[List[bytes], int]:
        """Encodes raw data bytes into n chunks (k data + m parity).

        Returns:
            A tuple of (chunks, original_length), where chunks is a list of
            n byte strings, each of identical size.
        """
        original_length = len(data)

        # Calculate chunk size (ceil division so all data fits across k chunks)
        chunk_size = math.ceil(original_length / self.k) if original_length > 0 else 1
        required_length = chunk_size * self.k
        padding_needed = required_length - original_length

        # Pad with zeros if necessary
        padded_data = data + (b"\x00" * padding_needed)

        # Slice padded data into k equal-sized chunks
        data_blocks = [
            padded_data[i * chunk_size : (i + 1) * chunk_size]
            for i in range(self.k)
        ]

        # Generate all n = k + m chunks using zfec
        # Note: zfec.Encoder.encode returns n blocks (first k are original data blocks)
        all_blocks = self._encoder.encode(data_blocks)
        return all_blocks, original_length

    def decode(
        self, available_blocks: List[bytes], block_indices: List[int], original_length: int
    ) -> bytes:
        """Reconstructs the original data given any k valid chunks.

        Args:
            available_blocks: At least k chunk byte strings.
            block_indices: The 0-based indices corresponding to each block in available_blocks.
            original_length: The original unpadded file size.

        Returns:
            The reconstructed byte string trimmed to original_length.
        """
        if len(available_blocks) < self.k:
            raise ValueError(
                f"Insufficient chunks for reconstruction: got {len(available_blocks)}, "
                f"need at least {self.k} (k={self.k}, m={self.m})"
            )

        if len(available_blocks) != len(block_indices):
            raise ValueError("available_blocks and block_indices must have matching lengths")

        # Pick exactly k chunks to feed into decoder
        selected_blocks = list(available_blocks[: self.k])
        selected_indices = list(block_indices[: self.k])

        reconstructed_blocks = self._decoder.decode(selected_blocks, selected_indices)
        full_data = b"".join(reconstructed_blocks)

        # Trim padding back to original file length
        return full_data[:original_length]
