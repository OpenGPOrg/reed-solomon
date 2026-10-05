"""Cluster manager for maintaining and tracking storage nodes."""

from pathlib import Path
from typing import Dict, List, Optional
from ..storage.base import StorageNode
from ..storage.local import LocalStorageNode
from ..config import NODES_DIR, NODE_NAME_PREFIX


class NodeCluster:
    """Manages the pool of available storage nodes in the RAID array."""

    def __init__(self, nodes: Optional[List[StorageNode]] = None):
        self._nodes: Dict[str, StorageNode] = {}
        if nodes:
            for node in nodes:
                self.register_node(node)

    def register_node(self, node: StorageNode) -> None:
        self._nodes[node.node_id] = node

    def get_node(self, node_id: str) -> Optional[StorageNode]:
        return self._nodes.get(node_id)

    @property
    def all_nodes(self) -> List[StorageNode]:
        return list(self._nodes.values())

    def healthy_nodes(self) -> List[StorageNode]:
        return [node for node in self._nodes.values() if node.is_healthy()]

    def select_nodes(self, count: int) -> List[StorageNode]:
        """Selects count healthy nodes for placing chunks."""
        healthy = self.healthy_nodes()
        if len(healthy) < count:
            raise RuntimeError(
                f"Insufficient healthy nodes: requested {count}, but only {len(healthy)} available"
            )
        return healthy[:count]

    @classmethod
    def create_local_cluster(cls, total_nodes: int, base_dir: Path = NODES_DIR) -> "NodeCluster":
        """Factory method to initialize a local simulated disk array."""
        cluster = cls()
        base_dir.mkdir(parents=True, exist_ok=True)
        for i in range(total_nodes):
            node_id = f"{NODE_NAME_PREFIX}{i}"
            node_path = base_dir / node_id
            cluster.register_node(LocalStorageNode(node_id=node_id, root_dir=node_path))
        return cluster

    def ensure_capacity(self, required_nodes: int, base_dir: Path = NODES_DIR) -> None:
        """Ensures that at least required_nodes are registered and available."""
        current_count = len(self._nodes)
        if current_count < required_nodes:
            for i in range(current_count, required_nodes):
                node_id = f"{NODE_NAME_PREFIX}{i}"
                node_path = base_dir / node_id
                self.register_node(LocalStorageNode(node_id=node_id, root_dir=node_path))
