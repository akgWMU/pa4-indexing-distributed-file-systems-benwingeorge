import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
from collections import defaultdict

from utils import FileMetadata

class BPlusNode:
    """Node in a B+ tree"""
    def __init__(self, order, is_leaf=False):
        self.order = order  # Maximum number of children
        self.keys = []  # List of keys (FileMetadata for leaf, filenames for internal)
        self.children = []  # Child nodes (for internal) or None (for leaf)
        self.is_leaf = is_leaf
        self.next = None  # Pointer to next leaf (for range queries)
        self.parent = None
    
    def is_full(self):
        return len(self.keys) >= self.order - 1
    
    def __repr__(self):
        if self.is_leaf:
            return f"Leaf({len(self.keys)} keys)"
        return f"Internal({len(self.keys)} keys, {len(self.children)} children)"


class BPlusTree:
    """
    B+ Tree implementation for metadata indexing.
    All data stored in leaves, internal nodes only store keys for navigation.
    """
    def __init__(self, order=100):
        self.order = order  # Typical B+ tree order (fanout)
        self.root = BPlusNode(order, is_leaf=True)
        self.lock = threading.RLock()
        self.size = 0
        self.height = 1
        
        # Secondary indices
        self.tag_index = defaultdict(list)
        self.tag_lock = threading.RLock()
    
    def insert(self, metadata: FileMetadata):
        """Insert metadata (thread-safe)"""
        with self.lock:
            # Find leaf node
            leaf = self._find_leaf(self.root, metadata.filename)
            
            # Insert into leaf
            self._insert_into_leaf(leaf, metadata)
            
            # Check if split needed
            if len(leaf.keys) >= self.order:
                self._split_leaf(leaf)
            
            self.size += 1
            
            # Update tag index
            with self.tag_lock:
                for tag in metadata.tags:
                    self.tag_index[tag].append(metadata)
    
    def _find_leaf(self, node, filename):
        """Find the leaf node where key should be inserted"""
        if node.is_leaf:
            return node
        
        # Find appropriate child
        for i, key in enumerate(node.keys):
            if filename < key:
                return self._find_leaf(node.children[i], filename)
        
        return self._find_leaf(node.children[-1], filename)
    
    def _insert_into_leaf(self, leaf, metadata):
        """Insert metadata into leaf node in sorted order"""
        inserted = False
        for i, existing in enumerate(leaf.keys):
            if metadata.filename == existing.filename:
                # Update existing
                leaf.keys[i] = metadata
                return
            if metadata.filename < existing.filename:
                leaf.keys.insert(i, metadata)
                inserted = True
                break
        
        if not inserted:
            leaf.keys.append(metadata)
    
    def _split_leaf(self, leaf):
        """Split a full leaf node"""
        mid = len(leaf.keys) // 2
        
        # Create new leaf
        new_leaf = BPlusNode(self.order, is_leaf=True)
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.next = leaf.next
        leaf.keys = leaf.keys[:mid]
        leaf.next = new_leaf
        
        # Get key to push up
        push_up_key = new_leaf.keys[0].filename
        
        # Insert into parent
        if leaf.parent is None:
            # Create new root
            new_root = BPlusNode(self.order, is_leaf=False)
            new_root.keys = [push_up_key]
            new_root.children = [leaf, new_leaf]
            leaf.parent = new_root
            new_leaf.parent = new_root
            self.root = new_root
            self.height += 1
        else:
            self._insert_into_parent(leaf.parent, push_up_key, new_leaf)
    
    def _insert_into_parent(self, parent, key, new_child):
        """Insert key and child into parent node"""
        # Find position
        inserted = False
        for i, existing_key in enumerate(parent.keys):
            if key < existing_key:
                parent.keys.insert(i, key)
                parent.children.insert(i + 1, new_child)
                inserted = True
                break
        
        if not inserted:
            parent.keys.append(key)
            parent.children.append(new_child)
        
        new_child.parent = parent
        
        # Check if parent needs splitting
        if len(parent.keys) >= self.order:
            self._split_internal(parent)
    
    def _split_internal(self, node):
        """Split a full internal node"""
        mid = len(node.keys) // 2
        push_up_key = node.keys[mid]
        
        # Create new internal node
        new_node = BPlusNode(self.order, is_leaf=False)
        new_node.keys = node.keys[mid + 1:]
        new_node.children = node.children[mid + 1:]
        
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        
        # Update parent pointers
        for child in new_node.children:
            child.parent = new_node
        
        # Insert into parent
        if node.parent is None:
            # Create new root
            new_root = BPlusNode(self.order, is_leaf=False)
            new_root.keys = [push_up_key]
            new_root.children = [node, new_node]
            node.parent = new_root
            new_node.parent = new_root
            self.root = new_root
            self.height += 1
        else:
            self._insert_into_parent(node.parent, push_up_key, new_node)
    
    def search_by_filename(self, filename: str) -> Optional[FileMetadata]:
        """Search by filename"""
        with self.lock:
            leaf = self._find_leaf(self.root, filename)
            
            for metadata in leaf.keys:
                if metadata.filename == filename:
                    return metadata
            return None
    
    def search_by_tag(self, tag: str) -> List[FileMetadata]:
        """Search by tag"""
        with self.tag_lock:
            return self.tag_index.get(tag, []).copy()
    
    def list_files(self, order='asc') -> List[FileMetadata]:
        """List all files in order (efficient due to leaf linking)"""
        with self.lock:
            result = []
            
            # Find leftmost leaf
            node = self.root
            while not node.is_leaf:
                node = node.children[0]
            
            # Traverse leaves
            while node is not None:
                result.extend(node.keys)
                node = node.next
            
            if order == 'desc':
                result.reverse()
            
            return result
    
    def get_height(self):
        with self.lock:
            return self.height
    
    def get_size(self):
        with self.lock:
            return self.size
    
    def get_stats(self):
        """Get B+ tree statistics"""
        with self.lock:
            return {
                'size': self.size,
                'height': self.height,
                'order': self.order
            }
