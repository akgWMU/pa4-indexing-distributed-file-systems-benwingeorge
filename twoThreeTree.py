import threading
import random
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Optional
from collections import defaultdict
from utils import FileMetadata, MetadataGenerator

class TwoThreeNode:
    """Node in a 2-3 tree with 1-2 keys and 2-3 children"""
    def __init__(self):
        self.keys = []  # List of FileMetadata objects (1 or 2)
        self.children = []  # List of child nodes (0, 2, or 3)
    
    def is_leaf(self):
        return len(self.children) == 0
    
    def is_full(self):
        return len(self.keys) == 2
    
    def __repr__(self):
        key_names = [k.filename for k in self.keys]
        return f"Node(keys={key_names}, children={len(self.children)})"


class TwoThreeTree:
    """
    2-3 Tree implementation for metadata indexing.
    Supports concurrent access with read-write locks.
    """
    def __init__(self):
        self.root = None
        self.lock = threading.RLock()  # Reentrant lock for nested calls
        self.size = 0
        self.height = 0
        
        # Secondary indices for tag-based search
        self.tag_index = defaultdict(list)  # tag -> [FileMetadata]
        self.tag_lock = threading.RLock()
    
    def insert(self, metadata: FileMetadata):
        """Insert metadata into the tree (thread-safe)"""
        with self.lock:
            if self.root is None:
                self.root = TwoThreeNode()
                self.root.keys.append(metadata)
                self.height = 1
            else:
                new_root = self._insert_helper(self.root, metadata)
                if new_root is not None:
                    self.root = new_root
                    self.height += 1
            
            self.size += 1
            
            # Update tag index
            with self.tag_lock:
                for tag in metadata.tags:
                    self.tag_index[tag].append(metadata)
    
    def _insert_helper(self, node, metadata):
        """Recursive insertion helper"""
        if node.is_leaf():
            return self._insert_into_node(node, metadata)
        else:
            # Find appropriate child
            child_index = self._find_child_index(node, metadata)
            new_child = self._insert_helper(node.children[child_index], metadata)
            
            if new_child is not None:
                # Child split occurred, insert middle key into current node
                middle_key = new_child.keys[0]
                return self._insert_into_internal(node, middle_key, new_child, child_index)
            return None
    
    def _insert_into_node(self, node, metadata):
        """Insert into a leaf node"""
        # Insert in sorted order
        inserted = False
        for i, key in enumerate(node.keys):
            if metadata < key:
                node.keys.insert(i, metadata)
                inserted = True
                break
        if not inserted:
            node.keys.append(metadata)
        
        # Check if split is needed
        if len(node.keys) == 3:
            return self._split_node(node)
        return None
    
    def _insert_into_internal(self, node, middle_key, new_child, child_index):
        """Insert middle key from split child into internal node"""
        # Insert middle key
        inserted = False
        for i, key in enumerate(node.keys):
            if middle_key < key:
                node.keys.insert(i, middle_key)
                node.children.insert(i + 1, new_child.children[1])
                inserted = True
                break
        
        if not inserted:
            node.keys.append(middle_key)
            node.children.append(new_child.children[1])
        
        # Update child pointer
        node.children[child_index] = new_child.children[0]
        
        # Check if split needed
        if len(node.keys) == 3:
            return self._split_node(node)
        return None
    
    def _split_node(self, node):
        """Split a node with 3 keys into two nodes with 1 key each"""
        left = TwoThreeNode()
        right = TwoThreeNode()
        middle = TwoThreeNode()
        
        # Distribute keys
        left.keys = [node.keys[0]]
        middle.keys = [node.keys[1]]
        right.keys = [node.keys[2]]
        
        # Distribute children if not leaf
        if not node.is_leaf():
            left.children = node.children[0:2]
            right.children = node.children[2:4]
        
        middle.children = [left, right]
        return middle
    
    def _find_child_index(self, node, metadata):
        """Find which child to traverse for insertion/search"""
        for i, key in enumerate(node.keys):
            if metadata < key:
                return i
        return len(node.keys)
    
    def search_by_filename(self, filename: str) -> Optional[FileMetadata]:
        """Search for metadata by filename (thread-safe)"""
        with self.lock:
            if self.root is None:
                return None
            return self._search_helper(self.root, filename)
    
    def _search_helper(self, node, filename):
        """Recursive search helper"""
        # Check keys in current node
        for key in node.keys:
            if key.filename == filename:
                return key
        
        # If leaf, not found
        if node.is_leaf():
            return None
        
        # Traverse to appropriate child
        for i, key in enumerate(node.keys):
            if filename < key.filename:
                return self._search_helper(node.children[i], filename)
        
        return self._search_helper(node.children[-1], filename)
    
    def search_by_tag(self, tag: str) -> List[FileMetadata]:
        """Search for all files with a specific tag (thread-safe)"""
        with self.tag_lock:
            return self.tag_index.get(tag, []).copy()
    
    def list_files(self, order='asc') -> List[FileMetadata]:
        """List all files in order (thread-safe)"""
        with self.lock:
            result = []
            self._inorder_traversal(self.root, result)
            if order == 'desc':
                result.reverse()
            return result
    
    def _inorder_traversal(self, node, result):
        """In-order traversal of the tree"""
        if node is None:
            return
        
        if node.is_leaf():
            result.extend(node.keys)
        else:
            for i, key in enumerate(node.keys):
                self._inorder_traversal(node.children[i], result)
                result.append(key)
            self._inorder_traversal(node.children[-1], result)
    
    def get_height(self):
        """Return current tree height"""
        with self.lock:
            return self.height
    
    def get_size(self):
        """Return number of elements"""
        with self.lock:
            return self.size

class PerformanceMetrics:
    """Track and report performance metrics"""
    def __init__(self):
        self.insert_times = []
        self.search_times = []
        self.lock = threading.Lock()
    
    def record_insert(self, duration):
        with self.lock:
            self.insert_times.append(duration)
    
    def record_search(self, duration):
        with self.lock:
            self.search_times.append(duration)
    
    def get_stats(self):
        with self.lock:
            return {
                'avg_insert_time': sum(self.insert_times) / len(self.insert_times) if self.insert_times else 0,
                'avg_search_time': sum(self.search_times) / len(self.search_times) if self.search_times else 0,
                'total_inserts': len(self.insert_times),
                'total_searches': len(self.search_times)
            }


# ==================== WORKER THREADS ====================

def insert_worker(tree, metadata_list, metrics):
    """Worker thread for inserting metadata"""
    for metadata in metadata_list:
        start = time.perf_counter()
        tree.insert(metadata)
        end = time.perf_counter()
        metrics.record_insert(end - start)

def search_worker(tree, filenames, metrics):
    """Worker thread for searching metadata"""
    for filename in filenames:
        start = time.perf_counter()
        result = tree.search_by_filename(filename)
        end = time.perf_counter()
        metrics.record_search(end - start)


# ==================== MAIN SIMULATION ====================

def run_simulation(num_entries=10000, num_threads=5):
    """Run complete simulation and benchmarking"""
    print("=" * 60)
    print("METADATA INDEXING SYSTEM SIMULATION")
    print("=" * 60)
    
    # Initialize
    tree = TwoThreeTree()
    metrics = PerformanceMetrics()
    
    # Generate dataset
    print(f"\n[1/4] Generating {num_entries} metadata entries...")
    metadata_list = MetadataGenerator.generate_metadata(num_entries)
    print(f"✓ Generated {len(metadata_list)} entries")
    
    # Concurrent insertions
    print(f"\n[2/4] Simulating concurrent insertions ({num_threads} threads)...")
    chunk_size = len(metadata_list) // num_threads
    threads = []
    
    start_time = time.time()
    for i in range(num_threads):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < num_threads - 1 else len(metadata_list)
        chunk = metadata_list[start_idx:end_idx]
        
        t = threading.Thread(
            target=insert_worker,
            args=(tree, chunk, metrics),
            name=f"InsertThread-{i}"
        )
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    insert_duration = time.time() - start_time
    print(f"✓ Inserted {tree.get_size()} entries in {insert_duration:.2f}s")
    
    # Concurrent searches
    print(f"\n[3/4] Simulating concurrent searches ({num_threads} threads)...")
    search_filenames = random.sample([m.filename for m in metadata_list], k=min(1000, len(metadata_list)))
    search_chunk_size = len(search_filenames) // num_threads
    threads = []
    
    start_time = time.time()
    for i in range(num_threads):
        start_idx = i * search_chunk_size
        end_idx = start_idx + search_chunk_size if i < num_threads - 1 else len(search_filenames)
        chunk = search_filenames[start_idx:end_idx]
        
        t = threading.Thread(
            target=search_worker,
            args=(tree, chunk, metrics),
            name=f"SearchThread-{i}"
        )
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    search_duration = time.time() - start_time
    print(f"✓ Completed {len(search_filenames)} searches in {search_duration:.2f}s")
    
    # Performance metrics
    print(f"\n[4/4] Performance Metrics:")
    print("-" * 60)
    stats = metrics.get_stats()
    print(f"Average Insert Time:  {stats['avg_insert_time']*1000:.4f} ms")
    print(f"Average Search Time:  {stats['avg_search_time']*1000:.4f} ms")
    print(f"Tree Height:          {tree.get_height()}")
    print(f"Tree Size:            {tree.get_size()}")
    print(f"Total Inserts:        {stats['total_inserts']}")
    print(f"Total Searches:       {stats['total_searches']}")
    
    # Test specific operations
    print(f"\n[BONUS] Testing Additional Operations:")
    print("-" * 60)
    
    # Search by tag
    test_tag = "work"
    tag_results = tree.search_by_tag(test_tag)
    print(f"Files with tag '{test_tag}': {len(tag_results)}")
    
    # List files
    all_files = tree.list_files(order='asc')
    print(f"Total files listed: {len(all_files)}")
    print(f"First 3 files: {[f.filename for f in all_files[:3]]}")
    
    print("\n" + "=" * 60)
    print("SIMULATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_simulation(num_entries=1000, num_threads=5)