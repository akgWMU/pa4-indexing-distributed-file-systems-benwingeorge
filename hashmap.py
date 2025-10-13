from utils import FileMetadata
from typing import List, Optional, Dict, Tuple
import threading
from collections import defaultdict

class HashTableIndex:
    """
    Hash table implementation for metadata indexing.
    Uses separate chaining for collision resolution.
    """
    def __init__(self, initial_capacity=1024, load_factor=0.75):
        self.capacity = initial_capacity
        self.load_factor = load_factor
        self.size = 0
        self.buckets = [[] for _ in range(self.capacity)]
        self.lock = threading.RLock()
        
        # Secondary indices
        self.tag_index = defaultdict(list)
        self.tag_lock = threading.RLock()
    
    def _hash(self, key: str) -> int:
        """Hash function using Python's built-in hash"""
        return hash(key) % self.capacity
    
    def _resize(self):
        """Resize hash table when load factor exceeded"""
        old_buckets = self.buckets
        self.capacity *= 2
        self.buckets = [[] for _ in range(self.capacity)]
        self.size = 0
        
        # Rehash all entries
        for bucket in old_buckets:
            for metadata in bucket:
                self._insert_no_lock(metadata)
    
    def _insert_no_lock(self, metadata: FileMetadata):
        """Insert without acquiring lock (for internal use)"""
        index = self._hash(metadata.filename)
        bucket = self.buckets[index]
        
        # Check for duplicate and update if exists
        for i, item in enumerate(bucket):
            if item.filename == metadata.filename:
                bucket[i] = metadata
                return
        
        # Add new entry
        bucket.append(metadata)
        self.size += 1
        
        # Check load factor and resize if needed
        if self.size / self.capacity > self.load_factor:
            self._resize()
    
    def insert(self, metadata: FileMetadata):
        """Insert metadata (thread-safe)"""
        with self.lock:
            self._insert_no_lock(metadata)
            
            # Update tag index
            with self.tag_lock:
                for tag in metadata.tags:
                    self.tag_index[tag].append(metadata)
    
    def search_by_filename(self, filename: str) -> Optional[FileMetadata]:
        """Search by filename with O(1) average complexity"""
        with self.lock:
            index = self._hash(filename)
            bucket = self.buckets[index]
            
            for metadata in bucket:
                if metadata.filename == filename:
                    return metadata
            return None
    
    def search_by_tag(self, tag: str) -> List[FileMetadata]:
        """Search by tag"""
        with self.tag_lock:
            return self.tag_index.get(tag, []).copy()
    
    def list_files(self, order='asc') -> List[FileMetadata]:
        """List all files (requires sorting for ordered output)"""
        with self.lock:
            all_files = []
            for bucket in self.buckets:
                all_files.extend(bucket)
            
            all_files.sort(key=lambda x: x.filename, reverse=(order == 'desc'))
            return all_files
    
    def get_size(self):
        with self.lock:
            return self.size
    
    def get_capacity(self):
        with self.lock:
            return self.capacity
    
    def get_stats(self):
        """Get hash table statistics"""
        with self.lock:
            non_empty = sum(1 for bucket in self.buckets if bucket)
            max_chain = max(len(bucket) for bucket in self.buckets)
            avg_chain = self.size / non_empty if non_empty > 0 else 0
            
            return {
                'size': self.size,
                'capacity': self.capacity,
                'load_factor': self.size / self.capacity,
                'non_empty_buckets': non_empty,
                'max_chain_length': max_chain,
                'avg_chain_length': avg_chain
            }