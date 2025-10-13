import threading
import random
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
from collections import defaultdict
import sys

from hashmap import HashTableIndex
from utils import FileMetadata, MetadataGenerator
from bPlusTree import BPlusTree

class PerformanceComparator:
    """Compare performance of different indexing structures"""
    
    def __init__(self, num_entries=10000, num_threads=5):
        self.num_entries = num_entries
        self.num_threads = num_threads
        self.metadata_list = MetadataGenerator.generate_metadata(num_entries)
        self.results = {}
    
    def benchmark_structure(self, structure, name):
        """Benchmark a single indexing structure"""
        print(f"\n{'='*60}")
        print(f"Benchmarking: {name}")
        print(f"{'='*60}")
        
        metrics = {
            'insert_times': [],
            'search_times': [],
            'list_times': [],
            'memory_estimate': 0
        }
        
        # Insertion benchmark
        print(f"[1/3] Testing insertions...")
        chunk_size = len(self.metadata_list) // self.num_threads
        threads = []
        
        start_time = time.perf_counter()
        for i in range(self.num_threads):
            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size if i < self.num_threads - 1 else len(self.metadata_list)
            chunk = self.metadata_list[start_idx:end_idx]
            
            t = threading.Thread(
                target=self._insert_worker,
                args=(structure, chunk, metrics)
            )
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        insert_duration = time.perf_counter() - start_time
        
        # Search benchmark
        print(f"[2/3] Testing searches...")
        search_filenames = random.sample([m.filename for m in self.metadata_list], 
                                        k=min(1000, len(self.metadata_list)))
        
        start_time = time.perf_counter()
        for filename in search_filenames:
            search_start = time.perf_counter()
            result = structure.search_by_filename(filename)
            search_end = time.perf_counter()
            metrics['search_times'].append(search_end - search_start)
        
        search_duration = time.perf_counter() - start_time
        
        # List benchmark
        print(f"[3/3] Testing list operation...")
        list_start = time.perf_counter()
        all_files = structure.list_files()
        list_end = time.perf_counter()
        metrics['list_times'].append(list_end - list_start)
        
        # Memory estimate (rough)
        metrics['memory_estimate'] = sys.getsizeof(structure)
        
        # Compile results
        result = {
            'name': name,
            'total_insert_time': insert_duration,
            'avg_insert_time': sum(metrics['insert_times']) / len(metrics['insert_times']) if metrics['insert_times'] else 0,
            'total_search_time': search_duration,
            'avg_search_time': sum(metrics['search_times']) / len(metrics['search_times']),
            'list_time': metrics['list_times'][0],
            'memory_estimate': metrics['memory_estimate'],
            'size': structure.get_size()
        }
        
        # Add structure-specific stats
        if hasattr(structure, 'get_stats'):
            result['stats'] = structure.get_stats()
        
        self.results[name] = result
        
        print(f"\n✓ {name} Benchmark Complete")
        print(f"  Total Insert Time: {result['total_insert_time']:.4f}s")
        print(f"  Avg Insert Time:   {result['avg_insert_time']*1000:.6f}ms")
        print(f"  Avg Search Time:   {result['avg_search_time']*1000:.6f}ms")
        print(f"  List Time:         {result['list_time']:.4f}s")
        
        return result
    
    def _insert_worker(self, structure, metadata_list, metrics):
        """Worker for inserting metadata"""
        for metadata in metadata_list:
            start = time.perf_counter()
            structure.insert(metadata)
            end = time.perf_counter()
            metrics['insert_times'].append(end - start)
    
    def print_comparison(self):
        """Print comprehensive comparison"""
        print(f"\n{'='*80}")
        print(f"PERFORMANCE COMPARISON SUMMARY")
        print(f"{'='*80}")
        print(f"Dataset: {self.num_entries} entries, {self.num_threads} threads\n")
        
        # Create comparison table
        print(f"{'Metric':<30} {'Hash Table':<20} {'B+ Tree':<20}")
        print(f"{'-'*70}")
        
        ht = self.results.get('Hash Table', {})
        bt = self.results.get('B+ Tree', {})
        
        print(f"{'Total Insert Time (s)':<30} {ht.get('total_insert_time', 0):<20.4f} {bt.get('total_insert_time', 0):<20.4f}")
        print(f"{'Avg Insert Time (ms)':<30} {ht.get('avg_insert_time', 0)*1000:<20.6f} {bt.get('avg_insert_time', 0)*1000:<20.6f}")
        print(f"{'Avg Search Time (ms)':<30} {ht.get('avg_search_time', 0)*1000:<20.6f} {bt.get('avg_search_time', 0)*1000:<20.6f}")
        print(f"{'List Time (s)':<30} {ht.get('list_time', 0):<20.4f} {bt.get('list_time', 0):<20.4f}")
        print(f"{'Final Size':<30} {ht.get('size', 0):<20} {bt.get('size', 0):<20}")
        
        print(f"\n{'='*80}")
        print("STRUCTURE-SPECIFIC STATISTICS")
        print(f"{'='*80}")
        
        if 'Hash Table' in self.results and 'stats' in self.results['Hash Table']:
            print("\nHash Table:")
            stats = self.results['Hash Table']['stats']
            print(f"  Capacity:            {stats['capacity']}")
            print(f"  Load Factor:         {stats['load_factor']:.4f}")
            print(f"  Max Chain Length:    {stats['max_chain_length']}")
            print(f"  Avg Chain Length:    {stats['avg_chain_length']:.2f}")
        
        if 'B+ Tree' in self.results and 'stats' in self.results['B+ Tree']:
            print("\nB+ Tree:")
            stats = self.results['B+ Tree']['stats']
            print(f"  Height:              {stats['height']}")
            print(f"  Order:               {stats['order']}")
        
        print(f"\n{'='*80}")
        print("ANALYSIS")
        print(f"{'='*80}")
        
        # Winner analysis
        ht_search = ht.get('avg_search_time', float('inf'))
        bt_search = bt.get('avg_search_time', float('inf'))
        
        print("\n🏆 Winners:")
        print(f"  Fastest Search:  {'Hash Table' if ht_search < bt_search else 'B+ Tree'}")
        print(f"  Fastest Insert:  {'Hash Table' if ht.get('total_insert_time', float('inf')) < bt.get('total_insert_time', float('inf')) else 'B+ Tree'}")
        print(f"  Fastest List:    {'Hash Table' if ht.get('list_time', float('inf')) < bt.get('list_time', float('inf')) else 'B+ Tree'}")


# MAIN COMPARISON

def run_comparison(num_entries=5000, num_threads=5):
    """Run complete comparison"""
    print(f"\n{'='*80}")
    print(f"METADATA INDEXING STRUCTURE COMPARISON")
    print(f"{'='*80}")
    print(f"Entries: {num_entries} | Threads: {num_threads}")
    
    comparator = PerformanceComparator(num_entries, num_threads)
    
    # Benchmark Hash Table
    hash_table = HashTableIndex(initial_capacity=1024)
    comparator.benchmark_structure(hash_table, "Hash Table")
    
    # Benchmark B+ Tree
    bplus_tree = BPlusTree(order=50)
    comparator.benchmark_structure(bplus_tree, "B+ Tree")
    
    # Print comparison
    comparator.print_comparison()


if __name__ == "__main__":
    run_comparison(num_entries=5000, num_threads=5)