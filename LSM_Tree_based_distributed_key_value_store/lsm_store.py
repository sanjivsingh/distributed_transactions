import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from typing import Any, Optional
from commons.uniqueid import SnowflakeIDGenerator
import random

class SkipListNode:
    def __init__(self, key: str, value: str, seq_number: int, level: int):
        self.key = key
        self.value = value
        self.seq_number = seq_number
        self.deleted = False
        self.forward = [None] * (level + 1)

class SkipList:
    def __init__(self, max_level: int = 16):
        self.max_level = max_level
        self.level = 0
        # Header node with minimum possible key
        self.header = SkipListNode("", "", 0, max_level)
        
    def _random_level(self) -> int:
        level = 0
        while random.random() < 0.5 and level < self.max_level:
            level += 1
        return level
    
    def search(self, key: str) -> Optional[SkipListNode]:
        current = self.header
        
        # Start from highest level and go down
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
        
        # Move to next node at level 0
        current = current.forward[0]
        
        if current and current.key == key:
            return current
        return None
    
    def insert(self, key: str, value: str, seq_number: int):
        update = [None] * (self.max_level + 1)
        current = self.header
        
        # Find position to insert
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current
        
        current = current.forward[0]
        
        # If key exists, update with newer sequence number
        if current and current.key == key:
            if seq_number > current.seq_number:
                current.value = value
                current.seq_number = seq_number
                current.deleted = False
            return
        
        # Create new node
        new_level = self._random_level()
        
        if new_level > self.level:
            for i in range(self.level + 1, new_level + 1):
                update[i] = self.header
            self.level = new_level
        
        new_node = SkipListNode(key, value, seq_number, new_level)
        
        # Update forward pointers
        for i in range(new_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node
    
    def delete_key(self, key: str, seq_number: int):
        """Mark key as deleted with tombstone"""
        self.insert(key, "<DELETED>", seq_number)  # Insert with special value
        node = self.search(key)
        if node:
            node.deleted = True
    
    def get_all_entries(self):
        """Get all entries in sorted order"""
        entries = []
        current = self.header.forward[0]
        
        while current:
            entries.append({
                'key': current.key,
                'value': current.value,
                'seq_number': current.seq_number,
                'deleted': current.deleted
            })
            current = current.forward[0]
        
        return entries
    
    def size(self) -> int:
        count = 0
        current = self.header.forward[0]
        
        while current:
            if not current.deleted:
                count += 1
            current = current.forward[0]
        
        return count

class Memtable:
    def __init__(self, max_size: int = 1000):
        self.skip_list = SkipList()
        self.id_generator = SnowflakeIDGenerator(worker_id=1)
        self.max_size = max_size
        self._size = 0

    def put(self, key: str, value: str) -> None:
        seq_number = self.id_generator.generate_id()
        
        # Check if key exists to determine if we're updating or inserting
        existing_node = self.skip_list.search(key)
        is_new_key = existing_node is None or existing_node.deleted
        
        self.skip_list.insert(key, value, seq_number)
        
        # Update size only for new keys
        if is_new_key:
            self._size += 1
    
    def has_key(self, key: str) -> bool:
        """Check if key exists in memtable, regardless of deleted status"""
        node = self.skip_list.search(key)
        return node is not None
    
    def has_value(self, key: str) -> bool:
        node = self.skip_list.search(key)
        return node is not None and not node.deleted

    def get(self, key: str) -> Optional[str]:
        node = self.skip_list.search(key)
        if node and not node.deleted:
            return node.value
        return None

    def size(self) -> int:
        return self._size
    
    def is_full(self) -> bool:
        return self._size >= self.max_size

    def delete(self, key: str) -> None:
        seq_number = self.id_generator.generate_id()
        existing_node = self.skip_list.search(key)

        if not existing_node:
            # If key doesn't exist, still insert tombstone to mark deletion
            self.skip_list.delete_key(key, seq_number)
            # Don't change size since it wasn't there
        elif existing_node and not existing_node.deleted:
            self.skip_list.delete_key(key, seq_number)
            self._size -= 1

    def flush(self) -> 'SSTable':
        """Convert memtable to SSTable and clear memtable"""
        entries = self.skip_list.get_all_entries()
        sstable = SSTable()
        sstable.entries = entries
        
        # Clear memtable
        self.skip_list = SkipList()
        self._size = 0
        
        return sstable

class BloomFilter:
    def __init__(self, size: int, hash_count: int):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = [False] * size

    def _hash(self, key: str, seed: int) -> int:
        """Simple hash function with seed"""
        hash_value = hash(key + str(seed))
        return abs(hash_value) % self.size

    def add(self, key: str) -> None:
        for i in range(self.hash_count):
            index = self._hash(key, i)
            self.bit_array[index] = True

    def might_contain(self, key: str) -> bool:
        for i in range(self.hash_count):
            index = self._hash(key, i)
            if not self.bit_array[index]:
                return False
        return True

class SSTable:
    def __init__(self):
        self.entries = []
        self.bloom_filter = None

    def has_key(self, key: str) -> bool:
        if self.bloom_filter and not self.bloom_filter.might_contain(key):
            return False
        
        # Binary search since entries are sorted
        left, right = 0, len(self.entries) - 1
        
        while left <= right:
            mid = (left + right) // 2
            if self.entries[mid]['key'] == key:
                return not self.entries[mid]['deleted']
            elif self.entries[mid]['key'] < key:
                left = mid + 1
            else:
                right = mid - 1
        
        return False

    def get(self, key: str) -> Optional[str]:
        if self.bloom_filter and not self.bloom_filter.might_contain(key):
            return None
        
        # Binary search
        left, right = 0, len(self.entries) - 1
        
        while left <= right:
            mid = (left + right) // 2
            if self.entries[mid]['key'] == key:
                if not self.entries[mid]['deleted']:
                    return self.entries[mid]['value']
                return None
            elif self.entries[mid]['key'] < key:
                left = mid + 1
            else:
                right = mid - 1
        
        return None

    def range_search(self, start_key: str, end_key: str) -> dict[str, str]:
        result = {}
        
        for entry in self.entries:
            if start_key <= entry['key'] <= end_key and not entry['deleted']:
                result[entry['key']] = entry['value']
        
        return result

class Compactor:
    def __init__(self, sstables: list[SSTable]):
        self.sstables = sstables

    def compact(self) -> list[SSTable]:
        """Merge multiple SSTables into fewer SSTables"""
        if len(self.sstables) <= 1:
            return self.sstables
        
        # Merge all entries from all SSTables
        all_entries = []
        for sstable in self.sstables:
            all_entries.extend(sstable.entries)
        
        # Sort by key, then by sequence number (descending for latest value)
        all_entries.sort(key=lambda x: (x['key'], -x['seq_number']))
        
        # Keep only the latest version of each key
        merged_entries = []
        prev_key = None
        
        for entry in all_entries:
            if entry['key'] != prev_key:
                merged_entries.append(entry)
                prev_key = entry['key']
        
        # Create new SSTable with merged entries
        new_sstable = SSTable()
        new_sstable.entries = merged_entries
        
        return [new_sstable]

class LSMStore:
    def __init__(self, memtable_max_size: int = 1000):
        self.memtable_max_size = memtable_max_size
        self.memtable = Memtable(memtable_max_size)
        self.sstables = []

    def put(self, key: str, value: str) -> None:
        # Check if memtable is full
        if self.memtable.is_full():
            self._flush_memtable()
        
        self.memtable.put(key, value)

    def get(self, key: str) -> Optional[str]:
        # First check if key is in memtable (even if deleted)
        if self.memtable.has_key(key):
            print(f"getting {key} from memtable")
            return self.memtable.get(key)  # Returns None if deleted
        
        # Check SSTables from newest to oldest
        for sstable in reversed(self.sstables):
            print(f"getting {key} from sstable : {sstable}")
            value = sstable.get(key)
            if value is not None:
                return value
        
        return None

    def range_search(self, start_key: str, end_key: str) -> dict[str, str]:
        result = {}
        
        # Get from all SSTables
        for sstable in self.sstables:
            sstable_result = sstable.range_search(start_key, end_key)
            result.update(sstable_result)
        
        # Get from memtable (overwrites older values)
        current = self.memtable.skip_list.header.forward[0]
        while current:
            if start_key <= current.key <= end_key and not current.deleted:
                result[current.key] = current.value
            current = current.forward[0]
        
        return result

    def delete(self, key: str) -> None:
        if self.memtable.is_full():
            self._flush_memtable()
        
        self.memtable.delete(key)
    
    def _flush_memtable(self):
        """Flush memtable to SSTable"""
        print("Flushing memtable to SSTable...")
        if self.memtable.size() > 0:
            sstable = self.memtable.flush()
            self.sstables.append(sstable)
            
            # Trigger compaction if too many SSTables
            if len(self.sstables) > 4:
                self._compact()
    
    def _compact(self):
        """Compact SSTables"""
        if len(self.sstables) > 1:
            compactor = Compactor(self.sstables)
            self.sstables = compactor.compact()

if __name__ == "__main__":
    lsm_store = LSMStore(memtable_max_size=5)

    # Insert some key-value pairs
    for i in range(20):
        lsm_store.put(f"key{i}", f"value{i}")
        print(f"Inserted: key{i} -> value{i}")

    # Retrieve some values
    for i in range(20):
        value = lsm_store.get(f"key{i}")
        print(f"Retrieved: key{i} -> {value}")

    # Range search
    range_result = lsm_store.range_search("key3", "key7")
    print("Range Search (key3 to key7):", range_result)

    # Delete a key
    lsm_store.delete("key5")
    print("Deleted: key5")

    # Try to retrieve deleted key
    value = lsm_store.get("key5")
    print(f"Retrieved after deletion: key5 -> {value}")