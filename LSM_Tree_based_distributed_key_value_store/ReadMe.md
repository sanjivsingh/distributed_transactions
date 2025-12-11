# LSM ((Log-Structured Merge Tree)) Tree based Distributed Key-Value Store
-  Implement a distributed key-value store using LSM Tree architecture.
-  Support basic operations: Put, Get, Delete and **Range** Queries.
-  Ensure data replication and fault tolerance across multiple nodes.
-  Optimize for high write throughput and low-latency reads.
-  Implement compaction and merging of SSTables to manage storage efficiently.
-  Benchmark performance under various workloads and data distributions.
-  Design a simple client-server architecture for interaction with the key-value store.

## Design Considerations

  ### Memtable for in-memory writes.
        - **Not using HashMap for memtable** :
            - `Inefficient Range Scans`. HashMaps do not maintain any order among keys, making range queries inefficient.
            - `Inefficient Flush to Disk (SSTable Creation)`. When flushing the memtable to disk as an SSTable, having the data in sorted order is crucial for efficient storage and retrieval. HashMaps would require additional sorting steps during flush operations, increasing latency and complexity.
        - **Skip List or B-Tree for memtable implementation**
            - Memory Efficiency. Skip Lists and B-Trees can be more memory-efficient for certain workloads   Because these structures maintain order, you find the starting key `($O(log n)$)` and then simply traverse sequentially along the bottom level to find all the subsequent keys in the range `($O(k)$`, where $k$ is the number of results). This is extremely fast.   

  ### SSTables(Sorted String Table) for on-disk storage.
        A single SSTable is not just one file
        - **Data File (.dat or .db)**: sorted key-value pairs with metadata and checksum.
        - **Index File (.idx)**: Sparse Index mapping keys to data file offsets.
        - **Bloom Filter File (.filter)**: Probabilistic Existence Check

    #### The Multi-Level SSTable Hierarchy (LSM-Tree Levels)
        - **L0: The Ingestion Layer**
            - New SSTables are created here directly from the flushed in-memory Memtable.
            - These SSTables may have overlapping key ranges, which can lead to increased read amplification.
        - **L1, L2, ... Ln: Compacted Levels** 
            - Higher levels like L2 or L3 are never created directly from the Memtable, compaction.
            - **Compaction is triggered when a level, $L_N$ (e.g., L1), exceeds a predefined size threshold (e.g., L1 is 10 times the size of L0, L2 is 10 times L1, etc.).**
            - `Non-Overlapping Guarantee**`: A crucial property of L1 and all subsequent higher levels (L2, L3,...) is that SSTables within the same level never have overlapping key ranges. **

  ### Write-Ahead Log (WAL) for durability.
  ### Compaction strategies to merge SSTables.
  ### Data replication across nodes for fault tolerance.
  ### Consistent hashing for data distribution.
  ### Range query support using sorted Memtable, SSTables and Bloom filters.
  ### Client-server model for communication.
  ###  WAL for durability.
  ### Compaction strategies to merge SSTables.

## Implementation:

-  Focus on core LSM Tree functionalities: 
    - Memtable, 
    - SSTables,
    - Compaction, 
    - basic operations (Put, Get, Delete, Range Queries).
-  **Sharding and replication is not considered in this implementation. Focus is on single node LSM Tree functionality.**
-  **WAL is not implemented in this version.**
-  Use Python for implementation.
-  Modular design for easy testing and future enhancements.
-  Basic client-server architecture using sockets for interaction.

## Testing and Benchmarking