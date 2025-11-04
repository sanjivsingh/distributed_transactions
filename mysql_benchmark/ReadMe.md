## 4-byte ID Vs 16-byte ID and Index Size:

Source Code : [id_4byte_vs_16_byte.py.](id_4byte_vs_16_byte.py)

#### Scenario

-   Number of records : 10 million records

|Table Definition |time to insert 10 million records | 
|-----------------------------------------------------------------------------------|-------------------|
| CREATE TABLE TableId4Byte ( ID INT AUTO_INCREMENT PRIMARY KEY, Age INT NOT NULL)  | 120.62 seconds|
| CREATE TABLE IF NOT EXISTS TableId16Byte ( ID VARCHAR(16) PRIMARY KEY, Age INT NOT NULL) | 1324.83 seconds |

#### Results : Size of index in. Mysql
|Index   |Index size in. bytes | 
|-----------------------------------------------------------------------------------|-------------------|
| CREATE INDEX idx_age ON TableId4Byte (Age)  | 148652032 bytes|
| CREATE INDEX idx_age ON TableId16Byte (Age) | 387973120 bytes |

### Observation and conclusion :
- **Observation**: Using a VARCHAR(16) for the primary key significantly increases insertion time (over 10x slower) and index size (over 2.5x larger) compared to an INT AUTO_INCREMENT key. This is due to the overhead of string comparisons, storage, and indexing for variable-length strings.
- **Conclusion**: `For performance-critical applications with large datasets, prefer fixed-size integer primary keys (e.g., INT) over string-based keys to reduce storage costs and improve insert/query speeds. Only use string keys if they provide functional benefits like readability or compatibility.`

## Flickr Ticketing Service and Id generation

Source Code : [Flickr_Ticketing_Service_id_generation.py](mysql_benchmark/Flickr_Ticketing_Service_id_generation.py)

### Observation and conclusion :
- **Observation**: Sharding distributes data across multiple MySQL instances, but generating unique IDs across shards requires careful coordination to avoid collisions. The implementation uses shard-specific auto-increments with offsets or a global ID generator.
- **Conclusion**: `For distributed systems, implement a centralized or coordinated ID generation strategy (e.g., Snowflake or database sequences) to ensure uniqueness and scalability without sacrificing performance.`

## MySQL Pagination Benchmark 

Source Code : [benchmark_pagination.py](benchmark_pagination.py)

### Results:
-   Data Size : 1 GB
-   Number of records : 10M
-   Compare Page/Offest Vs KeySet based pagination 
    ![line chart](pagination_benchmarck_compare.png)

### Observation and conclusion :
- **Observation**: For large datasets (10M records, 1GB), traditional LIMIT/OFFSET pagination becomes inefficient as page numbers increase due to deep scanning. KeySet pagination (using WHERE clauses on indexed columns) maintains consistent performance by leveraging indexes and avoiding offset calculations.
- **Conclusion**: `Adopt KeySet pagination for large tables to ensure scalable and fast pagination, especially in web applications with high user loads. LIMIT/OFFSET is suitable only for small datasets or low-traffic scenarios OR user are looking for only immediate data(1-2 pages).`