# DynamoDB on MySQL

This project implements a simplified DynamoDB-like interface and features on top of MySQL, including sharding, tables with hash and sort keys, local secondary indexes, and basic CRUD operations. It's designed to demonstrate distributed database concepts using a relational backend.

## Features

- **Table Creation**: Create tables with hash keys, sort keys, and additional columns.
- **Local Secondary Indexes**: Add indexes on non-key columns for efficient querying.
- **Sharding**: Support for multiple shards to distribute data across MySQL instances.
- **CRUD Operations**: Insert and query records with support for primary key lookups, index scans, and full table scans.
- **Thread-Safe**: Uses locks for concurrent access to shared resources.
- **Logging**: Integrated logging via commons module.

## Architecture

- **Sharding**: Each `DynamoDBShard` instance represents a shard, handling a subset of data. Shards connect to separate MySQL databases or instances.
- **Tables**: Main tables store data with composite primary keys (hash_key, sort_key). Index tables store mappings for secondary indexes.
- **Indexes**: Local indexes are shard-specific; global indexes could be added for cross-shard queries.
- **Operations**: Queries prioritize primary key lookups, then index scans, falling back to full scans.

### Architecture Diagram

![Architecture Diagram](architecture_diagram.png)

*(Render the PlantUML below at [plantuml.com](https://plantuml.com/) and save as `architecture_diagram.png`)*

```plantuml
@startuml Architecture Diagram

title DynamoDB on MySQL Architecture

[Client] --> [DynamoDBShard] : CRUD Operations
[DynamoDBShard] --> [MySQL Database] : Data Storage
[DynamoDBShard] --> [Index Tables] : Secondary Indexes
[DynamoDBShard] --> [Main Tables] : Primary Data

note right of DynamoDBShard : Handles sharding, locking, and queries
note right of MySQL Database : Relational backend for persistence
note right of Index Tables : For efficient non-PK queries
note right of Main Tables : Core data with hash/sort keys

@enduml
```

## Prerequisites

- Python 3.8+
- MySQL server running
- commons module (for logging)
- mysql_setup module (for configuration)

## Installation

1. Ensure MySQL is running and configured via mysql_setup.
2. Install dependencies (e.g., pymysql).
3. Place the project in your workspace.

## Usage

### Initialize a Shard

```python
from shard import DynamoDBShard
from mysql_setup import config as mysql_config, constants as mysql_constants

shard = DynamoDBShard(
    shard_index=1,
    host=mysql_config.configurations[mysql_constants.HOST],
    user=mysql_config.configurations[mysql_constants.USER],
    password=mysql_config.configurations[mysql_constants.PASSWORD],
    port=mysql_config.configurations[mysql_constants.PORT],
    database="dynamodb_shard_1"
)
```

### Create a Table

```python
shard.create_table({
    "table": "order_table",
    "hash_key": "order_id",
    "sort_key": "create_date",
    "columns": ["name", "price"]
})
```

### Create a Local Secondary Index

```python
shard.create_local_secondary_index({
    "table": "order_table",
    "index_column": "price"
})
```

### Insert a Record

```python
shard.insert_record("order_table", {
    "order_id": "1",
    "create_date": "2023-01-01",
    "name": "item1",
    "price": "20"
})
```

### Query Records

```python
# Get all records
records = shard.get_records("order_table", filter=None)

# Query by primary key
records = shard.get_records("order_table", filter={"order_id": "1", "create_date": "2023-01-01"})

# Query by index
records = shard.get_records("order_table", filter={"price": "20"})
```

### Disconnect

```python
shard.disconnect()
```

## Example

See the `if __name__ == "__main__"` block in `shard.py` for a complete example of creating a table, indexes, inserting data, and querying.

## Technologies Used

- Python
- PyMySQL
- MySQL
- Commons (logging)

## Contributing

- Add more index types (e.g., global secondary indexes).
- Implement cross-shard queries.
- Add error handling and retries.