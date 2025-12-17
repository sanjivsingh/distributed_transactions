# Distributed Transactions Project

This repository contains a collection of distributed systems projects and utilities demonstrating real-time messaging, hashtag analytics, offline message handling, and shared configurations using modern technologies like FastAPI, Kafka, Redis, MySQL, and MongoDB.

## Overview

The projects focus on building scalable, distributed applications with features like asynchronous processing, pubsub messaging, data persistence, and thread safety. Each sub-project is self-contained with its own README for detailed setup and usage. Utilities provide shared configurations and helpers.

## Sub-Projects and Utilities

### Common Module
- **Commons** : Shared utilities for logging, unique ID generation (Snowflake), and other common functions used across projects.
    - **Key Features**: Logger setup, Snowflake ID generator
    - **Link to Implementation**: [./commons/](./commons/)

- **Setup**
    -   **Kafka Setup** Configuration module for Kafka connections, including producer and consumer settings. [./kafka_setup/](./kafka_setup/)
    -    **MongoDB Setup** Configuration module for MongoDB connections, including server and port settings.  [./mongodb_setup/](./mongodb_setup/)
    -    **MySQL Setup** Configuration module for MySQL connections, including host, user, password, and port settings.  [./mysql_setup/](./mysql_setup/)
    -    **Redis Setup** : Configuration module for Redis connections, including server and port settings.  [./redis_setup/](./redis_setup/)

### Know your System Limits
A detailed exploration of TCP socket limit constraints in distributed systems, covering theoretical maximum connections, ephemeral port exhaustion, file descriptor limits, system memory constraints, and kernel configuration for TCP TIME_WAIT state.
- **Link to Implementation**: [./know_your_system_limits/](./know_your_system_limits/)
- **README**: [./know_your_system_limits/ReadMe.md](./know_your_system_limits/ReadMe.md)

![TCP_Socket_Limit_Constraints](know_your_system_limits/images/TCP_Socket_Limit_Constraints.png)

![Operating_System_Limits_on_Concurrency](know_your_system_limits/images/Operating_System_Limits_on_Concurrency.png)

![Operating_System_Limits_on_Concurrency_details](know_your_system_limits/images/Operating_System_Limits_on_Concurrency_details.png)


### Instagram Hashtag Service
A service for managing Instagram-like posts with real-time hashtag analytics. Users can register, create posts with captions containing hashtags, and view hashtag summaries (number of posts, top posts). Uses Kafka for asynchronous processing, MongoDB for hashtag stats, and MySQL for relational data.

- **Technologies**: FastAPI, MySQL, MongoDB, Kafka, Jinja2
- **Key Features**: Post creation, hashtag extraction, batch processing with locking, web interface
- **Link to Implementation**: [./instagram_hashtags/](./instagram_hashtags/)
- **README**: [./instagram_hashtags/ReadMe.md](./instagram_hashtags/ReadMe.md)

![Architecture Diagram](instagram_hashtags/HashTag_Service_Block_Diagram.png)


### Slack-like Application
A real-time messaging app similar to Slack, supporting user registration, one-to-one group creation, and messaging. Messages are delivered via WebSockets and Redis pubsub, with offline messages stored in MongoDB and delivered on reconnect.

- **Technologies**: FastAPI, MySQL, Redis, MongoDB, WebSockets
- **Key Features**: Real-time messaging, offline message handling, heartbeat mechanism, thread-safe connections
- **Link to Implementation**: [./slack_application/](./slack_application/)
- **README**: [./slack_application/ReadMe.md](./slack_application/ReadMe.md)

![Block Diagram](slack_application/Slack_Block_Diagram.png)


### Airline Check-in System
A distributed system for managing airline check-ins, bookings, and seat assignments. Supports passenger registration, flight booking, and check-in processes with distributed transactions to ensure consistency across services.

- **Technologies**: FastAPI, MySQL, Kafka, Redis (assumed based on project patterns)
- **Key Features**: Booking management, check-in processing, distributed transaction handling
- **Link to Implementation**: [./airline_checkin_system/](./airline_checkin_system/)


### Online Status Application
A REST API-based application for managing user online status. Users can update their status, and others can query online users, with data persisted in Redis for fast access.

- **Technologies**: FastAPI, Redis
- **Key Features**: Status updates, online user queries, TTL-based expiration
- **Link to Implementation**: [./online_status_application/](./online_status_application/)

### Online Status Application WebSocket
A WebSocket-based application for real-time online status updates. Users connect via WebSocket to receive live updates on online statuses, with broadcasting to connected clients.

- **Technologies**: FastAPI, Redis, WebSockets
- **Key Features**: Real-time status broadcasting, WebSocket connections, heartbeat handling
- **Link to Implementation**: [./online_status_application_websocket/](./online_status_application_websocket/)

![Architecture Diagram](online_status_application_websocket/architecture.png)

![Block Diagram](online_status_application_websocket/Block_Diagram.png)

### Toy KV Store on MySQL
A simple key-value store implemented using MySQL for persistence. Demonstrates basic CRUD operations with a relational database backend, suitable for learning distributed storage concepts.

- **Technologies**: Python, MySQL
- **Key Features**: Key-value operations, data persistence, basic querying
- **Link to Implementation**: [./toy_keyvalue_store_on_mysql/](./toy_keyvalue_store_on_mysql/)



### Two-Phase Commit Zomato Delivery
An implementation of the two-phase commit protocol for a Zomato-like food delivery system. Ensures atomicity across multiple services (e.g., order placement, payment, restaurant confirmation) using distributed transactions.

- **Technologies**: Python, MySQL, Kafka (assumed for coordination)
- **Key Features**: Two-phase commit logic, order management, transaction rollback
- **Link to Implementation**: [./two_phase_commit_zomato_delivery/](./two_phase_commit_zomato_delivery/)

### File Converter Webapp
A web application for uploading and converting files between different formats (e.g., XML, JSON , PARQUET, CSV). Supports asynchronous processing and download of converted files.

- **Technologies**: FastAPI, Python libraries for file processing (e.g., pdf2image, Pillow)
- **Key Features**: File upload, format conversion, asynchronous processing, download
- **Link to Implementation**: [./file_convertor_webapp/](./file_convertor_webapp/)

### DynamoDB on MySQL
A simulation of DynamoDB's API and features built on top of MySQL. Provides a NoSQL-like interface for tables, items, and queries using a relational database backend, including sharding and local secondary indexes.

- **Technologies**: Python, MySQL, FastAPI (assumed for API)
- **Key Features**: Table operations, item CRUD, query support, partitioning simulation, sharding
- **Link to Implementation**: [./dynamodb_on_mysql/](./dynamodb_on_mysql/)
- **README**: [./dynamodb_on_mysql/ReadMe.md](./dynamodb_on_mysql/ReadMe.md)

![Architecture Diagram](dynamodb_on_mysql/DynamoOn_MySQL_Block_diagram.png)

![Flow Diagram](dynamodb_on_mysql/DynamoDB_on_MySQL_flow_diagram.png)


### MySQL Benchmark
A benchmarking tool for MySQL sharding and ID generation. Implements sharded databases with auto-increment IDs across multiple shards, demonstrating distributed insertion and querying for performance testing.

- **Technologies**: Python, PyMySQL, MySQL
- **Key Features**: 
    - Flickr Ticketing Service Id Generation [link](mysql_benchmark/Flickr_Ticketing_Service_id_generation.py) 

    - Primary key 4 byte Vs 16 byte and Index Size [link](./mysql_benchmark/id_4byte_vs_16_byte.py) 
    - MySQL Pagination Benchmark 
    ![line chart](mysql_benchmark/pagination_benchmarck_compare.png)


Sharding, auto-increment ID generation, insertion benchmarking, max ID tracking
- **Link to Implementation**: [./mysql_benchmark/](./mysql_benchmark/)

### User Recent Search Application
A web application for user registration and product search with recent search suggestions. Supports fuzzy and full-text search on Elasticsearch, recent searches stored in Redis with set-like behavior, and event logging to Kafka for analytics.

- **Technologies**: FastAPI, MySQL, Redis, Elasticsearch, Kafka
- **Key Features**: User registration, product search with dropdown recent searches, fuzzy search, event logging with user geography/IP
- **Link to Implementation**:  [./user_recent_search_app/](./user_recent_search_app/)
- **README**: ReadMe.md

![Block Diagram](user_recent_search_app/recent_search_block_diagram.png)

### Hot Key Multi-Tenant Elasticsearch
A program for multi-tenant document indexing in Elasticsearch with hot tenant handling to prevent shard hotspots. Generates 100,000 documents for 50 tenants, distributing 80% to hot tenants across multiple shards using custom routing.

- **Technologies**: Python, Elasticsearch
- **Key Features**: Multi-tenant indexing, hot tenant routing for load distribution, shard monitoring, document count per shard per tenant
- **Link to Implementation**: [./hotkey_multi_tanent_elasticseach/ReadMe.md](./hotkey_multi_tanent_elasticseach/ReadMe.md) 
- **README**: ReadMe.md
    - Shard ownning Tenants indexes 
    ![Shard_owning_tenants](hotkey_multi_tanent_elasticseach/Shard_owning_tenants.png)
    - Tenants Index distribution on shards
    ![Tenant_distribution_on_shards](hotkey_multi_tanent_elasticseach/Tenant_distribution_on_shards.png)

###  Bitcast Project

A Bitcast implementation for efficient storage and retrieval of key-value pairs using bit manipulation and type casting techniques. Designed for high-performance distributed systems requiring compact data representation.
 - `Embedded Key-Value (KV) Databases`
 - `Log-Structured` :Hash Table for Fast Key/Value Data
 - `Append-Only` File for Durability
 - `In-Memory Index`for Low Latency Lookups
 -  `Sequential Log Writing`: Data is written sequentially to an append-only log file and there will be pointers for each key pointing to the position of its log entry in the file.
 - `Compaction and Merging`: Periodic compaction to merge log segments and remove deleted/expired entries and update the in-memory index accordingly.

# Important : 
 -  `Assumes that index fits in memory for fast lookups.`
 -  `No range queries, only exact key lookups.`

- **Technologies**: Python
- **Link to Implementation**: Python based Implementation code [./bitcask_implementation/ReadMe.md](./bitcask_implementation/ReadMe.md) 

![Bitcast_Block_Diagram](bitcask_implementation/Bitcast_Block_Diagram.png)

![Bitcast_Block_Diagram](bitcask_implementation/Bitcast_architecture_diagram.png)

![Bitcast_Block_Diagram](bitcask_implementation/Bitcast_Data_Structure.png)



###   Stream Ingestion
A project for real-time stream data ingestion and processing in distributed systems. Handles high-volume data streams, processing and routing them to appropriate storage or analytics services.

- **Technologies**: Python, Kafka, Redis
- **Key Features**: Real-time data ingestion, stream processing, event routing, scalability
- **Link to Implementation**: [./stream_ingestion/ReadMe.md](./stream_ingestion/ReadMe.md) 

![login_evert_stream_processing](stream_ingestion/login_evert_stream_processing.png)

###    Word Dictionary Without DB
A project for implementing a word dictionary without using a database, utilizing in-memory data structures or file-based storage for fast word lookups and definitions.

- **Technologies**: Python
- **Key Features**: In-memory word storage, fast lookups, file-based persistence, no database dependency
- **Link to Implementation**: [./word_dictionary_without_database/ReadMe.md](./word_dictionary_without_database/ReadMe.md) 

![Lookup timing comparison](word_dictionary_without_database/Lookup_time_compare.png)

![word_dictionary_without_database](word_dictionary_without_database/word_dictionary_block_diagram.png)


### Toy JWT Implementation
A simple implementation of JSON Web Tokens (JWT) for authentication and authorization in distributed systems. Demonstrates token generation, validation, and usage for secure user sessions.

- **Technologies**: Python, PyJWT
- **Key Features**: Token generation, validation, user authentication, secure session management
- **Link to Implementation**: [./toy_jwt_implementation/ReadMe.md](./toy_jwt_implementation/ReadMe.md) 

![Architecture Diagram](toy_jwt_implementation/JWT_Architechture_Diagram.png)

### Distributed Multitenant Task Scheduler
A tenant-aware task scheduling platform that shards task data across MySQL, uses ZooKeeper for tenant→shard and worker orchestration, materializes cron schedules, and routes prioritized work via Kafka to dynamically provisioned executors.

- **Technologies**: FastAPI, PyMySQL, ZooKeeper, Kafka (confluent-kafka), croniter
- **Key Features**: Tenant isolation via shard mapping, ZooKeeper-driven puller/executor orchestration, cron materialiser ➜ executable tasks, priority queues (high/medium/low), dynamic worker provisioning
- **Link to Implementation**: [./distributed_multitenant_task_scheduler/ReadMe.md](./distributed_multitenant_task_scheduler/ReadMe.md)

![Scheduler Block Diagram](distributed_multitenant_task_scheduler/task_scheduler_architechture.png)

### Real-time Event Streaming Distinct Users Range Search
A system for efficiently counting distinct users in real-time event streams within specified time ranges. Uses Kafka for event ingestion, HyperLogLog for space-efficient distinct counting, and hierarchical time window aggregation (minute → hour → day → month → year) for optimal query performance.

- **Technologies**: FastAPI, Kafka, Redis, MongoDB (DynamoDB), HyperLogLog, CDC via Redis Streams
- **Key Features**: 
  - **HyperLogLog**: Probabilistic distinct counting with ~1.5KB memory per aggregation (~0.81% error)
  - **Lambda Architecture**: Fast path (real-time minute HLL) + Slow path (pre-aggregated rollups)
  - **Hierarchical Aggregation**: Scheduler merges minute→hour→day→month→year for range query optimization
  - **CDC Pipeline**: Redis streams capture HLL changes, sync binary data to MongoDB for persistence
  - **Smart Range Queries**: Search service intelligently selects optimal time windows and merges HLLs
- **Link to Implementation**: [./realtime_event_streaming_distinct_users_range_search/](./realtime_event_streaming_distinct_users_range_search/)
- **README**: [./realtime_event_streaming_distinct_users_range_search/ReadMe.md](./realtime_event_streaming_distinct_users_range_search/ReadMe.md)

![Distinct_Users_Architechture](realtime_event_streaming_distinct_users_range_search/Distinct_Users_Architechture.png)


### Dropbox File Sync Application
A simplified Dropbox-like file synchronization application supporting chunk-based uploads, deduplication, user isolation, versioning, and reversion. Clients monitor local directories for changes and sync with the server, using a last-write-wins conflict resolution.

- **Technologies**: FastAPI, MongoDB, Boto3 (S3),  Watchdog, Requests
- **Key Features**: Chunk-based deduplication, user-level versioning, file reversion, real-time sync via directory monitoring, storage abstraction (local/S3), encryption for security
- **Link to Implementation**: [./dropbox_filesync_app/](./dropbox_filesync_app/)
- **README**: [./dropbox_filesync_app/ReadMe.md](./dropbox_filesync_app/ReadMe.md)

![dropbox_architecture_diagram](dropbox_filesync_app/dropbox_architecture_diagram.png)


### Uber Driver-Rider Matching Application
A microservices-based ride matching system similar to Uber, featuring real-time driver-rider matching using geospatial queries, city-based Redis sharding, WebSocket communication, and service discovery. Supports ride estimation, driver location updates, and real-time ride offers with distributed matching logic.

- **Technologies**: FastAPI, MySQL, Redis (city-based sharding), WebSockets, Zookeeper (service discovery), Lua scripts
- **Key Features**: 
  - **Geospatial Matching**: Redis GEORADIUS with Lua scripts for atomic driver matching based on location, car type, and payment preferences
  - **City-based Sharding**: Redis sharded by geographic regions (cities) for optimal locality and scalability
  - **Service Discovery**: Zookeeper-based service registry with consistent hashing for Redis shard discovery
  - **Real-time Communication**: WebSocket gateway for live driver updates and ride offers via Redis pubsub
  - **Microservices Architecture**: Location service, match service, ride estimate service, and WebSocket gateway with service integration
  - **Distributed State Management**: Race condition handling with Redis locks, ride status coordination across services
- **Link to Implementation**: [./uber_driver_riders_match_app/](./uber_driver_riders_match_app/)
- **README**: [./uber_driver_riders_match_app/ReadMe.md](./uber_driver_riders_match_app/ReadMe.md)

![uber_driver_match_architecture](uber_driver_riders_match_app/uber_driver_match_architecture.png)


## Prerequisites
- Python 3.8+
- MySQL, MongoDB, Redis, Kafka (as needed per project)
- Virtual environment

## How to Run

1. Clone the repository.
2. For each sub-project, follow its README for setup and running.
3. Ensure databases and services are running (e.g., MySQL on 3306, Redis on 6379).

## Contributing

- Each sub-project has its own structure; contribute via pull requests.
- Ensure compatibility with the shared commons module.

For more details, refer to individual project READMEs.