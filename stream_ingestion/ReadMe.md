# Stream Ingestion Framework

This project implements a scalable stream ingestion framework for processing login events from multiple clients. It uses Kafka for lightweight queuing, batch processing for efficiency, disk usage monitoring to prevent overload, and stores raw data in cloud storage while processed data in MongoDB. The system ensures high throughput with minimal latency and automatic failover.

## Features

- **Stream Ingestion**: Receives events from clients via HTTP and synchronously sends to Kafka.
- **Light Load on Kafka**: Uses efficient producers/consumers with batching to minimize Kafka overhead.
- **Batch Processing**: Workers process events in batches for optimized I/O and database inserts.
- **Disk Usage Monitoring**: Monitors local storage and makes the service unavailable if thresholds are exceeded.
- **Database Batch Load**: Inserts processed data into MongoDB in batches for performance.
- **Cloud Storage**: Stores raw event files permanently in cloud (e.g., S3) for archival.
- **MongoDB Processing**: Aggregates and processes events (e.g., counts by type) for analytics.
- **Configurable Thresholds**: Adjustable batch sizes, disk limits, and timeouts.
- **Graceful Shutdown**: Handles signals for clean shutdown and final flushes.
- **Error Handling**: Retries and dead letter queues for failed processing.
- **Dynamic Config Refresh**: Reloads config without restart.

## Architecture

- **Event Processing Service**: Ingests events from clients and produces to Kafka.
- **Kafka**: Acts as a lightweight queue for decoupling ingestion from processing.
- **Workers**: Consume from Kafka in batches, save to local files, upload to cloud, and aggregate to MongoDB.
- **Local Storage**: Temporary batch files with disk monitoring.
- **Cloud Storage**: Permanent raw data storage.
- **MongoDB**: Processed/aggregated data for queries.
- **Optimizations**:
  - **Batching**: Reduces I/O and DB load.
  - **Disk Monitoring**: Prevents storage exhaustion by alerting and stopping ingestion.
  - **Asynchronous Processing**: Threads for file I/O and cloud uploads.

### Block Diagram

![login_evert_stream_processing](login_evert_stream_processing.png)

## Prerequisites

- Python 3.8+
- Kafka cluster
- MongoDB
- Cloud storage (e.g., AWS S3)
- Libraries: confluent-kafka, pymongo, pandas, etc.

## Installation

1. Clone or download the project.
2. Install dependencies: `pip install -r requirements.txt`.
3. Set up Kafka, MongoDB, and cloud storage.
4. Configure `config.py` with brokers, DB details, etc.

## Usage

# Start 
```
.venv/bin/python -m stream_ingestion.stream_ingestion_batch
```

```
.venv/bin/python -m stream_ingestion.move_file_to_cloud_worker
```

```
.venv/bin/python -m stream_ingestion.database_save_worker
```


## Performance Considerations

- **Light Load on Kafka**: Batching reduces message frequency; use compression for large payloads.
- **Batch Processing**: Configurable batch sizes (e.g., 1000 events) for DB efficiency.
- **Disk Usage**: Monitors usage; if >70% threshold, raises exception and stops ingestion.
- **Database Batch Load**: Uses `update_many` or bulk inserts for MongoDB.
- **Cloud Store**: Raw files uploaded asynchronously; permanent archival.
- **MongoDB**: Stores processed data (e.g., event counts) for fast queries.

## Troubleshooting

- **High Disk Usage**: Check `disk_space_usage` logs; clean up files or increase threshold.
- **Kafka Errors**: Ensure brokers are running; check consumer group.
- **MongoDB Failures**: Verify connection; use retries for inserts.
- **Service Unavailable**: Triggered by disk monitor; restart after cleanup.

## Example

Run server and workers, then send events. Workers will batch, monitor disk, save to cloud/MongoDB.

## Technologies Used

- Python (Kafka, MongoDB, Pandas)

## Contributing

- Add more event types.
- Implement dead letter queues.
- Enhance monitoring with metrics.

For more details, refer to the code in `stream_ingestion.py` and `database_save_worker.py`. If you need enhancements, let me know!