# real time_event_streaming_distinct_users_range_search
 - RealTime Event Streaming Distinct Users Range Search Application
    - This application processes real-time event streams to identify distinct users within a specified range.
    - The application supports range queries to filter events based on start and end timestamps.
    - It provides an API for querying distinct users in real-time, enabling quick insights into user activity.  
    - Technologies used include Apache Kafka for event streaming, Apache Spark for distributed processing, and Flask for the API layer.
    - The application is designed to scale horizontally, ensuring high availability and fault tolerance.
    - Application uses redis probabilistic data structures HyperLogLog to efficiently count distinct users in the specified range.




```
cd {HOME_DIR}/distributed_transactions/
export PYTHONPATH=../distributed_transactions:$PYTHONPATH

```

## To run the FastAPI application:
```
.venv/bin/python -m uvicorn realtime_event_streaming_distinct_users_range_search.app:app --reload --port 8000

or 

.venv/bin/python -m realtime_event_streaming_distinct_users_range_search.app

```

## To run the ingestion worker:

```
.venv/bin/python -m realtime_event_streaming_distinct_users_range_search.ingestion_worker
```

## To run load database through CDC sync:
```
.venv/bin/python -m realtime_event_streaming_distinct_users_range_search.sync_database
```

# To run the scheduler for range search:
```
.venv/bin/python -m realtime_event_streaming_distinct_users_range_search.scheduler
```
