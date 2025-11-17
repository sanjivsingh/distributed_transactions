# Redis Setup and Utilities

This project provides setup instructions and utility scripts for working with Redis in distributed systems. It includes examples for counters, duplicate checking, blocking queues, leaderboards, and more, demonstrating Redis data structures and operations.

## Prerequisites

- Python 3.8+
- Redis server

### Installing Redis

On macOS (using Homebrew):

```bash
brew install redis
```

To start Redis now and restart at login:

```bash
brew services start redis
```

Or, if you don't want a background service:

```bash
/usr/local/opt/redis/bin/redis-server redis.conf
```

On other systems, follow the [official Redis installation guide](https://redis.io/download).

### Python Dependencies

Install required packages:

```bash
pip install redis
```

## Project Structure

- `config.py` & `constants.py`: Configuration for Redis connection.
- `redis_counter.py`: Concurrent counter increment using Redis.
- `redis_duplicate.py`: Duplicate checking with Redis sets.
- `redis_blocking_queue.py`: Blocking queue with producers/consumers and failure handling.
- `redis_sortedSet_leaderboard.py`: Leaderboard using Redis sorted sets with background updates.

## Features and Examples

### 1. Redis Counter (`redis_counter.py`)
Demonstrates concurrent increments of a Redis counter using threading.

- **Usage**:
  ```bash
  python redis_counter.py
  ```
- **Features**: Increments "order_count" 100,000 times concurrently, checks final value.

### 2. Redis Duplicate Checker (`redis_duplicate.py`)
Uses Redis sets for checking duplicates with TTL.

- **Usage**:
  ```python
  from redis_duplicate import RedisDuplicate
  dup = RedisDuplicate(host, port, set_name)
  dup.set_key("item")
  is_dup = dup.is_present("item")
  ```
- **Features**: Thread-safe duplicate detection.

### 3. Redis Blocking Queue (`redis_blocking_queue.py`)
Implements a producer-consumer queue with blocking dequeue and failure recovery.

- **Usage**:
  ```bash
  python redis_blocking_queue.py
  ```
- **Features**: Producers add items, consumers process with retries and DLQ.

### 4. Redis Leaderboard (`redis_sortedSet_leaderboard.py`)
Maintains a leaderboard using sorted sets, with background score updates.

- **Usage**:
  ```bash
  python redis_sortedSet_leaderboard.py
  ```
- **Features**: Real-time leaderboard printing while scores update in background.

## Configuration

Update `config.py` with your Redis host/port:

```python
configurations = {
    REDIS_SERVER: "localhost",
    REDIS_PORT: 6379
}
```

## Running the Examples

1. Start Redis server.
2. Run any script, e.g., `python redis_counter.py`.
3. Monitor Redis with `redis-cli` (e.g., `KEYS *`, `GET order_count`).

## Performance Considerations

- Redis is fast for in-memory operations.
- Use pipelining for bulk operations.
- Monitor memory usage for large datasets.

## Troubleshooting

- **Connection Errors**: Ensure Redis is running on the specified host/port.
- **OOM in Scripts**: Reduce concurrency or increase Redis memory.
- **Data Persistence**: Redis is in-memory; use snapshots or AOF for persistence.

## Contributing

- Add more Redis data structure examples (e.g., pub/sub, streams).
- Improve error handling and logging.

For more details, refer to the code in each file. If you need full API docs or extensions, let me know!
