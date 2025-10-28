# Online Users Status Application

This is a real-time web application that tracks and displays online users. Users send periodic heartbeats to the server, which stores their status in Redis with a TTL (Time To Live) of 10 seconds. The application provides a near real-time list of online users without storing historical data.

## Features

- **Real-time Online Status**: Users appear online if they send a heartbeat within the last 10 seconds.
- **Heartbeat Mechanism**: Clients send heartbeats every 5 seconds to stay online.
- **No History Storage**: Only current online status is maintained; no logs or history.
- **Web Interface**: Simple HTML/JS frontend for user interaction.
- **API Endpoints**: RESTful APIs for heartbeat and fetching online users.
- **Scalable Backend**: Uses FastAPI for high performance and Redis for fast, TTL-based storage.

## Architecture

- **Backend**: FastAPI (Python) handles API requests and serves the web page.
- **Database**: Redis stores user IDs as keys with TTL 10 seconds.
- **Frontend**: HTML/JS (with jQuery) for user input, heartbeat sending, and displaying online users.
- **Communication**: AJAX calls for heartbeats and fetching user lists.

## Prerequisites

- Python 3.8+
- Redis server running on localhost:6379
- Virtual environment (recommended)

## Installation

1. **Clone or navigate to the project directory**:
   ```bash
   cd /Users/sanjivsingh/Projects/VS_workspace/distributed_transactions/online_status_application
   ```

2. **Create a virtual environment** (if not already created):
   ```bash
   python -m venv .venv
   ```

3. **Activate the virtual environment**:
   ```bash
   source .venv/bin/activate
   ```

4. **Install the required packages**:
   ```bash
   pip install uvicorn fastapi redis
   ```

## Usage

### Web Interface

1. Open your browser to `http://localhost:8000` (default port).
3. Submit the form to start conversion.
4. Check status, view, edit, or download the result.

### Run the application

use uvicorn:
```bash
uvicorn file_convertor_webapp.ConverterWebApp:app --reload

.venv/bin/python -m uvicorn online_status_application.online_status:app --reload --port 8000
```


