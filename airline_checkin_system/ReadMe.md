# Airline Checkin System

This application simulates an airline check-in system using MySQL for data storage. It manages flight bookings and check-ins, allowing bulk and single record operations with connection pooling for efficiency. The system handles seat assignments during check-in and ensures thread-safe operations.

## Features

- **Flight Booking Management**: Store and manage flight bookings with traveler details.
- **Check-in Process**: Assign seats to travelers based on available seats for a flight.
- **Bulk Operations**: Insert multiple records at once for bookings and check-ins.
- **Single Operations**: Insert or update individual records.
- **Connection Pooling**: Uses DBUtils for efficient MySQL connections.
- **Thread-Safe**: Handles concurrent check-ins safely.
- **Data Persistence**: Stores data in MySQL tables with proper schemas.

## Architecture

- **Backend**: Python with pymysql and DBUtils for database interactions.
- **Database**: MySQL with tables for `flights_checkin` and `flight_booking`.
- **Connection Management**: Pooled connections to handle multiple operations efficiently.
- **Logic**: Check-in assigns the first available seat for a flight and traveler.

## Prerequisites

- Python 3.8+
- MySQL server running on localhost:3306
- MySQL user: root, password: rootadmin (update in code if different)
- Virtual environment (recommended)

## Installation

1. **Navigate to the project directory**:
   ```bash
   cd ......./distributed_transactions/airline_checkin_system
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv .venv
   ```

3. **Activate the virtual environment**:
   ```bash
   source .venv/bin/activate
   ```

4. **Install the required packages**:
   ```bash
   pip install pymysql DBUtils
   ```

## Running the Application

- Run the application
   ```bash
   .venv/bin/python -m airline_checkin_system.checkin
   ```
    This executes the sample code: creates tables, inserts bulk data, and performs check-ins.
-   Expected Output:
        Logs for table creation, bulk inserts, and check-in results.
        Assertions to verify data integrity.

## Usage

# Database Tables

-   **flights_checkin**: Stores check-in details.
    Columns: flight_id (VARCHAR), flight_date (DATE), seat_number (VARCHAR), traveler_id (VARCHAR)
Primary Key: (flight_id, flight_date, seat_number)
-   **flight_booking**: Stores booking details.
    Columns: booking_id (VARCHAR PRIMARY KEY), traveler_id (VARCHAR), flight_date (DATE), flight_id (VARCHAR)


# Key Methods

-   init_db(): Creates tables and clears data.
-   bulk_insert_flights_checkin(records): Inserts multiple check-in records.
-   bulk_insert_flight_booking(records): Inserts multiple booking records.
-   flight_checkin(flight_id, flight_date, traveler_id): Assigns a seat to a traveler for a flight.


# Example Code

```
from airline_checkin_system.checkin import AirlineDatabase

db = AirlineDatabase()

# Bulk inserts
checkin_records = [('FL001', '2023-10-01', 'SEAT_0', None), ...]
booking_records = [('BK0', 'T0', '2023-10-01', 'FL001'), ...]
db.bulk_insert_flights_checkin(checkin_records)
db.bulk_insert_flight_booking(booking_records)

# Check-in
db.flight_checkin('FL001', '2023-10-01', 'T0')
```

## Troubleshooting
-   **Connection Errors**: Ensure MySQL is running and credentials are correct.
-   **Import Errors**: Activate the virtual environment and install dependencies.
-   **Table Creation Fails**: Check MySQL permissions for creating databases/tables.
-   **No Seats Available**: Ensure check-in records have available seats (traveler_id IS NULL).
-   Logs: Check console for error messages.

## Development
-   Code Structure:
    -   **checkin.py**: Main class with database methods.
-   Extending: Add more tables, validation, or integrate with web APIs.