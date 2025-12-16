# Uber Ride matching app
 - Focus only ride matching between drivers and riders.
 - Not a complete Uber clone.
    - No ride tracking or navigation features.
    - No payment gateway integration.
    - No user authentication.


## Objective:
 - Build a simple app to match Uber drivers with riders based on proximity.
 - Allow riders to request rides and drivers to accept them.
 - Ensure- 
     - Hih-availability
     - Scalability
     - Fault Tolerance
     - SLA (**ride match as early as possible**)

## Architecture Overview:

![uber_driver_match_architecture](uber_driver_match_architecture.png)

### Design Design Considerations
 -  Websocket for real-time communication between drivers and the server.
    -   `Single persistent WebSocket connection per driver`.
 -  `Redis Geo Index` for efficient proximity searches.
    -  `Driver locations` stored with `TTL` to reflect availability.
    -  `Driver metadata(car type , payment preference, rating)` stored in a separate Redis hash.
    -  Avoid complex DB queries for location-based searches. 
    -  Multiple replicas per shard for high availability.
    -  `Lua scripts` for finding nearest drivers and matching criteria efficiently and atomically.
 -  Redis sharded Geo Index for scalability.
    - `Shard Manager` maps Driver location to specific Redis shard.
    - One of the shards can go down, others continue to function. Shard manager reroutes location to healthy shards.
 - `Decoupled services using Pub/Sub for communication`.
    - Match Service publishes ride offers to drivers via an internal messaging system (`Redis Pub/Sub`).
    - WebSocket Gateway Service subscribes to these messages and pushes them to drivers.
 - `Redis for fast state management`.
    - Ride request status (available, taken) stored in Redis for low-latency access during high contention.
    - Critical section handled with Redis transactions to ensure only one driver can accept a ride.
 - `Stateless Match Service`. 
    - one instance list of nearby and matching drivers and publish ride offers.
    - other instances can leverage to update and assign driver to ride.

## Communication Flow Diagram

The key is that the driver's client establishes one single WebSocket connection to the WGS. All subsequent communication (both location data and match offers) flows through the WGS.

### Phase 1: Driver Connection and Location Update
- **WS Connection**: The Driver Client establishes a persistent WebSocket connection to the WebSocket Gateway Service (WGS).
- **Connection Mapping**: The WGS validates the driver's authentication token, extracts the DriverID, and stores this mapping in memory: Map<DriverID, WebSocket Object>.
- **Location Stream**: Every 10 seconds, the Driver Client sends its location over this same WS connection to the WGS.
- **Internal Routing**: The WGS routes the location payload to the Location Update Service (LUS).
- **Geo Indexing**: The LUS extracts the data and updates the Redis Geo Index (with 30s TTL).

### Phase 2: Ride Matching and Notification (Push Offer)

This is where the decoupled services communicate to push the offer to the driver.
- **Rider Request**: Rider Client -> Match Service (MS).
- **Driver Selection**: The MS queries the Redis GEO Index (updated by the LUS) and finds `N` nearest available drivers and that matches other criteria (car type, driver payment preference , driver rating, etc.).
- **Publish Offer**: For each of the `N` selected drivers, the MS does not try to use a WS connection. Instead, it publishes a notification message to a fast internal messaging system (like Redis Pub/Sub or a Kafka topic).
	`Payload: {"DriverID": "D123", "Type": "RIDE_OFFER", "Payload": {ride details...}}`
- **WGS Consumes**: The WebSocket Gateway Service (WGS) is subscribed to this Pub/Sub channel. It receives the message.
- **Driver Push**: The WGS looks up the DriverID in its internal connection map, finds the active WebSocket object, and pushes the ride offer payload directly to the driver's client.


### Phase 3: Acceptance and Confirmation/Rejection (Race Condition)
-	**Driver Accepts**: The winning Driver Client taps "Accept" and sends an ACCEPT message back to the WGS over the same persistent WS connection.
-   **Routing Acceptance**: The WGS routes the ACCEPT message to the Match Service (MS).
-  **Transactional Lock**: The MS executes the critical transactional lock logic (as described in the previous answer):
	- It checks the ride's status in the `Redis DB`.
    - If still available, it marks it as taken by this DriverID.    
	- Only the first successful transaction assigns the RideID to the DriverID.
- **Confirmation/Rejection**:
	- `Winner`: MS updates the `Redis DB` and update DB from drive_id assignment to rider_id and response `202 ACCEPTED <RideID>` to WGS. 
	- `Losers`: MS finds the other drivers who were offered the ride and response `409 CONFLICT` to WGS.
- **Final Delivery**: The WGS consumes these final messages and pushes the confirmation/rejection over the WebSocket connections.


### Communication Flow Summary Table

| Flow Direction | 	Component| 	Mechanism | 	Purpose | 
|---------------|--------|-------|------|
| Driver to WSG | WebSocket Gateway Service (WGS) | Persistent WebSocket Connection | Continuous location updates and bidirectional communication. |
| Driver $\rightarrow$ MS (Acceptance) | WGS -> Match Service (MS) | Synchronous (REST/gRPC) | Fast, transactional attempt to secure the ride lock. |
| WGS to Location Update Service (LUS) | WebSocket Gateway Service (WGS) -> Location Update Service (LUS) | Synchronous (gRPC/REST) | Transmit location updates for geo-indexing. |
| MS to Driver (Confirmation)  | Match Service (MS) -> WGS | Asynchronous (Redis Pub/Sub) | Fan-out push notification to the winner and rejection messages to the losers. |

## Implementation Details


## Run Application

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```  

2. Start Redis server/zookeeper/mysql server:
   ```bash
   redis-server
    ```

3. Start Service Discovery (Zookeeper):
```bash
.venv/bin/python -m uvicorn uber_driver_riders_match_app.service_discovery:app --reload --port 8500
```

4. Register Redis shards:
```bash
.venv/bin/python -m uber_driver_riders_match_app.shard_register
```

4.  Start Location Update Service:
```bash
.venv/bin/python -m uvicorn uber_driver_riders_match_app.location_update_service:app --reload --port 8001
```  

5. Start Match Service:
```bash
.venv/bin/python -m uvicorn uber_driver_riders_match_app.match_service:app --reload --port 8002
```  

6. Ride Estimate Service:
```bash
.venv/bin/python -m uvicorn uber_driver_riders_match_app.ride_estimate_service:app --reload --port 8003
```     

7. Start WebSocket Gateway Service:
```bash
.venv/bin/python -m uvicorn uber_driver_riders_match_app.websocket_gateway_service:app --reload --port 8004
```     

7. Start driver client app:
```bash
.venv/bin/python -m uber_driver_riders_match_app.driver_client
```

7. Start rider client app:
```bash
.venv/bin/python -m uber_driver_riders_match_app.rider_client
```

## Future Improvements




