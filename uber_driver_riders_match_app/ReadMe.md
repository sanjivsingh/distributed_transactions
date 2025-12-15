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


## Communication Flow Diagram

The key is that the driver's client establishes one single WebSocket connection to the WGS. All subsequent communication (both location data and match offers) flows through the WGS.

### Phase 1: Driver Connection and Location Update
- **WS Connection**: The Driver Client establishes a persistent WebSocket connection to the WebSocket Gateway Service (WGS).
- Connection Mapping: The WGS validates the driver's authentication token, extracts the DriverID, and stores this mapping in memory: Map<DriverID, WebSocket Object>.
- **Location Stream**: Every 10 seconds, the Driver Client sends its location over this same WS connection to the WGS.
- **Internal Routing**: The WGS routes the location payload to the Location Update Service (LUS).
- **Geo Indexing**: The LUS extracts the data and updates the Redis Geo Index (with 30s TTL).


### Phase 2: Ride Matching and Notification (Push Offer)

This is where the decoupled services communicate to push the offer to the driver.
- **Rider Request**: Rider Client $\rightarrow$ Match Service (MS).
- **Driver Selection**: The MS queries the Redis GEO Index (updated by the LUS) and finds $N$ nearest available drivers.
- **Publish Offer**: For each of the $N$ selected drivers, the MS does not try to use a WS connection. Instead, it publishes a notification message to a fast internal messaging system (like Redis Pub/Sub or a Kafka topic).
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
	- `Losers`: MS finds the other drivers who were offered the ride and reponse `409 CONFLICT` to WGS.
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


## Future Improvements




