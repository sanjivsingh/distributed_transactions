import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import websockets
import json
import time
import threading
from typing import Optional, Dict, Any, Callable
import requests
from service_integration import ServiceIntegration

from config import LOCATION_SERVICE_PORT, WEBSOCKET_GATEWAY_SERVICE_NAME, WEBSOCKET_GATEWAY_SERVICE_PORT

class DriverClient:
    def __init__(self, driver_id: str, car_type: str = "economy", payment_preference: str = "both"):
        self.driver_id = driver_id
        self.car_type = car_type
        self.payment_preference = payment_preference
        self.websocket = None
        self.is_connected = False
        self.current_location = {"lat": None, "lng": None}
        self.status = "offline"  # offline, available, busy
        self.current_ride_id = None

        # Callbacks for handling events
        self.on_ride_offer_callback = None
        self.on_ride_confirmed_callback = None
        self.on_ride_rejected_callback = None
        self.on_location_updated_callback = None
        
        # Service discovery integration
        self.service_integration = ServiceIntegration(
            service_name="driver_client",
            host="localhost",
            port=9001,  # Placeholder port for driver client
            health_url="/health"
        )
        
        # Register driver client as a service
        self.service_integration.register_service(metadata={
            "type": "client",
            "driver_id": driver_id,
            "car_type": car_type,
            "payment_preference": payment_preference
        })
    
    def _get_websocket_gateway_url(self) -> Optional[str]:
        """Get WebSocket Gateway service URL from service discovery"""
        try:
            service_info = self.service_integration.discover_service(WEBSOCKET_GATEWAY_SERVICE_NAME)
            if service_info:
                return f"ws://{service_info['host']}:{service_info['port']}/ws/{self.driver_id}"
            return None
        except Exception as e:
            print(f" Service discovery failed for websocket_gateway, using fallback: {e}")
            return f"ws://localhost:{WEBSOCKET_GATEWAY_SERVICE_PORT}/ws/{self.driver_id}"  # Fallback
    
    def _get_location_service_url(self) -> Optional[str]:
        """Get location update service URL from service discovery"""
        try:
            return self.service_integration.get_service_url("location_service")
        except Exception as e:
            print(f" Service discovery failed for location_service, using fallback: {e}")
            return f"http://localhost:{LOCATION_SERVICE_PORT}"  # Fallback
    
    async def connect(self):
        """Connect to WebSocket Gateway"""
        try:
            ws_url = self._get_websocket_gateway_url()
            if not ws_url:
                print(" Unable to discover WebSocket Gateway service")
                return False
            
            print(f" Connecting to WebSocket Gateway: {ws_url}")
            
            self.websocket = await websockets.connect(ws_url)
            self.is_connected = True
            self.status = "available"
            
            print(f" Driver {self.driver_id} connected successfully")
            print(f"   Car type: {self.car_type}")
            print(f"   Payment preference: {self.payment_preference}")
            print(f"   Status: {self.status}")
            
            # Start listening for messages
            asyncio.create_task(self._listen_for_messages())
            return True
            
        except Exception as e:
            print(f" Failed to connect: {e}")
            self.is_connected = False
            return False
    
    async def disconnect(self):
        """Disconnect from WebSocket Gateway"""
        try:
            self.is_connected = False
            self.status = "offline"
            
            if self.websocket:
                await self.websocket.close()
                print(f" Driver {self.driver_id} disconnected")
            
        except Exception as e:
            print(f" Error disconnecting: {e}")
    
    async def _listen_for_messages(self):
        """Listen for incoming messages from WebSocket Gateway"""
        try:
            async for message in self.websocket:
                data = json.loads(message)
                await self._handle_message(data)
                
        except websockets.exceptions.ConnectionClosed:
            print(f" WebSocket connection closed for driver {self.driver_id}")
            self.is_connected = False
        except Exception as e:
            print(f" Error listening for messages: {e}")
    
    async def _handle_message(self, data: Dict[str, Any]):
        """Handle incoming messages"""
        try:
            message_type = data.get('type')
            
            if message_type == 'ride_offer':
                await self._handle_ride_offer(data)
            elif message_type == 'ride_accepted':
                await self._handle_ride_confirmed(data)
            elif message_type == 'ride_rejected':
                await self._handle_ride_rejected(data)
            elif message_type == 'ride_unavailable':
                await self._handle_ride_unavailable(data)
            elif message_type == 'location_updated':
                await self._handle_location_updated(data)
            elif message_type == 'location_update_error':
                await self._handle_location_error(data)
            elif message_type == 'error':
                await self._handle_error(data)
            else:
                print(f" Unknown message type: {message_type}")
                
        except Exception as e:
            print(f" Error handling message: {e}")
    
    async def _handle_ride_offer(self, data: Dict[str, Any]):
        """Handle incoming ride offer"""
        try:
            payload = data.get('payload', {})
            ride_id = payload.get('ride_id')
            rider_id = payload.get('rider_id')
            estimated_cost = payload.get('estimated_cost')
            distance_km = payload.get('estimated_distance_km')
            driver_distance = payload.get('driver_distance_miles')
            
            print(f"\n RIDE OFFER RECEIVED for {self.driver_id}")
            print(f"   Ride ID: {ride_id}")
            print(f"   Rider: {rider_id}")
            print(f"   Estimated cost: ${estimated_cost}")
            print(f"   Trip distance: {distance_km} km")
            print(f"   You are {driver_distance} miles away")
            print(f"   Source: ({payload.get('source', {}).get('lat')}, {payload.get('source', {}).get('lng')})")
            print(f"   Destination: ({payload.get('destination', {}).get('lat')}, {payload.get('destination', {}).get('lng')})")
            
            if self.on_ride_offer_callback:
                self.on_ride_offer_callback(payload)
            
        except Exception as e:
            print(f" Error handling ride offer: {e}")
    
    async def _handle_ride_confirmed(self, data: Dict[str, Any]):
        """Handle ride confirmation"""
        try:
            ride_id = data.get('ride_id')
            self.current_ride_id = ride_id
            self.status = "busy"
            
            print(f"\n RIDE CONFIRMED for {self.driver_id}")
            print(f"   Ride ID: {ride_id}")
            print(f"   Status changed to: {self.status}")
            
            if self.on_ride_confirmed_callback:
                self.on_ride_confirmed_callback(data)
            
        except Exception as e:
            print(f" Error handling ride confirmation: {e}")
    
    async def _handle_ride_rejected(self, data: Dict[str, Any]):
        """Handle ride rejection"""
        try:
            ride_id = data.get('ride_id')
            reason = data.get('reason', 'Unknown reason')
            
            print(f"\n RIDE REJECTED for {self.driver_id}")
            print(f"   Ride ID: {ride_id}")
            print(f"   Reason: {reason}")
            
            if self.on_ride_rejected_callback:
                self.on_ride_rejected_callback(data)
            
        except Exception as e:
            print(f" Error handling ride rejection: {e}")
    
    async def _handle_ride_unavailable(self, data: Dict[str, Any]):
        """Handle ride unavailable notification"""
        try:
            payload = data.get('payload', {})
            ride_id = payload.get('ride_id')
            reason = payload.get('reason', 'Ride no longer available')
            
            print(f"\n RIDE NO LONGER AVAILABLE for {self.driver_id}")
            print(f"   Ride ID: {ride_id}")
            print(f"   Reason: {reason}")
            
        except Exception as e:
            print(f" Error handling ride unavailable: {e}")
    
    async def _handle_location_updated(self, data: Dict[str, Any]):
        """Handle location update confirmation"""
        try:
            print(f" Location updated for {self.driver_id}")
            
            if self.on_location_updated_callback:
                self.on_location_updated_callback(data)
            
        except Exception as e:
            print(f" Error handling location update: {e}")
    
    async def _handle_location_error(self, data: Dict[str, Any]):
        """Handle location update error"""
        try:
            message = data.get('message', 'Location update failed')
            print(f" Location update error for {self.driver_id}: {message}")
            
        except Exception as e:
            print(f" Error handling location error: {e}")
    
    async def _handle_error(self, data: Dict[str, Any]):
        """Handle general error messages"""
        try:
            message = data.get('message', 'Unknown error')
            print(f" Error for {self.driver_id}: {message}")
            
        except Exception as e:
            print(f" Error handling error message: {e}")
    
    async def update_location(self, lat: float, lng: float):
        """Send location update via WebSocket"""
        try:
            if not self.is_connected:
                print(" Not connected to WebSocket Gateway")
                return False
            
            self.current_location = {"lat": lat, "lng": lng}
            
            location_message = {
                "type": "location",
                "lat": lat,
                "lng": lng,
                "car_type": self.car_type,
                "payment_preference": self.payment_preference,
                "timestamp": str(int(time.time()))
            }
            
            await self.websocket.send(json.dumps(location_message))
            print(f" Location update sent for {self.driver_id}: (lat : {lat}, lng : {lng})")
            return True
            
        except Exception as e:
            print(f" Error updating location: {e}")
            return False
    
    async def accept_ride(self, ride_id: str):
        """Accept a ride offer"""
        try:
            if not self.is_connected:
                print(" Not connected to WebSocket Gateway")
                return False
            
            accept_message = {
                "type": "accept",
                "ride_id": ride_id,
                "driver_id": self.driver_id
            }
            
            await self.websocket.send(json.dumps(accept_message))
            print(f" Ride acceptance sent for {self.driver_id}, Ride ID: {ride_id}")
            return True
            
        except Exception as e:
            print(f" Error accepting ride: {e}")
            return False
    
    def update_status_directly(self, status: str):
        """Update driver status directly via location service"""
        try:
            service_url = self._get_location_service_url()
            if not service_url:
                print(" Unable to discover location service")
                return False
            
            response = requests.put(
                f"{service_url}/driver/{self.driver_id}/status",
                params={"status": status},
                timeout=5
            )
            
            if response.status_code == 200:
                self.status = status
                print(f" Status updated for {self.driver_id}: {status}")
                return True
            else:
                print(f" Failed to update status: {response.status_code}")
                return False
                
        except Exception as e:
            print(f" Error updating status: {e}")
            return False
    
    async def start_location_updates(self, lat: float, lng: float, interval: int = 10):
        """Start periodic location updates"""
        print(f" Starting location updates every {interval} seconds...")
        
        while self.is_connected and self.status != "offline":
            await self.update_location(lat, lng)
            
            # Simulate small movement
            lat += (time.time() % 0.001) - 0.0005  # Small random movement
            lng += (time.time() % 0.001) - 0.0005
            
            await asyncio.sleep(interval)
    
    def set_ride_offer_callback(self, callback: Callable[[Dict], None]):
        """Set callback for ride offers"""
        self.on_ride_offer_callback = callback
    
    def set_ride_confirmed_callback(self, callback: Callable[[Dict], None]):
        """Set callback for ride confirmations"""
        self.on_ride_confirmed_callback = callback
    
    def set_ride_rejected_callback(self, callback: Callable[[Dict], None]):
        """Set callback for ride rejections"""
        self.on_ride_rejected_callback = callback
    
    def set_location_updated_callback(self, callback: Callable[[Dict], None]):
        """Set callback for location updates"""
        self.on_location_updated_callback = callback
    
    def __del__(self):
        """Cleanup on destruction"""
        if hasattr(self, 'service_integration'):
            self.service_integration.deregister_service()

class InteractiveDriverClient:
    """Interactive driver client for demo purposes"""
    
    def __init__(self, driver_id: str, car_type: str = "economy", payment_preference: str = "both"):
        self.driver_client = DriverClient(driver_id, car_type, payment_preference)
        self.pending_offers = {}
        self.auto_accept = False
        
    def setup_callbacks(self):
        """Setup event callbacks"""
        self.driver_client.set_ride_offer_callback(self._on_ride_offer)
        self.driver_client.set_ride_confirmed_callback(self._on_ride_confirmed)
        self.driver_client.set_ride_rejected_callback(self._on_ride_rejected)
    
    def _on_ride_offer(self, offer_data: Dict):
        """Handle ride offer with user interaction"""
        ride_id = offer_data.get('ride_id')
        self.pending_offers[ride_id] = offer_data
        
        if not self.auto_accept:
            print(f"\n Type 'accept {ride_id}' to accept this ride")
            print(f"   Type 'reject {ride_id}' to reject this ride")
            print(f"   Type 'auto' to enable auto-accept mode")
        else:
            print(f"\n Auto-accepting ride {ride_id}...")
            asyncio.create_task(self.driver_client.accept_ride(ride_id))
    
    def _on_ride_confirmed(self, data: Dict):
        """Handle ride confirmation"""
        ride_id = data.get('ride_id')
        if ride_id in self.pending_offers:
            del self.pending_offers[ride_id]
        print(f"\n You are now assigned to ride {ride_id}!")
        print(" Type 'complete' when the ride is finished")
    
    def _on_ride_rejected(self, data: Dict):
        """Handle ride rejection"""
        ride_id = data.get('ride_id')
        if ride_id in self.pending_offers:
            del self.pending_offers[ride_id]
    
    async def run_interactive_session(self, initial_lat: float, initial_lng: float):
        """Run interactive driver session"""
        print(f" Starting interactive session for {self.driver_client.driver_id}")
        print("Commands:")
        print("  location <lat> <lng> - Update location")
        print("  accept <ride_id> - Accept a ride")
        print("  reject <ride_id> - Reject a ride") 
        print("  status <available|busy|offline> - Change status")
        print("  auto - Toggle auto-accept mode")
        print("  quit - Exit")
        print()
        
        # Connect and setup
        await self.driver_client.connect()
        self.setup_callbacks()
        
        # Start location updates
        location_task = asyncio.create_task(
            self.driver_client.start_location_updates(initial_lat, initial_lng, interval=15)
        )
        
        # Interactive command loop
        try:
            while self.driver_client.is_connected:
                try:
                    # Use asyncio to handle input without blocking
                    command = await asyncio.to_thread(input, f"{self.driver_client.driver_id}> ")
                    
                    if command.lower() == 'quit':
                        break
                    elif command.lower() == 'auto':
                        self.auto_accept = not self.auto_accept
                        print(f" Auto-accept mode: {'ON' if self.auto_accept else 'OFF'}")
                    elif command.startswith('location '):
                        try:
                            _, lat_str, lng_str = command.split()
                            lat, lng = float(lat_str), float(lng_str)
                            await self.driver_client.update_location(lat, lng)
                        except ValueError:
                            print(" Invalid location format. Use: location <lat> <lng>")
                    elif command.startswith('accept '):
                        try:
                            _, ride_id = command.split()
                            await self.driver_client.accept_ride(ride_id)
                        except ValueError:
                            print(" Invalid format. Use: accept <ride_id>")
                    elif command.startswith('status '):
                        try:
                            _, status = command.split()
                            if status in ['available', 'busy', 'offline']:
                                self.driver_client.update_status_directly(status)
                            else:
                                print(" Invalid status. Use: available, busy, or offline")
                        except ValueError:
                            print(" Invalid format. Use: status <status>")
                    elif command.lower() == 'complete':
                        self.driver_client.current_ride_id = None
                        self.driver_client.update_status_directly('available')
                        print(" Ride completed, status changed to available")
                    else:
                        print(" Unknown command")
                        
                except EOFError:
                    break
                except Exception as e:
                    print(f" Error processing command: {e}")
                
        except KeyboardInterrupt:
            print("\n Interrupted by user")
        
        finally:
            location_task.cancel()
            await self.driver_client.disconnect()

async def main():
    """Demo driver client usage"""
    import sys
    
    # Driver configurations
    drivers = [
        {"id": "driver_001", "lat": 39.0009, "lng": -77.5106, "car_type": "economy", "payment": "both"},
        {"id": "driver_002", "lat": 39.0405, "lng": -77.4512, "car_type": "premium", "payment": "card"},
        {"id": "driver_003", "lat": 38.9685, "lng": -77.3863, "car_type": "luxury", "payment": "both"},
        {"id": "driver_004", "lat": 39.0010, "lng": -77.5110, "car_type": "premium", "payment": "both"},
        {"id": "driver_005", "lat": 39.0411, "lng": -77.4515, "car_type": "economy", "payment": "both"},
        {"id": "driver_006", "lat": 38.9690, "lng": -77.3870, "car_type": "luxury", "payment": "both"},
    ]
    
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        # Interactive mode with single driver
        driver_config = drivers[0]
        interactive_driver = InteractiveDriverClient(
            driver_config["id"], 
            driver_config["car_type"], 
            driver_config["payment"]
        )
        await interactive_driver.run_interactive_session(
            driver_config["lat"], 
            driver_config["lng"]
        )
    else:
        # Simulation mode with multiple drivers
        print(" Starting driver simulation...")
        tasks = []
        
        for driver_config in drivers:
            driver = DriverClient(
                driver_config["id"],
                driver_config["car_type"],
                driver_config["payment"]
            )
            
            # Setup auto-accept for simulation
            driver.set_ride_offer_callback(
                lambda offer, d=driver: asyncio.create_task(d.accept_ride(offer['ride_id']))
            )
            
            # Create tasks
            async def run_driver(driver, lat, lng):
                await driver.connect()
                await driver.start_location_updates(lat, lng, interval=20)
            
            task = asyncio.create_task(run_driver(
                driver, 
                driver_config["lat"], 
                driver_config["lng"]
            ))
            tasks.append(task)
        
        try:
            print(" Simulation running... Press Ctrl+C to stop")
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print("\n Simulation stopped")
            for task in tasks:
                task.cancel()

if __name__ == "__main__":
    asyncio.run(main())