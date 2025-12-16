import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import requests
import json
import time
from typing import Optional, Dict, Any
from service_integration import ServiceIntegration

class RiderClient:
    def __init__(self, rider_id: str):
        self.rider_id = rider_id
        self.current_ride_id = None
        
        # Initialize service discovery integration
        self.service_integration = ServiceIntegration(
            service_name="rider_client",
            host="localhost", 
            port=9000,  # Placeholder port for rider client
            health_url="/health"
        )
        
        # Register rider client as a service (optional)
        self.service_integration.register_service(metadata={
            "type": "client",
            "rider_id": rider_id
        })
        
    def _get_ride_estimate_service_url(self) -> Optional[str]:
        """Get ride estimate service URL from service discovery"""
        try:
            return self.service_integration.get_service_url("ride_estimate_service")
        except Exception as e:
            print(f" Service discovery failed for ride_estimate_service, using fallback: {e}")
            return "http://localhost:8003"  # Fallback
    
    def _get_match_service_url(self) -> Optional[str]:
        """Get match service URL from service discovery"""
        try:
            return self.service_integration.get_service_url("match_service")
        except Exception as e:
            print(f" Service discovery failed for match_service, using fallback: {e}")
            return "http://localhost:8002"  # Fallback
        
    def get_ride_estimate(self, source_lat: float, source_lng: float, 
            dest_lat: float, dest_lng: float, car_type: str = "economy") -> Optional[Dict]:
        """Get ride estimate and create ride request"""
        try:
            estimate_payload = {
                'rider_id': self.rider_id,
                'source_lat': source_lat,
                'source_lng': source_lng,
                'dest_lat': dest_lat,
                'dest_lng': dest_lng,
                'car_type': car_type
            }
            
            print(f" Requesting ride estimate for {self.rider_id}...")
            print(f"   From: ({source_lat}, {source_lng}) To: ({dest_lat}, {dest_lng})")
            print(f"   Car type: {car_type}")
            
            # Get service URL from service discovery
            service_url = self._get_ride_estimate_service_url()
            if not service_url:
                print(" Unable to discover ride estimate service")
                return None
            
            print(f" Using ride estimate service: {service_url}")
            
            response = requests.post(
                f"{service_url}/estimate",
                json=estimate_payload,
                timeout=10
            )
            
            if response.status_code == 200:
                estimate_data = response.json()
                self.current_ride_id = estimate_data['ride_id']
                
                print(" Ride estimate received:")
                print(f"   Ride ID: {estimate_data['ride_id']}")
                print(f"   Estimated Cost: ${estimate_data['estimated_cost']}")
                print(f"   Distance: {estimate_data['estimated_distance_km']} km")
                print(f"   Duration: {estimate_data['estimated_duration_minutes']} minutes")
                
                return estimate_data
            else:
                print(f" Failed to get ride estimate: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f" Error getting ride estimate: {e}")
            return None
    
    def find_match_for_ride(self, ride_id: str) -> Optional[Dict]:
        """Find drivers for the ride using ride_id"""
        try:
            print(f"\n Finding drivers for ride {ride_id}...")
            
            # Get service URL from service discovery
            service_url = self._get_match_service_url()
            if not service_url:
                print(" Unable to discover match service")
                return None
            
            print(f" Using match service: {service_url}")
            
            response = requests.post(
                f"{service_url}/find_match/{ride_id}",
                timeout=15
            )
            
            if response.status_code == 200:
                match_data = response.json()
                
                print(" Driver matching completed:")
                print(f"   Drivers found: {match_data.get('drivers_found', 0)}")
                print(f"   Exact matches: {match_data.get('exact_matches', 0)}")
                print(f"   Offers sent: {match_data.get('offers_sent', 0)}")
                
                if match_data.get('drivers'):
                    print("   Nearby drivers:")
                    for i, driver in enumerate(match_data['drivers'][:5], 1):  # Show first 5
                        match_status = "✓ Exact match" if driver.get('match_criteria', False) else "○ Nearby"
                        metadata = driver.get('metadata', {})
                        car_type = metadata.get('car_type', 'unknown')
                        payment = metadata.get('payment_preference', 'unknown')
                        print(f"     {i}. {driver['driver_id']} - {driver['distance_miles']:.1f} mi away")
                        print(f"        Car: {car_type}, Payment: {payment} {match_status}")
                
                return match_data
            else:
                print(f" Failed to find match: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f" Error finding match: {e}")
            return None
    
    def check_ride_status(self, ride_id: str) -> Optional[Dict]:
        """Check current status of the ride"""
        try:
            service_url = self._get_match_service_url()
            if not service_url:
                print(" Unable to discover match service for status check")
                return None
            
            response = requests.get(
                f"{service_url}/ride/{ride_id}/status",
                timeout=5
            )
            
            if response.status_code == 200:
                status_data = response.json()
                
                print(f"\n Ride {ride_id} status:")
                print(f"   Database status: {status_data.get('database_status')}")
                print(f"   Redis status: {status_data.get('redis_status')}")
                if status_data.get('driver_id'):
                    print(f"   Assigned driver: {status_data.get('driver_id')}")
                
                return status_data
            else:
                print(f" Failed to get ride status: {response.status_code}")
                return None
                
        except Exception as e:
            print(f" Error checking ride status: {e}")
            return None
    
    def get_ride_details(self, ride_id: str) -> Optional[Dict]:
        """Get complete ride details from estimate service"""
        try:
            service_url = self._get_ride_estimate_service_url()
            if not service_url:
                print(" Unable to discover ride estimate service for details")
                return None
            
            response = requests.get(
                f"{service_url}/ride/{ride_id}",
                timeout=5
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f" Failed to get ride details: {response.status_code}")
                return None
                
        except Exception as e:
            print(f" Error getting ride details: {e}")
            return None
    
    def wait_for_driver_assignment(self, ride_id: str, timeout_seconds: int = 60) -> Optional[str]:
        """Wait for a driver to accept the ride"""
        print(f"\n Waiting for driver assignment (timeout: {timeout_seconds}s)...")
        
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status = self.check_ride_status(ride_id)
            if status:
                db_status = status.get('database_status')
                driver_id = status.get('driver_id')
                
                if db_status == 'matched' and driver_id:
                    print(f" Driver assigned: {driver_id}")
                    return driver_id
                elif db_status in ['cancelled', 'expired']:
                    print(f" Ride {db_status}")
                    return None
            
            time.sleep(3)  # Check every 3 seconds
            print(".", end="", flush=True)
        
        print(f"\n Timeout waiting for driver assignment")
        return None
    
    def request_full_ride(self, source_lat: float, source_lng: float,
            dest_lat: float, dest_lng: float, car_type: str = "economy") -> Optional[str]:
        """Complete ride request flow: estimate -> find match -> wait for assignment"""
        
        print("=" * 60)
        print(f" UBER RIDE REQUEST - {self.rider_id}")
        print("=" * 60)
        
        # Show discovered services
        estimate_url = self._get_ride_estimate_service_url()
        match_url = self._get_match_service_url()
        print(f" Services discovered:")
        print(f"   Ride Estimate: {estimate_url}")
        print(f"   Match Service: {match_url}")
        print()
        
        # Step 1: Get ride estimate
        estimate = self.get_ride_estimate(source_lat, source_lng, dest_lat, dest_lng, car_type)
        if not estimate:
            return None
        
        ride_id = estimate['ride_id']
        
        # Step 2: Find matching drivers
        match_result = self.find_match_for_ride(ride_id)
        if not match_result:
            return None
        
        if match_result.get('drivers_found', 0) == 0:
            print(" No drivers found nearby. Please try again later.")
            return None
        
        # Step 3: Wait for driver assignment
        assigned_driver = self.wait_for_driver_assignment(ride_id, timeout_seconds=90)
        
        if assigned_driver:
            print(f"\n Ride confirmed!")
            print(f"   Ride ID: {ride_id}")
            print(f"   Driver: {assigned_driver}")
            print(f"   Estimated cost: ${estimate['estimated_cost']}")
            return ride_id
        else:
            print(f"\n Unable to find a driver. Ride request cancelled.")
            return None
    
    def refresh_service_discovery(self):
        """Refresh service discovery cache"""
        print(" Refreshing service discovery cache...")
        self.service_integration.clear_cache()
        
        # Test connectivity to services
        estimate_url = self._get_ride_estimate_service_url()
        match_url = self._get_match_service_url()
        
        print(f" Refreshed service URLs:")
        print(f"   Ride Estimate: {estimate_url}")
        print(f"   Match Service: {match_url}")
    
    def __del__(self):
        """Cleanup on destruction"""
        if hasattr(self, 'service_integration'):
            self.service_integration.deregister_service()

def main():
    """Demo rider client usage with service discovery"""
    
    # Example ride requests
    test_rides = [
        {
            "rider_id": "rider_001",
            "source_lat": 39.0009,
            "source_lng": -77.5106,
            "dest_lat": 39.0405,
            "dest_lng": -77.4512,
            "car_type": "economy",
            "description": "Ashburn to One Loudoun"
        },
        {
            "rider_id": "rider_002", 
            "source_lat": 38.9685,
            "source_lng": -77.3863,
            "dest_lat": 39.0009,
            "dest_lng": -77.5106,
            "car_type": "premium",
            "description": "Herndon to Ashburn"
        }
    ]
    
    for i, ride_request in enumerate(test_rides, 1):
        print(f"\n\n{'='*20} RIDE REQUEST {i} {'='*20}")
        print(f" {ride_request['description']}")
        
        # Create rider client with rider_id
        rider = RiderClient(ride_request["rider_id"])
        
        # Refresh service discovery for each request
        rider.refresh_service_discovery()
        
        ride_id = rider.request_full_ride(
            ride_request["source_lat"],
            ride_request["source_lng"], 
            ride_request["dest_lat"],
            ride_request["dest_lng"],
            ride_request["car_type"]
        )
        
        if ride_id:
            print(f" Successfully booked ride: {ride_id}")
            
            # Show final ride details
            details = rider.get_ride_details(ride_id)
            if details:
                print(f"\n Final ride details:")
                print(f"   Created: {details.get('created_at', 'Unknown')}")
                print(f"   Status: {details.get('status', 'Unknown')}")
        else:
            print(" Ride booking failed")
        
        # Wait between requests
        if i < len(test_rides):
            print("\n Waiting 10 seconds before next request...")
            time.sleep(10)

# Individual functions for backward compatibility
def request_ride(rider_id: str, lat: float, lng: float):
    """Legacy function - now uses full ride request flow with service discovery"""
    rider = RiderClient(rider_id)
    # Assume destination is nearby for legacy compatibility
    dest_lat = lat + 0.01  # ~1km north
    dest_lng = lng + 0.01  # ~1km east
    
    return rider.request_full_ride(lat, lng, dest_lat, dest_lng)

def get_estimate_only(rider_id: str, source_lat: float, source_lng: float, 
        dest_lat: float, dest_lng: float, car_type: str = "economy"):
    """Get only ride estimate without booking"""
    rider = RiderClient(rider_id)
    return rider.get_ride_estimate(source_lat, source_lng, dest_lat, dest_lng, car_type)

if __name__ == "__main__":
    # Check if running as main script
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        main()
    else:
        # Simple single ride request
        print(" Simple ride request demo with service discovery")
        rider = RiderClient("rider_demo")
        
        # Show service discovery in action
        rider.refresh_service_discovery()
        
        ride_id = rider.request_full_ride(
            source_lat=39.0009,
            source_lng=-77.5106,
            dest_lat=39.0405, 
            dest_lng=-77.4512,
            car_type="economy"
        )
        
        if ride_id:
            print(f"\n Ride booked successfully: {ride_id}")
        else:
            print("\n Failed to book ride")