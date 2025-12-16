import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


import uuid
import pymysql
from pymysql.cursors import DictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from typing import Optional
import math
from commons import logger
from config import DB_CONFIG , RIDE_ESTIMATE_SERVICE_NAME , RIDE_ESTIMATE_SERVICE_PORT
from service_integration import  create_service_integration

app = FastAPI()
log = logger.setup_logger(__name__)

# Service integration
service_integration = create_service_integration(
    {
        "service_name": RIDE_ESTIMATE_SERVICE_NAME,
        "host": "localhost", # replace with actual host if needed
        "port": RIDE_ESTIMATE_SERVICE_PORT,
        "health_url": "/health",
    }
)

class RideEstimateRequest(BaseModel):
    rider_id: str
    source_lat: float
    source_lng: float
    dest_lat: float
    dest_lng: float
    car_type: str

class RideEstimateResponse(BaseModel):
    ride_id: str
    estimated_cost: float
    estimated_distance_km: float
    estimated_duration_minutes: int

class DatabaseManager:
    def __init__(self):
        self.connection = None
        
    def connect(self):
        """Establish database connection and create tables if needed"""
        try:
            # Create database if not exists
            temp_conn = pymysql.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                port=DB_CONFIG['port'],
                cursorclass=DictCursor
            )
            temp_conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            temp_conn.commit()
            temp_conn.close()
            
            # Connect to the database
            self.connection = pymysql.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                port=DB_CONFIG['port'],
                cursorclass=DictCursor
            )
            
            self.create_tables()
            log.info(f"Database connected successfully to {DB_CONFIG['database']} at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
            
        except Exception as e:
            log.error(f"Database connection failed: {e}")
            raise
    
    def create_tables(self):
        """Create ride_estimates table if not exists"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS ride_estimates (
            ride_id VARCHAR(36) PRIMARY KEY,
            rider_id VARCHAR(50) NOT NULL,
            source_lat DECIMAL(10, 8) NOT NULL,
            source_lng DECIMAL(11, 8) NOT NULL,
            dest_lat DECIMAL(10, 8) NOT NULL,
            dest_lng DECIMAL(11, 8) NOT NULL,
            car_type VARCHAR(20) NOT NULL,
            estimated_cost DECIMAL(10, 2) NOT NULL,
            estimated_distance_km DECIMAL(8, 2) NOT NULL,
            estimated_duration_minutes INT NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            driver_id VARCHAR(50) NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_rider_id (rider_id),
            INDEX idx_status (status),
            INDEX idx_driver_id (driver_id)
        )
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_table_sql)
            self.connection.commit()
            log.info("Tables created successfully")
        except Exception as e:
            log.error(f"Error creating tables: {e}")
            raise
    
    def insert_ride_estimate(self, ride_data: dict) -> str:
        """Insert new ride estimate and return ride_id"""
        try:
            cursor = self.connection.cursor()
            insert_sql = """
            INSERT INTO ride_estimates 
            (ride_id, rider_id, source_lat, source_lng, dest_lat, dest_lng, 
            car_type, estimated_cost, estimated_distance_km, estimated_duration_minutes)
            VALUES (%(ride_id)s, %(rider_id)s, %(source_lat)s, %(source_lng)s, 
                    %(dest_lat)s, %(dest_lng)s, %(car_type)s, %(estimated_cost)s,
                    %(estimated_distance_km)s, %(estimated_duration_minutes)s)
            """
            cursor.execute(insert_sql, ride_data)
            self.connection.commit()
            return ride_data['ride_id']
        except Exception as e:
            log.error(f"Error inserting ride estimate: {e}")
            raise
    
    def get_ride_details(self, ride_id: str) -> Optional[dict]:
        """Get ride details by ride_id"""
        try:
            cursor = self.connection.cursor()
            select_sql = "SELECT * FROM ride_estimates WHERE ride_id = %s"
            cursor.execute(select_sql, (ride_id,))
            return cursor.fetchone()
        except Exception as e:
            log.error(f"Error fetching ride details: {e}")
            raise
    
    def update_ride_status(self, ride_id: str, status: str, driver_id: Optional[str] = None):
        """Update ride status and optionally assign driver"""
        try:
            cursor = self.connection.cursor()
            if driver_id:
                update_sql = "UPDATE ride_estimates SET status = %s, driver_id = %s WHERE ride_id = %s"
                cursor.execute(update_sql, (status, driver_id, ride_id))
            else:
                update_sql = "UPDATE ride_estimates SET status = %s WHERE ride_id = %s"
                cursor.execute(update_sql, (status, ride_id))
            self.connection.commit()
        except Exception as e:
            log.error(f"Error updating ride status: {e}")
            raise

# Global database manager
db_manager = DatabaseManager()

class PricingCalculator:
    @staticmethod
    def calculate_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points using Haversine formula"""
        R = 6371  # Earth's radius in kilometers
        
        dlat = math.radians(lat2 - lat1)
        dlng = math.radians(lng2 - lng1)
        
        a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlng / 2) * math.sin(dlng / 2))
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        
        return round(distance, 2)
    
    @staticmethod
    def calculate_cost(distance_km: float, car_type: str) -> float:
        """Calculate ride cost based on distance and car type"""
        pricing = {
            'economy': 1.5,
            'premium': 2.0,
            'luxury': 3.0
        }
        
        base_fare = {
            'economy': 3.0,
            'premium': 5.0,
            'luxury': 8.0
        }
        
        rate = pricing.get(car_type.lower(), pricing['economy'])
        base = base_fare.get(car_type.lower(), base_fare['economy'])
        
        total_cost = base + (distance_km * rate)
        return round(total_cost, 2)
    
    @staticmethod
    def calculate_duration(distance_km: float) -> int:
        """Estimate duration in minutes"""
        avg_speed_kmh = 30
        duration_hours = distance_km / avg_speed_kmh
        duration_minutes = int(duration_hours * 60)
        return max(duration_minutes, 5)

@app.on_event("startup")
async def startup_event():
    """Initialize database and register service"""
    db_manager.connect()
    
    metadata = {
        "version": "1.0",
        "capabilities": ["ride_estimation", "pricing", "database_storage"],
        "database_config": {
            "host": DB_CONFIG['host'], 
            "database": DB_CONFIG['database'],
            "port": DB_CONFIG['port']
        }
    }
    service_integration.register_service(metadata)

@app.post("/estimate", response_model=RideEstimateResponse)
async def create_ride_estimate(request: RideEstimateRequest):
    """Create a new ride estimate"""
    try:
        # Generate unique ride ID
        ride_id = str(uuid.uuid4())
        
        # Calculate distance
        distance = PricingCalculator.calculate_distance(
            request.source_lat, request.source_lng,
            request.dest_lat, request.dest_lng
        )
        
        # Calculate cost and duration
        cost = PricingCalculator.calculate_cost(distance, request.car_type)
        duration = PricingCalculator.calculate_duration(distance)
        
        # Prepare ride data
        ride_data = {
            'ride_id': ride_id,
            'rider_id': request.rider_id,
            'source_lat': request.source_lat,
            'source_lng': request.source_lng,
            'dest_lat': request.dest_lat,
            'dest_lng': request.dest_lng,
            'car_type': request.car_type,
            'estimated_cost': cost,
            'estimated_distance_km': distance,
            'estimated_duration_minutes': duration
        }
        
        # Insert into database
        db_manager.insert_ride_estimate(ride_data)
        
        log.info(f"Ride estimate created: {ride_id} for rider {request.rider_id}")
        
        return RideEstimateResponse(
            ride_id=ride_id,
            estimated_cost=cost,
            estimated_distance_km=distance,
            estimated_duration_minutes=duration
        )
        
    except Exception as e:
        log.error(f"Error creating ride estimate: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/ride/{ride_id}")
async def get_ride_details(ride_id: str):
    """Get ride details by ride_id"""
    try:
        ride_details = db_manager.get_ride_details(ride_id)
        if not ride_details:
            raise HTTPException(status_code=404, detail="Ride not found")
        
        return ride_details
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error fetching ride details: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.put("/ride/{ride_id}/status")
async def update_ride_status(ride_id: str, status: str, driver_id: Optional[str] = None):
    """Update ride status"""
    try:
        # Check if ride exists
        ride_details = db_manager.get_ride_details(ride_id)
        if not ride_details:
            raise HTTPException(status_code=404, detail="Ride not found")
        
        db_manager.update_ride_status(ride_id, status, driver_id)
        
        return {"message": "Ride status updated successfully", "ride_id": ride_id, "status": status}
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error updating ride status: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        db_manager.connection.ping()
        return {
            "status": "healthy",
            "service": "ride_estimate_service",
            "instance_id": service_integration.instance_id,
            "database_connected": True,
            "database_config": {
                "host": DB_CONFIG['host'],
                "database": DB_CONFIG['database'],
                "port": DB_CONFIG['port']
            }
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "ride_estimate_service",
            "instance_id": service_integration.instance_id,
            "database_connected": False,
            "error": str(e)
        }

@app.on_event("shutdown")
async def shutdown_event():
    """Deregister service on shutdown"""
    service_integration.deregister_service()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=RIDE_ESTIMATE_SERVICE_PORT)