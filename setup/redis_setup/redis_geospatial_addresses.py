import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut
import redis
import random
from threading import Thread
from  setup.redis_setup import config
from  setup.redis_setup import constants

geolocator = Nominatim(user_agent="geo_fetcher")

random_addresses = [
    ["23274 Hanworth St, Ashburn, VA",        39.0009, -77.5106], # Example my location
    ["One Loudoun, Ashburn, VA",              39.0405, -77.4512],
    ["Brambleton Town Center, Ashburn, VA",   38.9836777, -77.5378631],
    ["Dulles Greenway, Ashburn, VA",          39.0183, -77.4770],
    ["Ashburn Village Center, Ashburn, VA",   39.0416, -77.4870],
    ["Belmont Ridge Road, Ashburn, VA",       39.0120, -77.5293]            ,
    ["Windmill Drive, Ashburn, VA",           39.0281813, -77.5011255],
    ["Broadlands Marketplace, Ashburn, VA",   39.0106, -77.5094],
    ["Mooreview Parkway, Ashburn, VA",        39.0484, -77.4854],
    ["Ryan Park Center, Ashburn, VA",         39.0332, -77.4922],
    ["Claiborne Parkway, Ashburn, VA",        39.0179, -77.5166],
    ["Herndon Town Green, Herndon, VA",       38.9685, -77.3863],
    ["Elden Street, Herndon, VA",             38.9690, -77.3869],
    ["Reston Hospital Center, Herndon, VA",   38.9586, -77.3578],
    ["Fox Mill Shopping Center, Herndon, VA", 38.9236, -77.3906],
    ["Worldgate Drive, Herndon, VA",          38.9573, -77.3959],
    ["Sunset Hills Road, Herndon, VA",        38.9509, -77.3443],
    ["Center Street, Herndon, VA",            38.9698, -77.3868],
    ["Coppermine Road, Herndon, VA",          38.9554, -77.4200],
    ["McNair Farms Drive, Herndon, VA",       38.9517, -77.4175],
    ["Herndon Parkway, Herndon, VA",          38.9614149, -77.3721864],
]

address_location = {}
try :
    for index in range(len(random_addresses)):
            location = geolocator.geocode(random_addresses[index][0])
            if location:
                random_addresses[index][1], random_addresses[index][2] = location.latitude, location.longitude
except (GeocoderUnavailable, GeocoderTimedOut) as e:
    print(f"Geocoding service is unavailable or timed out: {e}")
except Exception as e:
    print(f"An error occurred during geocoding: {e}")

print("Geocoded Addresses:")
for addr in random_addresses:
    print(f"{addr[0]}: ({addr[1]}, {addr[2]})")

class Driver:
    def __init__(self, name: str):
        self.name = name
        self.latitude = None
        self.longitude = None
        self.address = None
        self.redis   = redis.Redis(host=config.configurations[constants.REDIS_SERVER], port=config.configurations[constants.REDIS_PORT], db=0)

    def move(self):
        index= random.randint(0, len(random_addresses)-1)
        self.address, self.latitude, self.longitude = random_addresses[index][0], random_addresses[index][1], random_addresses[index][2]    

        while True:
            self.redis.geoadd("driver_locations", (self.latitude, self.longitude, self.name))
            print(f"{self.name} moved to {self.address} at ({self.latitude}, {self.longitude})")
            time.sleep( random.randint(10,30))  # Simulate time delay between movements


    def close(self):
        if self.redis:
            self.redis.close()   
    def __del__(self):
        self.close()


if __name__ == "__main__":


    drivers = []
    threadpool = []
    print("\nData added to Redis Geospatial index 'driver_locations'")
    client = None
    try:
        client = redis.Redis(host=config.configurations[constants.REDIS_SERVER], port=config.configurations[constants.REDIS_PORT], db=0)
        client.expire("driver_locations", 30)  # Expire in 30 seconds 

        print("Set expiration for 'driver_locations' to 30 seconds")
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        exit(1)

    try :

        for driver_id in range(20):
            driver = Driver(name=f"Driver_{driver_id}")
            t = Thread(target=driver.move)
            t.start()
            threadpool.append(t)

        my_location_lan, my_location_lat = random_addresses[0][1], random_addresses[0][2]  # Using first address as reference
        while True:

            near_by_drivers = client.georadius("driver_locations", longitude=my_location_lan, latitude=my_location_lat, radius=5, unit='mi', withdist=True, withcoord=True, sort='ASC' ,count=5)
            print(f"\n---------------------------------\ngeoradius Nearby drivers within 5 miles of my location ({my_location_lan}, {my_location_lat}):")
            for driver in near_by_drivers:
                name = driver[0].decode('utf-8')
                distance = driver[1]
                coord = driver[2]
                print(f"        Driver: {name}, Distance: {distance:.2f} miles, Coordinates: {coord}")

            near_by_drivers = client.georadiusbymember("driver_locations", "Driver_1", radius=5, unit='mi', withdist=True, withcoord=True, sort='ASC' ,count=5)
            print(f"\n---------------------------------\ngeoradiusbymember Nearby drivers within 5 miles of my location ({my_location_lan}, {my_location_lat}):")
            for driver in near_by_drivers:
                name = driver[0].decode('utf-8')
                distance = driver[1]
                coord = driver[2]
                print(f"        Driver: {name}, Distance: {distance:.2f} miles, Coordinates: {coord}")

            near_by_drivers = client.geosearch("driver_locations", "Driver_1", radius=5, unit='mi', withdist=True, withcoord=True, sort='ASC' ,count=5)
            print(f"\n---------------------------------\ngeosearch Nearby drivers within 5 miles of my location ({my_location_lan}, {my_location_lat}):")
            for driver in near_by_drivers:
                name = driver[0].decode('utf-8')
                distance = driver[1]
                coord = driver[2]
                print(f"        Driver: {name}, Distance: {distance:.2f} miles, Coordinates: {coord}")

            time.sleep(30)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if client:
            client.close()
        for driver in drivers:
            driver.close()
        print("Exiting...")
    



