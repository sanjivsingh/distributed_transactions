import pymysql
from dbutils.pooled_db import PooledDB
from abc import ABC
import logging
import concurrent.futures
from datetime import datetime
# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from setup.mysql_setup import config as mysql_config, constants as mysql_constants
class AirlineDatabase(ABC):
    def __init__(self):
        # MySQL connection pool configuration using DBUtils
        self.pool = PooledDB(
            creator=pymysql,
            host=mysql_config.configurations[mysql_constants.HOST],
            user=mysql_config.configurations[mysql_constants.USER],
            password=mysql_config.configurations[mysql_constants.PASSWORD],
            port=mysql_config.configurations[mysql_constants.PORT],
            database='airline_db',
            mincached=30,  # Minimum cached connections
            maxcached=30,  # Maximum cached connections
            maxconnections=30,  # Maximum connections
            autocommit=False
        )
        self.init_db()

    def init_db(self):
        """Create tables if they don't exist."""
        conn = self.pool.connection()
        try:
            with conn.cursor() as cursor:
                # Create flights_checkin table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS flights_checkin (
                        flight_id VARCHAR(50),
                        flight_date DATE,
                        seat_number VARCHAR(10),
                        traveler_id VARCHAR(50),
                        PRIMARY KEY (flight_id, flight_date, seat_number)
                    )
                """)
                # Optional: Clear existing data
                cursor.execute("DELETE FROM flights_checkin")
                logger.info("Created and cleared table flights_checkin")

                # Create flight_booking table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS flight_booking (
                        booking_id VARCHAR(50) PRIMARY KEY,
                        traveler_id VARCHAR(50),
                        flight_date DATE,
                        flight_id VARCHAR(50)
                    )
                """)
                # Optional: Clear existing data
                cursor.execute("DELETE FROM flight_booking")
                logger.info("Created and cleared table flight_booking")
                conn.commit()
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            conn.rollback()
        finally:
            conn.close()

    def bulk_insert_flights_checkin(self, records):
        """Bulk insert records into flights_checkin table."""
        conn = self.pool.connection()
        try:
            with conn.cursor() as cursor:
                cursor.executemany("""
                    INSERT INTO flights_checkin (flight_id, flight_date, seat_number, traveler_id)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE traveler_id=VALUES(traveler_id)
                """, records)
                conn.commit()
                logger.info(f"Bulk inserted {len(records)} records into flights_checkin")
        except Exception as e:
            logger.error(f"Error bulk inserting into flights_checkin: {e}")
            conn.rollback()
        finally:
            conn.close()

    def bulk_insert_flight_booking(self, records):
        """Bulk insert records into flight_booking table."""
        conn = self.pool.connection()
        try:
            with conn.cursor() as cursor:
                cursor.executemany("""
                    INSERT INTO flight_booking (booking_id, traveler_id, flight_date, flight_id)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE traveler_id=VALUES(traveler_id), flight_date=VALUES(flight_date), flight_id=VALUES(flight_id)
                """, records)
                conn.commit()
                logger.info(f"Bulk inserted {len(records)} records into flight_booking")
        except Exception as e:
            logger.error(f"Error bulk inserting into flight_booking: {e}")
            conn.rollback()
        finally:
            conn.close()

    def flight_checkin(self, flight_id, flight_date, traveler_id):
        conn = self.pool.connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        seat_number 
                    FROM 
                        flights_checkin 
                    WHERE 
                        flight_id = %s
                        AND flight_date = %s
                        AND traveler_id IS NULL
                    ORDER BY seat_number
                    LIMIT 1 FOR UPDATE SKIP LOCKED
                """, (flight_id, flight_date))

                available_seat = cursor.fetchone()
                if available_seat:
                    seat_number = available_seat[0]  # fetchone returns a tuple
                    cursor.execute("""
                        UPDATE 
                            flights_checkin
                        SET
                            traveler_id = %s
                        WHERE 
                            flight_id = %s
                            AND flight_date = %s
                            AND seat_number = %s 
                    """, (traveler_id, flight_id, flight_date, seat_number))
                    conn.commit()
                    logger.info(f"Checked in traveler {traveler_id} to seat {seat_number} on flight {flight_id} for {flight_date}")
                else:
                    logger.warning(f"No available seats for flight {flight_id} on {flight_date}")
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during checkin: {e}")
        finally:
            conn.close()

# Sample usage
if __name__ == "__main__":
    db = AirlineDatabase()

    # Sample bulk insert data
    checkin_records = []
    for i in range(1000):
        checkin_records.append(('FL001', '2023-10-01', f"SEAT_{i}", None))
    
    booking_records = []
    for i in range(2000):
        booking_records.append((f"BK{i}", f"T{i}", '2023-10-01', 'FL001'))

    # Bulk inserts
    db.bulk_insert_flights_checkin(checkin_records)
    db.bulk_insert_flight_booking(booking_records)
    start_time = datetime.now()
    # Use ThreadPoolExecutor to limit concurrent threads and avoid TooManyConnections
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(db.flight_checkin, booking_record[3], booking_record[2], booking_record[1]) for booking_record in booking_records]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Wait for completion and handle exceptions if any
            except Exception as e:
                logger.error(f"Error in thread: {e}")
    end_time = datetime.now()
    print( (end_time - start_time).total_seconds()*1000)