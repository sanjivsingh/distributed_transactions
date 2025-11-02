import json
import pymysql
from abc import ABC
from datetime import datetime
from commons import logger
from pymysql.cursors import DictCursor
import threading

class TicketingBookingShard:

    def __init__(self, host, user, password , port, database , table , index , numberofshards):
        self.host = 'localhost'
        self.user = 'root'
        self.password = 'rootadmin'
        self.port = 3306
        self.index = index
        self.numberofshards = numberofshards
        self.database = f"{database}_{index}"
        self.table = table 
        self.insert_count = 0
        self.connection = None

        # Setup logger
        self.log = logger.setup_logger(__name__)
        self.global_lock = threading.RLock()
        self.key_locks  = {}

    def _get_or_create_lock(self, key: str) -> threading.RLock:
        with self.global_lock:
            if key not in self.key_locks:
                self.key_locks[key] = threading.RLock()
            return self.key_locks[key]

    def connect(self):
        if self.connection is None:
            try:
                self.connection = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    cursorclass=DictCursor
                )
                with self.connection.cursor() as cursor:
                    cursor.execute(f"""
                        SET SESSION auto_increment_offset = {self.index}; 
                    """)
                    cursor.execute(f"""
                        SET SESSION auto_increment_increment = {self.numberofshards}; 
                    """)
                self.log.info(f"shard[{self.index}] connected.....auto_increment_offset : {self.index} auto_increment_increment: {self.numberofshards}")
            except Exception as e:
                raise Exception("error in database connection ")

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def init_db(self):
        self.connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f"""
                        CREATE DATABASE IF NOT EXISTS {self.database};
                """)
                cursor.execute(f"""
                        USE {self.database};
                """)
                cursor.execute(f"""
                        DROP TABLE IF EXISTS {self.table}
                """)
                cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {self.table} (
                        Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        booking_name VARCHAR(255),
                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                """)
                self.log.info(f"shard[{self.index}] created table {self.table}")
                self.connection.commit()
            except Exception as e:
                self.log.info(f"shard[{self.index}] error in db setup : {e}")
                self.connection.rollback()
            finally:
                self.log.info("db init completed")
     
    def max_id(self)    :
        self.connect()
        with self.connection.cursor() as cursor:
            sql = f"""
                select max(id) as max_id from {self.table} 
            """
            try:
                cursor.execute(sql)
                result = cursor.fetchone()
                return {"shard_index" : self.index, "max_id" : result['max_id'], "insert_count" : self.insert_count}
            except Exception as err:
                print(f"shard[{self.index}] Error max_id name : {err}")

    def insert(self, booking_name: str):
        if booking_name == None:
            raise RuntimeError("booking_name can't be null")
        self.connect()
        with self.connection.cursor() as cursor:
            sql = f"""
                INSERT INTO {self.table} (booking_name)
                VALUES (%s)
            """
            try:
                cursor.execute(sql, (booking_name))
                self.connection.commit()
                booking_id = cursor.lastrowid
                print(f"shard[{self.index}]  booking_name {booking_id}: {booking_name} insert successfully.")
                self.insert_count+=1
            except Exception as err:
                print(f"shard[{self.index}] Error insert booking_name '{booking_name}': {err}")
                self.connection.rollback()

class FlickrTicketingService:

    def __init__(self) -> None:
        self.log = logger.setup_logger(__name__)
        self.shards = []
        numberofshards= 3
        for i in range(numberofshards):  
            try:
                shard = TicketingBookingShard(host = 'localhost', user = 'root', password = 'rootadmin', port = 3306, database= "TicketingDB", table = f"ticket_booking",  index=  i+1, numberofshards = numberofshards)
                shard.init_db()
                self.shards.append(shard)
            except Exception as e:
                self.log.error(f"error connecting shard {i} : {e}")

        if not self.shards:
            raise RuntimeError("No shards available")

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        for shard in self.shards:
            try:
                shard.disconnect()
            except Exception as e:
                self.log.error(f"error disconnect shard: {e}")

    def get_shards_max_id(self):
        max_ids  = []
        for shard in self.shards:
            try:
                max_ids.append(shard.max_id())
            except Exception as e:
                self.log.error(f"error get_shards_max_id shard: {e}")
        return max_ids 
    def __get_shard(self, key : str):
        if not self.shards:
            raise RuntimeError("No shards available")
        shard_index  = abs(hash(key)) % len(self.shards)
        return self.shards[shard_index]

    def insert(self, booking_name : str):
        try:
            self.__get_shard(booking_name).insert(booking_name)
        except Exception as e:
            self.log.error(f"error in insert {booking_name}  : {e} ")

if __name__ == "__main__":

    manager = FlickrTicketingService()
    
    for i in range(100):
        manager.insert(f"booking_{i}")

    print(manager.get_shards_max_id())
    manager.disconnect()