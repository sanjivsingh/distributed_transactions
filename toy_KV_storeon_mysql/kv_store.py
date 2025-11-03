import json
import pymysql
from abc import ABC
from datetime import datetime
from commons import logger
from pymysql.cursors import DictCursor
import threading

from mysql_setup import config as mysql_config, constants as mysql_constants

class ToyKVStore:

    def __init__(self, host, user, password , port, database , table):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.table = table
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
                self.log.info("connected.....")
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
                        CREATE TABLE IF NOT EXISTS {self.table} (
                        `key_name` VARCHAR(255) NOT NULL PRIMARY KEY,
                        `value` JSON,
                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                """)
                self.log.info("created table conversion_request")
                self.connection.commit()
            except Exception as e:
                self.log.info("error in db setup : {e}")
                self.connection.rollback()
            finally:
                self.log.info("db init completed")
            
    def set(self, key: str, value):
        if key == None:
            raise RuntimeError("key can't be null")
        self.connect()
        lock = self._get_or_create_lock(key)
        with lock:
            with self.connection.cursor() as cursor:
                """
                Inserts or updates a key-value pair.
                Uses INSERT ... ON DUPLICATE KEY UPDATE for atomic upserts.
                """
                sql = f"""
                    INSERT INTO {self.table} (`key_name`, `value`)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE `value` = %s
                """
                # Convert Python object to JSON string for storage
                json_value = json.dumps(value)
                try:
                    cursor.execute(sql, (key, json_value, json_value))
                    self.connection.commit()
                    print(f"Key '{key}' set successfully.")
                except Exception as err:
                    print(f"Error setting key '{key}': {err}")
                    self.connection.rollback()

    def get(self, key: str):
        if key == None:
            raise RuntimeError("key can't be null")
        self.connect()
        lock = self._get_or_create_lock(key)
        with lock:
            with self.connection.cursor() as cursor:
                """
                Retrieves the value for a given key.
                Returns None if the key does not exist.
                """
                sql = f"SELECT `value` FROM {self.table} WHERE `key_name` = %s"
                try:
                    cursor.execute(sql, (key,))
                    result = cursor.fetchone()
                    if result:
                        # Convert JSON string back to Python object
                        return json.loads(result["value"])
                    return None
                except Exception as err:
                    print(f"Error getting key '{key}': {err}")
                    return None

    def delete(self, key: str):
        if key == None:
            raise RuntimeError("key can't be null")
        self.connect()
        lock = self._get_or_create_lock(key)
        with lock:
            with self.connection.cursor() as cursor:
                """Deletes a key-value pair from the store."""
                sql = f"DELETE FROM {self.table} WHERE `key_name` = %s"
                try:
                    cursor.execute(sql, (key,))
                    self.connection.commit()
                    if cursor.rowcount > 0:
                        print(f"Key '{key}' deleted successfully.")
                    else:
                        print(f"Key '{key}' not found.")
                except Exception as err:
                    print(f"Error deleting key '{key}': {err}")
                    self.connection.rollback()

# Example usage
if __name__ == '__main__':
    # Configure your MySQL connection details
    store = ToyKVStore(host=mysql_config.configurations[mysql_constants.HOST],
            user=mysql_config.configurations[mysql_constants.USER],
            password=mysql_config.configurations[mysql_constants.PASSWORD],
            port=mysql_config.configurations[mysql_constants.PORT], database= "kv_store", table = "data")
    store.init_db()

    # Example operations
    store.set("user:101", {"name": "Alice", "email": "alice@example.com"})
    store.set("app:settings", {"theme": "dark", "version": 1.5})
    
    # Retrieve a value
    user_data = store.get("user:101")
    print(f"Retrieved user_data: {user_data}")
    
    # Update a value
    store.set("app:settings", {"theme": "light", "version": 2.0})
    app_settings = store.get("app:settings")
    print(f"Retrieved app_settings: {app_settings}")

    # Delete a key
    store.delete("user:101")
    
    # Attempt to retrieve a deleted key
    user_data_after_delete = store.get("user:101")
    print(f"Retrieved user_data after deletion: {user_data_after_delete}")

    store.disconnect()

