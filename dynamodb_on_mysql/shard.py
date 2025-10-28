import json
import pymysql
from abc import ABC
from datetime import datetime
from commons import logger
from pymysql.cursors import DictCursor
import threading
from typing import List, Dict, Any, Tuple


class DynamoDBShard:

    def __init__(self, shard_index , host, user, password, port, database):
        self.shard_index = shard_index
        self.host = "localhost"
        self.user = "root"
        self.password = "rootadmin"
        self.port = 3306
        self.database = database
        self.connection: pymysql.Connection[DictCursor] = None

        # Setup logger
        self.log = logger.setup_logger(__name__)
        self.global_lock = threading.RLock()
        self.key_locks = {}

        self.local_indexes = {}
        self.table_pks = {}

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
                    cursorclass=DictCursor,
                )
                self.log.info("connected.....")
                with self.connection.cursor() as cursor:
                    self.create_database_if_not_exists(cursor)
            except Exception as e:
                raise Exception(f"error in database connection : {e}")

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        if self.connection:
            self.connection.commit()
            self.connection.close()
            self.connection = None

    def execute_sql(self, cursor, sql):
        try:
            """Helper method to print and execute SQL."""
            cursor.execute(sql)
        except Exception as e:
            raise Exception(f" error in sql : {sql}" ,e)

    def create_database_if_not_exists(self, cursor):
        sql = f""" CREATE DATABASE IF NOT EXISTS {self.database} """
        self.execute_sql(cursor, sql)
        sql = f""" USE {self.database} """
        self.execute_sql(cursor, sql)

    def drop_table_if_exists(self, cursor, table):
        sql = f"""
                DROP TABLE IF EXISTS {table}
            """
        self.execute_sql(cursor, sql)

    def create_main_table(self, cursor, table, hash_key, sort_key, columns):
        self.log.info(f" shard_index : {self.shard_index} -> create_main_table : {table}")
        sql = f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    `{hash_key}` VARCHAR(255) NOT NULL ,
                    `{sort_key}` VARCHAR(255) NOT NULL ,
                    {",".join([f" {column} VARCHAR(255) "for column in columns])},
                    PRIMARY KEY({hash_key},{sort_key} )
                    ) ENGINE=InnoDB;    
                """
        self.execute_sql(cursor, sql)

    def create_index_table(self, table, index_column, cursor = None):
        self.log.info(f" shard_index : {self.shard_index} -> create_index_table : {table}")
        sql = f"""
                CREATE TABLE IF NOT EXISTS {table}_{index_column} (
                    `index_key` VARCHAR(255) NOT NULL PRIMARY KEY,
                    `pks` VARCHAR(255) NOT NULL
                    ) ENGINE=InnoDB;    
            """
        if not cursor:  
            self.connect()
            cursor = self.connection.cursor()
        self.execute_sql(cursor, sql)

    def create_global_index_table(self, table, index_column, cursor = None):
        self.log.info(f" shard_index : {self.shard_index} -> create_global_index_table : {table}")
        sql = f"""
                CREATE TABLE IF NOT EXISTS {table}_{index_column}_seconday_index (
                    `index_key` VARCHAR(255) NOT NULL PRIMARY KEY,
                    `pks` VARCHAR(255) NOT NULL
                    ) ENGINE=InnoDB;    
            """
        if not cursor:  
            self.connect()
            cursor = self.connection.cursor()
        self.execute_sql(cursor, sql)

    def select_all(self, cursor, table):
        self.log.warning(f"shard_index : {self.shard_index} ->  full Shard scan : table : {table}")
        sql = f"""
            SELECT * FROM {table} 
        """
        self.execute_sql(cursor, sql)
        return cursor.fetchall()

    def select_by_pk(self, cursor, table,  filter):
        self.log.info(f" shard_index : {self.shard_index} -> pointed Shard look up based on primary key : {filter}")
        pks = self.table_pks[table]
        sql = f"""
            SELECT * FROM {table} 
            WHERE { " AND ".join([ f" {key} = {filter[key]}" for key in pks])}
        """
        self.execute_sql(cursor, sql)
        return cursor.fetchall()

    def select_from_index(self, cursor, table, index_column, filter):
        self.log.info(f" shard_index : {self.shard_index} -> shard index for column {index_column} table : {table}")
        sql = f"""
        SELECT pks FROM {table}_{index_column} 
        WHERE { " AND ".join([ f" index_key = '{filter[index_column]}' "])}
        """
        self.execute_sql(cursor, sql)
        pk_values = cursor.fetchall()
        if pk_values:
            return pk_values[0]["pks"].split(",")
        else:
            print(f"INFO : no entry in index {table}_{index_column} ")
            return None

    def select_from_global_index(self, table, index_column, filter):
        self.log.info(f" shard_index : {self.shard_index} -> shard global index for column {index_column}")
        self.connect()
        with self.connection.cursor() as cursor:
            sql = f"""
            SELECT pks FROM {table}_{index_column}_seconday_index 
            WHERE { " AND ".join([ f" index_key = {filter[index_column]}"])}
            """
            self.execute_sql(cursor, sql)
            pk_values = cursor.fetchall()
            if pk_values:
                return pk_values[0]["pks"].split(",")
            else:
                print(f"INFO : no entry in index {table}_{index_column} ")
                return None

    def select_by_pks(self, table, pk_values):
        self.log.info(f" shard_index : {self.shard_index} -> shard by pks {pk_values}")
        self.connect()
        with self.connection.cursor() as cursor:
            pks = self.table_pks[table]
            fvalues = [
                f""" ( '{final_pk.split('|')[0]}' , '{final_pk.split('|')[1]}' ) """
                for final_pk in pk_values
            ]
            sql = f"""
                SELECT * FROM {table} 
                WHERE ({pks[0]},{pks[1]}) IN
                (
                    { " , ".join(fvalues)}
                )
            """
            self.execute_sql(cursor, sql)
            return cursor.fetchall()

    def select_with_filter(self, cursor, table, filter):
        self.log.warning("shard_index : {self.shard_index} -> full Shard scan with filter")
        sql = f"""
            SELECT * FROM {table} 
            WHERE { " AND ".join([ f" {key} = {filter[key]}" for key in filter])}
        """
        self.execute_sql(cursor, sql)
        return cursor.fetchall()

    def insert_into_main(self, cursor, table, record):
        self.log.info(f" shard_index : {self.shard_index} -> insert_into_main  : {table}, {record}")
        sql = f"""
                    INSERT INTO {table} (
                        {", ".join(record.keys())}
                    ) 
                    VALUES
                    ( {", ".join([ f"'{record[key]}'" for key in record.keys()])})
                    ;
                    """
        self.execute_sql(cursor, sql)

    def insert_into_index(self, cursor, table, index_column, index_column_value, hash_key_value, sort_key_value):
        self.log.info(f" shard_index : {self.shard_index} -> insert_into_index  : {index_column}, {index_column_value}")
        value = f"'{hash_key_value}|{sort_key_value}'"
        sql = f"""  
                INSERT INTO {table}_{index_column} (index_key, pks)
                VALUES ('{index_column_value}', {value})
                ON DUPLICATE KEY UPDATE
                    pks = CONCAT(pks, ',', {value})
                ;
                """
        self.execute_sql(cursor, sql)

    def insert_into_global_index(self, table, index_column, index_column_value, hash_key_value, sort_key_value):
        self.log.info(f" shard_index : {self.shard_index} -> insert_into_global_index  : {index_column}, {index_column_value}")
        self.connect()
        with self.connection.cursor() as cursor:
            value = f"'{hash_key_value}|{sort_key_value}'"
            sql = f"""  
                    INSERT INTO {table}_{index_column}_seconday_index (index_key, pks)
                    VALUES ('{index_column_value}', {value})
                    ON DUPLICATE KEY UPDATE
                        pks = CONCAT(pks, ',', {value})
                    ;
                    """
            self.execute_sql(cursor, sql)

    def create_table(self, definition):
        table = definition["table"]
        hash_key = definition["hash_key"]
        sort_key = definition["sort_key"]
        columns = definition["columns"]
        self.connect()
        with self.connection.cursor() as cursor:
            self.drop_table_if_exists(cursor, table)
            self.create_main_table(cursor, table, hash_key, sort_key, columns)
            self.table_pks[table] = [hash_key, sort_key]

    def create_local_secondary_index(self, definition):
        table = definition["table"]
        index_column = definition["index_column"]
        self.connect()
        with self.connection.cursor() as cursor:
            self.drop_table_if_exists(cursor, table + "_" + index_column)
            self.create_index_table( table, index_column,cursor)
            if table not in self.local_indexes:
                self.local_indexes[table] = []
            self.local_indexes[table].append(index_column)

    def get_records(self, table, filter) -> Tuple[Dict[str, Any]]:
        self.connect()
        with self.connection.cursor() as cursor:
            if not filter:
                return self.select_all(cursor, table)

            filter_columns = filter.keys()
            pks = self.table_pks[table]
            if len(pks) == len(
                [
                    filter_column
                    for filter_column in filter_columns
                    if filter_column in pks
                ]
            ):
                return self.select_by_pk(cursor, table, filter)

            if table in self.local_indexes:
                index_columns = self.local_indexes[table]

                if len(filter_columns) == len(
                    [
                        filter_column
                        for filter_column in filter_columns
                        if filter_column in index_columns
                    ]
                ):
                    self.log.info(f" shard_index : {self.shard_index} -> local index found for filter column : {index_columns}")
                    final_pks = []
                    for index_column in filter_columns:
                        index_pks = self.select_from_index(
                            cursor, table, index_column, filter
                        )
                        if index_pks is not None:
                            final_pks = set(final_pks).intersection(set(index_pks)) if final_pks else index_pks
                        else:
                            return []

                    return self.select_by_pks(table, final_pks)
            return self.select_with_filter(cursor, table, filter)

    def insert_record(self, table, record):
        self.connect()
        with self.connection.cursor() as cursor:
            self.insert_into_main(cursor, table, record)
            hash_sort_key = self.table_pks[table]

            if table in self.local_indexes:
                for index_column in self.local_indexes[table]:
                    self.insert_into_index(
                        cursor, table, index_column, record[index_column], record[hash_sort_key[0]],record[hash_sort_key[1]]
                    )


# Example usage
if __name__ == "__main__":
    # Configure your MySQL connection details
    store = DynamoDBShard(
        shard_index = 20,
        host="localhost",
        user="root",
        password="rootadmin",
        port=3306,
        database="dynamodb",
    )

    # Example operations
    store.create_table(
        {
            "table": "order_table",
            "hash_key": "order_id",
            "sort_key": "create_date",
            "columns": ["name", "price"],
        }
    )
    store.create_local_secondary_index(
        {"table": "order_table", "index_column": "price"}
    )
    store.create_local_secondary_index({"table": "order_table", "index_column": "name"})

    # Insert
    user_data = store.insert_record(
        table="order_table",
        record={"order_id": "1", "create_date": "1", "name": "item1", "price": "20"},
    )
    user_data = store.insert_record(
        table="order_table",
        record={"order_id": "2", "create_date": "2", "name": "item2", "price": "21"},
    )
    user_data = store.insert_record(
        table="order_table",
        record={"order_id": "3", "create_date": "1", "name": "item2", "price": "20"},
    )

    # Select
    # Select without filter
    user_data = store.get_records(table="order_table", filter=None)
    print(user_data)
    # Select on index column
    user_data = store.get_records(table="order_table", filter={"price": "20"})
    print(user_data)
    # Select on NON-Index column
    user_data = store.get_records(table="order_table", filter={"create_date": "1"})
    print(user_data)
    # Select on primary key column
    user_data = store.get_records(
        table="order_table", filter={"order_id": "1", "create_date": "1"}
    )
    print(user_data)

    store.disconnect()
