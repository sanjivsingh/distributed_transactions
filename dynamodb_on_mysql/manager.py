
from dynamodb_on_mysql.shard import DynamoDBShard
from commons import logger
from typing import Set

from setup.mysql_setup import config as mysql_config, constants as mysql_constants

class HashBasedShardManager:

    def __init__(self) -> None:
        self.log = logger.setup_logger(__name__)
        self.shards : list[DynamoDBShard] = []

        for i in range(10):  
            try:
                shard = DynamoDBShard(shard_index = i, 
                    host = mysql_config.configurations[mysql_constants.HOST], 
                    user = mysql_config.configurations[mysql_constants.USER], 
                    password = mysql_config.configurations[mysql_constants.PASSWORD], 
                    port = mysql_config.configurations[mysql_constants.PORT], 
                    database = f"dynamodb_{i}")
                self.shards.append(shard)
            except Exception as e:
                self.log.error(f"error connecting shard {i} : {e}")

        if not self.shards:
            raise RuntimeError("No shards available")

        self.global_indexes  = {}
        self.local_indexes  = {}
        self.table_pks  = {}
        self.tables  = []

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        for shard in self.shards:
            try:
                shard.disconnect()
            except Exception as e:
                self.log.error(f"error disconnect shard: {e}")

    def __get_shard(self, key : str) -> DynamoDBShard:
        if not self.shards:
            raise RuntimeError("No shards available")
        shard_index  = abs(hash(key)) % len(self.shards)
        return self.shards[shard_index]

    def create_table(self, definition):
        try:
            for shard in self.shards:
                shard.create_table(definition)

            table = definition["table"]
            self.tables.append(table)

            hash_key = definition["hash_key"]
            sort_key = definition["sort_key"]
            self.table_pks[table] = [hash_key,sort_key]
            self.log.info(f"Table {table} created")
        except Exception as e:
            self.log.error(f"error in create_table : {definition} : {e}")

    def create_local_secondary_index(self, definition): 
        try:
            table = definition["table"]
            self.__check_table_exists(table)
            for shard in self.shards:
                shard.create_local_secondary_index(definition)

            if table not in self.local_indexes:
                self.local_indexes[table] = []
            index_column = definition["index_column"]
            self.local_indexes[table].append(index_column)
            self.log.info(f"local index created : {definition}")
        except Exception as e:
            self.log.error(f"error in create_local_secondary_index : {definition} : {e}")

    def create_global_secondary_index(self, definition):
        try:
            table = definition["table"]
            self.__check_table_exists(table)

            index_column = definition["index_column"]
            for shard in self.shards:
                shard.create_global_index_table(table= table , index_column = index_column)

            if table not in self.global_indexes:
                self.global_indexes[table] = []
            self.global_indexes[table].append(index_column)
            self.log.info(f"global index created : {definition}")

        except Exception as e:
            self.log.error(f"error in create_global_secondary_index : {definition} : {e}")

    def delete(self, key: str):
        try:
            # return self.__get_shard(key).get(key)
            pass
        except Exception as e:
            self.log.error(f"error in delete {key} : {e} ")

    def update(self, key: str):
        try:
            # return self.__get_shard(key).get(key)
            pass
        except Exception as e:
            self.log.error(f"error in delete {key} : {e} ")

    def get(self, table, filter):
        self.log.info(f" get {table} : {filter}")
        try:
            self.__check_table_exists(table)

            if not filter:
                self.log.warning(f"full Tables(all shards) scan for table without filter : {table}")
                all_records = [] 
                for shard in self.shards:
                    all_records.extend(shard.get_records(table,filter =None))
                return all_records

            filter_columns = filter.keys()
            pks = self.table_pks[table]
            if len(pks) == len(
                [
                    filter_column
                    for filter_column in filter_columns
                    if filter_column in pks
                ]
            ):
                self.log.info(f"pointed table fetch based on table :  {table}")
                return self.__get_shard(filter[pks[0]]).get_records(table, filter)

            if table in self.global_indexes:
                index_columns = self.global_indexes[table]

                if len(filter_columns) == len(
                    [
                        filter_column
                        for filter_column in filter_columns
                        if filter_column in index_columns
                    ]
                ):

                    pk_values = []
                    for filter_column in filter_columns:
                        self.log.info(f"global index found for filter column : {filter_column}")
                        index_pks = self.__get_shard(filter[filter_column]).select_from_global_index(
                            table, filter_column, filter
                        )
                        if index_pks is not None:
                            pk_values = set(pk_values).intersection(set(index_pks)) if pk_values else index_pks
                        else:
                            return []

                    shards_with_kp_values = set([])
                    # get the only shard which are holding values
                    for pk_value in pk_values:
                        hash_key = pk_value.split("|")[0]
                        shards_with_kp_values.add(self.__get_shard(hash_key))
                    all_records = [] 
                    for shard in shards_with_kp_values:
                        all_records.extend(shard.select_by_pks(table,pk_values))
                    return all_records
            
            self.log.warning(f"all shards scan for table : {table} filter : {filter}")
            all_records = [] 
            for shard in self.shards:
                all_records.extend(shard.get_records(table,filter =filter))
            return all_records

        except Exception as e:
            self.log.error(f"error in get {table} : {filter} : {e} ")
            return None

    def insert(self, table, record):
        self.log.info(f" insert {table}: {record}")
        try:
            self.__check_table_exists(table)

            hash_key = self.table_pks[table][0]
            hash_key_value = record[hash_key]
            self.__get_shard(hash_key_value).insert_record(table, record)

            hash_sort_key = self.table_pks[table]

            if table in self.global_indexes:
                for index_column in self.global_indexes[table]:
                    index_column_value = record[index_column]
                    self.__get_shard(index_column_value).insert_into_global_index(
                        table, index_column, record[index_column], record[hash_sort_key[0]],record[hash_sort_key[1]]
                    )

            self.log.info(f"insert completed : {record} ")
        except Exception as e:
            self.log.error(f"error in set {record} : {e} ")

    def __check_table_exists(self, table):
        if table not in self.tables:
            raise Exception(f"{table} not found")

if __name__ == "__main__":
    manager = HashBasedShardManager()
    
    # Example operations
    manager.create_table({"table": "order_table","hash_key": "order_id","sort_key": "create_date", "columns" : ["name","price"]})
    manager.create_local_secondary_index({"table": "order_table","index_column": "price"})
    manager.create_local_secondary_index({"table": "order_table","index_column": "name"})

    manager.create_global_secondary_index({"table": "order_table","index_column": "price"})

    # Insert
    user_data = manager.insert(
        table="order_table",
        record={"order_id": "1", "create_date": "1", "name": "item1", "price": "20"},
    )
    user_data = manager.insert(
        table="order_table",
        record={"order_id": "2", "create_date": "2", "name": "item2", "price": "21"},
    )
    user_data = manager.insert(
        table="order_table",
        record={"order_id": "3", "create_date": "1", "name": "item3", "price": "20"},
    )
    user_data = manager.insert(
        table="order_table",
        record={"order_id": "4", "create_date": "3", "name": "item4", "price": "40"},
    )

    # Select
    # Select without filter
    print("*"*200)
    user_data = manager.get(table="order_table", filter=None)
    print(user_data)
    # Select on index column
    print("*"*200)
    user_data = manager.get(table="order_table", filter={"price": "20"})
    print(user_data)
    # Select on index column
    print("*"*200)
    user_data = manager.get(table="order_table", filter={"name": "item3"})
    print(user_data)
    # Select on NON-Index column
    print("*"*200)
    user_data = manager.get(table="order_table", filter={"create_date": "1"})
    print(user_data)
    # Select on primary key column
    print("*"*200)
    user_data = manager.get(
        table="order_table", filter={"order_id": "1", "create_date": "1"}
    )
    print(user_data)

    manager.disconnect()