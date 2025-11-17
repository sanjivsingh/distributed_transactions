from typing import Any
import os
from commons import compare_hash

import random
import string
import time

# Function to generate a random word
def generate_random_word(length=4):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Function to generate a random meaning (simple sentence)
def generate_random_meaning():
    words = ['a', 'the', 'is', 'of', 'to', 'and', 'in', 'that', 'it', 'with']
    return ' '.join(random.choices(words, k=random.randint(5, 100)))


class BitCast:
    
    def __init__(self, data_directory: str) -> None:

        self.data_directory = data_directory
        self.file_index = 0
        self.file = None
        self.offset = 0
        self.current_data_file = None

        self.DELETE_MARKER = "__DELETED__"

        self.indexes = {}

        self.cache   = {}
        self.cache_size = 0

        if not os.path.exists(data_directory):
            print("Creating data directory:", data_directory)
            os.makedirs(data_directory) 
            self.__create_data_file()
        elif len(os.listdir(data_directory)) == 0:
            self.__create_data_file()
        else:
            entries_names = os.listdir(data_directory)
            if  entries_names:
                # Sort the list in descending order by name
                entries_names.sort()
                for entry_name in entries_names:
                    self.current_data_file = entry_name
                    self.file_index = int(entry_name.split('_')[1])
                    self.__load_index__()

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        self.__save_cache_to_disk()
        if self.file:
            #print("Closing file:", self.file.name , "Total offset written:", self.offset),
            self.file.close()
            self.file = None
        #print("Closed BitCast data files.")
        
    def __create_data_file(self) -> None:
        self.close()
        self.file_index += 1
        self.current_data_file = f"data_{self.file_index}"
        self.file = open(os.path.join(self.data_directory, self.current_data_file), 'a')
        self.offset = 0
        #print("Created new data file:", self.current_data_file)

    def current_data_full_file_path(self) -> str:
        return os.path.join(self.data_directory, self.current_data_file)

    def __load_index__(self) -> None:
        self.offset = 0
        self.file = open(os.path.join(self.data_directory, self.current_data_file), 'a')
        while True:
            self.file.seek(self.offset)
            try:
                check_sum_record_length = self.file.read(32+4)   
            except Exception as e:
                print(f"Error reading file {self.file.name} at offset {self.offset}: {e}")
                break

            if not check_sum_record_length:
                print("Empty File ", self.file.name) 
                return  

            # Read checksum + record length
            check_sum = check_sum_record_length[0:32]
            record_length = int(check_sum_record_length[32:].lstrip('0'))
            self.file.seek(self.offset+32)
            data_record = self.file.read(record_length)
            if compare_hash.Util.calculate_md5_checksum(data_record) != check_sum:
                raise RuntimeError(f"Data Corruption detected for check_sum {check_sum} offset : {self.offset} data_record : {data_record}")
            
            key_length = int(data_record[4:7].lstrip('0'))
            key = data_record[7: 7+key_length]
            self.indexes[key] = (self.file.name, self.offset, len(check_sum)+record_length)

            self.offset += len(check_sum) + record_length
    
    def get(self, key: str) -> str:

        if key in self.cache:
            if self.cache[key] == self.DELETE_MARKER:
                raise RuntimeError(f"Key {key} not found")
            return self.cache[key]

        if key not in self.indexes:
            raise RuntimeError(f"Key {key} not found")
        else:
            file_name, offset , record_length = self.indexes[key]
            with open(os.path.join(self.data_directory, file_name), 'r') as f:
                f.seek(offset)
                record = f.read(record_length)
                check_sum = record[0:32]
                data_record = record[32:]
                if compare_hash.Util.calculate_md5_checksum(data_record) != check_sum:
                    raise RuntimeError(f"Data Corruption detected for key {key}")
                
                record_length = data_record[0:4]
                key_length = int(data_record[4:7].lstrip('0'))
                key = data_record[7: 7+key_length]
                value_length = int(data_record[7+key_length:7+key_length+4])
                value = data_record[7+key_length+4: 7+key_length+4+value_length]
                if value == self.DELETE_MARKER:
                    raise RuntimeError(f"Key {key} not found")
                return value

    def __save_cache_to_disk(self) -> None:
        for key, value in self.cache.items():

            record = f"{len(key):03d}{key}{len(value):04d}{value}"
            record = f"{len(record)+4:04d}{record}"
            checksum = compare_hash.Util.calculate_md5_checksum(record)
            record = f"{checksum}{record}"
            self.file.write(record)

            # update index
            self.indexes[key] = (self.current_data_file, self.offset, len(record))
            self.offset += len(record)
        if self.file:
            self.file.flush()
            #print(f"Flushed {self.cache_size } characters from cache to disk in file {self.file.name}")
        self.cache = {}
        self.cache_size = 0
        

    def put(self, key: str, value: str) -> bool:

        # if file size + new record size > 10 MB , create new data file
        if self.offset + len(key) + len(value) >= 1024 * 1024 * 1024:  # 1 GB
            self.__save_cache_to_disk()
            self.__create_data_file()

        # if cache size + new record size > 04 KB , flush to disk
        if self.cache_size + len(key) + len(value) >= 1024 * 4096:
            self.__save_cache_to_disk()

        self.cache[key] = value
        self.cache_size += len(key) + len(value)
        return True

    def delete(self, key: str) -> bool:
        return self.put(key, self.DELETE_MARKER)


if __name__ == "__main__":

    bitcast = BitCast(data_directory="Bitcask_Implementation/bitcask_data/")

    bitcast.put("key1", "value1")
    bitcast.put("key2", "value2")

    value = bitcast.get("key1")
    print("Retrieved value for key1:", value)   
    bitcast.get("key2")
    print("Retrieved value for key2:", value)   
    bitcast.delete("key1")
    try:
        bitcast.get("key1")
    except RuntimeError as e:
        print(e)
    bitcast.put("key2","value2-updated")
    value = bitcast.get("key2")
    print("Retrieved value for key2 after update:", value)


    for i in range(1000000000):
        bitcast.put(generate_random_word(),     generate_random_meaning())