from logging import Logger
import os
import threading
import time  # Add this import
from commons import compare_hash
from commons import logger  

logger = logger.setup_logger(__name__)

class BitCast:
    
    def __init__(self, data_directory: str) -> None:

        self.file_size = 100 * 1024 * 1024  # 100 MB
        self.cache_size_limit = 1 * 1024 * 1024  # 1 MB
        self.OLD_FILELIMIT_FOR_COMPACTION = 10  # Minimum old files required to trigger compaction

        self.data_directory = data_directory
        self.file_index = 0
        self.file = None
        self.offset = 0
        self.current_data_file = None
        self.DELETE_MARKER = "__DELETED__"
        self.indexes = {}
        self.cache = {}
        self.cache_size = 0
        self.compaction_lock = threading.Lock()  # To prevent concurrent compaction or access issues
        self.update_index_lock = threading.Lock()
        if not os.path.exists(data_directory):
            print("Creating data directory:", data_directory)
            os.makedirs(data_directory) 
            self.__create_data_file()
        elif len(self.__data_files()) == 0:
            self.__create_data_file()
        else:
            entries_names = self.__data_files()
            if entries_names:
                entries_names.sort()
                for entry_name in entries_names:
                    self.current_data_file = entry_name
                    self.file_index = int(entry_name.split('_')[0])
                    self.__load_index__()

                    self.file = open(os.path.join(self.data_directory, self.current_data_file), 'a')
                    self.file.seek(self.offset)

    def __data_files(self) -> list[str]:
        return sorted([entry_name for entry_name in os.listdir(self.data_directory) if entry_name.endswith(".bitcast")])

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        self.__save_cache_to_disk()
        if self.file:
            # print("Closing file: %s Total offset written: %d", self.file.name, self.offset)
            self.file.close()
            self.file = None
        # print("Closed BitCast data files.")
        
    def __create_data_file(self) -> None:
        self.close()
        self.file_index += 1
        self.current_data_file = f"{self.file_index:04d}_data.bitcast"
        self.file = open(os.path.join(self.data_directory, self.current_data_file), 'a')
        self.offset = 0
        print("Created new data file:", self.current_data_file)

    def current_data_full_file_path(self) -> str:
        return os.path.join(self.data_directory, self.current_data_file)

    def __load_index__(self) -> None:
        self.offset = 0
        file = open(os.path.join(self.data_directory, self.current_data_file), 'r')
        while True:
            file.seek(self.offset)
            try:
                check_sum_record_length = file.read(32+4)   
            except Exception as e:
                print(f"Error reading file {file.name} at offset {self.offset}: {e}")
                break

            if not check_sum_record_length:
                #print("Reached end of file ", file.name) 
                return  

            # Read checksum + record length
            check_sum = check_sum_record_length[0:32]
            record_length = int(check_sum_record_length[32:].lstrip('0'))
            file.seek(self.offset+32)
            data_record = file.read(record_length)
            if compare_hash.Util.calculate_md5_checksum(data_record) != check_sum:
                raise RuntimeError(f"Data Corruption detected for check_sum {check_sum} offset : {self.offset} data_record : {data_record}")
            
            key_length = int(data_record[4:7].lstrip('0'))
            key = data_record[7: 7+key_length]
            self.indexes[key] = (self.current_data_file, self.offset, len(check_sum)+record_length)

            self.offset += len(check_sum) + record_length
    
    def get(self, key: str) -> str:

        if key in self.cache:
            if self.cache[key] == self.DELETE_MARKER:
                raise RuntimeError(f"Key {key} was deleted")
            return self.cache[key]

        with self.update_index_lock:
            if key not in self.indexes:
                raise RuntimeError(f"Key {key} not found")
            else:
                file_name, offset, record_length = self.indexes[key]
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
        with self.compaction_lock:
            # if file size + new record size > 10 MB , create new data file
            if self.offset + len(key) + len(value) >= self.file_size:  # 1 GB
                self.__save_cache_to_disk()
                self.__create_data_file()

            # if cache size + new record size > 04 KB , flush to disk
            if self.cache_size + len(key) + len(value) >= self.cache_size_limit:
                self.__save_cache_to_disk()

            self.cache[key] = value
            self.cache_size += len(key) + len(value)
            return True

    def delete(self, key: str) -> bool:
        return self.put(key, self.DELETE_MARKER)

    def compact_async(self) -> None:
        """Start periodic compaction in the background every 60 seconds."""
        if hasattr(self, '_compaction_scheduler') and self._compaction_scheduler.is_alive():
            print("Periodic compaction already running.")
            return
        self._compaction_scheduler = threading.Thread(target=self.__periodic_compaction, daemon=True)
        self._compaction_scheduler.start()

    def __periodic_compaction(self) -> None:
        """Run compaction every 60 seconds in the background."""
        while True:
            if not self.compaction_lock.locked():
                threading.Thread(target=self.__compact_old_files, daemon=True).start()
            time.sleep(60)

    def __compact_old_files(self) -> None:

        with self.compaction_lock:
            """Compact older data files asynchronously, update index when ready, then delete old files."""

            # Get old files (exclude current)
            data_files = self.__data_files()
            old_files = [f for f in data_files if f != self.current_data_file]
            if not old_files or len(old_files) <= self.OLD_FILELIMIT_FOR_COMPACTION:
                print("Skipping compaction as not enough old files found.")
                return

            print("Starting asynchronous compaction of old files...")     
            # Create new compacted file for old data
            compacted_file_index = len([f for f in self.__data_files() if f.endswith("_compacted.bitcast")]) +1
            compacted_file_name = f"{compacted_file_index:04d}_compacted.bitcast"
            compacted_file_path = os.path.join(self.data_directory, compacted_file_name)
            print(f"Compacted file: {compacted_file_path}")
            new_index = {}
            compacted_file =  open(compacted_file_path, 'w')
            compacted_offset = 0
            # Collect all live keys from old files (latest non-deleted versions)
            for file_name in old_files:
                file_path = os.path.join(self.data_directory, file_name)
                print(f"Compacting file: {file_name}")
                with open(file_path, 'r') as f:
                    offset = 0
                    while True:
                        f.seek(offset)
                        check_sum_record_length = f.read(32 + 4)
                        if not check_sum_record_length:
                            break
                        check_sum = check_sum_record_length[0:32]
                        record_length = int(check_sum_record_length[32:].lstrip('0'))
                        f.seek(offset + 32)
                        data_record = f.read(record_length)
                        if compare_hash.Util.calculate_md5_checksum(data_record) != check_sum:
                            raise RuntimeError(f"Data Corruption during compaction for check_sum {check_sum}")
                        
                        key_length = int(data_record[4:7].lstrip('0'))
                        key = data_record[7: 7 + key_length]

                        # this is old entry either deleted or updated
                        if self.indexes[key][0] != file_name:
                            # This is not the latest version of the key
                            offset += len(check_sum) + record_length
                            continue

                        value_length = int(data_record[7 + key_length:7 + key_length + 4])
                        value = data_record[7 + key_length + 4: 7 + key_length + 4 + value_length]
                        
                        if value == self.DELETE_MARKER:
                            offset += len(check_sum) + record_length
                            continue  # Key is deleted

                        offset += len(check_sum) + record_length
                        record = f"{len(key):03d}{key}{len(value):04d}{value}"
                        record = f"{len(record)+4:04d}{record}"
                        checksum = compare_hash.Util.calculate_md5_checksum(record)
                        record = f"{checksum}{record}"

                        if compacted_offset + len(record) >= self.file_size:  # 1 GB
                            compacted_file.close()
                            compacted_file_index += 1
                            compacted_file_name = f"{compacted_file_index:04d}_compacted.bitcast"
                            compacted_file_path = os.path.join(self.data_directory, compacted_file_name)
                            print(f"Compacted file: {compacted_file_path}")
                            compacted_file = open(compacted_file_path, 'w')
                            compacted_offset = 0

                        compacted_file.write(record)

                        new_index[key] = (compacted_file_name, compacted_offset, len(record))
                        compacted_offset += len(record)
                
            compacted_file.close()

            # Update indexes with compacted data (merge with current index)
            with self.update_index_lock:
                for key in new_index:
                    self.indexes[key] = new_index[key]

            # Delete old files
            for file_name in old_files:
                os.remove(os.path.join(self.data_directory, file_name))
                #print(f"Removed old file: {file_name}")
            
            print(f"Asynchronous compaction complete. Compacted file: {compacted_file_name}, Live keys: {len(new_index)}")


