
from bit_cast import BitCast
import random
import os

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


def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # Skip if it's a symbolic link that points to a non-existent file
            # or if it's a broken symlink, as os.path.getsize will raise an error
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp) / (1024 * 1024) # size in MB
    return total_size

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

    bitcast.compact_async()
    file = open("Bitcask_Implementation/directory.txt", "a")
    try:
        i = 0
        while True:
            #bitcast.put(generate_random_word(),     generate_random_meaning())

            # simulate high volume writes and compaction
            bitcast.put(f"key_{random.randint(1, 100000)}",     generate_random_meaning())
            if i == 0 or i % 100000 == 0:
                size = get_directory_size("Bitcask_Implementation/bitcask_data/")
                log_entry = f"{i},{size:.2f}\n"
                file.write(log_entry)
                file.flush()
                time.sleep(0.1)    
            i += 1
    finally:
        if file :
            file.close()