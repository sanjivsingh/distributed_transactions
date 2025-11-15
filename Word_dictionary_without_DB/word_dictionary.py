from Word_dictionary_without_DB.csv_dictionary import CSVWordDictionaryWriter, CSVWordDictionaryLookup
from Word_dictionary_without_DB.indexed_dictionary import IndexedWordDictionaryWriter, IndexedWordDictionaryLookup
import random
import string
import time

# Function to generate a random word
def generate_random_word(length=7):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Function to generate a random meaning (simple sentence)
def generate_random_meaning():
    words = ['a', 'the', 'is', 'of', 'to', 'and', 'in', 'that', 'it', 'with']
    return ' '.join(random.choices(words, k=random.randint(5, 1000)))


def write_records(writer):
    start_time = time.time()
    # Initialize the writer (for file operations)
    manifest_file = "Word_dictionary_without_DB/data/manifest.txt"
    dic_writer = writer(manifest_file)
    # Initialize the lookup (in-memory map, but we'll use writer for persistence
    num_records = 1000  # 1M records
    batch_size = 100    # Process in batches of 10K
    cache_for_lookup = []
    for i in range(0, num_records, batch_size):
        batch_end = min(i + batch_size, num_records)        
        # Generate and add batch to in-memory map and file
        for j in range(i, batch_end):
            if j % 100000  == 0:
                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} Inserted {j} records so far...")
            word = generate_random_word()
            meaning = generate_random_meaning()
            if j in [1,11,111,1111,11111,111111,1111111,11111111,111111111,1111111111]:
                cache_for_lookup.append((word, meaning,j))
            dic_writer.add_word(word, meaning) 
    dic_writer.save()

    end_time = time.time()
    print(f"Inserted {num_records} records in {end_time - start_time} seconds")
    return manifest_file , cache_for_lookup

def read_records(reader, manifest_file, cache_for_lookup):
    with open(manifest_file, "r") as file:
        content = file.read()

    file_path = content.strip()
    print("loading data from manifest_file:",     manifest_file)
    total_start_time = time.time()
    dict_reader   = reader(manifest_file)
    for word, meaning , index in cache_for_lookup:
        start_time = time.time()
        retrieved_meaning = dict_reader.get_meaning(word)
        #assert retrieved_meaning == meaning, f"Mismatch for word {word} in dictionary"
        end_time = time.time()
        print(f"Lookup {index}: Word '{word}' found with correct meaning in {end_time - start_time} seconds")
    dict_reader.close()
    total_end_time = time.time()
    print(f"Lookup of {len(cache_for_lookup)} records took {total_end_time - total_start_time} seconds")

    import os
    mb_size = os.path.getsize(file_path) / (1024 * 1024)
    print(f"File size: {mb_size} MB")
    os.remove(file_path)
    print(f"File '{file_path}' deleted successfully.")


def checkNumbers(writer, reader):
    manifest_file, cache_for_lookup = write_records(writer)
    read_records(reader, manifest_file, cache_for_lookup)

# Main program to insert 1M records
def main():

    print("Testing CSV-based Word Dictionary")
    checkNumbers(CSVWordDictionaryWriter, CSVWordDictionaryLookup)
    print("Testing Indexed Word Dictionary")
    checkNumbers(IndexedWordDictionaryWriter, IndexedWordDictionaryLookup)

if __name__ == "__main__":
    main()