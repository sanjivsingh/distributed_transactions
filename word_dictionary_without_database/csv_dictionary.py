import time
class CSVWordDictionaryWriter:

    def __init__(self, manifest_file):
        self.manifest_file = manifest_file
        self.file_path  = f"Word_dictionary_without_DB/data/word_dictionary_{int(time.time())}.txt" 
        self.file = open(self.file_path, "w+")

    def add_word(self, word, meaning):
        self.file.write(f"{word},{meaning}\n")

    def save(self):
        self.file.close()
        with open(self.manifest_file, 'w') as mfile:
            mfile.write(self.file_path)

class CSVWordDictionaryRefresh:

    def __init__(self, manifest_file):
        self.manifest_file = manifest_file
        self.file_path  = f"Word_dictionary_without_DB/data/word_dictionary_{int(time.time())}.txt" 
        

    def update_dictionary(self, new_dict):
        import os 
        old_file = None
        if os.path.exists(self.manifest_file):
            with open(self.manifest_file, "r") as file:
                old_file = file.read()

        self.file = open(self.file_path, "w+")
        if old_file and os.path.exists(old_file.strip()):
            with open(old_file, "r") as file:
                for line in file:
                    word, meaning = line.strip().split(",", 1)
                    if word not in new_dict:
                        self.file.write(f"{word},{meaning}\n")

        for word, meaning in new_dict.items():
            self.file.write(f"{word},{meaning}\n")

        self.file.close()
        with open(self.manifest_file, 'w') as mfile:
            mfile.write(self.file_path)


class CSVWordDictionaryLookup:

    def __init__(self, manisfest_file):
        with open(manisfest_file, "r") as file:
            content = file.read()
        self.file_path = content.strip()
        
    def get_meaning(self, search):
        with open(self.file_path, "r") as file:
            for line in file:
                word, meaning = line.strip().split(",", 1)
                if search == word:
                    return meaning
        raise RuntimeError(f"Word {search} not found")

    def close(self):
        pass