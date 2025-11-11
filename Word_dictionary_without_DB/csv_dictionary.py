class CSVWordDictionaryWriter:

    def __init__(self, file_path):
        self.file_path = file_path
        self.file = open(self.file_path, "w+")

    def add_word(self, word, meaning):
        self.file.write(f"{word},{meaning}\n")

    def save(self):
        self.file.close()


class CSVWordDictionaryLookup:

    def __init__(self, file_path):
        self.file_path = file_path
        
    def get_meaning(self, search):
        with open(self.file_path, "r") as file:
            for line in file:
                word, meaning = line.strip().split(",", 1)
                if search == word:
                    return meaning
        raise RuntimeError(f"Word {search} not found")


    def close(self):
        pass