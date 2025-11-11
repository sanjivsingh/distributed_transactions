class IndexedWordDictionaryLookup:

    def __init__(self, file_path):
        self.file_path = file_path
        self.index = {}
        self.file = open(self.file_path, "r")
        self.__load_index__()


    def __load_index__(self):
        print("__load_index__ called")
        self.file.seek(0)
        index_offset_str = self.file.read(32)
        index_start = int(index_offset_str.lstrip('0'))
        self.file.seek(index_start)
        while True:
            length_bytes = self.file.read(4)
            if len(length_bytes) < 4:
                break
            record_length = int(length_bytes)
            record_str = self.file.read(record_length-4)
            word_length = int(record_str[:3].lstrip('0'))
            word = record_str[3: 3+word_length]
            pos_length = int(record_str[3+word_length:3+word_length+4].lstrip('0'))
            pos = record_str[3+word_length+4: 3+word_length+4+pos_length]
            self.index[word] = (int(pos.split(":")[0]),int(pos.split(":")[1]))
            index_start += record_length
            self.file.seek(index_start)
            
    def get_meaning(self, word):
        if word not in self.index:
            raise RuntimeError(f"Word {word} not found")
        else:
            (offset,length) = self.index[word]
            self.file.seek(offset)
            record_length = int(self.file.read(4).lstrip('0'))
            record_str = self.file.read(record_length-4)
            word_length = int(record_str[:3].lstrip('0'))
            word = record_str[3: 3+word_length]
            meaning_length = int(record_str[3+word_length:3+word_length+4])
            meaning = record_str[3+word_length+4: 3+word_length+4+meaning_length]
            return meaning

    def close(self):
        if self.file:
            self.file.close()


class IndexedWordDictionaryWriter:

    def __init__(self, file_path):
        self.file_path = file_path
        self.file = open(self.file_path, "w+")
        self.current_offset = 0
        self.file.seek(self.current_offset)
        self.file.write(" "*32)
        self.current_offset = 32
        self.index = {} 

    def add_word(self, word, meaning):
        record = f"{len(word):03d}{word}{len(meaning):04d}{meaning}"
        record = f"{len(record)+4:04d}{record}"
        self.file.seek(self.current_offset)
        self.file.write(record)
        self.index[word] = f"{self.current_offset}:{len(record)}"
        self.current_offset += len(record)

    def save(self):
        # Write index at the end of the file
        index_offset = self.current_offset
        for word, pos in self.index.items():
            index_record = f"{len(word):03d}{word}{len(pos):04d}{pos}"
            index_record = f"{len(index_record)+4:04d}{index_record}"
            self.file.seek(self.current_offset)
            self.file.write(index_record)
            self.current_offset += len(index_record)

        # Write index offset at the start of the file
        self.file.seek(0)
        self.file.write(f"{index_offset:032d}")
        self.file.close()