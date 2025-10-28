from abc import ABC, abstractmethod
from file_convertor import models

class IFileHandler(ABC):

        @abstractmethod
        def load(self, input_file_path:str) -> list[dict]:
            pass

        @abstractmethod
        def create(self, records: list[dict], output_file_path:str) -> models.Response:
            pass

        @abstractmethod
        def validate(self, file_path:str) -> models.Response:
            pass