from .models import Response
from .file_handler_interface import IFileHandler
from abc import ABC
import inspect
from commons import logger

# Setup logger
logger = logger.setup_logger(__name__)


class Creator(ABC):

    @staticmethod
    def _discover_handlers() -> dict:
        # Discover all classes that implement IFileHandler in file_handler_impl
        from . import file_handler_impl
        handlers = {}
        for name, obj in inspect.getmembers(file_handler_impl, inspect.isclass):
            if issubclass(obj, IFileHandler) and obj is not IFileHandler:
                # Use class name (without 'FileHandler') as format key, uppercased
                fmt = name.replace("FileHandler", "").upper()
                handlers[fmt] = obj
        return handlers

    @staticmethod
    def supported_formats() -> list[str]:
        return list(Creator._discover_handlers().keys())

    @staticmethod
    def create(type: str) -> IFileHandler:
        if not type:
            raise RuntimeError("type can't be empty")
        handlers = Creator._discover_handlers()
        handler_cls = handlers.get(type.upper())
        if handler_cls is None:
            raise RuntimeError(f"Unsupported Type : {type}")
        return handler_cls()


class Driver:

    @staticmethod
    def convert(
        input_type: str, input_path: str, output_type: str, output_path: str
    ) -> Response:

        input_data = Creator.create(input_type).load(input_path)
        response = Creator.create(output_type).create(input_data, output_path)

        return response


def main():
    response = Driver.convert(
        "JSON", "test/input_data.json", "PARQUET", "test/output_data.parquet"
    )
    logger.info(response)
    response = Driver.convert(
        "JSON", "test/input_data.json", "XML", "test/output_data.xml"
    )
    logger.info(response)


if __name__ == "__main__":
    main()

