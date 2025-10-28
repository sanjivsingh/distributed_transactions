import pyarrow.parquet as pq
import pyarrow as pa
from file_convertor.models  import Response 
from file_convertor.file_handler_interface  import IFileHandler
import json 
import os
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from typing import List, Dict
import csv
from commons import logger


import yaml  # Add this import at the top

class ParquetFileHandler(IFileHandler):

        def __init__(self) -> None:
            super().__init__()
            self.log = logger.setup_logger(__name__)

        def load(self, input_file_path:str) -> list[dict]:
            filename = os.path.basename(input_file_path)
            try:
                # Read the Parquet file into a PyArrow Table
                table = pq.read_table(input_file_path)
    
                # Convert the table back to a list of Python dictionaries
                self.log.info(f"Success: Parquet data loaded from {filename}")
                return table.to_pylist()

            except FileNotFoundError:
                raise  RuntimeError(f"❌ Error: The file {filename} was not found.")
            except Exception as e:
                raise  RuntimeError(f"❌ Error: Failed to decode JSON from {filename}. Reason: {e}")

        def create(self, records:list[dict], output_file_path:str) -> Response:
            """
            Infers schema and writes a Parquet file from a list of JSON-like records.
            """
            filename = os.path.basename(output_file_path)
            try:
                table = pa.Table.from_pylist(records)
                pq.write_table(table, output_file_path)
                return Response(message=f"✔ Success: Parquet file created: {filename}")
            except Exception as e:
                return Response(status=False, message=f"❌ Error: Failed to write Parquet file. Reason: {e}")

        def validate(self, file_path:str) -> Response:
            filename = os.path.split(file_path)[-1]
            try:
                pq.read_table(file_path)
                return Response(message = "✔ OK: Parquet file {filename} read successfully. Contents:")
            except Exception as e:
                return Response(status = False, message = f"❌ FAILED: Could not read the created Parquet file. Reason: {e}")

class JsonFileHandler(IFileHandler):

        def __init__(self) -> None:
            super().__init__()
            self.log = logger.setup_logger(__name__)

        def load(self, input_file_path : str) -> list[dict]:
            filename = os.path.basename(input_file_path)
            try:
                with open(input_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.log.info(f"Success: JSON data loaded from {filename}")
                return data

            except FileNotFoundError:
                raise  RuntimeError(f"❌ Error: The file {filename} was not found.")
            except Exception as e:
                raise  RuntimeError(f"❌ Error: Failed to load JSON from {filename}. Reason: {e}")

        def create(self, records:list[dict], output_file_path:str) -> Response:
            filename = os.path.basename(output_file_path)
            self.log.info(f"creating file: {filename}")
            try : 
                # Write the data to a JSON file
                with open(output_file_path, 'w', encoding='utf-8') as f:
                    json.dump(records, f, indent=4, default=str)

                return Response(message = f"\n✔ Success: JSON file created {filename}")
            except Exception as e:
                return Response(status = False, message = f"❌ Error: Failed to convert Parquet to JSON. Reason: {e}")

        def validate(self, file_path : str) -> Response:
            filename = os.path.basename(file_path)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    json.load(f)
                return Response(message="✔ OK: JSON file read successfully.")

            except FileNotFoundError:
                raise  RuntimeError(f"❌ Error: The file {filename} was not found.")
            except Exception as e:
                raise  RuntimeError(f"❌ Error: Failed to load JSON from {filename}. Reason: {e}")


class XmlFileHandler(IFileHandler):

    def __init__(self) -> None:
        super().__init__()
        self.log = logger.setup_logger(__name__)

    """
    A file handler for converting to and from XML format.
    """
    def _add_elements(self, parent_element: ET.Element, data: Dict):
        """
        A recursive helper function to build nested XML elements from a dictionary.
        """
        for key, value in data.items():
            child = ET.SubElement(parent_element, key)
            if isinstance(value, dict):
                # If the value is a dictionary, recurse
                self._add_elements(child, value)
            elif isinstance(value, list):
                # If the value is a list, create a nested element for each item
                for item in value:
                    list_item_element = ET.SubElement(child, "item")
                    if isinstance(item, dict):
                        self._add_elements(list_item_element, item)
                    else:
                        list_item_element.text = str(item)
            else:
                # Otherwise, set the text of the current element
                child.text = str(value)

    def load(self, input_file_path: str) -> List[Dict]:
        """
        Reads a nested XML file and converts it into a list of nested dictionaries.
        """
        filename = os.path.basename(input_file_path)
        def _parse_element(element: ET.Element) -> Dict:
            """Recursively parses an XML element and its children into a dictionary."""
            record = {}
            for child in element:
                # If the child has children, it's a nested dictionary
                if list(child):
                    if child.tag in record:
                        # If the key already exists, convert to a list
                        if not isinstance(record[child.tag], list):
                            record[child.tag] = [record[child.tag]]
                        record[child.tag].append(_parse_element(child))
                    else:
                        record[child.tag] = _parse_element(child)
                # If the child has no children, it's a simple key-value pair
                else:
                    record[child.tag] = child.text
            return record

        try:
            tree = ET.parse(input_file_path)
            root = tree.getroot()
            records = []
            for item in root.findall('.//item'):
                records.append(_parse_element(item))
            return records
        except FileNotFoundError:
            raise  RuntimeError(f"❌ Error: The file {filename} was not found.")
        except Exception as e:
            raise  RuntimeError(f"❌ Error: Failed to load XML from {filename}. Reason: {e}")


    def create(self, records: List[Dict], output_file_path: str) -> Response:
        """
        Writes a list of dictionaries to a new XML file, handling nested structures.
        """
        filename = os.path.basename(output_file_path)
        if not records:
            return Response(status=False, message="❌ Error: No records to write.")
        try:
            root = ET.Element("data")
            for record in records:
                item = ET.SubElement(root, "item")
                self._add_elements(item, record)
            
            # Pretty-print the XML
            xml_str = ET.tostring(root, 'utf-8')
            dom = minidom.parseString(xml_str)
            pretty_xml = dom.toprettyxml(indent="  ")
            
            with open(output_file_path, 'w', encoding='utf-8') as f:
                f.write(pretty_xml)
                
            
            return Response(message=f"✔ Success: XML file created: {filename}")
        except Exception as e:
            return Response(status=False, message=f"❌ Error: Failed to write XML file. Reason: {e}")

    def validate(self, file_path: str) -> Response:
        """
        Validates an XML file by attempting to parse it.
        """
        filename = os.path.basename(file_path)
        try:
            ET.parse(file_path)
            return Response(message="✔ OK: XML file read successfully.")
        except Exception as e:
            return Response(status=False, message=f"❌ FAILED: Could not read the created XML file. Reason: {e}")


class CsvFileHandler(IFileHandler):

    def __init__(self) -> None:
        super().__init__()
        self.log = logger.setup_logger(__name__)

    """
    A file handler for converting to and from CSV format.
    """
    def load(self, input_file_path: str) -> List[Dict]:
        """
        Reads a CSV file and converts it into a list of dictionaries.
        """
        filename = os.path.basename(input_file_path)
        try:
            with open(input_file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                return [row for row in reader]
        except FileNotFoundError:
            raise  RuntimeError(f"❌ Error: The file {filename} was not found.")
        except Exception as e:
            raise  RuntimeError(f"❌ Error: Failed to load CSV from {filename}. Reason: {e}")

    def create(self, records: List[Dict], output_file_path: str) -> Response:
        """
        Writes a list of dictionaries to a new CSV file.
        """
        filename = os.path.basename(output_file_path)
        if not records:
            return Response(status=False, message="❌ Error: No records to write.")
        try:
            # Get the fieldnames from the keys of the first record
            fieldnames = records[0].keys()
            with open(output_file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)
            return Response(message=f"Success: CSV file created : {filename}")
        except Exception as e:
            return Response(status=False, message=f"❌ Error: Failed to write CSV file. Reason: {e}")

    def validate(self, file_path: str) -> Response:
        """
        Validates a CSV file by attempting to read its header.
        """
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Read header to check if file is valid
            return Response(message="✔ OK: CSV file read successfully.")
        except Exception as e:
            return Response(status=False, message=f"❌ FAILED: Could not read the created CSV file. Reason: {e}")


class YamlFileHandler(IFileHandler):

    def __init__(self) -> None:
        super().__init__()
        self.log = logger.setup_logger(__name__)

    """
    A file handler for converting to and from YAML format.
    """
    def load(self, input_file_path: str) -> list[dict]:
        filename = os.path.basename(input_file_path)
        try:
            with open(input_file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            self.log.info(f"Success: YAML data loaded from {filename}")
            # YAML can be a dict or list; always return a list of dicts
            if isinstance(data, dict):
                return [data]
            elif isinstance(data, list):
                return data
            else:
                raise RuntimeError(f"❌ Error: Unexpected YAML structure in {filename}.")
        except FileNotFoundError:
            raise RuntimeError(f"❌ Error: The file {filename} was not found.")
        except Exception as e:
            raise RuntimeError(f"❌ Error: Failed to load YAML from {filename}. Reason: {e}")

    def create(self, records: list[dict], output_file_path: str) -> Response:
        filename = os.path.basename(output_file_path)
        try:
            with open(output_file_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(records, f, default_flow_style=False, sort_keys=False)
            return Response(message=f"✔ Success: YAML file created: {filename}")
        except Exception as e:
            return Response(status=False, message=f"❌ Error: Failed to write YAML file. Reason: {e}")

    def validate(self, file_path: str) -> Response:
        filename = os.path.basename(file_path)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                yaml.safe_load(f)
            return Response(message="✔ OK: YAML file read successfully.")
        except Exception as e:
            return Response(status=False, message=f"❌ FAILED: Could not read the created YAML file. Reason: {e}")
