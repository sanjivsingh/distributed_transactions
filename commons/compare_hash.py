import hashlib

class Util:

    @staticmethod
    def compute_sha256(file_path: str) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def calculate_md5_checksum(input_string):
        """Calculates the MD5 checksum of a string."""
        # Encode the string to bytes (UTF-8 is a common choice)
        encoded_string = input_string.encode('utf-8')
        # Create an MD5 hash object
        md5_hash = hashlib.md5()
        # Update the hash object with the encoded string
        md5_hash.update(encoded_string)
        # Get the hexadecimal representation of the digest
        return md5_hash.hexdigest()