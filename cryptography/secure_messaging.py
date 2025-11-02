from typing import Any
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature
import base64

class MessagingService:
    """Mediator service that manages public keys only and facilitates message exchange."""
    def __init__(self):
        self.public_keys = {}  # {username: public_key_pem}

    def register_public_key(self, username, public_key_pem):
        print("""Register a user {username}public key.""")
        self.public_keys[username] = public_key_pem

    def get_public_key(self, username):
        """Get the public key of a user."""
        if username not in self.public_keys:
            raise ValueError(f"User {username} not found")
        return self.public_keys[username]

class Sender:
    """Entity responsible for sending encrypted and signed messages. Holds its own private key."""
    def __init__(self, username,  service: MessagingService):
        self.username = username
        self.service = service
        print(f"User {self.username} generating keys...")
        self.private_key, self.public_key = KeysGenerator().generate_key_pair()
        self.service.register_public_key(self.username, self.public_key)

    def send_message(self, message, recipient_username):
        """Encrypt and sign a message for the recipient."""
        recipient_public_key_pem = self.service.get_public_key(recipient_username)

        # Sign the message with sender's private key
        signature = self.private_key.sign(
            message.encode('utf-8'),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        signature_b64 = base64.b64encode(signature).decode('utf-8')

        # Encrypt the message with recipient's public key
        recipient_public_key_obj = serialization.load_pem_public_key(
            recipient_public_key_pem,
            backend=default_backend()
        )
        encrypted = recipient_public_key_obj.encrypt(
            message.encode('utf-8'),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        encrypted_b64 = base64.b64encode(encrypted).decode('utf-8')

        return {
            'encrypted_message': encrypted_b64,
            'signature': signature_b64,
            'sender': self.username,
            'recipient': recipient_username
        }

class Receiver:
    """Entity responsible for receiving and decrypting messages. Holds its own private key."""
    def __init__(self, username, service: MessagingService):
        self.username = username
        self.service = service
        self.private_key, self.public_key = KeysGenerator().generate_key_pair()
        print(f"User {self.username} generating keys...")
        self.service.register_public_key(self.username, self.public_key)

    def receive_message(self, message_data):
        """Decrypt and verify a message."""
        encrypted_b64 = message_data['encrypted_message']
        signature_b64 = message_data['signature']
        sender_username = message_data['sender']

        # Decrypt the message with receiver's private key
        encrypted = base64.b64decode(encrypted_b64)
        decrypted = self.private_key.decrypt(
            encrypted,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        message = decrypted.decode('utf-8')

        # Verify the signature with sender's public key
        sender_public_key_pem = self.service.get_public_key(sender_username)
        sender_public_key_obj = serialization.load_pem_public_key(
            sender_public_key_pem,
            backend=default_backend()
        )
        signature = base64.b64decode(signature_b64)
        try:
            sender_public_key_obj.verify(
                signature,
                message.encode('utf-8'),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            is_valid = True
        except InvalidSignature:
            is_valid = False
        return {
            'message': message,
            'signature_valid': is_valid,
            'sender': sender_username
        }

# Helper function to generate key pair (used by users)

class KeysGenerator:

    def generate_key_pair(self):
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        public_key = private_key.public_key()
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return private_key, public_pem

# Example usage
if __name__ == "__main__":
    # Initialize the service
    service = MessagingService()

    # User A generates keys and registers public key
    sender = Sender("A", service)

    # User B generates keys and registers public key
    receiver = Receiver("B", service)

    # Sender A sends a message to Receiver B
    message = "Hello, User B! This is a secure message."
    print(f"\nSender A sending message: {message}")
    sent_data = sender.send_message(message, "B")
    print(f"Sent data keys: {list(sent_data.keys())}")

    # Receiver B receives and processes the message
    received_data = receiver.receive_message(sent_data)
    print(f"Receiver B received: {received_data}")

    sent_data["encrypted_message"] = sent_data["encrypted_message"]+ "Corrupt" # Corrupt the message
    received_data = receiver.receive_message(sent_data)
    print(f"Receiver B received: {received_data}")

    print("\nEnd-to-end encryption and integrity verified. Service only holds public keys.")