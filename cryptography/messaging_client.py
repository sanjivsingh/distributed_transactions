import requests
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature
import base64

SERVER_URL = "http://localhost:8000"

class MessagingClient:
    def __init__(self, username):
        self.username = username
        self.private_key = None
        self.public_key_pem = None

    def generate_keys(self):
        """Generate RSA key pair."""
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        public_key = self.private_key.public_key()
        self.public_key_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')

    def register(self):
        """Register user with server."""
        if not self.public_key_pem:
            self.generate_keys()
        response = requests.post(f"{SERVER_URL}/register", json={
            "username": self.username,
            "public_key": self.public_key_pem
        })
        if response.status_code == 200:
            print(f"User {self.username} registered successfully.")
        else:
            print(f"Registration failed: {response.json()}")

    def get_public_key(self, username):
        """Fetch public key of another user."""
        response = requests.get(f"{SERVER_URL}/get_public_key/{username}")
        if response.status_code == 200:
            return response.json()["public_key"]
        else:
            raise ValueError(f"User {username} not found")

    def send_message(self, recipient, message):
        """Encrypt and sign message, send to server."""
        # Get recipient's public key
        recipient_public_key_pem = self.get_public_key(recipient)
        recipient_public_key = serialization.load_pem_public_key(
            recipient_public_key_pem.encode('utf-8'),
            backend=default_backend()
        )

        # Sign the message
        signature = self.private_key.sign(
            message.encode('utf-8'),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        signature_b64 = base64.b64encode(signature).decode('utf-8')

        # Encrypt the message
        encrypted = recipient_public_key.encrypt(
            message.encode('utf-8'),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        encrypted_b64 = base64.b64encode(encrypted).decode('utf-8')

        # Send to server
        response = requests.post(f"{SERVER_URL}/send_message", json={
            "sender": self.username,
            "recipient": recipient,
            "encrypted_message": encrypted_b64,
            "signature": signature_b64
        })
        if response.status_code == 200:
            print(f"Message sent to {recipient}.")
        else:
            print(f"Send failed: {response.json()}")

    def receive_messages(self):
        """Fetch and decrypt messages from server."""
        response = requests.post(f"{SERVER_URL}/receive_messages", json={
            "username": self.username
        })
        if response.status_code != 200:
            print("Receive failed.")
            return

        messages = response.json()["messages"]
        for msg in messages:
            sender = msg["sender"]
            encrypted_b64 = msg["encrypted_message"]
            signature_b64 = msg["signature"]

            # Decrypt
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

            # Verify signature
            sender_public_key_pem = self.get_public_key(sender)
            sender_public_key = serialization.load_pem_public_key(
                sender_public_key_pem.encode('utf-8'),
                backend=default_backend()
            )
            signature = base64.b64decode(signature_b64)
            try:
                sender_public_key.verify(
                    signature,
                    message.encode('utf-8'),
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
                is_valid = "Valid"
            except InvalidSignature:
                is_valid = "Invalid"

            print(f"From {sender}: {message} (Signature: {is_valid})")

# Example usage
if __name__ == "__main__":
    # User A
    client_a = MessagingClient("Alice")
    client_a.register()

    # User B
    client_b = MessagingClient("Bob")
    client_b.register()

    # Alice sends message to Bob
    client_a.send_message("Bob", "Hello Bob, this is a secure message!")

    # Bob receives messages
    client_b.receive_messages()