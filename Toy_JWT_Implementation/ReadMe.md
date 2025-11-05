# Toy JWT Implementation

This project implements a basic authentication server using custom JWT (JSON Web Token) logic for user registration, login, and protected API access. It demonstrates JWT creation, validation, and secret rotation without relying on external JWT libraries.

## Features

- **User Registration**: Users can register with a username and password.
- **JWT Authentication**: Login to obtain a JWT token, which is used to access protected resources.
- **Protected APIs**: GET and POST endpoints for phone data, requiring valid JWT.
- **Custom JWT Logic**: Implements JWT encoding/decoding and HMAC-SHA256 signing from scratch.
- **Secret Rotation**: Supports rotating JWT secrets internally to enhance security, with backward compatibility for old tokens.
- **Simple HTML Interface**: Web-based UI for registration, login, and API interactions.
- **In-Memory Storage**: Basic user storage (use a database for production).

## Architecture

- **Server**: Built with FastAPI for RESTful APIs and HTML serving.
- **JWT Handling**: Custom functions for creating and verifying JWTs using base64url encoding and HMAC signatures.
- **Security**: Passwords hashed with SHA256; JWTs expire after 1 hour; secret rotation handled internally.
- **Rotation**: Maintains a list of valid secrets; new JWTs use the latest secret, verification checks all; rotates on startup and periodically.

### Architecture Diagram

![Architecture Diagram](architecture_diagram.png)

*(Render the PlantUML below at [plantuml.com](https://plantuml.com/) and save as `architecture_diagram.png`)*

```plantuml
@startuml Architecture Diagram

[User Browser] --> [FastAPI Server] : HTTP Requests
[FastAPI Server] --> [In-Memory Users] : Store/Retrieve Users
[FastAPI Server] --> [JWT Logic] : Create/Verify Tokens
[JWT Logic] --> [Secrets List] : Use for Signing/Verification
[Background Task] --> [JWT Logic] : Periodic Rotation

note right of User Browser : HTML interface for UI
note right of FastAPI Server : Handles auth and API endpoints
note right of In-Memory Users : Temporary storage (dict)
note right of JWT Logic : Custom encoding/decoding
note right of Secrets List : For rotation support
note right of Background Task : Internal secret rotation

@enduml
```

## Prerequisites

- Python 3.8+
- FastAPI and dependencies (install via pip)

## Installation

1. **Navigate to the project directory**:
   ```bash
   cd .../distributed_transactions/
   ```

2. **Create and activate a virtual environment**:
   ```
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate   
   ```

3.  **Install dependencies**:

    ```
    pip install -r Toy_JWT_Implementation/requirements.txt
    ```



## Running the Application

-   **Run the application**:

    ```
    .venv/bin/python -m uvicorn Toy_JWT_Implementation.auth_server:app --reload --port 8000
    ```

The app will start on http://localhost:8000.
--reload enables auto-restart on code changes.

-   **Access the web interface**:

Open http://localhost:8000 in your browser.

### Web Interface
- **Register**: Enter username and password.
- **Login**: Enter credentials to get a JWT token (stored in browser localStorage).
- **Get Phones**: Fetch protected data (requires login).
- **Add Phone**: Post data to protected endpoint.

### API Usage
Use tools like curl or Postman.

- Register:
  ```bash
  curl -X POST http://localhost:8000/register -d "username=test&password=pass"
  ```

- Login:
  ```bash
  curl -X POST http://localhost:8000/login -d "username=test&password=pass"
  ```
  Response: `{"token": "jwt_token_here"}`

- Access Protected API:
  ```bash
  curl -H "Authorization: Bearer jwt_token_here" http://localhost:8000/api/phone/
  ```

## API Reference

### Endpoints

- **GET /**: Serve HTML interface.
- **POST /register**: Register user. Body: username, password.
- **POST /login**: Login and get JWT. Body: username, password.
- **GET /api/phone/**: Get phone data (requires JWT).
- **POST /api/phone/**: Add phone data (requires JWT). Body: phone.

### JWT Structure
- **Header**: {"alg": "HS256", "typ": "JWT"}
- **Payload**: {"user": username, "exp": expiration_timestamp}
- **Signature**: HMAC-SHA256 of header.payload with secret.

## Security Considerations

- **Passwords**: Hashed with SHA256 (use bcrypt in production).
- **JWT Expiration**: 1 hour; implement refresh tokens for longer sessions.
- **Secret Rotation**: Rotates internally on startup and every hour; old secrets allow grace period.
- **Production**: Use HTTPS, secure secrets, persistent database, rate limiting, and key management services.

## Troubleshooting

- **Import Errors**: Ensure dependencies are installed.
- **Token Invalid**: Check expiration or secret rotation.
- **Port Issues**: Ensure 8000 is free.
- **HTML Not Loading**: Verify templates directory.

## Example

1. Register a user via the web interface.
2. Login to get a token.
3. Use the token to access /api/phone/ endpoints.

## Technologies Used

- Python
- FastAPI
- Jinja2 (for HTML)
- Custom JWT logic (hmac, hashlib, base64)

## Contributing

- Add database integration.
- Implement refresh tokens.
- Enhance UI with JavaScript frameworks.