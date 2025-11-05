import hashlib
import hmac
import json
import base64
import time
import secrets
import asyncio
from typing import Dict, Optional, List
from fastapi import FastAPI, HTTPException, Request, Form, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os

app = FastAPI()

# In-memory user storage (use a database in production)
users: Dict[str, str] = {}  # username: hashed_password

# JWT Secrets for rotation (list of valid secrets, latest is current)
VALID_SECRETS: List[str] = ["your_secret_key_here"]  # Start with initial secret

# Templates
templates = Jinja2Templates(directory="Toy_JWT_Implementation/templates")
os.makedirs("Toy_JWT_Implementation/templates", exist_ok=True)

# JWT Helper Functions (Custom Implementation)
def base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode('utf-8').rstrip('=')

def base64url_decode(data: str) -> bytes:
    padding = 4 - (len(data) % 4)
    if padding != 4:
        data += '=' * padding
    return base64.urlsafe_b64decode(data)

def create_jwt(payload: Dict) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    header_encoded = base64url_encode(json.dumps(header).encode('utf-8'))
    payload_encoded = base64url_encode(json.dumps(payload).encode('utf-8'))
    message = f"{header_encoded}.{payload_encoded}"
    signature = hmac.new(VALID_SECRETS[-1].encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()  # Use latest secret
    signature_encoded = base64url_encode(signature)
    return f"{message}.{signature_encoded}"

def verify_jwt(token: str) -> Optional[Dict]:
    try:
        header_encoded, payload_encoded, signature_encoded = token.split('.')
        message = f"{header_encoded}.{payload_encoded}"
        payload = json.loads(base64url_decode(payload_encoded).decode('utf-8'))
        if payload.get('exp', 0) < time.time():
            return None
        # Try verifying with each valid secret
        for secret in VALID_SECRETS:
            expected_signature = hmac.new(secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()
            expected_signature_encoded = base64url_encode(expected_signature)
            if hmac.compare_digest(signature_encoded, expected_signature_encoded):
                return payload
        return None
    except:
        return None

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def get_current_user(request: Request) -> str:
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    token = auth_header.split(' ')[1]
    payload = verify_jwt(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    return payload['user']

def rotate_secret():
    """Rotate the JWT secret by adding a new one and optionally removing old ones."""
    new_secret = secrets.token_hex(32)  # Generate a secure random secret
    VALID_SECRETS.append(new_secret)
    # Optionally, keep only the last 2 secrets for a grace period
    if len(VALID_SECRETS) > 2:
        VALID_SECRETS.pop(0)
    print(f"JWT secret rotated. New secret added. Total valid secrets: {len(VALID_SECRETS)}")

async def periodic_rotation():
    """Background task to rotate secret every hour."""
    while True:
        await asyncio.sleep(3600)  # 1 hour
        rotate_secret()

@app.on_event("startup")
async def startup_event():
    """Rotate secret on startup and start periodic rotation."""
    rotate_secret()  # Initial rotation
    asyncio.create_task(periodic_rotation())

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/register")
async def register(username: str = Form(...), password: str = Form(...)):
    if username in users:
        raise HTTPException(status_code=400, detail="User already exists")
    users[username] = hash_password(password)
    return {"message": "User registered successfully"}

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    if username not in users or users[username] != hash_password(password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    payload = {"user": username, "exp": int(time.time()) + 3600}  # 1 hour expiration
    token = create_jwt(payload)
    return {"token": token}

@app.get("/api/phone/")
async def get_phone(current_user: str = Depends(get_current_user)):
    # Simulate getting phone data
    return {"phones": ["iPhone", "Android"], "user": current_user}

@app.post("/api/phone/")
async def post_phone(phone: str = Form(...), current_user: str = Depends(get_current_user)):
    # Simulate posting phone data
    return {"message": f"Phone {phone} added by {current_user}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)