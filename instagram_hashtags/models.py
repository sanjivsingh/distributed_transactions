from pydantic import BaseModel

class RegisterRequest(BaseModel):
    username: str

class CreatePostRequest(BaseModel):
    user_id: int
    caption: str
    image_count: int = 0  # Number of images