# server/app.py
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from pymongo import MongoClient
import boto3
import hashlib
import config
import io
app = FastAPI()


# MongoDB connection

mongo_client = MongoClient(
    config.MONGO_SERVER,
    config.MONGO_PORT
)
db = mongo_client[config.DB_NAME]

class ChunkBlobHandler:

    def __init__(self):
        super().__init__()
        self.is_local  = config.is_local

    def store_chunk(self, user_id : str , chunk_hash: str, chunk_data: bytes):
        if self.is_local:
            os.makedirs(f"{config.SERVER_CHUNK_PREFIX}/{user_id}", exist_ok=True)
            with open(f"{config.SERVER_CHUNK_PREFIX}/{user_id}/{chunk_hash}", "wb") as f:
                f.write(chunk_data)
            return
        else :
            s3_key = f"{config.SERVER_CHUNK_PREFIX}/{user_id}/{chunk_hash}"
            s3_client = boto3.client('s3')
            s3_client.put_object(Bucket=config.S3_BUCKET, Key=s3_key, Body=chunk_data)

    def get_chunk(self, user_id : str , chunk_hash: str):
        if self.is_local:
            with open(f"{config.SERVER_CHUNK_PREFIX}/{user_id}/{chunk_hash}", "rb") as f:
                response = {}
                response['Body'] =f.read()
                return response
        else:
            s3_key = f"{config.SERVER_CHUNK_PREFIX}/{user_id}/{chunk_hash}"
            s3_client = boto3.client('s3')
            s3_response = s3_client.get_object(Bucket=config.S3_BUCKET, Key=s3_key)
            chunk_data = s3_response['Body'].read()
            response = {}
            response['Body'] =chunk_data
            return response   

class FileMetadata(BaseModel):
    user_id: str
    file_path: str
    chunk_hashes: list[str]

@app.post("/metadata_commit")
def metadata_commit(metadata: FileMetadata):
    print(f"Committing metadata for file {metadata.file_path} for user {metadata.user_id}")
    fils_collection = db["files"]
    chunk_collection = db["chunks"]
    user_version_collection = db["user_versions"]
    
    existing_chunks = set()
    for chunk_hash in metadata.chunk_hashes:
        if chunk_collection.find_one({"user_id": metadata.user_id, "hash": chunk_hash}):
            existing_chunks.add(chunk_hash)
    
    missing_chunks = [h for h in metadata.chunk_hashes if h not in existing_chunks]
    
    if missing_chunks:
        return {"status": "pending", "missing_chunks": missing_chunks}
    else:
        # All chunks present, commit metadata with new user-level version
        user_doc = user_version_collection.find_one({"user_id": metadata.user_id})
        current_version = user_doc["version"] if user_doc else 0
        
        file_doc = fils_collection.find_one({"user_id": metadata.user_id, "file_path": metadata.file_path}, sort=[("version", -1)])
        if file_doc:
            if file_doc['chunk_hashes'] == metadata.chunk_hashes:
                # No changes in file content
                return {"status": "committed", "version_id": file_doc.get("version", 0)}
        
        new_version = current_version + 1
        user_version_collection.update_one(
            {"user_id": metadata.user_id},
            {"$set": {"version": new_version}},
            upsert=True
        )
        # Always insert a new entry for versioning
        fils_collection.insert_one({
            "user_id": metadata.user_id,
            "file_path": metadata.file_path,
            "chunk_hashes": metadata.chunk_hashes,
            "status": "committed",
            "version": new_version
        })
        return {"status": "committed", "version_id": new_version}

@app.post("/revert_file/{user_id}/{file_path}")
def revert_file(user_id: str, file_path: str):
    print(f"Reverting file {file_path} for user {user_id} to previous version")
    files_collection = db["files"]
    user_version_collection = db["user_versions"]
    
    # Find the latest two versions
    docs = list(files_collection.find({"user_id": user_id, "file_path": file_path}).sort("version", -1).limit(2))
    if len(docs) < 2:
        raise HTTPException(status_code=404, detail="No previous version available")
    
    # Get the previous version's chunk_hashes
    previous_doc = docs[1]
    chunk_hashes = previous_doc["chunk_hashes"]
    
    # Get current user version
    user_doc = user_version_collection.find_one({"user_id": user_id})
    current_version = user_doc["version"] if user_doc else 0
    new_version = current_version + 1
    
    # Update user version
    user_version_collection.update_one(
        {"user_id": user_id},
        {"$set": {"version": new_version}},
        upsert=True
    )
    # Insert new entry with previous chunk_hashes
    files_collection.insert_one({
        "user_id": user_id,
        "file_path": file_path,
        "chunk_hashes": chunk_hashes,
        "status": "committed",
        "version": new_version
    })
    return {"status": "reverted", "version_id": new_version, "from_version_id": previous_doc.get("version", 0), "previous_version_id": docs[0].get("version", 0)}

@app.post("/upload_chunk/{user_id}/{chunk_hash}")
def upload_chunk(user_id: str, chunk_hash: str, file: UploadFile = File(...)):
    print(f"Uploading chunk {chunk_hash} for user {user_id}")
    chunk_data = file.file.read()
    
    # Verify hash
    if hashlib.sha256(chunk_data).hexdigest() != chunk_hash:
        raise HTTPException(status_code=400, detail="Chunk hash mismatch")
    
    chunk_handler = ChunkBlobHandler()
    chunk_handler.store_chunk(user_id , chunk_hash, chunk_data)

    # Update chunk DB
    chunk_collection = db["chunks"]
    chunk_collection.update_one(
        {"user_id": user_id, "hash": chunk_hash},
        {"$set": {"user_id": user_id, "hash": chunk_hash}},
        upsert=True
    )
    return {"status": "uploaded"}

@app.get("/download_chunk/{user_id}/{chunk_hash}")
def download_chunk(user_id: str, chunk_hash: str):
    print(f"Downloading chunk {chunk_hash} for user {user_id}")
    try:
        chunk_handler = ChunkBlobHandler()
        response  = chunk_handler.get_chunk(user_id , chunk_hash)
        chunk_data = response['Body']
        return StreamingResponse(io.BytesIO(chunk_data), media_type="application/octet-stream")
    except Exception as e:
        print(f"Error downloading chunk: {e}")
        raise HTTPException(status_code=404, detail="Chunk not found")

@app.get("/file_metadata/{user_id}/{file_path:path}")
def get_file_metadata(user_id: str, file_path: str):
    print(f"Getting metadata for file {file_path} for user {user_id}")
    collection = db["files"]
    # Get the latest version
    file_doc = collection.find_one({"user_id": user_id, "file_path": file_path}, sort=[("version", -1)])
    if not file_doc:
        raise HTTPException(status_code=404, detail="File not found")
    
    return {"chunk_hashes": file_doc["chunk_hashes"], "version": file_doc.get("version", 0)}

@app.get("/list_files/{user_id}")
def list_files(user_id: str):
    print(f"Listing files for user {user_id}")
    collection = db["files"]
    # Get distinct file_paths with their latest versions
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$sort": {"version": -1}},
        {"$group": {"_id": "$file_path", "version": {"$first": "$version"}}},
        {"$project": {"file_path": "$_id", "version": 1, "_id": 0}}
    ]
    files = list(collection.aggregate(pipeline))
    return {"files": files}

@app.get("/get_updates/{user_id}/{since_version}")
def get_updates(user_id: str, since_version: int):
    print(f"Getting updates for user {user_id} since version {since_version}")
    collection = db["files"]
    # Get files with version > since_version, latest per file_path
    pipeline = [
        {"$match": {"user_id": user_id, "version": {"$gt": since_version}}},
        {"$sort": {"version": -1}},
        {"$group": {"_id": "$file_path", "version": {"$first": "$version"}}},
        {"$project": {"file_path": "$_id", "version": 1, "_id": 0}}
    ]
    files = list(collection.aggregate(pipeline))
    return {"updated_files": files}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)