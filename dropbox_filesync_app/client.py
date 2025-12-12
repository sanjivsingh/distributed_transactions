# client/client.py
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import hashlib
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import config 
class DropBoxDeomon:

    def __init__(self, user_id : str, sync_dir : str) -> None:
        self.user_id = user_id
        self.sync_dir = sync_dir

        self.last_synced_version = 0
        self.__load_version_file()

        """
        # Initial sync: upload all files
        for root, dirs, files in os.walk(self.sync_dir):
            for file in files:
                if file.startswith('.'):
                    continue  # Skip hidden files
                file_path = os.path.join(root, file)
                self.upload_file(file_path)
        """

    def __load_version_file(self):

        os.makedirs(self.sync_dir, exist_ok=True)
        if not os.path.exists(os.path.join(self.sync_dir, '.synced_version')):
            with open(os.path.join(self.sync_dir, '.synced_version'), 'w') as f:
                f.write('0')
            self.last_synced_version = 0
            return
        with open(os.path.join(sync_dir, '.synced_version'), 'r') as f:
            self.last_synced_version = int(f.read().strip()) if f.read().strip() else 0  # Track last synced version

    def __save_version_file(self):
        with open(os.path.join(self.sync_dir, '.synced_version'), 'w') as f:
            f.write(str(self.last_synced_version))

    def split_file(self,file_path):
        chunks = []
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(config.CHUNK_SIZE)
                if not chunk:
                    break
                chunk_hash = hashlib.sha256(chunk).hexdigest()
                chunks.append((chunk, chunk_hash))
        return chunks

    def upload_chunk(self,chunk, chunk_hash):
        # Send chunk to server
        response = requests.post(
            f"{config.SERVER_URL}/upload_chunk/{self.user_id}/{chunk_hash}",
            files={"file": chunk}
        )
        if response.status_code != 200:
            raise Exception(f"Upload failed : {response.text}")

    def download_chunk(self, chunk_hash):
        response = requests.get(f"{config.SERVER_URL}/download_chunk/{self.user_id}/{chunk_hash}")
        if response.status_code == 200:
            return response.content
        else:
            raise Exception(f"Chunk {chunk_hash} download failed")

    def upload_file(self, file_path):
        chunks = self.split_file(file_path)
        chunk_hashes = [h for _, h in chunks]
        relative_path = os.path.relpath(file_path, self.sync_dir)
        while True:
            # Call metadata_commit
            response = requests.post(f"{config.SERVER_URL}/metadata_commit", json={
                "user_id": self.user_id,
                "file_path": relative_path,
                "chunk_hashes": chunk_hashes
            })
            result = response.json()
            
            if result["status"] == "committed":
                version_id = result["version_id"]
                self.last_synced_version = version_id
                self.__save_version_file()
                print(f"File committed successfully with version {version_id}")
                break
            elif result["status"] == "pending":
                missing_chunks = result["missing_chunks"]
                # Upload missing chunks
                for chunk, chunk_hash in chunks:
                    if chunk_hash in missing_chunks:
                        self.upload_chunk(chunk, chunk_hash)
                # Loop again to commit

    def download_file(self, file_path, local_path):
        response = requests.get(f"{config.SERVER_URL}/file_metadata/{self.user_id}/{file_path}")
        data = response.json()
        chunk_hashes = data["chunk_hashes"]
        
        with open(local_path, 'wb') as f:
            for chunk_hash in chunk_hashes:
                chunk = self.download_chunk(chunk_hash)
                f.write(chunk)

    def sync_updates(self):
        response = requests.get(f"{config.SERVER_URL}/get_updates/{self.user_id}/{self.last_synced_version}")
        updates = response.json()["updated_files"]
        for file_info in updates:
            relative_file_path = file_info["file_path"]
            version = file_info["version"]
            # Download the updated file
            local_path = os.path.join(self.sync_dir, relative_file_path)  # Assuming sync_dir is set
            self.download_file(relative_file_path, local_path)
            self.last_synced_version = version
            self.__save_version_file()
            print(f"Synced {relative_file_path} -> {local_path} to version {version}")

    def list_user_files(self):
        response = requests.get(f"{config.SERVER_URL}/list_files/{self.user_id}")
        return response.json()["files"]

class SyncHandler(FileSystemEventHandler):

    def __init__(self, client: DropBoxDeomon):
        self.client = client    

    def on_modified(self, event):
        if not event.is_directory and not os.path.basename(os.path.abspath(event.src_path)).startswith('.'):
            self.client.upload_file(event.src_path)

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) != 3:
        print("Usage: python client.py <user_id> <sync_directory>")
        #sys.exit(1)
        user_id = "user_id_12"
        sync_dir = "/Users/sanjivsingh/Projects/VS_workspace/distributed_transactions/dropbox_filesync_app/data/client/user_id_12_dir"
    else :
        user_id = sys.argv[1] 
        sync_dir = sys.argv[2]
    print(f"Starting client for user_id: {user_id}, sync_dir: {sync_dir}")
    client = DropBoxDeomon(user_id, sync_dir)

    # Sync any updates
    client.sync_updates()
    
    # Monitor for changes
    event_handler = SyncHandler(client)
    observer = Observer()
    observer.schedule(event_handler, sync_dir, recursive=True)
    observer.start()
    
    try:
        while True:
            # Periodically sync updates
            client.sync_updates()
            observer.join(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()