import os
import datetime

from shutil import disk_usage
import sys
import threading

from commons import logger
log = logger.setup_logger(__name__)
import config

def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # Skip if it's a symbolic link that points to a non-existent file
            # or if it's a broken symlink, as os.path.getsize will raise an error
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp) / (1024 * 1024) # size in MB
    return total_size

class LoginEventProcessor:

    def __init__(self,  worker_id = 1):

        self.events_pending = []
        self.events_pending_bytes = 0
        self.events_inprogress = []

        self.events_bytes_threshold = config.events_bytes_threshold

        self.local_dir = os.path.join(os.getcwd(), config.local_directory)
        if not os.path.exists(self.local_dir):
            os.makedirs(self.local_dir)

        self.inprogress_lock = threading.Lock()

        # disk space monitoring can be added here if needed
        self.disk_space_threshold_mb = config.disk_space_threshold_mb  # Example threshold in MB
        self.disk_space_usage = 0

    def create_local_login_event_file(self):
        with self.inprogress_lock:
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            file_name =  f"events_{timestamp}.log"
            local_file_path = os.path.join(self.local_dir, file_name)
            with open(local_file_path, "w") as f:
                f.write("Event\n")
                for event in self.events_inprogress:
                    f.write(event + "\n")
            log.info(f"Saved {len(self.events_inprogress)} events to local storage at {local_file_path}")
            self.events_inprogress.clear()


    def __save_to_local_storage(self):
        with self.inprogress_lock:
            self.events_pending, self.events_inprogress = self.events_inprogress, self.events_pending
            threading.Thread(target=self.create_local_login_event_file).start()
            self.events_pending_bytes = 0

    def process_login_event(self, event: str):

        if self.disk_space_usage >= self.disk_space_threshold_mb * config.disk_space_usage_percent_alert:
            log.error(f"Local storage usage {self.disk_space_usage} MB exceeds 70% of threshold {self.disk_space_threshold_mb} MB. Consider cleaning up.")
            raise Exception(f"Insufficient local storage space. disk_space_usage={self.disk_space_usage} MB  disk_space_threshold_mb={self.disk_space_threshold_mb} MB")

        if self.events_pending_bytes + len(event) >= self.events_bytes_threshold:
            # Flush existing events
            # Here we would normally write to local storage and upload to cloud storage
            log.info(f"Flushing {len(self.events_pending)} events to local storage.")
            self.__save_to_local_storage()

            self.disk_space_usage = get_directory_size(self.local_dir)
            log.info(f"Current local storage usage: {self.disk_space_usage} MB")

        self.events_pending.append(event)
        self.events_pending_bytes += len(event)

    def __del__(self):
        self.close()

    def close(self):
        # Flush any remaining events before closing 
        print("Closing LoginEventProcessor...")
        if self.events_pending:
            log.info(f"Final flush of {len(self.events_pending)} events to local storage before closing.")
            self.__save_to_local_storage()

# Function to generate random events
def generate_random_event():
    import random
    users = [f"user_{i}" for i in range(1, 101)]  # 100 sample users
    actions = ["login", "logout"]
    countries = ["USA", "India", "UK", "Germany", "Canada", "Australia", "France", "Japan", "Brazil", "China"]
    devices = ["Mobile", "Desktop", "Tablet"]
    browsers = ["Chrome", "Firefox", "Safari", "Edge"]
    user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)"]
    referrers = ["https://example.com", "https://google.com", "Direct"]
    words = ['a', 'the', 'is', 'of', 'to', 'and', 'in', 'that', 'it', 'with']
    user = random.choice(users)
    action = random.choice(actions)
    country = random.choice(countries)
    ip_address = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
    device = random.choice(devices)
    browser = random.choice(browsers)
    session_id = f"session_{random.randint(1000, 9999)}"
    user_agent = random.choice(user_agents)
    referrer = random.choice(referrers)
    # Random time in the last 24 hours
    random_time = datetime.datetime.now() - datetime.timedelta(hours=random.randint(0, 24), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    time_str = random_time.strftime("%Y-%m-%d %H:%M:%S")
    details = ' '.join(random.choices(words, k=random.randint(100, 1000)))
    event = f"{user} {action} at {time_str} from {ip_address} in {country} using {device} with {browser} session {session_id} user_agent '{user_agent}' referrer '{referrer}' details '{details}'"
    return event

if __name__ == "__main__":
    worker_id = sys.argv[1] if sys.argv and len(sys.argv) > 1 else 1
    processor = LoginEventProcessor(worker_id=int(worker_id))

    try :
        # Generate and process random events
        for i in range(1000000): # Generate 1000 random events
            event = generate_random_event()
            processor.process_login_event(event)
            import time
            time.sleep(0.01)  # Simulate time gap between events
    finally: 
        processor.close()